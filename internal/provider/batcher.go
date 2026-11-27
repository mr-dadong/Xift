package provider

import (
	"context"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mr-dadong/Xift/internal/filter"
)

const (
	// DefaultBatchWindow is the architecture-mandated 150ms micro-batch cancellation window.
	DefaultBatchWindow = 150 * time.Millisecond
	// DefaultRingCapacity is the bounded RingBuffer size (65,536 events) per V2.1 Section 6.3.
	DefaultRingCapacity = 65536
	// DefaultHighWaterPct is the backlog threshold (%) above which fine-grained events degrade to EventRescanDir.
	DefaultHighWaterPct = 80
	// DefaultMaxRescanDirs caps unique EventRescanDir entries per flush to guarantee bounded memory.
	DefaultMaxRescanDirs = 4096
)

// SubmitResult describes how Batcher.Submit handled an incoming raw FileEvent.
type SubmitResult uint8

const (
	// SubmitDroppedPruned indicates the event was intercepted and dropped at the source by Pruner.
	SubmitDroppedPruned SubmitResult = iota
	// SubmitQueued indicates the event entered the bounded RingBuffer for 150ms coalescing.
	SubmitQueued
	// SubmitDegradedRescan indicates ring backlog exceeded HighWaterPct and the event was folded into EventRescanDir.
	SubmitDegradedRescan
)

// EventBatch is the coalesced output emitted once per micro-batch window.
type EventBatch struct {
	Events  []FileEvent
	Cursors map[uint16]uint64 // Highest observed cursor per VolID (even if all events cancelled out)
}

// BatcherStats exposes lock-free diagnostic counters for monitoring and tests.
type BatcherStats struct {
	Received        uint64
	PrunedDropped   uint64
	CancelledPairs  uint64
	CoalescedCount  uint64
	DegradedRescans uint64
	FlushedBatches  uint64
	FlushedEvents   uint64
}

// BatcherOptions configures a Batcher instance.
type BatcherOptions struct {
	Window             time.Duration
	RingCapacity       int
	HighWaterPct       int
	MaxRescanDirs      int
	Pruner             *filter.Pruner
	OnBatch            func(batch EventBatch)
	DisableLowPriority bool
}

type nodeCompositeKey struct {
	volID   uint16
	nodeKey uint64
}

type rescanKey struct {
	volID   uint16
	dirKey  uint64
	oldPath string
}

// Batcher implements source-level Pruner interception, a bounded RingBuffer with >80%
// overload degradation to EventRescanDir, and a 150ms micro-batch cancellation state machine.
type Batcher struct {
	window             time.Duration
	ringCap            int
	highWater          int
	maxRescanDirs      int
	pruner             *filter.Pruner
	onBatch            func(batch EventBatch)
	disableLowPriority bool

	mu         sync.Mutex
	ring       []FileEvent
	ringHead   int
	ringTail   int
	ringCount  int
	rescanDirs map[rescanKey]FileEvent
	cursors    map[uint16]uint64
	wakeCh     chan struct{}
	started    bool
	closed     bool

	flushMu sync.Mutex // Serializes OnBatch delivery order across timer & explicit FlushNow calls.

	stopCh chan struct{}
	doneCh chan struct{}

	// Reusable drain slice protected by flushMu to keep steady-state allocations minimal.
	drainBuf []FileEvent

	received        atomic.Uint64
	prunedDropped   atomic.Uint64
	cancelledPairs  atomic.Uint64
	coalescedCount  atomic.Uint64
	degradedRescans atomic.Uint64
	flushedBatches  atomic.Uint64
	flushedEvents   atomic.Uint64
}

// NewBatcher constructs a Batcher with architecture defaults for any omitted options.
func NewBatcher(opts BatcherOptions) *Batcher {
	win := opts.Window
	if win <= 0 {
		win = DefaultBatchWindow
	}
	rcap := opts.RingCapacity
	if rcap <= 0 {
		rcap = DefaultRingCapacity
	}
	pct := opts.HighWaterPct
	if pct <= 0 || pct > 100 {
		pct = DefaultHighWaterPct
	}
	highWater := (rcap * pct) / 100
	if highWater < 1 {
		highWater = 1
	}
	maxRescans := opts.MaxRescanDirs
	if maxRescans <= 0 {
		maxRescans = DefaultMaxRescanDirs
	}

	return &Batcher{
		window:             win,
		ringCap:            rcap,
		highWater:          highWater,
		maxRescanDirs:      maxRescans,
		pruner:             opts.Pruner,
		onBatch:            opts.OnBatch,
		disableLowPriority: opts.DisableLowPriority,
		ring:               make([]FileEvent, rcap),
		rescanDirs:         make(map[rescanKey]FileEvent, 16),
		cursors:            make(map[uint16]uint64, 4),
		wakeCh:             make(chan struct{}, 1),
		stopCh:             make(chan struct{}),
		doneCh:             make(chan struct{}),
	}
}

// Start launches the background 150ms flush loop. Idempotent; no-op if already started or closed.
func (b *Batcher) Start(ctx context.Context) {
	b.mu.Lock()
	if b.started || b.closed {
		b.mu.Unlock()
		return
	}
	b.started = true
	b.mu.Unlock()
	go b.runLoop(ctx)
}

func (b *Batcher) runLoop(ctx context.Context) {
	defer close(b.doneCh)

	if !b.disableLowPriority {
		restore := SetLowPriorityThread()
		defer restore()
	}

	var timer *time.Timer
	var timerCh <-chan time.Time

	stopTimer := func() {
		if timer != nil {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timerCh = nil
		}
	}
	defer stopTimer()

	for {
		select {
		case <-ctx.Done():
			b.FlushNow()
			return
		case <-b.stopCh:
			b.FlushNow()
			return
		case <-b.wakeCh:
			if timerCh == nil {
				if timer == nil {
					timer = time.NewTimer(b.window)
				} else {
					timer.Reset(b.window)
				}
				timerCh = timer.C
			}
		case <-timerCh:
			timerCh = nil
			b.FlushNow()
			// If new events arrived while flushing, arm the next window immediately.
			if b.hasPending() {
				timer.Reset(b.window)
				timerCh = timer.C
			}
		}
	}
}

func (b *Batcher) hasPending() bool {
	b.mu.Lock()
	pending := b.ringCount > 0 || len(b.rescanDirs) > 0 || len(b.cursors) > 0
	b.mu.Unlock()
	return pending
}

// Close stops the background loop (if started) and synchronously flushes any remaining buffered events.
// Race-free with concurrent Start calls.
func (b *Batcher) Close() {
	b.mu.Lock()
	if b.closed {
		wasStarted := b.started
		b.mu.Unlock()
		if wasStarted {
			<-b.doneCh
		}
		return
	}
	b.closed = true
	wasStarted := b.started
	close(b.stopCh)
	b.mu.Unlock()

	if wasStarted {
		<-b.doneCh
	} else {
		b.FlushNow()
	}
}

// Stats returns a point-in-time snapshot of batcher metrics.
func (b *Batcher) Stats() BatcherStats {
	return BatcherStats{
		Received:        b.received.Load(),
		PrunedDropped:   b.prunedDropped.Load(),
		CancelledPairs:  b.cancelledPairs.Load(),
		CoalescedCount:  b.coalescedCount.Load(),
		DegradedRescans: b.degradedRescans.Load(),
		FlushedBatches:  b.flushedBatches.Load(),
		FlushedEvents:   b.flushedEvents.Load(),
	}
}

// Submit ingests a raw FileEvent:
//  1. Runs O(1) Pruner.InterceptEvent at the source (0 lock / 0 alloc drop path when Cursor == 0).
//  2. Enqueues surviving events into the bounded RingBuffer, degrading to directory-level EventRescanDir
//     when backlog reaches HighWaterPct (default 80%).
func (b *Batcher) Submit(ev FileEvent) SubmitResult {
	b.received.Add(1)

	var extraRescan *FileEvent
	var movedOutOfPruned bool

	if b.pruner != nil && ev.Type != EventRescanDir {
		op := toFilterOp(ev.Type)
		if op != 0 {
			dec := b.pruner.InterceptEvent(filter.EventInput{
				VolID:        ev.VolID,
				Op:           op,
				NodeKey:      ev.NodeKey,
				ParentKey:    ev.ParentKey,
				NewParentKey: ev.NewParentKey,
				Name:         ev.NewName,
				RelPath:      ev.RelPath,
				Depth:        ev.Depth,
				IsDir:        ev.IsDir,
			}, nil)

			switch dec.Action {
			case filter.DecisionDrop:
				b.prunedDropped.Add(1)
				if ev.Cursor > 0 {
					b.recordCursorOnly(ev.VolID, ev.Cursor)
				}
				return SubmitDroppedPruned

			case filter.DecisionDeleteFromIndex:
				ev.Type = EventDelete
				ev.Flags = mergePreservedFlags(ev.Flags, dec.Flags)
				ev.PrunedSubtree = ev.PrunedSubtree || dec.PrunedSubtree

			case filter.DecisionRescanDir:
				movedOutOfPruned = true
				ev.Flags = mergePreservedFlags(ev.Flags, dec.Flags)
				ev.ReplaceFlags = true
				targetKey := ev.NodeKey
				if targetKey == 0 {
					targetKey = ev.ParentKey
				}
				rc := FileEvent{
					VolID:     ev.VolID,
					Type:      EventRescanDir,
					NodeKey:   targetKey,
					ParentKey: ev.NewParentKey,
					OldPath:   ev.RelPath,
					NewName:   ev.NewName,
					Flags:     ev.Flags,
					IsDir:     true,
					Cursor:    ev.Cursor,
				}
				if rc.ParentKey == 0 {
					rc.ParentKey = ev.ParentKey
				}
				extraRescan = &rc

			case filter.DecisionPass:
				ev.Flags = mergePreservedFlags(ev.Flags, dec.Flags)
				ev.PrunedSubtree = ev.PrunedSubtree || dec.PrunedSubtree
				if ev.Type == EventRename && (ev.NewName != "" || ev.RelPath != "") {
					ev.ReplaceFlags = true
				}
			}
		}
	}

	b.mu.Lock()
	if ev.Cursor > 0 {
		if prev := b.cursors[ev.VolID]; ev.Cursor > prev {
			b.cursors[ev.VolID] = ev.Cursor
		}
	}

	var res SubmitResult
	if ev.Type == EventRescanDir {
		b.recordRescanLocked(ev)
		res = SubmitQueued
	} else if b.ringCount >= b.highWater {
		dirRel := parentDirPath(ev.RelPath)
		if ev.Type == EventRename && ev.NewParentKey != 0 && ev.NewParentKey != ev.ParentKey {
			// Cross-directory move under overload: always rescan destination directory,
			// and only rescan source directory if it wasn't a pruned subtree.
			b.recordRescanLocked(FileEvent{
				VolID:   ev.VolID,
				Type:    EventRescanDir,
				NodeKey: ev.NewParentKey,
				OldPath: dirRel,
				IsDir:   true,
				Cursor:  ev.Cursor,
			})
			if !movedOutOfPruned && ev.ParentKey != 0 {
				b.recordRescanLocked(FileEvent{
					VolID:   ev.VolID,
					Type:    EventRescanDir,
					NodeKey: ev.ParentKey,
					OldPath: parentDirPath(ev.OldPath),
					IsDir:   true,
					Cursor:  ev.Cursor,
				})
			}
		} else {
			dirKey := ev.ParentKey
			if movedOutOfPruned && ev.NewParentKey != 0 {
				dirKey = ev.NewParentKey
			} else if ev.IsDir && dirKey == 0 {
				dirKey = ev.NodeKey
			}
			b.recordRescanLocked(FileEvent{
				VolID:   ev.VolID,
				Type:    EventRescanDir,
				NodeKey: dirKey,
				OldPath: dirRel,
				IsDir:   true,
				Cursor:  ev.Cursor,
			})
		}
		b.degradedRescans.Add(1)
		res = SubmitDegradedRescan
	} else {
		b.ring[b.ringTail] = ev
		b.ringTail++
		if b.ringTail == b.ringCap {
			b.ringTail = 0
		}
		b.ringCount++
		res = SubmitQueued
	}

	if extraRescan != nil {
		b.recordRescanLocked(*extraRescan)
	}
	b.mu.Unlock()

	select {
	case b.wakeCh <- struct{}{}:
	default:
	}
	return res
}

func parentDirPath(rel string) string {
	if rel == "" {
		return ""
	}
	d := path.Dir(rel)
	if d == "." {
		return ""
	}
	return d
}

func (b *Batcher) recordCursorOnly(volID uint16, cursor uint64) {
	b.mu.Lock()
	if prev := b.cursors[volID]; cursor > prev {
		b.cursors[volID] = cursor
	}
	b.mu.Unlock()
	select {
	case b.wakeCh <- struct{}{}:
	default:
	}
}

func (b *Batcher) recordRescanLocked(ev FileEvent) {
	// If this volume already collapsed to a full-volume rescan (dirKey == 0, oldPath == ""), skip fine entries.
	volCollapseKey := rescanKey{volID: ev.VolID, dirKey: 0, oldPath: ""}
	if _, collapsed := b.rescanDirs[volCollapseKey]; collapsed {
		return
	}

	rk := rescanKey{
		volID:  ev.VolID,
		dirKey: ev.NodeKey,
	}
	if rk.dirKey == 0 {
		rk.oldPath = ev.OldPath
	}
	if _, exists := b.rescanDirs[rk]; exists {
		return
	}

	// Enforce bounded rescanDirs memory budget: collapse volume to a single root rescan when capped.
	if len(b.rescanDirs) >= b.maxRescanDirs {
		for k := range b.rescanDirs {
			if k.volID == ev.VolID {
				delete(b.rescanDirs, k)
			}
		}
		b.rescanDirs[volCollapseKey] = FileEvent{
			VolID:   ev.VolID,
			Type:    EventRescanDir,
			NodeKey: 0,
			IsDir:   true,
			Cursor:  ev.Cursor,
		}
		return
	}
	b.rescanDirs[rk] = ev
}

func toFilterOp(t EventType) filter.EventOp {
	switch t {
	case EventCreate:
		return filter.EventCreate
	case EventDelete:
		return filter.EventDelete
	case EventModifyMeta:
		return filter.EventModifyMeta
	case EventRename:
		return filter.EventRename
	default:
		return 0
	}
}

// mergePreservedFlags combines Pruner's classification bits (Dir/Folded/System)
// with non-governance OS bits (Hidden/NonASCII/Symlink) supplied by the provider.
func mergePreservedFlags(orig, prunerFlags uint16) uint16 {
	const prunerMask = filter.FlagDir | filter.FlagFolded | filter.FlagSystem
	return (orig &^ prunerMask) | (prunerFlags & prunerMask)
}

// FlushNow drains the bounded RingBuffer, runs the deterministic cancellation/coalescing
// state machine, and invokes OnBatch synchronously. Safe for concurrent use.
func (b *Batcher) FlushNow() {
	b.flushMu.Lock()
	defer b.flushMu.Unlock()

	// Snapshot & clear under b.mu quickly so producers are never blocked by downstream IO.
	b.mu.Lock()
	if b.ringCount == 0 && len(b.rescanDirs) == 0 && len(b.cursors) == 0 {
		b.mu.Unlock()
		return
	}

	raw := b.drainBuf[:0]
	if cap(raw) < b.ringCount {
		raw = make([]FileEvent, 0, b.ringCount)
	}
	for b.ringCount > 0 {
		raw = append(raw, b.ring[b.ringHead])
		b.ring[b.ringHead] = FileEvent{} // Release string references promptly
		b.ringHead++
		if b.ringHead == b.ringCap {
			b.ringHead = 0
		}
		b.ringCount--
	}
	b.ringHead = 0
	b.ringTail = 0

	var rescans []FileEvent
	if len(b.rescanDirs) > 0 {
		rescans = make([]FileEvent, 0, len(b.rescanDirs))
		for _, r := range b.rescanDirs {
			rescans = append(rescans, r)
		}
		clear(b.rescanDirs)
	}

	var cursors map[uint16]uint64
	if len(b.cursors) > 0 {
		cursors = make(map[uint16]uint64, len(b.cursors))
		for k, v := range b.cursors {
			cursors[k] = v
		}
		clear(b.cursors)
	}
	b.mu.Unlock()

	b.drainBuf = raw

	events, cancelled, coalesced := coalesceEvents(raw, rescans)
	if cancelled > 0 {
		b.cancelledPairs.Add(cancelled)
	}
	if coalesced > 0 {
		b.coalescedCount.Add(coalesced)
	}

	if len(events) == 0 && len(cursors) == 0 {
		return
	}

	b.flushedBatches.Add(1)
	b.flushedEvents.Add(uint64(len(events)))

	if b.onBatch != nil {
		b.onBatch(EventBatch{
			Events:  events,
			Cursors: cursors,
		})
	}
}

type coalesceSlot struct {
	ev        FileEvent
	cancelled bool
}

func coalesceEvents(raw []FileEvent, rescans []FileEvent) (out []FileEvent, cancelled uint64, coalesced uint64) {
	if len(raw) == 0 && len(rescans) == 0 {
		return nil, 0, 0
	}

	slots := make([]coalesceSlot, 0, len(raw))
	idxByKey := make(map[nodeCompositeKey]int, len(raw))

	for i := range raw {
		ev := raw[i]
		if ev.NodeKey == 0 {
			slots = append(slots, coalesceSlot{ev: ev})
			continue
		}

		ck := nodeCompositeKey{volID: ev.VolID, nodeKey: ev.NodeKey}
		slotIdx, exists := idxByKey[ck]
		if !exists {
			idxByKey[ck] = len(slots)
			slots = append(slots, coalesceSlot{ev: ev})
			continue
		}

		cur := &slots[slotIdx]
		if cur.cancelled {
			// A previous Create+Delete cancelled out; treat new event as fresh state.
			cur.ev = ev
			cur.cancelled = false
			continue
		}

		prev := &cur.ev
		switch prev.Type {
		case EventCreate:
			switch ev.Type {
			case EventModifyMeta:
				mergeMeta(prev, ev, true)
				coalesced++
			case EventRename:
				applyRenameOntoCreate(prev, ev)
				coalesced++
			case EventDelete:
				// Create (+ N*Modify / Rename) + Delete -> complete cancellation!
				cur.cancelled = true
				cancelled++
			case EventCreate:
				*prev = ev
				coalesced++
			}

		case EventModifyMeta:
			switch ev.Type {
			case EventModifyMeta:
				mergeMeta(prev, ev, true)
				coalesced++
			case EventRename:
				merged := ev
				if merged.Size == SizeUnknown && prev.Size != SizeUnknown {
					merged.Size = prev.Size
				} else if merged.Size == 0 && prev.Size > 0 && merged.ModTime == 0 {
					// Rename record omitted metadata entirely -> preserve prior ModifyMeta size/mtime.
					merged.Size = prev.Size
					merged.ModTime = prev.ModTime
				}
				if merged.ModTime == 0 && prev.ModTime != 0 {
					merged.ModTime = prev.ModTime
				}
				*prev = merged
				coalesced++
			case EventDelete:
				*prev = ev
				coalesced++
			case EventCreate:
				*prev = ev
				coalesced++
			}

		case EventRename:
			switch ev.Type {
			case EventModifyMeta:
				mergeMeta(prev, ev, true)
				coalesced++
			case EventRename:
				// Preserve original OldPath and original source ParentKey, adopt final destination & name.
				if prev.OldPath == "" {
					prev.OldPath = ev.OldPath
				}
				if ev.NewParentKey != 0 {
					prev.NewParentKey = ev.NewParentKey
				} else if ev.ParentKey != 0 && prev.NewParentKey == 0 {
					prev.NewParentKey = ev.ParentKey
				}
				if ev.NewName != "" {
					prev.NewName = ev.NewName
				}
				if ev.RelPath != "" {
					prev.RelPath = ev.RelPath
				}
				if ev.Flags != 0 || ev.ReplaceFlags {
					prev.Flags = ev.Flags
					prev.ReplaceFlags = prev.ReplaceFlags || ev.ReplaceFlags
				}
				prev.PrunedSubtree = prev.PrunedSubtree || ev.PrunedSubtree
				mergeMeta(prev, ev, false)
				coalesced++
			case EventDelete:
				ev.PrunedSubtree = ev.PrunedSubtree || prev.PrunedSubtree
				*prev = ev
				coalesced++
			case EventCreate:
				*prev = ev
				coalesced++
			}

		case EventDelete:
			switch ev.Type {
			case EventCreate:
				// Same NodeKey reused in the same window (e.g. inode reuse).
				*prev = ev
				coalesced++
			case EventDelete, EventModifyMeta, EventRename:
				// Keep Delete; preserve PrunedSubtree and advance cursor if newer.
				prev.PrunedSubtree = prev.PrunedSubtree || ev.PrunedSubtree
				if ev.Cursor > prev.Cursor {
					prev.Cursor = ev.Cursor
				}
				coalesced++
			}
		}
	}

	out = make([]FileEvent, 0, len(slots)+len(rescans))
	for i := range slots {
		if !slots[i].cancelled {
			out = append(out, slots[i].ev)
		}
	}
	out = append(out, rescans...)
	return out, cancelled, coalesced
}

func mergeMeta(dst *FileEvent, src FileEvent, explicitMetaUpdate bool) {
	// Explicit ModifyMeta with Size == 0 represents a real truncation to 0 bytes;
	// Rename events with zero Size & zero ModTime omit metadata and preserve dst.Size.
	if src.Size != SizeUnknown {
		if explicitMetaUpdate || src.Size > 0 || src.ModTime != 0 {
			dst.Size = src.Size
		}
	}
	if src.ModTime != 0 {
		dst.ModTime = src.ModTime
	}
	if src.Flags != 0 {
		// Preserve existing OS bits (Hidden/Symlink/NonASCII) while adopting updated governance bits.
		const osBitsMask = ^(filter.FlagDir | filter.FlagFolded | filter.FlagSystem)
		if src.Flags&osBitsMask != 0 {
			dst.Flags = src.Flags
		} else {
			dst.Flags = mergePreservedFlags(dst.Flags, src.Flags)
		}
	}
	dst.PrunedSubtree = dst.PrunedSubtree || src.PrunedSubtree
	if src.Cursor > dst.Cursor {
		dst.Cursor = src.Cursor
	}
}

func applyRenameOntoCreate(dst *FileEvent, src FileEvent) {
	if src.NewParentKey != 0 {
		dst.ParentKey = src.NewParentKey
	} else if src.ParentKey != 0 {
		dst.ParentKey = src.ParentKey
	}
	dst.NewParentKey = 0
	if src.NewName != "" {
		dst.NewName = src.NewName
	}
	if src.RelPath != "" {
		dst.RelPath = src.RelPath
	}
	if src.Flags != 0 || src.ReplaceFlags {
		dst.Flags = src.Flags
	}
	mergeMeta(dst, src, false)
}
