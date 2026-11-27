package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Options configures the Tier 1 columnar storage engine.
type Options struct {
	// DisableWALSync skips fsync on every WAL batch write (useful for high-throughput benchmarks).
	DisableWALSync bool
	// Tier2Threshold is the live node threshold at which RecommendedTier returns Tier2LSM.
	// Defaults to DefaultTier2Threshold (5,000,000).
	Tier2Threshold uint64
	// LowMemoryMode forces RecommendedTier to return Tier2LSM regardless of live node count.
	LowMemoryMode bool
	// MemTableFlushThreshold is the MemTable entry count at which AutoFlush triggers.
	// Defaults to DefaultMemTableFlushThreshold (65,536).
	MemTableFlushThreshold int
	// AutoFlush automatically flushes MemTable to a delta segment when MemTableFlushThreshold is reached.
	AutoFlush bool
}

// OpenStats reports diagnostic metrics collected during Engine cold start.
type OpenStats struct {
	ReplayedWALCount      int
	TruncatedWALBytes     int
	OrphanSegmentsRemoved int
	OpenDuration          time.Duration
}

// Engine is the Tier 1 columnar hot/cold storage engine with mmap hot columns,
// zero-GC CoW MVCC MemTable, pointer-free DirByKey, atomic MANIFEST, and crash-safe WAL.
type Engine struct {
	dataDir  string
	indexDir string
	segDir   string
	opts     Options

	mu               sync.RWMutex
	closed           bool
	denseBuildActive bool
	manifest         *Manifest
	extPool          *ExtPool
	wal              *WAL
	segments         []*Segment // ordered oldest (base) -> newest (deltas)
	mem              *MemTable
	memShared        atomic.Bool // true when at least one Snapshot shares e.mem (CoW on next write)
	writeGen         uint64      // monotonically incremented under e.mu.Lock on any mutation/segment change
	dirByKey         *KeyMap
	openStats        OpenStats
}

// Open mounts an existing index under <dataDir>/index/ or initializes a new one.
func Open(dataDir string, opts Options) (*Engine, error) {
	start := time.Now()
	if opts.Tier2Threshold == 0 {
		opts.Tier2Threshold = DefaultTier2Threshold
	}
	if opts.MemTableFlushThreshold <= 0 {
		opts.MemTableFlushThreshold = DefaultMemTableFlushThreshold
	}

	indexDir := filepath.Join(dataDir, "index")
	segDir := filepath.Join(indexDir, "segments")
	if err := os.MkdirAll(segDir, 0o755); err != nil {
		return nil, fmt.Errorf("create index dirs: %w", err)
	}

	// Clean leftover atomic-write temp files in index/ before loading.
	cleanTempFiles(indexDir)

	manifest, _, err := LoadManifest(filepath.Join(indexDir, "MANIFEST"))
	if err != nil {
		return nil, err
	}

	extPool, err := LoadExtPool(filepath.Join(indexDir, "ext.pool"))
	if err != nil {
		return nil, err
	}

	// Open active segments referenced by MANIFEST with their committed tombstone generation.
	activeNames := make(map[string]struct{}, len(manifest.Segments))
	segments := make([]*Segment, 0, len(manifest.Segments))
	closeOpened := func() {
		for _, s := range segments {
			s.Release()
		}
	}
	for _, sm := range manifest.Segments {
		name := SegmentDirName(sm.ID)
		activeNames[name] = struct{}{}
		s, err := OpenSegmentWithGen(filepath.Join(segDir, name), sm.TombstoneGen)
		if err != nil {
			closeOpened()
			return nil, fmt.Errorf("open segment %s: %w", name, err)
		}
		segments = append(segments, s)
	}

	// Clean up orphan or incomplete segment directories not listed in MANIFEST.
	orphansRemoved := 0
	if entries, err := os.ReadDir(segDir); err == nil {
		for _, ent := range entries {
			if _, ok := activeNames[ent.Name()]; !ok {
				if err := os.RemoveAll(filepath.Join(segDir, ent.Name())); err == nil {
					orphansRemoved++
				}
			}
		}
	}

	// Rebuild pointer-free DirByKey quickly from dirs.col (without paging in hot.col / names.col).
	dirByKey := NewKeyMap(256)
	for _, s := range segments {
		if err := s.PopulateDirMap(dirByKey); err != nil {
			closeOpened()
			return nil, err
		}
	}

	// Open WAL fenced by MANIFEST.WALGeneration and replay committed incremental mutations.
	wal, replayed, truncatedBytes, err := OpenWAL(filepath.Join(indexDir, "wal.log"), manifest.WALGeneration, !opts.DisableWALSync)
	if err != nil {
		closeOpened()
		return nil, err
	}

	e := &Engine{
		dataDir:  dataDir,
		indexDir: indexDir,
		segDir:   segDir,
		opts:     opts,
		manifest: manifest,
		extPool:  extPool,
		wal:      wal,
		segments: segments,
		mem:      NewMemTable(max(len(replayed), 64)),
		dirByKey: dirByKey,
	}

	for i := range replayed {
		if err := e.applyMutationLocked(replayed[i]); err != nil {
			_ = e.Close()
			return nil, fmt.Errorf("replay wal[%d]: %w", i, err)
		}
	}

	e.openStats = OpenStats{
		ReplayedWALCount:      len(replayed),
		TruncatedWALBytes:     truncatedBytes,
		OrphanSegmentsRemoved: orphansRemoved,
		OpenDuration:          time.Since(start),
	}
	return e, nil
}

func cleanTempFiles(dir string) {
	if entries, err := os.ReadDir(dir); err == nil {
		for _, ent := range entries {
			if !ent.IsDir() && strings.HasPrefix(ent.Name(), ".xift-tmp-") {
				_ = os.Remove(filepath.Join(dir, ent.Name()))
			}
		}
	}
}

// OpenStats returns diagnostics recorded when the engine was opened.
func (e *Engine) OpenStats() OpenStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.openStats
}

// ExtPool returns the engine's extension intern pool.
func (e *Engine) ExtPool() *ExtPool {
	return e.extPool
}

// BeginDenseSegment flushes any pending WAL/MemTable checkpoint, reserves a segment ID and baseNodeID,
// and returns a streaming Dense SegmentBuilder. Only one dense build may be active at a time.
func (e *Engine) BeginDenseSegment() (*SegmentBuilder, error) {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil, ErrClosed
	}
	if e.denseBuildActive {
		e.mu.Unlock()
		return nil, fmt.Errorf("storage: another dense segment build is already active")
	}
	if e.mem.EntryCount() > 0 {
		if err := e.flushLocked(); err != nil {
			e.mu.Unlock()
			return nil, fmt.Errorf("flush before BeginDenseSegment: %w", err)
		}
	}
	segID := e.manifest.NextSegmentID
	e.manifest.NextSegmentID++
	baseNodeID := e.manifest.NextNodeID
	e.denseBuildActive = true
	e.mu.Unlock()

	dir := filepath.Join(e.segDir, SegmentDirName(segID))
	b, err := NewSegmentBuilder(dir, segID, baseNodeID, true, e.extPool)
	if err != nil {
		e.mu.Lock()
		e.denseBuildActive = false
		e.mu.Unlock()
		return nil, err
	}
	b.onAbort = func() {
		e.mu.Lock()
		e.denseBuildActive = false
		e.mu.Unlock()
	}
	return b, nil
}

// CommitSegment mounts a freshly built segment and atomically records it in MANIFEST,
// advancing WALGeneration to prevent stale WAL replay or cursor regression.
func (e *Engine) CommitSegment(meta SegmentMeta, cursor *VolumeCursor) error {
	segPath := filepath.Join(e.segDir, SegmentDirName(meta.ID))
	seg, err := OpenSegmentWithGen(segPath, meta.TombstoneGen)
	if err != nil {
		e.mu.Lock()
		e.denseBuildActive = false
		e.mu.Unlock()
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	e.denseBuildActive = false
	if e.closed {
		seg.Release()
		return ErrClosed
	}

	// Ensure no NodeID collision if concurrent ApplyBatch ran while builder was open.
	if meta.Dense && meta.NodeCount > 0 && meta.BaseNodeID < e.manifest.NextNodeID {
		seg.Release()
		return fmt.Errorf("storage: dense segment BaseNodeID %d overlaps allocated NextNodeID %d", meta.BaseNodeID, e.manifest.NextNodeID)
	}

	// Flush any pending MemTable/WAL mutations before advancing MANIFEST + VolumeCursor.
	if e.mem.EntryCount() > 0 {
		if err := e.flushLocked(); err != nil {
			seg.Release()
			return fmt.Errorf("flush before CommitSegment: %w", err)
		}
	}

	if e.extPool.Dirty() {
		if err := e.extPool.Save(filepath.Join(e.indexDir, "ext.pool")); err != nil {
			seg.Release()
			return err
		}
	}

	nextManifest := e.manifest.Clone()
	nextManifest.Segments = append(nextManifest.Segments, meta)
	if meta.ID >= nextManifest.NextSegmentID {
		nextManifest.NextSegmentID = meta.ID + 1
	}
	if meta.Dense {
		endID := meta.BaseNodeID + meta.NodeCount
		if endID > nextManifest.NextNodeID {
			nextManifest.NextNodeID = endID
		}
	}
	if cursor != nil {
		nextManifest.SetVolumeCursor(*cursor)
	}
	nextManifest.WALGeneration++

	if err := nextManifest.Save(filepath.Join(e.indexDir, "MANIFEST")); err != nil {
		seg.Release()
		return err
	}

	// Post-MANIFEST commit: update in-memory state immediately so segIDs never collide, then rotate WAL.
	e.manifest = nextManifest
	e.segments = append(e.segments, seg)
	e.writeGen++
	if err := seg.PopulateDirMap(e.dirByKey); err != nil {
		return err
	}
	return e.wal.Reset(nextManifest.WALGeneration)
}

func (e *Engine) ensureMemWritableLocked() {
	if e.memShared.Swap(false) {
		e.mem = e.mem.CloneSnapshot()
	}
}

func (e *Engine) resetMemLocked() {
	if e.memShared.Swap(false) {
		e.mem = NewMemTable(64)
	} else {
		e.mem.Reset()
	}
}

// ApplyBatch durably logs muts to wal.log and applies them to MemTable / tombstones in memory.
func (e *Engine) ApplyBatch(muts []Mutation) error {
	if len(muts) == 0 {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrClosed
	}

	if err := e.wal.AppendBatch(muts); err != nil {
		return err
	}
	e.ensureMemWritableLocked()
	for i := range muts {
		if err := e.applyMutationLocked(muts[i]); err != nil {
			return err
		}
	}
	e.writeGen++

	if e.opts.AutoFlush && e.mem.EntryCount() >= e.opts.MemTableFlushThreshold {
		return e.flushLocked()
	}
	return nil
}

func (e *Engine) applyMutationLocked(m Mutation) error {
	switch m.Op {
	case OpCreate:
		parentIdx := e.resolveParentIdxLocked(m.VolID, m.ParentKey, m.NodeKey)
		isDir := (m.Flags & FlagDir) != 0

		var nodeID uint32
		if existingID, found := e.findLiveByKeyLocked(m.VolID, m.NodeKey); found {
			nodeID = existingID
			e.markDeletedInSegmentsLocked(nodeID)
		} else {
			nodeID = e.allocNodeIDLocked()
		}

		extID := e.extPool.InternFromName(m.Name, isDir)
		e.mem.Upsert(nodeID, m.VolID, m.NodeKey, parentIdx, m.Name, m.Flags, extID, m.Size, m.ModTime)
		if isDir {
			e.dirByKey.Put(m.VolID, m.NodeKey, nodeID)
		} else {
			e.dirByKey.Delete(m.VolID, m.NodeKey)
		}

	case OpDelete:
		nodeID, found := e.findLiveByKeyLocked(m.VolID, m.NodeKey)
		if !found {
			return nil
		}
		e.dirByKey.Delete(m.VolID, m.NodeKey)
		e.mem.MarkDeletedByNodeID(nodeID)
		e.markDeletedInSegmentsLocked(nodeID)

	case OpModifyMeta:
		nodeID, found := e.findLiveByKeyLocked(m.VolID, m.NodeKey)
		if !found {
			return nil
		}
		e.mem.SetColdOverrideWithKey(nodeID, m.NodeKey, ColdMeta{Size: m.Size, ModTime: m.ModTime})

	case OpRename:
		nodeID, oldNode, _, oldCold, found, err := e.findLiveFullByKeyLocked(m.VolID, m.NodeKey)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		parentIdx := oldNode.ParentIdx
		if m.NewParentKey != 0 {
			parentIdx = e.resolveParentIdxLocked(m.VolID, m.NewParentKey, m.NodeKey)
		} else if m.ParentKey != 0 {
			parentIdx = e.resolveParentIdxLocked(m.VolID, m.ParentKey, m.NodeKey)
		}

		isDir := (oldNode.Flags & FlagDir) != 0
		flags := oldNode.Flags
		if m.ReplaceFlags || m.Flags != 0 {
			flags = m.Flags
			if isDir {
				flags |= FlagDir
			} else {
				flags &^= FlagDir
			}
		}
		extID := e.extPool.InternFromName(m.Name, isDir)

		cm := oldCold
		if m.Size != 0 || m.ModTime != 0 {
			cm = ColdMeta{Size: m.Size, ModTime: m.ModTime}
		}

		e.markDeletedInSegmentsLocked(nodeID)
		e.mem.Upsert(nodeID, m.VolID, m.NodeKey, parentIdx, m.Name, flags, extID, cm.Size, cm.ModTime)
		if isDir {
			e.dirByKey.Put(m.VolID, m.NodeKey, nodeID)
		}

	case OpMoveDir:
		var nodeID uint32
		var found bool
		if id, ok := e.dirByKey.Get(m.VolID, m.NodeKey); ok {
			nodeID, found = id, true
		} else {
			nodeID, found = e.findLiveByKeyLocked(m.VolID, m.NodeKey)
		}
		if !found {
			return nil
		}
		oldNode, oldName, oldCold, ok, err := e.findLiveFullByNodeIDLocked(nodeID)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		newParentIdx := e.resolveParentIdxLocked(m.VolID, m.NewParentKey, m.NodeKey)
		e.markDeletedInSegmentsLocked(nodeID)
		e.mem.Upsert(nodeID, m.VolID, m.NodeKey, newParentIdx, oldName, oldNode.Flags|FlagDir, oldNode.ExtID, oldCold.Size, oldCold.ModTime)
		e.dirByKey.Put(m.VolID, m.NodeKey, nodeID)

	case OpCursorUpdate:
		e.manifest.SetVolumeCursor(VolumeCursor{
			VolID:    m.VolID,
			Cursor:   m.Cursor,
			RootPath: m.RootPath,
		})
	}
	return nil
}

func (e *Engine) allocNodeIDLocked() uint32 {
	id := e.manifest.NextNodeID
	e.manifest.NextNodeID++
	return id
}

func (e *Engine) resolveParentIdxLocked(volID uint16, parentKey, selfKey uint64) uint32 {
	if parentKey == 0 || parentKey == selfKey {
		return RootParentIdx
	}
	if dirID, ok := e.dirByKey.Get(volID, parentKey); ok {
		return dirID
	}
	return RootParentIdx
}

func (e *Engine) findLiveByKeyLocked(volID uint16, nodeKey uint64) (uint32, bool) {
	if slot, ok := e.mem.GetByKey(volID, nodeKey); ok {
		return e.mem.NodeIDAt(slot), true
	}
	for i := len(e.segments) - 1; i >= 0; i-- {
		if nodeID, _, ok := e.segments[i].LookupByKey(volID, nodeKey); ok {
			return nodeID, true
		}
	}
	return 0, false
}

func (e *Engine) findLiveFullByKeyLocked(volID uint16, nodeKey uint64) (uint32, HotNode, string, ColdMeta, bool, error) {
	if slot, ok := e.mem.GetByKey(volID, nodeKey); ok {
		return e.mem.NodeIDAt(slot), e.mem.NodeAt(slot), string(e.mem.NameBytesAt(slot)), e.mem.ColdMetaAt(slot), true, nil
	}
	for i := len(e.segments) - 1; i >= 0; i-- {
		seg := e.segments[i]
		if nodeID, localIdx, ok := seg.LookupByKey(volID, nodeKey); ok {
			cm, okOverride := e.mem.GetColdOverride(nodeID)
			if !okOverride {
				var err error
				cm, err = seg.ReadColdMeta(localIdx)
				if err != nil {
					return 0, HotNode{}, "", ColdMeta{}, false, err
				}
			}
			return nodeID, seg.NodeAt(localIdx), string(seg.NameBytesAt(localIdx)), cm, true, nil
		}
	}
	return 0, HotNode{}, "", ColdMeta{}, false, nil
}

func (e *Engine) findLiveFullByNodeIDLocked(nodeID uint32) (HotNode, string, ColdMeta, bool, error) {
	if slot, ok := e.mem.GetByNodeID(nodeID); ok {
		return e.mem.NodeAt(slot), string(e.mem.NameBytesAt(slot)), e.mem.ColdMetaAt(slot), true, nil
	}
	for i := len(e.segments) - 1; i >= 0; i-- {
		seg := e.segments[i]
		if localIdx, ok := seg.LookupNodeID(nodeID); ok {
			cm, okOverride := e.mem.GetColdOverride(nodeID)
			if !okOverride {
				var err error
				cm, err = seg.ReadColdMeta(localIdx)
				if err != nil {
					return HotNode{}, "", ColdMeta{}, false, err
				}
			}
			return seg.NodeAt(localIdx), string(seg.NameBytesAt(localIdx)), cm, true, nil
		}
	}
	return HotNode{}, "", ColdMeta{}, false, nil
}

func (e *Engine) markDeletedInSegmentsLocked(nodeID uint32) {
	for i := len(e.segments) - 1; i >= 0; i-- {
		seg := e.segments[i]
		if localIdx, ok := seg.LookupNodeID(nodeID); ok {
			seg.MarkDeleted(localIdx)
		}
	}
}

type flushRow struct {
	nodeID    uint32
	volID     uint16
	nodeKey   uint64
	parentIdx uint32
	name      string
	flags     uint16
	extID     uint16
	size      int64
	modTime   int64
}

// Flush writes MemTable (and materialized cold overrides) to a new immutable delta segment,
// stages new tombstone generations without overwriting committed ones, atomically commits MANIFEST,
// and rotates wal.log with the new WALGeneration.
func (e *Engine) Flush() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrClosed
	}
	return e.flushLocked()
}

func (e *Engine) flushLocked() error {
	rows := make([]flushRow, 0, e.mem.LiveCount()+e.mem.ColdOverrideCount())

	e.mem.ForEachLive(func(nodeID uint32, node HotNode, name []byte) bool {
		slot, _ := e.mem.GetByNodeID(nodeID)
		cm := e.mem.ColdMetaAt(slot)
		rows = append(rows, flushRow{
			nodeID:    nodeID,
			volID:     node.VolID,
			nodeKey:   e.mem.KeyAt(slot),
			parentIdx: node.ParentIdx,
			name:      string(name),
			flags:     node.Flags,
			extID:     node.ExtID,
			size:      cm.Size,
			modTime:   cm.ModTime,
		})
		return true
	})

	// Collect standalone cold overrides per owning segment index (without mutating live delBits yet).
	extraTombstones := make([][]uint32, len(e.segments))
	e.mem.ForEachColdOverride(func(nodeID uint32, nodeKey uint64, meta ColdMeta) bool {
		for i := len(e.segments) - 1; i >= 0; i-- {
			seg := e.segments[i]
			if localIdx, ok := seg.LookupNodeID(nodeID); ok {
				n := seg.NodeAt(localIdx)
				rows = append(rows, flushRow{
					nodeID:    nodeID,
					volID:     n.VolID,
					nodeKey:   nodeKey,
					parentIdx: n.ParentIdx,
					name:      string(seg.NameBytesAt(localIdx)),
					flags:     n.Flags,
					extID:     n.ExtID,
					size:      meta.Size,
					modTime:   meta.ModTime,
				})
				extraTombstones[i] = append(extraTombstones[i], localIdx)
				break
			}
		}
		return true
	})

	var newSeg *Segment
	var newMeta SegmentMeta
	nextManifest := e.manifest.Clone()

	if len(rows) > 0 {
		slices.SortFunc(rows, func(a, b flushRow) int {
			if a.nodeID < b.nodeID {
				return -1
			}
			if a.nodeID > b.nodeID {
				return 1
			}
			return 0
		})

		segID := nextManifest.NextSegmentID
		nextManifest.NextSegmentID++
		segPath := filepath.Join(e.segDir, SegmentDirName(segID))
		builder, err := NewSegmentBuilder(segPath, segID, 0, false, e.extPool)
		if err != nil {
			return err
		}
		for i := range rows {
			r := rows[i]
			if err := builder.Append(r.nodeID, r.volID, r.nodeKey, r.parentIdx, r.name, r.flags, r.extID, r.size, r.modTime); err != nil {
				builder.Abort()
				return err
			}
		}
		newMeta, err = builder.Finish()
		if err != nil {
			builder.Abort()
			return err
		}
		newSeg, err = OpenSegmentWithGen(segPath, 0)
		if err != nil {
			_ = os.RemoveAll(segPath)
			return err
		}
		nextManifest.Segments = append(nextManifest.Segments, newMeta)
	}

	// Stage new generation tombstone files (del_<nextGen>.bit) without touching committed tombstone files.
	type stagedTomb struct {
		segIdx int
		newGen uint32
		path   string
		extras []uint32
	}
	var staged []stagedTomb
	cleanupStaged := func() {
		for _, st := range staged {
			_ = os.Remove(st.path)
		}
		if newSeg != nil {
			newSeg.MarkForRemoval()
		}
	}

	for i, s := range e.segments {
		if !s.delDirty && len(extraTombstones[i]) == 0 {
			continue
		}
		nextGen := s.meta.TombstoneGen + 1
		stagedPath, err := s.StageTombstones(extraTombstones[i], nextGen)
		if err != nil {
			cleanupStaged()
			return err
		}
		staged = append(staged, stagedTomb{
			segIdx: i,
			newGen: nextGen,
			path:   stagedPath,
			extras: extraTombstones[i],
		})
		nextManifest.Segments[i].TombstoneGen = nextGen
	}

	if e.extPool.Dirty() {
		if err := e.extPool.Save(filepath.Join(e.indexDir, "ext.pool")); err != nil {
			cleanupStaged()
			return err
		}
	}

	// Atomic commit point: MANIFEST switch with incremented WALGeneration.
	nextManifest.WALGeneration++
	if err := nextManifest.Save(filepath.Join(e.indexDir, "MANIFEST")); err != nil {
		cleanupStaged()
		return err
	}

	// Post-commit: MANIFEST is durable on disk. Update in-memory state atomically, clean old del_*.bit, then reset WAL.
	for _, st := range staged {
		e.segments[st.segIdx].CommitStagedTombstones(st.extras, st.newGen)
	}
	e.manifest = nextManifest
	if newSeg != nil {
		e.segments = append(e.segments, newSeg)
	}
	e.resetMemLocked()

	return e.wal.Reset(nextManifest.WALGeneration)
}

// Compact merges all segments and MemTable into a single zero-tombstone Dense segment,
// remaps ParentIdx cleanly (preserving orphan detachment), rebuilds DirByKey, atomically commits MANIFEST,
// rotates WAL, and schedules old segments for removal once active snapshots release them.
func (e *Engine) Compact() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrClosed
	}

	var estLive uint64
	for _, s := range e.segments {
		estLive += uint64(s.LiveCount())
	}
	estLive += uint64(e.mem.LiveCount())

	nextManifest := e.manifest.Clone()
	nextManifest.WALGeneration++

	if estLive == 0 {
		nextManifest.Segments = nil
		nextManifest.NextNodeID = 0
		if err := nextManifest.Save(filepath.Join(e.indexDir, "MANIFEST")); err != nil {
			return err
		}
		oldSegs := e.segments
		e.segments = nil
		e.manifest = nextManifest
		e.dirByKey.Clear()
		e.resetMemLocked()
		for _, s := range oldSegs {
			s.MarkForRemoval()
		}
		return e.wal.Reset(nextManifest.WALGeneration)
	}

	// Pass 1: Assign dense new NodeIDs [0 .. liveCount) and invert keys.idx for each old segment.
	oldToNew := newU32Map(int(estLive))
	segKeys := make([][]uint64, len(e.segments))
	var newID uint32
	for si, s := range e.segments {
		keys := make([]uint64, s.Count())
		s.PopulateLocalKeys(keys)
		segKeys[si] = keys

		for localIdx := uint32(0); localIdx < s.Count(); localIdx++ {
			if s.IsDeleted(localIdx) {
				continue
			}
			oldToNew.Put(s.NodeIDAt(localIdx), newID)
			newID++
		}
	}
	e.mem.ForEachLive(func(nodeID uint32, _ HotNode, _ []byte) bool {
		oldToNew.Put(nodeID, newID)
		newID++
		return true
	})

	// Pass 2: Stream live rows into a single fresh Dense segment using buffered sequential cold-column streams.
	segID := nextManifest.NextSegmentID
	nextManifest.NextSegmentID++
	segPath := filepath.Join(e.segDir, SegmentDirName(segID))
	builder, err := NewSegmentBuilder(segPath, segID, 0, true, e.extPool)
	if err != nil {
		return err
	}
	newDirByKey := NewKeyMap(max(e.dirByKey.Len(), 64))

	remapParent := func(oldParent uint32) uint32 {
		if oldParent == RootParentIdx {
			return RootParentIdx
		}
		if mapped, ok := oldToNew.Get(oldParent); ok {
			return mapped
		}
		// Preserve detached status so children of deleted directories are never promoted to root.
		return OrphanParentIdx
	}

	for si, s := range e.segments {
		keys := segKeys[si]
		cs, err := s.openColdStream()
		if err != nil {
			builder.Abort()
			return err
		}
		for localIdx := uint32(0); localIdx < s.Count(); localIdx++ {
			diskCold, readErr := cs.Next()
			if readErr != nil {
				cs.Close()
				builder.Abort()
				return readErr
			}
			if s.IsDeleted(localIdx) {
				continue
			}
			oldID := s.NodeIDAt(localIdx)
			n := s.NodeAt(localIdx)
			cm, okOverride := e.mem.GetColdOverride(oldID)
			if !okOverride {
				cm = diskCold
			}
			assignedID, err := builder.AppendDense(
				n.VolID,
				keys[localIdx],
				remapParent(n.ParentIdx),
				string(s.NameBytesAt(localIdx)),
				n.Flags,
				n.ExtID,
				cm.Size,
				cm.ModTime,
			)
			if err != nil {
				cs.Close()
				builder.Abort()
				return err
			}
			if (n.Flags & FlagDir) != 0 {
				newDirByKey.Put(n.VolID, keys[localIdx], assignedID)
			}
		}
		cs.Close()
	}

	var memErr error
	e.mem.ForEachLive(func(nodeID uint32, n HotNode, name []byte) bool {
		slot, _ := e.mem.GetByNodeID(nodeID)
		cm := e.mem.ColdMetaAt(slot)
		nk := e.mem.KeyAt(slot)
		assignedID, err := builder.AppendDense(
			n.VolID,
			nk,
			remapParent(n.ParentIdx),
			string(name),
			n.Flags,
			n.ExtID,
			cm.Size,
			cm.ModTime,
		)
		if err != nil {
			memErr = err
			return false
		}
		if (n.Flags & FlagDir) != 0 {
			newDirByKey.Put(n.VolID, nk, assignedID)
		}
		return true
	})
	if memErr != nil {
		builder.Abort()
		return memErr
	}

	newMeta, err := builder.Finish()
	if err != nil {
		builder.Abort()
		return err
	}
	newSeg, err := OpenSegmentWithGen(segPath, 0)
	if err != nil {
		_ = os.RemoveAll(segPath)
		return err
	}

	if e.extPool.Dirty() {
		if err := e.extPool.Save(filepath.Join(e.indexDir, "ext.pool")); err != nil {
			newSeg.MarkForRemoval()
			return err
		}
	}

	// Atomic commit point: MANIFEST switch with incremented WALGeneration.
	nextManifest.Segments = []SegmentMeta{newMeta}
	nextManifest.NextNodeID = newMeta.NodeCount
	if err := nextManifest.Save(filepath.Join(e.indexDir, "MANIFEST")); err != nil {
		newSeg.MarkForRemoval()
		return err
	}

	// Post-commit: switch live engine state, release old segments, then rotate WAL.
	oldSegs := e.segments
	e.segments = []*Segment{newSeg}
	e.dirByKey = newDirByKey
	e.manifest = nextManifest
	e.writeGen++
	e.resetMemLocked()

	for _, s := range oldSegs {
		s.MarkForRemoval()
	}
	return e.wal.Reset(nextManifest.WALGeneration)
}

type snapSegment struct {
	seg       *Segment
	delBits   []uint64
	liveCount uint32
}

// AcquireSnapshot captures a zero-copy CoW MVCC point-in-time view of all segments and MemTable.
// Active snapshots remain completely isolated from concurrent ApplyBatch, Flush, and Compact operations.
// The caller MUST call Release() when done so compacted segments can unmap and clean up.
func (e *Engine) AcquireSnapshot() *Snapshot {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return &Snapshot{closed: true}
	}
	segs := make([]snapSegment, len(e.segments))
	for i, s := range e.segments {
		s.Retain()
		delBits, live := s.SnapshotDelBits()
		segs[i] = snapSegment{
			seg:       s,
			delBits:   delBits,
			liveCount: live,
		}
	}
	e.memShared.Store(true)
	return &Snapshot{
		segs: segs,
		mem:  e.mem,
		gen:  e.writeGen,
	}
}

// LiveCount returns the total number of non-deleted nodes across all segments and MemTable.
func (e *Engine) LiveCount() uint64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	var total uint64
	for _, s := range e.segments {
		total += uint64(s.LiveCount())
	}
	total += uint64(e.mem.LiveCount())
	return total
}

// MemTableEntryCount returns the number of entries currently buffered in MemTable.
func (e *Engine) MemTableEntryCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.mem.EntryCount()
}

// VolumeCursor returns the latest persisted/in-memory journal cursor for volID.
func (e *Engine) VolumeCursor(volID uint16) (VolumeCursor, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.manifest.GetVolumeCursor(volID)
}

// RecommendedTier evaluates Tier 1 vs Tier 2 based on LowMemoryMode and Tier2Threshold.
func (e *Engine) RecommendedTier() StorageTier {
	if e.opts.LowMemoryMode || e.LiveCount() >= e.opts.Tier2Threshold {
		return Tier2LSM
	}
	return Tier1Mmap
}

// ReadColdMeta performs an on-demand cold-column lookup for a single nodeID.
func (e *Engine) ReadColdMeta(nodeID uint32) (ColdMeta, error) {
	snap := e.AcquireSnapshot()
	defer snap.Release()
	if snap.closed {
		return ColdMeta{}, ErrClosed
	}
	return snap.ReadColdMeta(nodeID)
}

// ReadColdMetaBatch performs viewport batch cold-column lookups for nodeIDs into out.
func (e *Engine) ReadColdMetaBatch(nodeIDs []uint32, out []ColdMeta) error {
	snap := e.AcquireSnapshot()
	defer snap.Release()
	if snap.closed {
		return ErrClosed
	}
	return snap.ReadColdMetaBatch(nodeIDs, out)
}

// ResolvePath reconstructs the full '/' separated path for nodeID with cycle protection.
func (e *Engine) ResolvePath(nodeID uint32) (string, error) {
	snap := e.AcquireSnapshot()
	defer snap.Release()
	if snap.closed {
		return "", ErrClosed
	}
	return snap.ResolvePath(nodeID)
}

// Close releases all segment mappings and closes the WAL.
// Unflushed MemTable mutations remain safely in wal.log for replay on next Open.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true

	var firstErr error
	if e.extPool.Dirty() {
		if err := e.extPool.Save(filepath.Join(e.indexDir, "ext.pool")); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if e.wal != nil {
		if err := e.wal.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		e.wal = nil
	}
	for _, s := range e.segments {
		s.Release()
	}
	e.segments = nil
	return firstErr
}

// Snapshot provides lock-free, race-free, point-in-time isolated read access to segments and MemTable.
type Snapshot struct {
	segs     []snapSegment
	mem      *MemTable
	gen      uint64
	closed   bool
	released atomic.Bool
}

// Release drops segment references held by this Snapshot. Idempotent.
func (s *Snapshot) Release() {
	if s == nil || !s.released.CompareAndSwap(false, true) {
		return
	}
	for i := range s.segs {
		s.segs[i].seg.Release()
	}
	s.segs = nil
	s.mem = nil
}

// SegmentCount returns the number of mounted segments in this Snapshot.
func (s *Snapshot) SegmentCount() int {
	return len(s.segs)
}

// Segments exposes read-only mounted segments for downstream multi-core hot-column scanners (P1-04).
func (s *Snapshot) Segments() []*Segment {
	out := make([]*Segment, len(s.segs))
	for i := range s.segs {
		out[i] = s.segs[i].seg
	}
	return out
}

// MemTable exposes the immutable snapshot MemTable for downstream scanners.
func (s *Snapshot) MemTable() *MemTable {
	return s.mem
}

// LiveCount returns the total number of live nodes visible in this Snapshot.
func (s *Snapshot) LiveCount() uint64 {
	var total uint64
	for i := range s.segs {
		total += uint64(s.segs[i].liveCount)
	}
	if s.mem != nil {
		total += uint64(s.mem.LiveCount())
	}
	return total
}

// LookupNode resolves nodeID across MemTable and segments (newest -> oldest) using point-in-time tombstones.
func (s *Snapshot) LookupNode(nodeID uint32) (HotNode, []byte, bool) {
	if s.mem != nil {
		if slot, ok := s.mem.GetByNodeID(nodeID); ok {
			return s.mem.NodeAt(slot), s.mem.NameBytesAt(slot), true
		}
	}
	for i := len(s.segs) - 1; i >= 0; i-- {
		ss := s.segs[i]
		if localIdx, ok := ss.seg.LookupNodeIDWithDel(nodeID, ss.delBits); ok {
			return ss.seg.NodeAt(localIdx), ss.seg.NameBytesAt(localIdx), true
		}
	}
	return HotNode{}, nil, false
}

// ForEachLive iterates every live node across all segments and MemTable using point-in-time tombstones.
func (s *Snapshot) ForEachLive(fn func(nodeID uint32, node HotNode, name []byte) bool) {
	for i := range s.segs {
		ss := s.segs[i]
		stopped := false
		ss.seg.ForEachLiveWithDel(ss.delBits, func(nodeID uint32, node HotNode, name []byte) bool {
			if !fn(nodeID, node, name) {
				stopped = true
				return false
			}
			return true
		})
		if stopped {
			return
		}
	}
	if s.mem != nil {
		s.mem.ForEachLive(fn)
	}
}

// ReadColdMeta returns the latest Size and ModTime for nodeID (checking MemTable overrides first,
// then doing a direct ReadAt point lookup on the owning segment's cold columns).
func (s *Snapshot) ReadColdMeta(nodeID uint32) (ColdMeta, error) {
	if s.closed {
		return ColdMeta{}, ErrClosed
	}
	if s.mem != nil {
		if cm, ok := s.mem.GetColdOverride(nodeID); ok {
			return cm, nil
		}
	}
	for i := len(s.segs) - 1; i >= 0; i-- {
		ss := s.segs[i]
		if localIdx, ok := ss.seg.LookupNodeIDWithDel(nodeID, ss.delBits); ok {
			return ss.seg.ReadColdMeta(localIdx)
		}
	}
	return ColdMeta{}, ErrNotFound
}

// ReadColdMetaBatch populates out[i] with cold metadata for nodeIDs[i] (intended for 50..100 viewport rows).
func (s *Snapshot) ReadColdMetaBatch(nodeIDs []uint32, out []ColdMeta) error {
	if s.closed {
		return ErrClosed
	}
	if len(out) < len(nodeIDs) {
		return fmt.Errorf("out slice len %d < nodeIDs len %d", len(out), len(nodeIDs))
	}
	for i, id := range nodeIDs {
		cm, err := s.ReadColdMeta(id)
		if err != nil {
			return err
		}
		out[i] = cm
	}
	return nil
}

// ResolvePath reconstructs the full '/' separated path for nodeID, guarding against parent cycles.
func (s *Snapshot) ResolvePath(nodeID uint32) (string, error) {
	return s.ResolvePathWithSep(nodeID, '/')
}

// ResolvePathWithSep reconstructs the full path using sep as separator, guarding against parent cycles.
func (s *Snapshot) ResolvePathWithSep(nodeID uint32, sep byte) (string, error) {
	if s.closed {
		return "", ErrClosed
	}
	const maxDepth = 512
	var stackVisited [16]uint32
	visitedCount := 0
	var deepVisited map[uint32]struct{}

	parts := make([][]byte, 0, 12)
	cur := nodeID

	for depth := 0; depth < maxDepth; depth++ {
		if deepVisited != nil {
			if _, seen := deepVisited[cur]; seen {
				return "", ErrPathCycle
			}
			deepVisited[cur] = struct{}{}
		} else {
			for i := 0; i < visitedCount; i++ {
				if stackVisited[i] == cur {
					return "", ErrPathCycle
				}
			}
			if visitedCount < len(stackVisited) {
				stackVisited[visitedCount] = cur
				visitedCount++
			} else {
				deepVisited = make(map[uint32]struct{}, 32)
				for i := 0; i < visitedCount; i++ {
					deepVisited[stackVisited[i]] = struct{}{}
				}
				deepVisited[cur] = struct{}{}
			}
		}

		node, name, ok := s.LookupNode(cur)
		if !ok {
			return "", ErrNotFound
		}
		parts = append(parts, name)
		if node.ParentIdx == RootParentIdx {
			break
		}
		if node.ParentIdx == OrphanParentIdx {
			return "", ErrNotFound
		}
		cur = node.ParentIdx
		if depth == maxDepth-1 {
			return "", ErrPathCycle
		}
	}

	totalLen := 0
	for i := range parts {
		totalLen += len(parts[i])
	}
	if len(parts) > 1 {
		totalLen += len(parts) - 1
	}

	var b strings.Builder
	b.Grow(totalLen)
	for i := len(parts) - 1; i >= 0; i-- {
		if i < len(parts)-1 {
			b.WriteByte(sep)
		}
		b.Write(parts[i])
	}
	return b.String(), nil
}
