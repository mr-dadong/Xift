package provider

import (
	"fmt"
	"path"
	"sync"

	"github.com/mr-dadong/Xift/internal/storage"
)

// StorageSegmentWriter adapts FSProvider streaming ScanRecords directly into a Tier 1
// Dense Segment via storage.Engine.BeginDenseSegment / CommitSegment.
// It is safe for concurrent WriteRecord calls (e.g., from multi-threaded top-down scanners).
type StorageSegmentWriter struct {
	engine  *storage.Engine
	vol     VolumeInfo
	dw      *storage.DenseWriter
	extPool *storage.ExtPool

	mu     sync.Mutex
	dirIDs *storage.KeyMap
	closed bool
}

// NewStorageSegmentWriter begins a new Dense Segment build for vol on engine.
// Callers MUST call either Commit or Abort when scanning completes.
func NewStorageSegmentWriter(engine *storage.Engine, vol VolumeInfo) (*StorageSegmentWriter, error) {
	if engine == nil {
		return nil, fmt.Errorf("provider: nil storage engine")
	}
	dw, err := engine.NewDenseWriter(storage.DefaultDenseSegmentNodes)
	if err != nil {
		return nil, err
	}
	return &StorageSegmentWriter{
		engine:  engine,
		vol:     vol,
		dw:      dw,
		extPool: engine.ExtPool(),
		dirIDs:  storage.NewKeyMap(4096),
	}, nil
}

// WriteRecord appends a single pruned ScanRecord to the dense segment, resolving ParentIdx
// in O(1) via the pointer-free KeyMap. Directories MUST be emitted before their children
// (naturally guaranteed by top-down scans and MFT BFS topological emission).
func (w *StorageSegmentWriter) WriteRecord(rec ScanRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return storage.ErrClosed
	}

	volID := rec.VolID
	if volID == 0 {
		volID = w.vol.VolID
	}

	flags := rec.Flags
	isDir := (flags & storage.FlagDir) != 0
	if storage.HasNonASCII(rec.Name) {
		flags |= storage.FlagNonASCII
	}

	parentIdx := storage.RootParentIdx
	if rec.ParentKey != 0 && rec.ParentKey != rec.NodeKey {
		if id, ok := w.dirIDs.Get(volID, rec.ParentKey); ok {
			parentIdx = id
		}
	}

	size := rec.Size
	if size == SizeUnknown {
		size = 0
	}

	extID := w.extPool.InternFromName(rec.Name, isDir)
	nodeID, err := w.dw.Append(volID, rec.NodeKey, parentIdx, rec.Name, flags, extID, size, rec.ModTime)
	if err != nil {
		return err
	}
	if isDir && rec.NodeKey != 0 {
		w.dirIDs.Put(volID, rec.NodeKey, nodeID)
	}
	return nil
}

// Count returns the number of nodes written to this dense segment so far.
func (w *StorageSegmentWriter) Count() uint32 {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.dw == nil {
		return 0
	}
	return w.dw.Count()
}

// Commit finishes the dense segment, mounts it atomically in MANIFEST, and persists vol's journal cursor.
func (w *StorageSegmentWriter) Commit(cursor uint64) (storage.SegmentMeta, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return storage.SegmentMeta{}, storage.ErrClosed
	}
	w.closed = true

	vc := &storage.VolumeCursor{
		VolID:    w.vol.VolID,
		Cursor:   cursor,
		RootPath: w.vol.RootPath,
	}
	return w.dw.Commit(vc)
}

// Abort discards an uncommitted dense segment and releases engine build reservation. Idempotent.
func (w *StorageSegmentWriter) Abort() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return
	}
	w.closed = true
	if w.dw != nil {
		w.dw.Abort()
	}
}

// StorageEventSink bridges coalesced Batcher outputs into storage.Engine.ApplyBatch
// and forwards EventRescanDir requests to an optional targeted rescan callback.
type StorageEventSink struct {
	engine      *storage.Engine
	volRoots    map[uint16]string
	onRescanDir func(ev FileEvent) error

	mu      sync.Mutex
	lastErr error
}

// NewStorageEventSink creates a StorageEventSink writing to engine.
// volRoots maps VolID -> RootPath for OpCursorUpdate persistence (copied defensively).
func NewStorageEventSink(engine *storage.Engine, volRoots map[uint16]string, onRescanDir func(ev FileEvent) error) *StorageEventSink {
	roots := make(map[uint16]string, len(volRoots))
	for k, v := range volRoots {
		roots[k] = v
	}
	return &StorageEventSink{
		engine:      engine,
		volRoots:    roots,
		onRescanDir: onRescanDir,
	}
}

// LastError returns the most recent error encountered by HandleBatch, if any.
func (s *StorageEventSink) LastError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastErr
}

func (s *StorageEventSink) setError(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	s.lastErr = err
	s.mu.Unlock()
}

// HandleBatch converts batch into storage.Mutations, tombstones pruned subtrees when signaled,
// runs onRescanDir BEFORE advancing any volume cursor that had pending rescans, and applies
// mutations atomically via Engine.ApplyBatch.
func (s *StorageEventSink) HandleBatch(batch EventBatch) {
	if s.engine == nil {
		return
	}
	muts := make([]storage.Mutation, 0, len(batch.Events)+len(batch.Cursors))
	var rescans []FileEvent
	rescanVols := make(map[uint16]struct{})

	for i := range batch.Events {
		ev := batch.Events[i]
		if ev.Type == EventRescanDir {
			rescans = append(rescans, ev)
			rescanVols[ev.VolID] = struct{}{}
			continue
		}
		if ev.PrunedSubtree && ev.NodeKey != 0 {
			descKeys := s.collectSubtreeDescendantKeys(ev.VolID, ev.NodeKey)
			for _, dk := range descKeys {
				muts = append(muts, storage.Mutation{
					Op:      storage.OpDelete,
					VolID:   ev.VolID,
					NodeKey: dk,
				})
			}
		}
		if m, ok := s.fileEventToMutation(ev); ok {
			muts = append(muts, m)
		}
	}

	// Only advance cursors in the first ApplyBatch for volumes with NO pending rescans.
	// Volumes with pending rescans advance their cursor only after onRescanDir succeeds.
	for volID, cursor := range batch.Cursors {
		if cursor == 0 {
			continue
		}
		if _, hasRescan := rescanVols[volID]; hasRescan {
			continue
		}
		muts = append(muts, storage.Mutation{
			Op:       storage.OpCursorUpdate,
			VolID:    volID,
			Cursor:   cursor,
			RootPath: s.volRoots[volID],
		})
	}

	if len(muts) > 0 {
		if err := s.engine.ApplyBatch(muts); err != nil {
			s.setError(err)
			return
		}
	}

	if len(rescans) == 0 {
		return
	}

	failedRescanVols := make(map[uint16]struct{})
	if s.onRescanDir != nil {
		for i := range rescans {
			if err := s.onRescanDir(rescans[i]); err != nil {
				s.setError(err)
				failedRescanVols[rescans[i].VolID] = struct{}{}
			}
		}
	}

	// Now advance cursors for volumes whose rescans succeeded.
	var postCursors []storage.Mutation
	for volID := range rescanVols {
		if _, failed := failedRescanVols[volID]; failed {
			continue
		}
		if cursor := batch.Cursors[volID]; cursor > 0 {
			postCursors = append(postCursors, storage.Mutation{
				Op:       storage.OpCursorUpdate,
				VolID:    volID,
				Cursor:   cursor,
				RootPath: s.volRoots[volID],
			})
		}
	}
	if len(postCursors) > 0 {
		if err := s.engine.ApplyBatch(postCursors); err != nil {
			s.setError(err)
		}
	}
}

// collectSubtreeDescendantKeys returns the NodeKeys of all live descendants under (volID, rootDirKey)
// so folding/excluding a previously visible directory tombstones its entire indexed subtree.
func (s *StorageEventSink) collectSubtreeDescendantKeys(volID uint16, rootDirKey uint64) []uint64 {
	snap := s.engine.AcquireSnapshot()
	defer snap.Release()

	// Locate rootDirNodeID across MemTable and segments.
	var rootNodeID uint32
	foundRoot := false
	if mem := snap.MemTable(); mem != nil {
		if slot, ok := mem.GetByKey(volID, rootDirKey); ok {
			rootNodeID = mem.NodeIDAt(slot)
			foundRoot = true
		}
	}
	if !foundRoot {
		segs := snap.Segments()
		for i := len(segs) - 1; i >= 0; i-- {
			if nid, _, ok := segs[i].LookupByKey(volID, rootDirKey); ok {
				rootNodeID = nid
				foundRoot = true
				break
			}
		}
	}
	if !foundRoot {
		return nil
	}

	// Build parent -> child NodeID map for nodes on this volume, then BFS from rootNodeID.
	type liveRef struct {
		nodeID    uint32
		parentIdx uint32
	}
	children := make(map[uint32][]uint32, 64)
	snap.ForEachLive(func(nodeID uint32, node storage.HotNode, _ []byte) bool {
		if node.VolID == volID && nodeID != rootNodeID && node.ParentIdx != storage.RootParentIdx && node.ParentIdx != storage.OrphanParentIdx {
			children[node.ParentIdx] = append(children[node.ParentIdx], nodeID)
		}
		return true
	})

	if len(children[rootNodeID]) == 0 {
		return nil
	}

	visited := map[uint32]struct{}{rootNodeID: {}}
	queue := append([]uint32(nil), children[rootNodeID]...)
	descIDs := make(map[uint32]struct{}, len(queue))
	for head := 0; head < len(queue); head++ {
		id := queue[head]
		if _, seen := visited[id]; seen {
			continue
		}
		visited[id] = struct{}{}
		descIDs[id] = struct{}{}
		queue = append(queue, children[id]...)
	}

	// Resolve NodeID -> NodeKey for each descendant.
	var keys []uint64
	if mem := snap.MemTable(); mem != nil {
		mem.ForEachLive(func(nodeID uint32, node storage.HotNode, _ []byte) bool {
			if _, ok := descIDs[nodeID]; ok {
				if slot, okSlot := mem.GetByNodeID(nodeID); okSlot {
					keys = append(keys, mem.KeyAt(slot))
					delete(descIDs, nodeID)
				}
			}
			return len(descIDs) > 0
		})
	}
	if len(descIDs) > 0 {
		// Scan remaining segment keys via LookupNodeID is O(1), and we only need key when in segments.
		segs := snap.Segments()
		for i := len(segs) - 1; i >= 0 && len(descIDs) > 0; i-- {
			segs[i].ForEachKey(func(v uint16, nodeKey uint64, nodeID uint32) bool {
				if v == volID {
					if _, ok := descIDs[nodeID]; ok {
						keys = append(keys, nodeKey)
						delete(descIDs, nodeID)
					}
				}
				return len(descIDs) > 0
			})
		}
	}
	return keys
}

func (s *StorageEventSink) lookupExistingSize(volID uint16, nodeKey uint64) (int64, bool) {
	snap := s.engine.AcquireSnapshot()
	defer snap.Release()

	var nodeID uint32
	found := false
	if mem := snap.MemTable(); mem != nil {
		if slot, ok := mem.GetByKey(volID, nodeKey); ok {
			nodeID = mem.NodeIDAt(slot)
			found = true
		}
	}
	if !found {
		segs := snap.Segments()
		for i := len(segs) - 1; i >= 0; i-- {
			if nid, _, ok := segs[i].LookupByKey(volID, nodeKey); ok {
				nodeID = nid
				found = true
				break
			}
		}
	}
	if !found {
		return 0, false
	}
	cm, err := snap.ReadColdMeta(nodeID)
	if err != nil {
		return 0, false
	}
	return cm.Size, true
}

func (s *StorageEventSink) fileEventToMutation(ev FileEvent) (storage.Mutation, bool) {
	flags := ev.Flags
	if ev.IsDir {
		flags |= storage.FlagDir
	}
	if storage.HasNonASCII(ev.NewName) {
		flags |= storage.FlagNonASCII
	}

	size := ev.Size
	if size == SizeUnknown {
		if existing, ok := s.lookupExistingSize(ev.VolID, ev.NodeKey); ok {
			size = existing
		} else {
			size = 0
		}
	}

	switch ev.Type {
	case EventCreate:
		return storage.Mutation{
			Op:        storage.OpCreate,
			VolID:     ev.VolID,
			NodeKey:   ev.NodeKey,
			ParentKey: ev.ParentKey,
			Name:      ev.NewName,
			Flags:     flags,
			Size:      size,
			ModTime:   ev.ModTime,
		}, true

	case EventDelete:
		return storage.Mutation{
			Op:      storage.OpDelete,
			VolID:   ev.VolID,
			NodeKey: ev.NodeKey,
		}, true

	case EventModifyMeta:
		return storage.Mutation{
			Op:      storage.OpModifyMeta,
			VolID:   ev.VolID,
			NodeKey: ev.NodeKey,
			Size:    size,
			ModTime: ev.ModTime,
		}, true

	case EventRename:
		sameParent := ev.NewParentKey == 0 || ev.NewParentKey == ev.ParentKey
		if sameParent && ev.NewName == "" && !ev.ReplaceFlags {
			// No-op round-trip move (e.g. A -> B -> A within 150ms without renaming).
			return storage.Mutation{}, false
		}
		// Never emit OpRename with empty Name (which would erase the node's name);
		// nameless directory cross-dir moves always use O(1) OpMoveDir.
		if ev.IsDir && !sameParent && (ev.NewName == "" || (ev.OldPath != "" && path.Base(ev.OldPath) == ev.NewName && !ev.ReplaceFlags)) {
			return storage.Mutation{
				Op:           storage.OpMoveDir,
				VolID:        ev.VolID,
				NodeKey:      ev.NodeKey,
				ParentKey:    ev.ParentKey,
				NewParentKey: ev.NewParentKey,
			}, true
		}
		if ev.NewName == "" {
			return storage.Mutation{}, false
		}
		return storage.Mutation{
			Op:           storage.OpRename,
			VolID:        ev.VolID,
			NodeKey:      ev.NodeKey,
			ParentKey:    ev.ParentKey,
			NewParentKey: ev.NewParentKey,
			Name:         ev.NewName,
			Flags:        flags,
			ReplaceFlags: ev.ReplaceFlags,
			Size:         size,
			ModTime:      ev.ModTime,
		}, true

	default:
		return storage.Mutation{}, false
	}
}
