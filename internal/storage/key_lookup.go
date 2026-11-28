package storage

// NodeKeyRef identifies one node by its volume and filesystem key.
type NodeKeyRef struct {
	VolID   uint16
	NodeKey uint64
}

// LookupLiveNodeIDs resolves filesystem keys to live NodeIDs inside one
// point-in-time snapshot. Unresolvable refs (deleted or never indexed) are
// skipped, so the result may be shorter than refs.
func (e *Engine) LookupLiveNodeIDs(refs []NodeKeyRef) []uint32 {
	if e == nil || len(refs) == 0 {
		return nil
	}
	snap := e.AcquireSnapshot()
	defer snap.Release()
	if snap.closed {
		return nil
	}
	out := make([]uint32, 0, len(refs))
	for _, ref := range refs {
		if id, ok := snap.LookupLiveNodeID(ref.VolID, ref.NodeKey); ok {
			out = append(out, id)
		}
	}
	return out
}

// LookupLiveNodeID resolves (volID, nodeKey) against this snapshot.
// Mounted read-only snapshot segments are excluded: their key indexes carry
// package-local placeholders, not filesystem keys.
func (s *Snapshot) LookupLiveNodeID(volID uint16, nodeKey uint64) (uint32, bool) {
	if s == nil || s.closed {
		return 0, false
	}
	if s.mem != nil {
		if slot, ok := s.mem.GetByKey(volID, nodeKey); ok {
			return s.mem.NodeIDAt(slot), true
		}
	}
	for i := len(s.segs) - 1; i >= 0; i-- {
		if i < s.l1Segs {
			ss := s.segs[i]
			nodeID, local, ok := ss.seg.LookupByKey(volID, nodeKey)
			if !ok {
				continue
			}
			if isBitSet(ss.delBits, ss.seg.meta.NodeCount, local) {
				continue
			}
			return nodeID, true
		}
	}
	return 0, false
}

// ForEachKey iterates live (volID, nodeKey, nodeID) entries in this snapshot.
// Mounted read-only snapshot segments are skipped: their keys are package-local
// placeholders, not filesystem keys.
func (s *Snapshot) ForEachKey(fn func(volID uint16, nodeKey uint64, nodeID uint32) bool) {
	if s == nil || s.closed {
		return
	}
	for i := 0; i < s.l1Segs; i++ {
		ss := s.segs[i]
		stopped := false
		ss.seg.ForEachKey(func(volID uint16, nodeKey uint64, nodeID uint32) bool {
			if !fn(volID, nodeKey, nodeID) {
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
		s.mem.ForEachKey(fn)
	}
}
