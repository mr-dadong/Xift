package storage

// SegmentSnapshot is a read-only, MVCC point-in-time view of a mounted Segment
// paired with its snapshot-isolated tombstone bitmap. It is safe for lock-free
// multi-core sharded scanning while concurrent writers mutate or compact the Engine.
type SegmentSnapshot struct {
	seg       *Segment
	delBits   []uint64
	liveCount uint32
}

// SegmentSnapshotAt returns the MVCC point-in-time view of segment idx.
func (s *Snapshot) SegmentSnapshotAt(idx int) SegmentSnapshot {
	if s == nil || idx < 0 || idx >= len(s.segs) {
		return SegmentSnapshot{}
	}
	ss := s.segs[idx]
	return SegmentSnapshot{
		seg:       ss.seg,
		delBits:   ss.delBits,
		liveCount: ss.liveCount,
	}
}

// Generation returns the monotonically increasing Engine mutation generation
// captured when this Snapshot was acquired. Two snapshots with equal Generation
// observe identical storage state and layout.
func (s *Snapshot) Generation() uint64 {
	if s == nil {
		return 0
	}
	return s.gen
}

// Count returns total node slots in the segment (including logically deleted slots).
func (ss SegmentSnapshot) Count() uint32 {
	if ss.seg == nil {
		return 0
	}
	return ss.seg.meta.NodeCount
}

// LiveCount returns the number of live (non-deleted) nodes visible in this snapshot.
func (ss SegmentSnapshot) LiveCount() uint32 {
	return ss.liveCount
}

// HotNodes returns the read-only mmap-backed HotNode slice.
func (ss SegmentSnapshot) HotNodes() []HotNode {
	if ss.seg == nil {
		return nil
	}
	return ss.seg.nodes
}

// NamesBytes returns the read-only mmap-backed names.col byte pool.
func (ss SegmentSnapshot) NamesBytes() []byte {
	if ss.seg == nil {
		return nil
	}
	return ss.seg.names
}

// NodeIDAt returns the global NodeID for localIdx.
func (ss SegmentSnapshot) NodeIDAt(localIdx uint32) uint32 {
	if ss.seg == nil {
		return 0
	}
	return ss.seg.NodeIDAt(localIdx)
}

// NameBytesAt returns the zero-copy filename byte slice for localIdx.
func (ss SegmentSnapshot) NameBytesAt(localIdx uint32) []byte {
	if ss.seg == nil {
		return nil
	}
	return ss.seg.NameBytesAt(localIdx)
}

// IsDeleted reports whether localIdx is deleted in this point-in-time snapshot.
func (ss SegmentSnapshot) IsDeleted(localIdx uint32) bool {
	if ss.seg == nil {
		return true
	}
	return isBitSet(ss.delBits, ss.seg.meta.NodeCount, localIdx)
}
