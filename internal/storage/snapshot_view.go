package storage

import "github.com/mr-dadong/Xift/internal/storage/lsm"

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

// IsTier2 reports whether this segment uses the Tier 2 (inverted sidecar) layout.
func (ss SegmentSnapshot) IsTier2() bool {
	return ss.seg != nil && ss.seg.tier2
}

// SegmentID returns the committed segment ID.
func (ss SegmentSnapshot) SegmentID() uint32 {
	if ss.seg == nil {
		return 0
	}
	return ss.seg.meta.ID
}

// NodeAt returns the HotNode at localIdx. Tier 2 segments read through the
// shared block cache instead of exposing a whole-column slice.
func (ss SegmentSnapshot) NodeAt(localIdx uint32) HotNode {
	if ss.seg == nil {
		return HotNode{}
	}
	return ss.seg.NodeAt(localIdx)
}

// Tier2Indexes returns the segment's trigram and bigram inverted indexes.
// Both are nil for Tier 1 segments.
func (ss SegmentSnapshot) Tier2Indexes() (tri, bi *lsm.InvertedIndex) {
	if ss.seg == nil {
		return nil, nil
	}
	return ss.seg.triIdx, ss.seg.biIdx
}
