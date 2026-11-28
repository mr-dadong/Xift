package l2search

import (
	"os"
	"slices"
)

const (
	// mergeSegmentThreshold triggers a merge above this many segments.
	mergeSegmentThreshold = 8
	// mergeDeadRatio triggers a merge when this share of documents is dead.
	mergeDeadRatio = 0.5
)

// NeedsMerge reports whether segment count or dead-document ratio crossed the
// compaction thresholds.
func (s *Store) NeedsMerge() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.segs) > mergeSegmentThreshold {
		return true
	}
	total, dead := 0, 0
	for _, seg := range s.segs {
		total += int(seg.meta.Docs)
		dead += len(seg.dead) / 4
	}
	return total > 0 && float64(dead)/float64(total) > mergeDeadRatio
}

// DocPos identifies one document inside the L2 store.
type DocPos struct {
	SegID uint32
	Local uint32
}

// Merge rewrites all segments into one clean segment holding the newest live
// document per node, then removes the old segment directories. Callers that
// track per-path document references must use MergeWithLocations.
func (s *Store) Merge() error {
	_, err := s.merge()
	return err
}

// MergeWithLocations is Merge plus the node -> new document position map.
func (s *Store) MergeWithLocations() (map[uint32]DocPos, error) {
	return s.merge()
}

// DocNodeID returns the node ID stored at (segID, local).
func (s *Store) DocNodeID(segID, local uint32) (uint32, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	seg := s.segmentByIDLocked(segID)
	if seg == nil {
		return 0, false
	}
	rec, ok := docForSeg(seg, local)
	if !ok {
		return 0, false
	}
	return rec.NodeID, true
}

func (s *Store) merge() (map[uint32]DocPos, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.segs) <= 1 {
		return nil, nil
	}

	newest := make(map[uint32]Doc)
	gens := make(map[uint32]uint32)
	for _, seg := range s.segs {
		for local := uint32(0); local < seg.meta.Docs; local++ {
			if seg.isDead(local) {
				continue
			}
			rec, ok := docForSeg(seg, local)
			if !ok {
				continue
			}
			if gen, seen := gens[rec.NodeID]; seen && gen > rec.Gen {
				continue
			}
			text, ok := contentForSeg(seg, local)
			if !ok {
				continue
			}
			newest[rec.NodeID] = Doc{NodeID: rec.NodeID, MTime: rec.MTime, Text: text}
			gens[rec.NodeID] = rec.Gen
		}
	}

	ids := make([]uint32, 0, len(newest))
	for id := range newest {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	docs := make([]Doc, 0, len(ids))
	for _, id := range ids {
		docs = append(docs, newest[id])
	}

	old := s.segs
	oldMetas := s.mf.Segments
	s.segs = nil
	s.mf.Segments = nil
	if err := s.addSegmentLocked(docs); err != nil {
		s.segs = old
		s.mf.Segments = oldMetas
		return nil, err
	}
	newSegID := s.segs[len(s.segs)-1].meta.ID
	locations := make(map[uint32]DocPos, len(docs))
	for i := range docs {
		locations[docs[i].NodeID] = DocPos{SegID: newSegID, Local: uint32(i)}
	}

	for _, seg := range old {
		if seg.docs != nil {
			_ = seg.docs.Close()
		}
		if seg.content != nil {
			_ = seg.content.Close()
		}
		if seg.tri != nil {
			seg.tri.Close()
		}
		_ = os.RemoveAll(seg.dir)
	}
	return locations, nil
}

func docForSeg(seg *storeSegment, local uint32) (DocRecord, bool) {
	if local >= seg.meta.Docs {
		return DocRecord{}, false
	}
	var rec [docRecordSize]byte
	if _, err := seg.docs.ReadAt(rec[:], int64(local)*docRecordSize); err != nil {
		return DocRecord{}, false
	}
	return DocRecord{
		NodeID: uint32le(rec[0:4]),
		Gen:    uint32le(rec[4:8]),
		MTime:  int64(uint64le(rec[8:16])),
		Off:    uint64le(rec[16:24]),
	}, true
}

func contentForSeg(seg *storeSegment, local uint32) ([]byte, bool) {
	if seg.content == nil || local >= seg.meta.Docs {
		return nil, false
	}
	rec, ok := docForSeg(seg, local)
	if !ok {
		return nil, false
	}
	next := uint64(0)
	if local+1 < seg.meta.Docs {
		nrec, ok := docForSeg(seg, local+1)
		if !ok {
			return nil, false
		}
		next = nrec.Off
	} else {
		info, err := seg.content.Stat()
		if err != nil {
			return nil, false
		}
		next = uint64(info.Size())
	}
	if next < rec.Off || next-rec.Off > 1<<24 {
		return nil, false
	}
	buf := make([]byte, next-rec.Off)
	if _, err := seg.content.ReadAt(buf, int64(rec.Off)); err != nil {
		return nil, false
	}
	return buf, true
}

func uint32le(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}

func uint64le(b []byte) uint64 {
	return uint64(uint32le(b)) | uint64(uint32le(b[4:]))<<32
}
