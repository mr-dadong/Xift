package l2search

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/mr-dadong/Xift/internal/storage/lsm"
	"github.com/mr-dadong/Xift/internal/textkey"
)

// MarkDead records documents that no longer represent their file: superseded
// versions and deletions. The set is per segment, sorted and persisted.
func (s *Store) MarkDead(segID uint32, locals []uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	seg := s.segmentByIDLocked(segID)
	if seg == nil || len(locals) == 0 {
		return nil
	}
	merged := append(slices.Clone(seg.deadLocals()), locals...)
	slices.Sort(merged)
	merged = slices.Compact(merged)
	raw := make([]byte, len(merged)*4)
	for i, v := range merged {
		binary.LittleEndian.PutUint32(raw[i*4:], v)
	}
	if err := os.WriteFile(filepath.Join(seg.dir, "dead.bit"), raw, 0o644); err != nil {
		return err
	}
	seg.dead = raw
	seg.meta.DiskBytes = dirFileBytes(seg.dir)
	for i := range s.mf.Segments {
		if s.mf.Segments[i].ID == segID {
			s.mf.Segments[i].DiskBytes = seg.meta.DiskBytes
		}
	}
	return s.saveManifestLocked()
}

func (s *Store) segmentByIDLocked(id uint32) *storeSegment {
	for _, seg := range s.segs {
		if seg.meta.ID == id {
			return seg
		}
	}
	return nil
}

// deadLocals decodes the dead set. Callers hold s.mu.
func (seg *storeSegment) deadLocals() []uint32 {
	n := len(seg.dead) / 4
	out := make([]uint32, n)
	for i := 0; i < n; i++ {
		out[i] = binary.LittleEndian.Uint32(seg.dead[i*4:])
	}
	return out
}

func (seg *storeSegment) isDead(local uint32) bool {
	n := len(seg.dead) / 4
	lo, hi := 0, n
	for lo < hi {
		mid := (lo + hi) / 2
		cur := binary.LittleEndian.Uint32(seg.dead[mid*4:])
		switch {
		case cur < local:
			lo = mid + 1
		case cur > local:
			hi = mid
		default:
			return true
		}
	}
	return false
}

// Content returns the stored text of one document.
func (s *Store) Content(segIdx int, local uint32) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if segIdx < 0 || segIdx >= len(s.segs) {
		return nil, false
	}
	seg := s.segs[segIdx]
	if local >= seg.meta.Docs || seg.content == nil {
		return nil, false
	}
	var rec [docRecordSize]byte
	if _, err := seg.docs.ReadAt(rec[:], int64(local)*docRecordSize); err != nil {
		return nil, false
	}
	off := binary.LittleEndian.Uint64(rec[16:24])
	next := uint64(0)
	if local+1 < seg.meta.Docs {
		var nrec [docRecordSize]byte
		if _, err := seg.docs.ReadAt(nrec[:], int64(local+1)*docRecordSize); err != nil {
			return nil, false
		}
		next = binary.LittleEndian.Uint64(nrec[16:24])
	} else {
		info, err := seg.content.Stat()
		if err != nil {
			return nil, false
		}
		next = uint64(info.Size())
	}
	if next < off || next-off > 1<<24 {
		return nil, false
	}
	buf := make([]byte, next-off)
	if _, err := seg.content.ReadAt(buf, int64(off)); err != nil {
		return nil, false
	}
	return buf, true
}

// Search returns the live node IDs whose content contains term (exact
// substring, case-folded ASCII). truncated reports that the candidate budget
// stopped the scan early.
func (s *Store) Search(term []byte, budget int) ([]uint32, bool, error) {
	if len(term) == 0 {
		return nil, false, nil
	}
	needle := foldASCII(append([]byte(nil), term...))
	grams := textkey.AppendTrigramKeys(needle, nil)
	slices.Sort(grams)
	grams = slices.Compact(grams)

	s.mu.Lock()
	defer s.mu.Unlock()

	newest := make(map[uint32]uint32) // nodeID -> gen
	matched := make(map[uint32]struct{})
	verified := 0
	for si := range s.segs {
		seg := s.segs[si]
		tri, err := s.segmentIndexLocked(seg)
		if err != nil {
			return nil, false, err
		}
		if tri == nil {
			continue
		}
		views, ok, err := loadSegmentPostings(tri, seg.meta.ID, grams)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			continue
		}
		var stop bool
		lsm.IntersectAll(views, func(local uint32) bool {
			if seg.isDead(local) {
				return true
			}
			verified++
			content, ok := s.contentLocked(si, local)
			if !ok || !bytes.Contains(content, needle) {
				return budget <= 0 || verified < budget
			}
			rec, ok := s.docLocked(si, local)
			if !ok {
				return true
			}
			if prev, seen := newest[rec.NodeID]; !seen || rec.Gen >= prev {
				newest[rec.NodeID] = rec.Gen
				matched[rec.NodeID] = struct{}{}
			}
			if budget > 0 && verified >= budget {
				stop = true
				return false
			}
			return true
		})
		if stop {
			return sortedNodeIDs(matched), true, nil
		}
	}
	return sortedNodeIDs(matched), false, nil
}

func (s *Store) contentLocked(segIdx int, local uint32) ([]byte, bool) {
	seg := s.segs[segIdx]
	if seg.content == nil {
		return nil, false
	}
	var rec [docRecordSize]byte
	if _, err := seg.docs.ReadAt(rec[:], int64(local)*docRecordSize); err != nil {
		return nil, false
	}
	off := binary.LittleEndian.Uint64(rec[16:24])
	next := uint64(0)
	if local+1 < seg.meta.Docs {
		var nrec [docRecordSize]byte
		if _, err := seg.docs.ReadAt(nrec[:], int64(local+1)*docRecordSize); err != nil {
			return nil, false
		}
		next = binary.LittleEndian.Uint64(nrec[16:24])
	} else {
		info, err := seg.content.Stat()
		if err != nil {
			return nil, false
		}
		next = uint64(info.Size())
	}
	if next < off || next-off > 1<<24 {
		return nil, false
	}
	buf := make([]byte, next-off)
	if _, err := seg.content.ReadAt(buf, int64(off)); err != nil {
		return nil, false
	}
	return buf, true
}

func (s *Store) docLocked(segIdx int, local uint32) (DocRecord, bool) {
	seg := s.segs[segIdx]
	if local >= seg.meta.Docs {
		return DocRecord{}, false
	}
	var rec [docRecordSize]byte
	if _, err := seg.docs.ReadAt(rec[:], int64(local)*docRecordSize); err != nil {
		return DocRecord{}, false
	}
	return DocRecord{
		NodeID: binary.LittleEndian.Uint32(rec[0:4]),
		Gen:    binary.LittleEndian.Uint32(rec[4:8]),
		MTime:  int64(binary.LittleEndian.Uint64(rec[8:16])),
	}, true
}

// segmentIndexLocked opens a segment's trigram index on first use.
func (s *Store) segmentIndexLocked(seg *storeSegment) (*lsm.InvertedIndex, error) {
	if seg.tri != nil {
		return seg.tri, nil
	}
	path := filepath.Join(seg.dir, "trigram.idx")
	if _, err := os.Stat(path); err != nil {
		return nil, nil
	}
	tri, err := lsm.OpenInvertedIndex(path, seg.meta.ID, s.cache)
	if err != nil {
		return nil, fmt.Errorf("l2search: open %s: %w", path, err)
	}
	seg.tri = tri
	return tri, nil
}

func loadSegmentPostings(tri *lsm.InvertedIndex, segID uint32, grams []uint64) ([]*lsm.RoaringView, bool, error) {
	views := make([]*lsm.RoaringView, 0, len(grams))
	for _, g := range grams {
		raw, ok, err := tri.LookupPosting(segID, 0, g)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, nil
		}
		v, err := lsm.ParseRoaring(raw)
		if err != nil {
			return nil, false, err
		}
		views = append(views, v)
	}
	return views, true, nil
}

func sortedNodeIDs(set map[uint32]struct{}) []uint32 {
	out := make([]uint32, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	slices.Sort(out)
	return out
}

func foldASCII(b []byte) []byte {
	for i := range b {
		if b[i] >= 'A' && b[i] <= 'Z' {
			b[i] += 'a' - 'A'
		}
	}
	return b
}
