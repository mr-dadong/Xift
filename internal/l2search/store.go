package l2search

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/mr-dadong/Xift/internal/storage/lsm"
	"github.com/mr-dadong/Xift/internal/textkey"
)

const (
	manifestVersion = 1
	docRecordSize   = 24
)

// Doc is one file's indexable content.
type Doc struct {
	NodeID uint32
	MTime  int64
	Text   []byte
}

// DocRecord is the persisted header of one indexed document.
type DocRecord struct {
	NodeID uint32
	Gen    uint32
	MTime  int64
	Off    uint64
	Len    uint32
}

type segmentMeta struct {
	ID           uint32 `json:"id"`
	Docs         uint32 `json:"docs"`
	TrigramBytes int64  `json:"trigram_bytes"`
	DiskBytes    int64  `json:"disk_bytes"`
}

type manifest struct {
	Version       int           `json:"version"`
	NextSegmentID uint32        `json:"next_segment_id"`
	Segments      []segmentMeta `json:"segments"`
	Paused        bool          `json:"paused"`
}

type storeSegment struct {
	meta    segmentMeta
	dir     string
	docs    *os.File
	content *os.File
	dead    []byte // sorted local indexes of superseded documents
	tri     *lsm.InvertedIndex
}

// Store is the append-only L2 content index: each flush writes one immutable
// segment (docs.col + trigram.idx) and the MANIFEST records what exists.
type Store struct {
	dir   string
	cache *lsm.BlockCache

	mu      sync.Mutex
	mf      manifest
	segs    []*storeSegment
	diskCap int64
}

// ErrDiskCap reports that the L2 index reached its configured disk budget and
// new documents are paused until segments are merged or the cap is raised.
var ErrDiskCap = fmt.Errorf("l2search: content index disk cap reached")

// OpenStore opens or creates an L2 store rooted at dir.
func OpenStore(dir string, cache *lsm.BlockCache) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{dir: dir, cache: cache, diskCap: DefaultDiskCapBytes}
	raw, err := os.ReadFile(filepath.Join(dir, "MANIFEST"))
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		s.mf = manifest{Version: manifestVersion, NextSegmentID: 1}
		return s, nil
	}
	if err := json.Unmarshal(raw, &s.mf); err != nil {
		return nil, fmt.Errorf("l2search: corrupt MANIFEST: %w", err)
	}
	if s.mf.Version != manifestVersion {
		return nil, fmt.Errorf("l2search: unsupported MANIFEST version %d", s.mf.Version)
	}
	for _, m := range s.mf.Segments {
		seg, err := openStoreSegment(filepath.Join(dir, fmt.Sprintf("seg_%06d", m.ID)), m)
		if err != nil {
			s.Close()
			return nil, err
		}
		s.segs = append(s.segs, seg)
	}
	return s, nil
}

func openStoreSegment(dir string, m segmentMeta) (*storeSegment, error) {
	f, err := os.Open(filepath.Join(dir, "docs.col"))
	if err != nil {
		return nil, fmt.Errorf("l2search: open %s/docs.col: %w", dir, err)
	}
	seg := &storeSegment{meta: m, dir: dir, docs: f}
	if cf, err := os.Open(filepath.Join(dir, "content.col")); err == nil {
		seg.content = cf
	}
	if raw, err := os.ReadFile(filepath.Join(dir, "dead.bit")); err == nil {
		seg.dead = raw
	}
	return seg, nil
}

// DefaultDiskCapBytes bounds the content index's total on-disk size.
const DefaultDiskCapBytes int64 = 4 << 30

// DefaultCacheBytes is the L2 block cache budget, independent of L1's cache.
const DefaultCacheBytes int64 = 16 << 20

// Dir returns the on-disk directory holding the content index.
func (s *Store) Dir() string { return s.dir }

// SetDiskCap overrides the disk budget (0 disables the cap).
func (s *Store) SetDiskCap(bytes int64) {
	s.mu.Lock()
	s.diskCap = bytes
	s.mu.Unlock()
}

// AddSegment writes one immutable segment holding docs and updates the MANIFEST.
func (s *Store) AddSegment(docs []Doc) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.addSegmentLocked(docs)
}

// AddSegmentID is AddSegment returning the id of the newly written segment.
func (s *Store) AddSegmentID(docs []Doc) (uint32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(docs) == 0 {
		return 0, nil
	}
	id := s.mf.NextSegmentID
	if err := s.addSegmentLocked(docs); err != nil {
		return 0, err
	}
	if s.mf.NextSegmentID == id {
		return 0, nil
	}
	return id, nil
}

func (s *Store) addSegmentLocked(docs []Doc) error {
	if len(docs) == 0 {
		return nil
	}
	if s.diskCap > 0 && s.diskBytesLocked()+estimateSegmentBytes(docs) > s.diskCap {
		if !s.mf.Paused {
			s.mf.Paused = true
			return errors.Join(ErrDiskCap, s.saveManifestLocked())
		}
		return ErrDiskCap
	}
	id := s.mf.NextSegmentID
	s.mf.NextSegmentID++
	segDir := filepath.Join(s.dir, fmt.Sprintf("seg_%06d", id))
	if err := os.MkdirAll(segDir, 0o755); err != nil {
		return err
	}

	docsPath := filepath.Join(segDir, "docs.col")
	f, err := os.Create(docsPath)
	if err != nil {
		return err
	}
	cf, err := os.Create(filepath.Join(segDir, "content.col"))
	if err != nil {
		f.Close()
		return err
	}
	var rec [docRecordSize]byte
	var off uint64
	for i := range docs {
		binary.LittleEndian.PutUint32(rec[0:4], docs[i].NodeID)
		binary.LittleEndian.PutUint32(rec[4:8], id)
		binary.LittleEndian.PutUint64(rec[8:16], uint64(docs[i].MTime))
		binary.LittleEndian.PutUint64(rec[16:24], off)
		if _, err := f.Write(rec[:]); err != nil {
			f.Close()
			cf.Close()
			return err
		}
		if _, err := cf.Write(docs[i].Text); err != nil {
			f.Close()
			cf.Close()
			return err
		}
		off += uint64(len(docs[i].Text))
	}
	for _, fh := range []*os.File{f, cf} {
		if err := fh.Sync(); err != nil {
			fh.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		cf.Close()
		return err
	}
	if err := cf.Close(); err != nil {
		return err
	}

	b := lsm.NewInvertedBuilder()
	for i := range docs {
		keys := textkey.AppendTrigramKeys(docs[i].Text, nil)
		if err := b.AddName(keys, uint32(i)); err != nil {
			return err
		}
	}
	trigramPath := filepath.Join(segDir, "trigram.idx")
	stats, err := b.Finish(trigramPath, math.MaxInt32)
	if err != nil {
		return err
	}

	meta := segmentMeta{ID: id, Docs: uint32(len(docs)), TrigramBytes: stats.PostingsBytes}
	meta.DiskBytes = dirFileBytes(segDir)
	seg, err := openStoreSegment(segDir, meta)
	if err != nil {
		return err
	}
	s.segs = append(s.segs, seg)
	s.mf.Segments = append(s.mf.Segments, meta)
	s.mf.Paused = false
	return s.saveManifestLocked()
}

func estimateSegmentBytes(docs []Doc) int64 {
	var n int64
	for i := range docs {
		n += int64(len(docs[i].Text))
	}
	return n*3 + int64(len(docs))*docRecordSize
}

func (s *Store) diskBytesLocked() int64 {
	var total int64
	for _, seg := range s.segs {
		total += seg.meta.DiskBytes
	}
	return total
}

// Doc resolves a document record by segment index and local index.
func (s *Store) Doc(segIdx int, local uint32) (DocRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if segIdx < 0 || segIdx >= len(s.segs) {
		return DocRecord{}, false
	}
	seg := s.segs[segIdx]
	if local >= seg.meta.Docs {
		return DocRecord{}, false
	}
	var rec [docRecordSize]byte
	if _, err := seg.docs.ReadAt(rec[:], int64(local)*docRecordSize); err != nil {
		return DocRecord{}, false
	}
	off := binary.LittleEndian.Uint64(rec[16:24])
	next := uint64(0)
	if local+1 < seg.meta.Docs {
		var nrec [docRecordSize]byte
		if _, err := seg.docs.ReadAt(nrec[:], int64(local+1)*docRecordSize); err == nil {
			next = binary.LittleEndian.Uint64(nrec[16:24])
		}
	} else if info, err := seg.content.Stat(); err == nil {
		next = uint64(info.Size())
	}
	return DocRecord{
		NodeID: binary.LittleEndian.Uint32(rec[0:4]),
		Gen:    binary.LittleEndian.Uint32(rec[4:8]),
		MTime:  int64(binary.LittleEndian.Uint64(rec[8:16])),
		Off:    off,
		Len:    uint32(next - off),
	}, true
}

// SegmentCount returns the number of live segments.
func (s *Store) SegmentCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.segs)
}

// DocCount returns the number of indexed documents across segments.
func (s *Store) DocCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, seg := range s.segs {
		n += int(seg.meta.Docs)
	}
	return n
}

// DiskBytes returns the tracked on-disk size of all segments.
func (s *Store) DiskBytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.diskBytesLocked()
}

// Paused reports whether new documents are rejected because the disk cap was hit.
func (s *Store) Paused() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mf.Paused
}

// Close releases segment file handles.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, seg := range s.segs {
		if seg.docs != nil {
			_ = seg.docs.Close()
			seg.docs = nil
		}
		if seg.content != nil {
			_ = seg.content.Close()
			seg.content = nil
		}
		if seg.tri != nil {
			seg.tri.Close()
			seg.tri = nil
		}
	}
	return nil
}

func (s *Store) saveManifestLocked() error {
	raw, err := json.MarshalIndent(s.mf, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(s.dir, ".xift-l2-manifest.tmp")
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(s.dir, "MANIFEST")); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func dirFileBytes(dir string) int64 {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	var total int64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if info, err := e.Info(); err == nil {
			total += info.Size()
		}
	}
	return total
}
