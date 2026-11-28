package storage

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/mr-dadong/Xift/internal/storage/lsm"
	"github.com/mr-dadong/Xift/internal/textkey"
)

func tier2Corpus() []struct {
	name   string
	isDir  bool
	size   int64
	mtime  int64
	nodeID uint32 // for delta build
} {
	return []struct {
		name   string
		isDir  bool
		size   int64
		mtime  int64
		nodeID uint32
	}{
		{"src", true, 0, 100, 10},
		{"main.go", false, 1234, 200, 11},
		{"设计文档.md", false, 42, 300, 12},
		{"重设计.txt", false, 7, 400, 13},
		{"README.md", false, 900, 500, 14},
		{"a", false, 1, 600, 15},
		{"img_0001.png", false, 5, 700, 16},
	}
}

func buildTier2Segment(t *testing.T, dir string, dense bool) (*Segment, *lsm.BlockCache, SegmentMeta) {
	t.Helper()
	segDir := filepath.Join(dir, "seg_test")
	extPool := NewExtPool()
	b, err := NewTier2SegmentBuilder(segDir, 1, 0, dense, extPool)
	if err != nil {
		t.Fatal(err)
	}
	corpus := tier2Corpus()
	for i, c := range corpus {
		flags := uint16(0)
		if c.isDir {
			flags |= FlagDir
		}
		var nodeID uint32
		if dense {
			nodeID, err = b.AppendDense(1, uint64(1000+i), 0, c.name, flags, 0, c.size, c.mtime)
			if err != nil {
				b.Abort()
				t.Fatal(err)
			}
		} else {
			nodeID = c.nodeID
			if err := b.Append(nodeID, 1, uint64(1000+i), 0, c.name, flags, 0, c.size, c.mtime); err != nil {
				b.Abort()
				t.Fatal(err)
			}
		}
		_ = nodeID
	}
	meta, err := b.Finish()
	if err != nil {
		b.Abort()
		t.Fatal(err)
	}
	if !meta.Tier2 {
		t.Fatal("Finish must mark the segment Tier2")
	}

	cache := lsm.NewBlockCache(1 << 20)
	seg, err := OpenSegmentWithCache(segDir, 0, cache)
	if err != nil {
		cache.Close()
		t.Fatal(err)
	}
	return seg, cache, meta
}

func TestSegmentTier2BuildFileLayout(t *testing.T) {
	dir := t.TempDir()
	seg, cache, meta := buildTier2Segment(t, dir, true)
	defer func() { seg.Release(); cache.Close() }()

	wantFiles := []string{
		"hot.col", "sizes.col", "mtimes.col", "keys.idx", "dirs.col",
		"del.bit", "meta.hdr", "names.blk", "names.idx", "trigram.idx", "bigram.idx",
	}
	for _, f := range wantFiles {
		if _, err := os.Stat(filepath.Join(seg.Dir(), f)); err != nil {
			t.Fatalf("missing Tier 2 file %s: %v", f, err)
		}
	}
	if _, err := os.Stat(filepath.Join(seg.Dir(), "names.col")); err == nil {
		t.Fatal("Tier 2 segments must not write names.col")
	}
	if meta.NodeCount != uint32(len(tier2Corpus())) {
		t.Fatalf("node count = %d", meta.NodeCount)
	}
}

func TestSegmentTier2OpenParity(t *testing.T) {
	for _, dense := range []bool{true, false} {
		t.Run(map[bool]string{true: "dense", false: "delta"}[dense], func(t *testing.T) {
			dir := t.TempDir()
			seg, cache, meta := buildTier2Segment(t, dir, dense)
			defer func() { seg.Release(); cache.Close() }()

			corpus := tier2Corpus()
			if !seg.IsTier2() {
				t.Fatal("segment must report Tier 2")
			}
			if seg.HotNodes() != nil || seg.NamesBytes() != nil {
				t.Fatal("Tier 2 segments must not expose whole-column slices")
			}

			for i, c := range corpus {
				localIdx := uint32(i)
				n := seg.NodeAt(localIdx)
				if string(seg.NameBytesAt(localIdx)) != c.name {
					t.Fatalf("NameBytesAt(%d) = %q, want %q", localIdx, seg.NameBytesAt(localIdx), c.name)
				}
				isDir := (n.Flags & FlagDir) != 0
				if isDir != c.isDir {
					t.Fatalf("NodeAt(%d) dir = %v, want %v", localIdx, isDir, c.isDir)
				}
				cm, err := seg.ReadColdMeta(localIdx)
				if err != nil {
					t.Fatal(err)
				}
				if cm.Size != c.size || cm.ModTime != c.mtime {
					t.Fatalf("ReadColdMeta(%d) = %+v, want size=%d mtime=%d", localIdx, cm, c.size, c.mtime)
				}
				var wantID uint32
				if dense {
					wantID = meta.BaseNodeID + localIdx
				} else {
					wantID = c.nodeID
				}
				if got := seg.NodeIDAt(localIdx); got != wantID {
					t.Fatalf("NodeIDAt(%d) = %d, want %d", localIdx, got, wantID)
				}
				live, ok := seg.LookupNodeID(wantID)
				if !ok || live != localIdx {
					t.Fatalf("LookupNodeID(%d) = (%d,%v), want (%d,true)", wantID, live, ok, localIdx)
				}
				_, _, ok = seg.LookupByKey(1, uint64(1000+i))
				if !ok {
					t.Fatalf("LookupByKey(%d) miss", 1000+i)
				}
			}

			// ForEachLive must visit every corpus name exactly once.
			var seen []string
			seg.ForEachLive(func(_ uint32, _ HotNode, name []byte) bool {
				seen = append(seen, string(name))
				return true
			})
			slices.Sort(seen)
			want := make([]string, len(corpus))
			for i, c := range corpus {
				want[i] = c.name
			}
			slices.Sort(want)
			if !slices.Equal(seen, want) {
				t.Fatalf("ForEachLive = %v, want %v", seen, want)
			}

			// Tombstones: delete one, snapshot isolates, lookup hides.
			if seg.MarkDeleted(1) {
				if _, ok := seg.LookupNodeID(seg.NodeIDAt(1)); ok {
					t.Fatal("deleted node must not resolve")
				}
				if seg.LiveCount() != uint32(len(corpus))-1 {
					t.Fatalf("live count after delete = %d", seg.LiveCount())
				}
			} else {
				t.Fatal("MarkDeleted must succeed")
			}
		})
	}
}

func TestSegmentTier2InvertedLookup(t *testing.T) {
	dir := t.TempDir()
	seg, cache, _ := buildTier2Segment(t, dir, true)
	defer func() { seg.Release(); cache.Close() }()

	tri, bi := seg.Tier2Inverted()
	if tri == nil || bi == nil {
		t.Fatal("Tier 2 segment must expose inverted indexes")
	}

	// The trigram of "main.go" must post localIdx 1.
	gram := textkey.AppendTrigramKeys([]byte("main.go"), nil)[0]
	raw, ok, err := tri.LookupPosting(seg.Meta().ID, 0, gram)
	if err != nil || !ok {
		t.Fatalf("trigram lookup: ok=%v err=%v", ok, err)
	}
	v, err := lsm.ParseRoaring(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !v.Contains(1) {
		t.Fatal("trigram posting must contain the name's localIdx")
	}

	// Pinyin trigram of 设计文档.md -> (s,j,w).
	pyGram := textkey.GramNamespacePinyin | uint64('s')<<16 | uint64('j')<<8 | uint64('w')
	raw, ok, err = tri.LookupPosting(seg.Meta().ID, 0, pyGram)
	if err != nil || !ok {
		t.Fatalf("pinyin trigram lookup: ok=%v err=%v", ok, err)
	}
	v, err = lsm.ParseRoaring(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !v.Contains(2) { // 设计文档.md is localIdx 2
		t.Fatal("pinyin trigram posting must contain localIdx 2")
	}

	// Bigram index: "ma" of main.go.
	biGram := textkey.AppendBigramKeys([]byte("main.go"), nil)[0]
	raw, ok, err = bi.LookupPosting(seg.Meta().ID, 1, biGram)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		v, err = lsm.ParseRoaring(raw)
		if err != nil {
			t.Fatal(err)
		}
		if !v.Contains(1) {
			t.Fatal("bigram posting must contain localIdx 1")
		}
	}

	// Cached blocks must now hold data within budget.
	if cache.Bytes() == 0 {
		t.Fatal("expected cached blocks after lookups")
	}
	if cache.Bytes() > cache.Limit() {
		t.Fatalf("cache bytes %d exceeds limit %d", cache.Bytes(), cache.Limit())
	}
}

func TestSegmentTier2OldReaderRejected(t *testing.T) {
	dir := t.TempDir()
	seg, cache, _ := buildTier2Segment(t, dir, true)
	segDir := seg.Dir()
	seg.Release()
	cache.Close()

	// Simulate a pre-Tier2 reader: clear the Tier2 flag in meta.hdr.
	metaPath := filepath.Join(segDir, "meta.hdr")
	raw, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	raw[16] &^= 2 // clear segFlagTier2 bit in the LE flags word
	if err := os.WriteFile(metaPath, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSegmentWithGen(segDir, 0); err == nil {
		t.Fatal("legacy reader must reject Tier 2 segment (missing names.col)")
	}
}

func TestManifestTier2FlagRoundtrip(t *testing.T) {
	dir := t.TempDir()
	m := NewManifest()
	m.Segments = []SegmentMeta{
		{ID: 1, BaseNodeID: 0, NodeCount: 5, Dense: true, Tier2: true, TombstoneGen: 2},
		{ID: 2, NodeCount: 3, Tier2: true, TombstoneGen: 0},
		{ID: 3, NodeCount: 7, Dense: true, TombstoneGen: 1},
	}
	path := filepath.Join(dir, "MANIFEST")
	if err := m.Save(path); err != nil {
		t.Fatal(err)
	}
	got, existed, err := LoadManifest(path)
	if err != nil || !existed {
		t.Fatalf("LoadManifest: existed=%v err=%v", existed, err)
	}
	if len(got.Segments) != 3 {
		t.Fatalf("segments = %d", len(got.Segments))
	}
	if !got.Segments[0].Tier2 || !got.Segments[1].Tier2 || got.Segments[2].Tier2 {
		t.Fatalf("Tier2 flags = %v %v %v", got.Segments[0].Tier2, got.Segments[1].Tier2, got.Segments[2].Tier2)
	}
	if !got.Segments[0].Dense || got.Segments[1].Dense || !got.Segments[2].Dense {
		t.Fatal("Dense flags must roundtrip unchanged")
	}
	if got.Segments[0].TombstoneGen != 2 || got.Segments[2].TombstoneGen != 1 {
		t.Fatal("tombstone generations must roundtrip")
	}
}

func TestSegmentTier2NilCacheDirect(t *testing.T) {
	// Tier 2 segments must remain readable without a cache (direct reads).
	dir := t.TempDir()
	seg, cache, _ := buildTier2Segment(t, dir, true)
	segDir := seg.Dir()
	seg.Release()
	cache.Close()

	seg2, err := OpenSegmentWithGen(segDir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer seg2.Release()
	if string(seg2.NameBytesAt(0)) != "src" {
		t.Fatalf("direct name read = %q", seg2.NameBytesAt(0))
	}
	if _, _, ok := seg2.LookupByKey(1, 1002); !ok {
		t.Fatal("direct LookupByKey must succeed")
	}
}
