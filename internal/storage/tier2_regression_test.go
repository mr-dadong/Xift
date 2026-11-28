package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/mr-dadong/Xift/internal/storage/lsm"
	"github.com/mr-dadong/Xift/internal/textkey"
)

const (
	regression1MFiles = 1_000_000
	regression1MDirs  = 100_000
	regression1MBytes = 64 << 20
)

// buildRegression1M streams a 1.1M-node Dense Tier 2 segment into dataDir.
func buildRegression1M(t *testing.T, dataDir string) {
	t.Helper()
	store, err := Open(dataDir, Options{
		DisableWALSync:               true,
		LowMemoryMode:                true,
		DisableBackgroundMaintenance: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	builder, err := store.BeginDenseSegment()
	if err != nil {
		t.Fatalf("BeginDenseSegment: %v", err)
	}
	appendRow := func(key, parent uint64, name string, isDir bool) {
		var flags uint16
		if isDir {
			flags = FlagDir
		}
		parentID := uint32(0)
		if parent > 0 {
			parentID = uint32(parent - 1)
		}
		if _, err := builder.AppendDense(1, key, parentID, name, flags, 0, int64(key%10_000), int64(key)); err != nil {
			builder.Abort()
			t.Fatalf("AppendDense %q: %v", name, err)
		}
	}

	appendRow(1, 0, "root", true)
	for i := 0; i < regression1MDirs; i++ {
		appendRow(uint64(i+2), 1, fmt.Sprintf("dir_%06d", i), true)
	}
	for i := 0; i < regression1MFiles; i++ {
		parent := uint64(2 + i%regression1MDirs)
		name := fmt.Sprintf("report_%08d_annual.md", i)
		if i%10 == 0 {
			name = fmt.Sprintf("设计文档_%08d.md", i)
		}
		appendRow(uint64(2+regression1MDirs+i), parent, name, false)
	}

	meta, err := builder.Finish()
	if err != nil {
		builder.Abort()
		t.Fatalf("Finish: %v", err)
	}
	if err := store.CommitSegment(meta, nil); err != nil {
		t.Fatalf("CommitSegment: %v", err)
	}
	if got := store.ActiveTier(); got != Tier2LSM {
		t.Fatalf("active tier = %s, want tier2-lsm", got)
	}
	if want := uint64(1 + regression1MDirs + regression1MFiles); store.LiveCount() != want {
		t.Fatalf("LiveCount = %d, want %d", store.LiveCount(), want)
	}
}

func TestTier2Regression1M(t *testing.T) {
	dataDir := t.TempDir()
	buildRegression1M(t, dataDir)

	coldStart := time.Now()
	store, err := Open(dataDir, Options{
		LowMemoryMode:                true,
		DisableBackgroundMaintenance: true,
		BlockCacheBytes:              regression1MBytes,
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer store.Close()
	coldMs := float64(time.Since(coldStart).Microseconds()) / 1000.0
	if coldMs > 5_000 {
		t.Fatalf("cold start %.1f ms exceeds the wide CI budget", coldMs)
	}

	if used, limit := store.BlockCacheStats(); used != 0 || limit != regression1MBytes {
		t.Fatalf("fresh cache stats = (%d, %d)", used, limit)
	}

	snap := store.AcquireSnapshot()
	ss := snap.SegmentSnapshotAt(0)
	if !ss.IsTier2() {
		snap.Release()
		t.Fatal("regression segment must be Tier 2")
	}
	tri, _ := ss.Tier2Indexes()
	if tri == nil {
		snap.Release()
		t.Fatal("missing trigram index")
	}
	segID := ss.SegmentID()

	terms := []string{"report", "annual", "设计文档", "dir_000"}
	latencies := make([]time.Duration, 0, len(terms)*3)
	for round := 0; round < 3; round++ {
		for _, term := range terms {
			start := time.Now()
			matched := countInvertedMatches(t, ss, tri, segID, term)
			latencies = append(latencies, time.Since(start))
			if matched == 0 {
				snap.Release()
				t.Fatalf("term %q produced no matches", term)
			}
		}
	}
	snap.Release()

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p95 := latencies[int(float64(len(latencies)-1)*0.95)]
	if p95 > 2*time.Second {
		t.Fatalf("inverted lookup p95 = %v exceeds the wide CI budget", p95)
	}

	used, limit := store.BlockCacheStats()
	if used > limit {
		t.Fatalf("cache used %d exceeds limit %d", used, limit)
	}
	if used == 0 {
		t.Fatal("expected the block cache to be populated")
	}

	// Crash-recovery closed loop: an orphan segment directory (written but
	// never recorded) must be cleaned on the next open.
	orphan := filepath.Join(dataDir, "index", "segments", SegmentDirName(999_999))
	if err := os.MkdirAll(orphan, 0o755); err != nil {
		t.Fatalf("create orphan: %v", err)
	}
	if err := os.WriteFile(filepath.Join(orphan, "hot.col"), []byte("partial"), 0o644); err != nil {
		t.Fatalf("write orphan: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	again, err := Open(dataDir, Options{
		LowMemoryMode:                true,
		DisableBackgroundMaintenance: true,
		BlockCacheBytes:              regression1MBytes,
	})
	if err != nil {
		t.Fatalf("reopen after orphan: %v", err)
	}
	defer again.Close()
	if got := again.OpenStats().OrphanSegmentsRemoved; got < 1 {
		t.Fatalf("orphan segments removed = %d, want >= 1", got)
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatalf("orphan dir still present after recovery (err=%v)", err)
	}
	if want := uint64(1 + regression1MDirs + regression1MFiles); again.LiveCount() != want {
		t.Fatalf("post-recovery LiveCount = %d, want %d", again.LiveCount(), want)
	}
}

// countInvertedMatches performs one inverted lookup and verifies candidates by
// reading their names through the block cache.
func countInvertedMatches(t *testing.T, ss SegmentSnapshot, tri *lsm.InvertedIndex, segID uint32, term string) int {
	t.Helper()
	grams := textkey.AppendTrigramKeys([]byte(term), nil)
	views := make([]*lsm.RoaringView, 0, len(grams))
	for _, g := range grams {
		raw, ok, err := tri.LookupPosting(segID, 0, g)
		if err != nil || !ok {
			return 0
		}
		v, err := lsm.ParseRoaring(raw)
		if err != nil {
			t.Fatalf("parse posting: %v", err)
		}
		views = append(views, v)
	}
	if len(views) == 0 {
		return 0
	}
	needle := []byte(term)
	matched := 0
	lsm.IntersectAll(views, func(localIdx uint32) bool {
		if localIdx >= ss.Count() || ss.IsDeleted(localIdx) {
			return true
		}
		if bytes.Contains(ss.NameBytesAt(localIdx), needle) {
			matched++
		}
		return true
	})
	return matched
}
