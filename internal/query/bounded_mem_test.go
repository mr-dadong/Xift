package query

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/mr-dadong/Xift/internal/storage"
)

// TestBoundedMemoryTopK1M is the million-node regression for bounded Top-K and
// budgeted builds: a broad term matching ~10% of the index must stay bounded in
// latency and memory, and the build must split into budget-sized segments.
func TestBoundedMemoryTopK1M(t *testing.T) {
	const (
		nodes      = 1_000_000
		segMax     = 200_000
		buildBytes = 32 << 20
		cacheBytes = 32 << 20
	)
	dataDir := t.TempDir()

	buildStart := time.Now()
	store, err := storage.Open(dataDir, storage.Options{
		DisableWALSync:               true,
		DisableBackgroundMaintenance: true,
		LowMemoryMode:                true,
		BlockCacheBytes:              cacheBytes,
		DenseSegmentNodes:            segMax,
		BuildBudgetBytes:             buildBytes,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	dw, err := store.NewDenseWriter(segMax)
	if err != nil {
		t.Fatalf("NewDenseWriter: %v", err)
	}
	for i := 0; i < nodes; i++ {
		name := fmt.Sprintf("report_%07d.txt", i)
		if _, err := dw.Append(1, uint64(i+1), storage.RootParentIdx, name, 0, 0, int64(i), int64(i)); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if _, err := dw.Commit(nil); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	segments := len(dw.Segments())
	if segments < nodes/segMax {
		t.Fatalf("segments = %d, want >= %d", segments, nodes/segMax)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	t.Logf("built %d nodes in %d segments in %.1f ms", nodes, segments, float64(time.Since(buildStart).Milliseconds()))

	store, err = storage.Open(dataDir, storage.Options{
		DisableWALSync:               true,
		DisableBackgroundMaintenance: true,
		LowMemoryMode:                true,
		BlockCacheBytes:              cacheBytes,
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer store.Close()
	qe := NewEngine(store)

	ctx := context.Background()
	res, err := qe.Search(ctx, "report", SearchOptions{Limit: 100, CandidateBudget: 8192})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if !res.Truncated {
		t.Fatal("broad query must report truncation")
	}
	if res.Total != 8192 {
		t.Fatalf("Total = %d, want verified budget 8192", res.Total)
	}
	if len(res.Items) != 100 {
		t.Fatalf("returned %d items, want 100", len(res.Items))
	}

	lat := make([]time.Duration, 0, 5)
	for i := 0; i < 5; i++ {
		start := time.Now()
		if _, err := qe.Search(ctx, "report", SearchOptions{Limit: 100, CandidateBudget: 8192}); err != nil {
			t.Fatalf("Search: %v", err)
		}
		lat = append(lat, time.Since(start))
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	p95 := lat[len(lat)*95/100]
	if p95 > 250*time.Millisecond {
		t.Fatalf("wide query p95 = %v, want <= 250ms", p95)
	}

	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	if ms.HeapAlloc > 96<<20 {
		t.Fatalf("heap after wide queries = %.1f MiB, want <= 96 MiB", float64(ms.HeapAlloc)/(1<<20))
	}
}
