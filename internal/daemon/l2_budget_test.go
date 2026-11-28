package daemon

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/mr-dadong/Xift/internal/l2search"
	"github.com/mr-dadong/Xift/internal/query"
	"github.com/mr-dadong/Xift/internal/storage"
)

func p95Of(d []time.Duration) time.Duration {
	if len(d) == 0 {
		return 0
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	return d[len(d)*95/100]
}

// TestL2ResourceBudget verifies L1 query latency stays healthy while L2 indexes
// 100k text files, and that L2 keeps its own <=16 MiB cache budget.
func TestL2ResourceBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 100k-file L2 budget test in -short mode")
	}
	dataDir := t.TempDir()
	seedDaemonTestIndex(t, dataDir, 2000)

	store, err := storage.Open(dataDir, storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	defer store.Close()
	qe := query.NewEngine(store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	root := t.TempDir()
	const files = 100_000
	dirs := make([]string, 100)
	for d := range dirs {
		dirs[d] = filepath.Join(root, fmt.Sprintf("d%03d", d))
		if err := os.MkdirAll(dirs[d], 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < files; i++ {
		p := filepath.Join(dirs[i%len(dirs)], fmt.Sprintf("f%05d.txt", i))
		if err := os.WriteFile(p, []byte("budget needle sample"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	p, err := OpenL2Pipeline(ctx, dataDir, L2Config{
		VolRoots:  map[uint16]string{1: root},
		Resolver:  newPathIDResolver(),
		Debounce:  time.Minute,
		BatchDocs: 5000,
	})
	if err != nil {
		t.Fatalf("OpenL2Pipeline: %v", err)
	}
	defer func() { _ = p.Close() }()

	if got, want := p.CacheBytes(), l2search.DefaultCacheBytes; got != want {
		t.Fatalf("L2 cache budget = %d, want %d", got, want)
	}
	if p.CacheBytes() > 16<<20 {
		t.Fatalf("L2 cache budget exceeds 16 MiB: %d", p.CacheBytes())
	}

	sample := func(n int) []time.Duration {
		out := make([]time.Duration, 0, n)
		for i := 0; i < n; i++ {
			start := time.Now()
			if _, err := qe.Search(ctx, "doc", query.SearchOptions{Limit: 20}); err != nil {
				t.Fatalf("L1 search: %v", err)
			}
			out = append(out, time.Since(start))
		}
		return out
	}
	baseP95 := p95Of(sample(200))

	if _, err := p.AddRoot(root); err != nil {
		t.Fatalf("AddRoot: %v", err)
	}
	during := sample(300)
	durP95 := p95Of(during)
	budget := 3*baseP95 + 20*time.Millisecond
	if durP95 > budget {
		t.Fatalf("L1 p95 during L2 indexing = %v, budget %v (baseline %v)", durP95, budget, baseP95)
	}

	waitForL2(t, 5*time.Minute, func() bool {
		st := p.Status()
		return st.Indexed >= files && !st.Paused
	})
	if used := p.Status().DiskUsed; used <= 0 {
		t.Fatalf("L2 disk usage = %d", used)
	}
}
