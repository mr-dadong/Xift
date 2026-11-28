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
	"github.com/mr-dadong/Xift/internal/provider"
	"github.com/mr-dadong/Xift/internal/query"
	"github.com/mr-dadong/Xift/internal/storage"
)

// treeKey derives a deterministic filesystem key from a path for tests.
func treeKey(path string) uint64 {
	var h uint64 = 1469598103934665603
	for i := 0; i < len(path); i++ {
		h ^= uint64(path[i])
		h *= 1099511628211
	}
	return h
}

// treeScanner emits a root record followed by the real directory tree.
type treeScanner struct{ prefix string }

func (s treeScanner) ScanSubtreeWithRoot(ctx context.Context, root string, fn func(l2search.ScanRecord) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := fn(l2search.ScanRecord{
		VolID: 1, NodeKey: treeKey(root), ParentKey: treeKey(root), Name: filepath.Base(root), IsDir: true,
	}); err != nil {
		return err
	}
	return filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || path == root {
			return nil
		}
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		return fn(l2search.ScanRecord{
			VolID:     1,
			NodeKey:   treeKey(path),
			ParentKey: treeKey(filepath.Dir(path)),
			Name:      d.Name(),
			IsDir:     d.IsDir(),
		})
	})
}

// fixedScanner replays fixed records.
type fixedScanner struct{ recs []l2search.ScanRecord }

func (s fixedScanner) ScanSubtreeWithRoot(ctx context.Context, _ string, fn func(l2search.ScanRecord) error) error {
	for _, rec := range s.recs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(rec); err != nil {
			return err
		}
	}
	return nil
}

func writeL2File(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func l2Rel(root, path string) string {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		panic(err)
	}
	return filepath.ToSlash(rel)
}

func waitForL2(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("L2 condition not met before timeout")
}

// TestL2PipelineDoesNotBlockQueries verifies kernel batches only enqueue
// whitelisted paths, indexing runs off the query path, and L1 queries keep
// their latency while the content index is built.
func TestL2PipelineDoesNotBlockQueries(t *testing.T) {
	dataDir := t.TempDir()
	seedDaemonTestIndex(t, dataDir, 2000)

	store, err := storage.Open(dataDir, storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	defer store.Close()
	qe := query.NewEngine(store)

	contentRoot := t.TempDir()
	const files = 3000
	paths := make([]string, 0, files)
	for i := 0; i < files; i++ {
		p := filepath.Join(contentRoot, fmt.Sprintf("d%02d", i%20), fmt.Sprintf("f%04d.txt", i))
		writeL2File(t, p, "pipeline needle sample content used for the L2 routing test")
		paths = append(paths, p)
	}
	outside := filepath.Join(t.TempDir(), "outside.txt")
	writeL2File(t, outside, "outsideonly token must never be indexed")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p, err := OpenL2Pipeline(ctx, dataDir, store, L2Config{
		VolRoots:  map[uint16]string{1: contentRoot},
		Scanner:   treeScanner{},
		Debounce:  30 * time.Millisecond,
		BatchDocs: 200,
	})
	if err != nil {
		t.Fatalf("OpenL2Pipeline: %v", err)
	}
	defer func() {
		if err := p.Close(); err != nil {
			t.Errorf("L2 Close: %v", err)
		}
	}()

	// HandleBatch must stay an in-memory enqueue: no file reads, no index writes.
	batch := provider.EventBatch{Events: make([]provider.FileEvent, 0, files+2)}
	for _, path := range paths {
		batch.Events = append(batch.Events, provider.FileEvent{
			VolID: 1, Type: provider.EventCreate, NodeKey: treeKey(path), RelPath: l2Rel(contentRoot, path),
		})
	}
	batch.Events = append(batch.Events, provider.FileEvent{
		VolID: 1, Type: provider.EventCreate, NodeKey: treeKey(outside), OldPath: outside,
	})
	start := time.Now()
	p.HandleBatch(batch)
	if enqueue := time.Since(start); enqueue > 250*time.Millisecond {
		t.Fatalf("HandleBatch blocked for %v; it must only enqueue", enqueue)
	}

	roots, err := p.AddRoot(contentRoot)
	if err != nil {
		t.Fatalf("AddRoot: %v", err)
	}
	if len(roots) != 1 || roots[0] != contentRoot {
		t.Fatalf("roots = %v, want [%s]", roots, contentRoot)
	}

	// Hammer L1 while the background pass reads and indexes the whitelisted tree.
	const samples = 200
	lat := make([]time.Duration, 0, samples)
	for i := 0; i < samples; i++ {
		qstart := time.Now()
		res, qerr := qe.Search(ctx, "doc", query.SearchOptions{Limit: 20})
		lat = append(lat, time.Since(qstart))
		if qerr != nil {
			t.Fatalf("L1 search: %v", qerr)
		}
		if res.Total == 0 {
			t.Fatal("L1 search returned no rows")
		}
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	p95 := lat[len(lat)*95/100]
	if p95 > 250*time.Millisecond {
		t.Fatalf("L1 p95 latency = %v while L2 indexes, want <= 250ms", p95)
	}

	waitForL2(t, 30*time.Second, func() bool {
		keys, _, serr := p.searchKeys([]byte("pipeline needle"), 0)
		return serr == nil && len(keys) >= files/2
	})
	if st := p.Status(); st.Indexed == 0 || st.DiskUsed == 0 {
		t.Fatalf("stats = %+v, want indexed docs and disk usage", st)
	}

	// Events outside the whitelist must never reach the index.
	if keys, _, _ := p.searchKeys([]byte("outsideonly"), 0); len(keys) != 0 {
		t.Fatalf("outside whitelist content was indexed: %v", keys)
	}

	// Modify → visible, delete → invisible, both driven by routed events.
	edited := paths[0]
	writeL2File(t, edited, "secondversion token only in the edited file")
	p.HandleBatch(provider.EventBatch{Events: []provider.FileEvent{{
		VolID: 1, Type: provider.EventModifyMeta, NodeKey: treeKey(edited), RelPath: l2Rel(contentRoot, edited),
	}}})
	waitForL2(t, 10*time.Second, func() bool {
		keys, _, _ := p.searchKeys([]byte("secondversion"), 0)
		return len(keys) == 1
	})

	if err := os.Remove(edited); err != nil {
		t.Fatal(err)
	}
	p.HandleBatch(provider.EventBatch{Events: []provider.FileEvent{{
		VolID: 1, Type: provider.EventDelete, NodeKey: treeKey(edited), RelPath: l2Rel(contentRoot, edited),
	}}})
	waitForL2(t, 10*time.Second, func() bool {
		keys, _, _ := p.searchKeys([]byte("secondversion"), 0)
		return len(keys) == 0
	})

	// Removing the root purges its documents immediately.
	if _, err := p.RemoveRoot(contentRoot); err != nil {
		t.Fatalf("RemoveRoot: %v", err)
	}
	if keys, _, _ := p.searchKeys([]byte("pipeline needle"), 0); len(keys) != 0 {
		t.Fatalf("root removal left %d content hits", len(keys))
	}
}

// TestL2PipelineRequiresScanner verifies the pipeline refuses to open without
// the scanner and engine it needs for key-accurate indexing.
func TestL2PipelineRequiresScanner(t *testing.T) {
	if _, err := OpenL2Pipeline(context.Background(), t.TempDir(), nil, L2Config{Scanner: treeScanner{}}); err == nil {
		t.Fatal("OpenL2Pipeline must fail without a storage engine")
	}
	store, err := storage.Open(t.TempDir(), storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := OpenL2Pipeline(context.Background(), t.TempDir(), store, L2Config{}); err == nil {
		t.Fatal("OpenL2Pipeline must fail without a scanner")
	}
}

// TestDaemonRunStartsL2Pipeline verifies daemon.Run wires the L2 pipeline and
// shuts it down with the daemon.
func TestDaemonRunStartsL2Pipeline(t *testing.T) {
	dataDir := t.TempDir()
	seedDaemonTestIndex(t, dataDir, 10)
	contentRoot := t.TempDir()
	writeL2File(t, filepath.Join(contentRoot, "a.txt"), "daemon wired needle")
	ep := uniqueDaemonEndpoint(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan *L2Pipeline, 1)
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Config{
			DataDir:  dataDir,
			Endpoint: ep,
			L2: &L2Config{
				Roots:   []string{contentRoot},
				Scanner: treeScanner{},
			},
			OnL2Ready: func(p *L2Pipeline) { ready <- p },
		})
	}()

	var l2 *L2Pipeline
	select {
	case l2 = <-ready:
	case err := <-done:
		t.Fatalf("daemon exited before L2 ready: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("L2 pipeline was not wired into daemon.Run")
	}
	waitForL2(t, 10*time.Second, func() bool {
		keys, _, _ := l2.searchKeys([]byte("wired needle"), 0)
		return len(keys) == 1
	})
	if roots := l2.Roots(); len(roots) != 1 || roots[0] != contentRoot {
		t.Fatalf("L2 roots = %v, want [%s]", roots, contentRoot)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("daemon shutdown: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("daemon did not shut down")
	}
}
