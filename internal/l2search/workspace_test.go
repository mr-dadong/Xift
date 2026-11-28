package l2search

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type containsExcluder struct{ needle string }

func (e containsExcluder) Excluded(path string, _ bool) bool {
	return strings.Contains(filepath.ToSlash(path), e.needle)
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func openWorkspace(t *testing.T, opts WSOptions) (*Workspace, *Store) {
	t.Helper()
	st, err := OpenStore(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return NewWorkspace(st, opts), st
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

// fakeScanner replays a fixed subtree, optionally cancelling mid-stream.
type fakeScanner struct {
	recs     []ScanRecord
	cancelAt int
	cancel   func()
}

func (f *fakeScanner) ScanSubtreeWithRoot(ctx context.Context, root string, fn func(ScanRecord) error) error {
	for i, rec := range f.recs {
		if f.cancel != nil && i == f.cancelAt {
			f.cancel()
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(rec); err != nil {
			return err
		}
	}
	return nil
}

func scanRecordsFor(root, relDir string, files []string, baseKey uint64) []ScanRecord {
	recs := []ScanRecord{{VolID: 1, NodeKey: 1, ParentKey: 1, Name: filepath.Base(root), IsDir: true}}
	dirKey := uint64(2)
	if relDir != "" {
		recs = append(recs, ScanRecord{VolID: 1, NodeKey: dirKey, ParentKey: 1, Name: relDir, IsDir: true})
	} else {
		dirKey = 1
	}
	for i, name := range files {
		recs = append(recs, ScanRecord{VolID: 1, NodeKey: baseKey + uint64(i), ParentKey: dirKey, Name: name})
	}
	return recs
}

// TestWhitelistInheritsExcludes verifies only whitelisted, non-excluded files
// enter the content index.
func TestWhitelistInheritsExcludes(t *testing.T) {
	root := t.TempDir()
	keep := filepath.Join(root, "src", "a.txt")
	skip := filepath.Join(root, "node_modules", "pkg", "b.txt")
	inside := filepath.Join(root, "src", "sub", "c.txt")
	outside := filepath.Join(t.TempDir(), "outside.txt")
	writeFile(t, keep, "alphamarker in src")
	writeFile(t, skip, "betamarker in deps")
	writeFile(t, inside, "gammamarker nested")
	writeFile(t, outside, "deltamarker outside")

	ws, _ := openWorkspace(t, WSOptions{Excluder: containsExcluder{"node_modules"}, Debounce: time.Millisecond})
	ws.AddRoot(root)
	if err := ws.IndexRoot(context.Background(), root); err != nil {
		t.Fatalf("IndexRoot: %v", err)
	}

	check := func(term string, want int) {
		t.Helper()
		keys, _, err := ws.Search([]byte(term), 0)
		if err != nil {
			t.Fatalf("Search(%s): %v", term, err)
		}
		if len(keys) != want {
			t.Fatalf("Search(%s) = %d keys, want %d", term, len(keys), want)
		}
	}
	check("alphamarker", 1)
	check("gammamarker", 1)
	check("betamarker", 0)
	check("deltamarker", 0)
	if !ws.InRoot(keep) || ws.InRoot(outside) {
		t.Fatal("InRoot mismatch")
	}
}

// TestDebounceCoalesces verifies a burst of changes to one path produces one
// indexing pass, and that a delete removes the document.
func TestDebounceCoalesces(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "a.txt")
	writeFile(t, path, "first version")

	ws, st := openWorkspace(t, WSOptions{Debounce: 30 * time.Millisecond})
	ws.AddRoot(root)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ws.Start(ctx)
	defer ws.Stop()
	if err := ws.IndexRoot(ctx, root); err != nil {
		t.Fatalf("IndexRoot: %v", err)
	}
	keys, _, err := ws.Search([]byte("first"), 0)
	if err != nil || len(keys) != 1 {
		t.Fatalf("initial search = %v, %v", keys, err)
	}
	key := keys[0]

	writeFile(t, path, "mutex one")
	ws.EnqueueEvent(key, path, 0, true)
	writeFile(t, path, "mutex two")
	ws.EnqueueEvent(key, path, 0, true)
	writeFile(t, path, "mutex three")
	ws.EnqueueEvent(key, path, 0, true)

	waitFor(t, 2*time.Second, func() bool {
		keys, _, _ := ws.Search([]byte("three"), 0)
		return len(keys) == 1 && ws.Stats().Batches == 2
	})
	if got := ws.Stats().Batches; got != 2 {
		t.Fatalf("batches = %d, want 2 (initial + one debounced pass)", got)
	}

	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	ws.EnqueueEvent(key, path, 0, false)
	waitFor(t, 2*time.Second, func() bool {
		keys, _, _ := ws.Search([]byte("three"), 0)
		return len(keys) == 0
	})
	_ = st
}

// TestIndexInitialBatches verifies initial indexing batches documents and stops
// promptly on cancellation.
func TestIndexInitialBatches(t *testing.T) {
	root := t.TempDir()
	files := make([]string, 50)
	for i := range files {
		files[i] = pathName(i)
		writeFile(t, filepath.Join(root, "f", files[i]), "common marker text")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scanner := &fakeScanner{recs: scanRecordsFor(root, "f", files, 100), cancelAt: 12, cancel: cancel}
	ws, st := openWorkspace(t, WSOptions{Scanner: scanner, BatchDocs: 10})
	ws.AddRoot(root)
	err := ws.IndexScan(ctx, root)
	if err == nil {
		t.Fatal("IndexScan must surface context cancellation")
	}
	if st.DocCount() == 0 || st.DocCount() >= 50 {
		t.Fatalf("partial index docs = %d, want between 1 and 49", st.DocCount())
	}
	if st.SegmentCount() < 1 {
		t.Fatal("partial batches must still be flushed")
	}
}

// TestIndexScanFromStream verifies the scanner-driven initial index composes
// paths from parent keys and records keys exactly as emitted.
func TestIndexScanFromStream(t *testing.T) {
	root := t.TempDir()
	files := []string{"one.txt", "two.txt", "three.txt"}
	for i, name := range files {
		content := "scan marker common"
		if i == 1 {
			content = "uniquesecond scan"
		}
		writeFile(t, filepath.Join(root, "priv", name), content)
	}
	scanner := &fakeScanner{recs: scanRecordsFor(root, "priv", files, 400)}
	scanner.recs = append(scanner.recs, ScanRecord{VolID: 1, NodeKey: 999, ParentKey: 777, Name: "orphan.txt"})

	ws, st := openWorkspace(t, WSOptions{Scanner: scanner, BatchDocs: 2})
	ws.AddRoot(root)
	if err := ws.IndexScan(context.Background(), root); err != nil {
		t.Fatalf("IndexScan: %v", err)
	}
	if st.DocCount() != 3 {
		t.Fatalf("docs = %d, want 3", st.DocCount())
	}
	keys, _, err := ws.Search([]byte("uniquesecond"), 0)
	if err != nil || len(keys) != 1 {
		t.Fatalf("search = %v, %v", keys, err)
	}
	if keys[0] != (DocKey{VolID: 1, NodeKey: 401}) {
		t.Fatalf("key = %+v, want {1 401}", keys[0])
	}
	if ws.Stats().Skipped[SkipNoPath] != 1 {
		t.Fatalf("no_path skips = %d, want 1", ws.Stats().Skipped[SkipNoPath])
	}
}

// TestEnqueueByDocKey verifies delete+reindex for one key coalesces to the
// indexed state, and a plain delete removes the document.
func TestEnqueueByDocKey(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "doc.txt")
	writeFile(t, path, "initialtoken")

	ws, _ := openWorkspace(t, WSOptions{Debounce: time.Millisecond})
	ws.AddRoot(root)
	if err := ws.IndexRoot(context.Background(), root); err != nil {
		t.Fatalf("IndexRoot: %v", err)
	}
	keys, _, _ := ws.Search([]byte("initialtoken"), 0)
	if len(keys) != 1 {
		t.Fatalf("initial keys = %v", keys)
	}
	key := keys[0]

	writeFile(t, path, "updatedtoken")
	ws.EnqueueEvent(key, path, 0, false)
	ws.EnqueueEvent(key, path, 0, true)
	time.Sleep(5 * time.Millisecond)
	ws.flushIfDue(context.Background())

	keys, _, _ = ws.Search([]byte("updatedtoken"), 0)
	if len(keys) != 1 || keys[0] != key {
		t.Fatalf("updated keys = %v, want %v", keys, key)
	}
	if old, _, _ := ws.Search([]byte("initialtoken"), 0); len(old) != 0 {
		t.Fatalf("old content still matched: %v", old)
	}

	ws.EnqueueEvent(key, path, 0, false)
	time.Sleep(5 * time.Millisecond)
	ws.flushIfDue(context.Background())
	if keys, _, _ := ws.Search([]byte("updatedtoken"), 0); len(keys) != 0 {
		t.Fatalf("deleted doc still matched: %v", keys)
	}
}

// TestRenameKeepsContentHit verifies a same-key rename keeps the content hit and
// updates the tracked path.
func TestRenameKeepsContentHit(t *testing.T) {
	root := t.TempDir()
	oldPath := filepath.Join(root, "a.txt")
	newPath := filepath.Join(root, "b.txt")
	writeFile(t, oldPath, "renamemarker content")

	scanner := &fakeScanner{recs: []ScanRecord{
		{VolID: 1, NodeKey: 1, ParentKey: 1, Name: filepath.Base(root), IsDir: true},
		{VolID: 1, NodeKey: 500, ParentKey: 1, Name: "a.txt"},
	}}
	ws, _ := openWorkspace(t, WSOptions{Scanner: scanner, Debounce: time.Millisecond})
	ws.AddRoot(root)
	if err := ws.IndexScan(context.Background(), root); err != nil {
		t.Fatalf("IndexScan: %v", err)
	}
	keys, _, _ := ws.Search([]byte("renamemarker"), 0)
	if len(keys) != 1 || keys[0] != (DocKey{VolID: 1, NodeKey: 500}) {
		t.Fatalf("initial keys = %v", keys)
	}

	if err := os.Rename(oldPath, newPath); err != nil {
		t.Fatal(err)
	}
	ws.EnqueueEvent(DocKey{VolID: 1, NodeKey: 500}, newPath, 0, true)
	time.Sleep(5 * time.Millisecond)
	ws.flushIfDue(context.Background())

	keys, _, _ = ws.Search([]byte("renamemarker"), 0)
	if len(keys) != 1 || keys[0] != (DocKey{VolID: 1, NodeKey: 500}) {
		t.Fatalf("after rename keys = %v", keys)
	}
	ws.mu.Lock()
	path := ws.paths[DocKey{VolID: 1, NodeKey: 500}]
	ws.mu.Unlock()
	if path != newPath {
		t.Fatalf("tracked path = %q, want %q", path, newPath)
	}

	ws.RemoveRoot(root)
	if keys, _, _ := ws.Search([]byte("renamemarker"), 0); len(keys) != 0 {
		t.Fatalf("removed root still matched: %v", keys)
	}
}

func pathName(i int) string {
	return "f" + string(rune('a'+i%26)) + string(rune('a'+i/26)) + ".txt"
}
