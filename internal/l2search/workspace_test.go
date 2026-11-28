package l2search

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeResolver assigns deterministic node IDs by path, with an optional hook.
type fakeResolver struct {
	mu     sync.Mutex
	byPath map[string]uint32
	next   uint32
	hook   func(path string, n int)
	calls  int
}

func newFakeResolver() *fakeResolver {
	return &fakeResolver{byPath: make(map[string]uint32)}
}

func (r *fakeResolver) Resolve(path string) (uint32, int64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if r.hook != nil {
		r.hook(path, r.calls)
	}
	id, ok := r.byPath[path]
	if !ok {
		r.next++
		id = r.next
		r.byPath[path] = id
	}
	return id, 0, true
}

func (r *fakeResolver) idOf(path string) uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.byPath[path]
}

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

// TestWhitelistInheritsExcludes verifies only whitelisted, non-excluded files
// enter the content index.
func TestWhitelistInheritsExcludes(t *testing.T) {
	root := t.TempDir()
	keep := filepath.Join(root, "src", "a.txt")
	skip := filepath.Join(root, "node_modules", "pkg", "b.txt")
	inside := filepath.Join(root, "src", "sub", "c.txt")
	outside := filepath.Join(t.TempDir(), "outside.txt")
	writeFile(t, keep, "needle in src")
	writeFile(t, skip, "needle in deps")
	writeFile(t, inside, "needle nested")
	writeFile(t, outside, "needle outside")

	res := newFakeResolver()
	ws, _ := openWorkspace(t, WSOptions{Resolver: res, Excluder: containsExcluder{"node_modules"}, Debounce: time.Millisecond})
	ws.AddRoot(root)
	if err := ws.IndexRoot(context.Background(), root); err != nil {
		t.Fatalf("IndexRoot: %v", err)
	}
	// Even if a queued event points outside the whitelist, it is ignored.
	ws.Enqueue(outside)
	time.Sleep(5 * time.Millisecond)
	ws.flushIfDue(context.Background())

	ids, _, err := ws.Search([]byte("needle"), 0)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	want := []uint32{res.idOf(keep), res.idOf(inside)}
	if !reflect.DeepEqual(ids, want) {
		t.Fatalf("search ids = %v, want %v", ids, want)
	}
	if !ws.InRoot(keep) || ws.InRoot(outside) {
		t.Fatal("InRoot mismatch")
	}
}

// TestDebounceCoalesces verifies a burst of changes to one path produces one
// indexing pass, and that deletes remove the document.
func TestDebounceCoalesces(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "a.txt")
	writeFile(t, path, "first version")

	res := newFakeResolver()
	ws, st := openWorkspace(t, WSOptions{Resolver: res, Debounce: 30 * time.Millisecond})
	ws.AddRoot(root)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ws.Start(ctx)
	defer ws.Stop()

	writeFile(t, path, "mutex one")
	ws.Enqueue(path)
	writeFile(t, path, "mutex two")
	ws.Enqueue(path)
	writeFile(t, path, "mutex three")
	ws.Enqueue(path)

	waitFor(t, 2*time.Second, func() bool { return st.SegmentCount() == 1 && ws.Stats().Indexed >= 1 })
	if got := ws.Stats().Batches; got != 1 {
		t.Fatalf("batches = %d, want 1 (debounced)", got)
	}
	ids, _, err := ws.Search([]byte("three"), 0)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("Search(three) = %v, want one node", ids)
	}

	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	ws.EnqueueDelete(path)
	waitFor(t, 2*time.Second, func() bool {
		ids, _, _ := ws.Search([]byte("three"), 0)
		return len(ids) == 0
	})
}

// TestIndexInitialBatches verifies initial indexing batches documents and stops
// promptly on cancellation.
func TestIndexInitialBatches(t *testing.T) {
	root := t.TempDir()
	for i := 0; i < 50; i++ {
		writeFile(t, filepath.Join(root, "f", pathName(i)), "common needle text")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	res := newFakeResolver()
	res.hook = func(_ string, n int) {
		if n == 12 {
			cancel()
		}
	}
	ws, st := openWorkspace(t, WSOptions{Resolver: res, BatchDocs: 10})
	ws.AddRoot(root)
	err := ws.IndexRoot(ctx, root)
	if err == nil {
		t.Fatal("IndexRoot must surface context cancellation")
	}
	if st.DocCount() == 0 || st.DocCount() >= 50 {
		t.Fatalf("partial index docs = %d, want between 1 and 49", st.DocCount())
	}
	if st.SegmentCount() < 1 {
		t.Fatal("partial batches must still be flushed")
	}
}

func pathName(i int) string {
	return "f" + string(rune('a'+i%26)) + string(rune('a'+i/26)) + ".txt"
}
