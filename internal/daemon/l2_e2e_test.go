package daemon

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mr-dadong/Xift/internal/provider"
	"github.com/mr-dadong/Xift/internal/query"
	"github.com/mr-dadong/Xift/internal/storage"
)

type fixedResolver struct {
	mu  sync.Mutex
	ids map[string]uint32
}

func (r *fixedResolver) Resolve(path string) (uint32, int64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	id, ok := r.ids[path]
	return id, 0, ok
}

// TestL2EndToEnd drives the whitelist lifecycle through the daemon pipeline and
// the public query engine: create → content hit → modify → invisible → delete.
func TestL2EndToEnd(t *testing.T) {
	dataDir := t.TempDir()
	store, err := storage.Open(dataDir, storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	defer store.Close()

	b, err := store.BeginDenseSegment()
	if err != nil {
		t.Fatal(err)
	}
	rootID, err := b.AppendDense(1, 1, storage.RootParentIdx, "proj", storage.FlagDir, 0, 0, 1_700_000_000)
	if err != nil {
		t.Fatal(err)
	}
	idA, err := b.AppendDense(1, 2, rootID, "a.go", 0, 0, 64, 1_700_000_000)
	if err != nil {
		t.Fatal(err)
	}
	idB, err := b.AppendDense(1, 3, rootID, "b.go", 0, 0, 64, 1_700_000_000)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := b.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CommitSegment(meta, nil); err != nil {
		t.Fatal(err)
	}

	root := t.TempDir()
	fileA := filepath.Join(root, "a.go")
	fileB := filepath.Join(root, "b.go")
	writeL2File(t, fileA, "package main // needlemarker")
	writeL2File(t, fileB, "package other")

	resolver := &fixedResolver{ids: map[string]uint32{fileA: idA, fileB: idB}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p, err := OpenL2Pipeline(ctx, dataDir, L2Config{
		VolRoots:  map[uint16]string{1: root},
		Resolver:  resolver,
		Debounce:  3 * time.Second,
		BatchDocs: 100,
	})
	if err != nil {
		t.Fatalf("OpenL2Pipeline: %v", err)
	}
	defer func() { _ = p.Close() }()

	if _, err := p.AddRoot(root); err != nil {
		t.Fatalf("AddRoot: %v", err)
	}
	qe := query.NewEngine(store, query.WithContentSearcher(p))

	find := func(term string) []query.ResultItem {
		t.Helper()
		res, qerr := qe.Search(ctx, `content:"`+term+`"`, query.SearchOptions{Limit: 10})
		if qerr != nil {
			t.Fatalf("Search(%s): %v", term, qerr)
		}
		return res.Items
	}
	waitForL2(t, 30*time.Second, func() bool {
		return len(find("needlemarker")) == 1
	})

	// Modification is delivered as an event and becomes visible after the debounce.
	writeL2File(t, fileA, "package main // rewritten")
	p.HandleBatch(provider.EventBatch{Events: []provider.FileEvent{{
		VolID: 1, Type: provider.EventModifyMeta, RelPath: l2Rel(root, fileA),
	}}})
	waitForL2(t, 60*time.Second, func() bool { return len(find("needlemarker")) == 0 })

	// Deletion removes the document too.
	if err := os.Remove(fileA); err != nil {
		t.Fatal(err)
	}
	p.HandleBatch(provider.EventBatch{Events: []provider.FileEvent{{
		VolID: 1, Type: provider.EventDelete, RelPath: l2Rel(root, fileA),
	}}})
	waitForL2(t, 60*time.Second, func() bool { return len(find("rewritten")) == 0 })
}
