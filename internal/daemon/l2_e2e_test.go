package daemon

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mr-dadong/Xift/internal/l2search"
	"github.com/mr-dadong/Xift/internal/provider"
	"github.com/mr-dadong/Xift/internal/query"
	"github.com/mr-dadong/Xift/internal/storage"
)

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
	if _, err := b.AppendDense(1, 2, rootID, "a.go", 0, 0, 64, 1_700_000_000); err != nil {
		t.Fatal(err)
	}
	if _, err := b.AppendDense(1, 3, rootID, "b.go", 0, 0, 64, 1_700_000_000); err != nil {
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

	scanner := fixedScanner{recs: []l2search.ScanRecord{
		{VolID: 1, NodeKey: 1, ParentKey: 1, Name: filepath.Base(root), IsDir: true},
		{VolID: 1, NodeKey: 2, ParentKey: 1, Name: "a.go"},
		{VolID: 1, NodeKey: 3, ParentKey: 1, Name: "b.go"},
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p, err := OpenL2Pipeline(ctx, dataDir, store, L2Config{
		VolRoots:  map[uint16]string{1: root},
		Scanner:   scanner,
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
		VolID: 1, Type: provider.EventModifyMeta, NodeKey: 2, RelPath: l2Rel(root, fileA),
	}}})
	waitForL2(t, 60*time.Second, func() bool { return len(find("needlemarker")) == 0 })

	// A same-key rename keeps the content hit and follows the new path.
	renamed := filepath.Join(root, "renamed.go")
	if err := os.Rename(fileA, renamed); err != nil {
		t.Fatal(err)
	}
	p.HandleBatch(provider.EventBatch{Events: []provider.FileEvent{{
		VolID: 1, Type: provider.EventRename, NodeKey: 2, RelPath: l2Rel(root, renamed),
	}}})
	waitForL2(t, 60*time.Second, func() bool { return len(find("rewritten")) == 1 })

	// Deletion removes the document too.
	if err := os.Remove(renamed); err != nil {
		t.Fatal(err)
	}
	p.HandleBatch(provider.EventBatch{Events: []provider.FileEvent{{
		VolID: 1, Type: provider.EventDelete, NodeKey: 2, RelPath: l2Rel(root, renamed),
	}}})
	waitForL2(t, 60*time.Second, func() bool { return len(find("rewritten")) == 0 })
}
