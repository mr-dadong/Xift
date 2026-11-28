package daemon

import (
	"context"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/mr-dadong/Xift/internal/l2search"
	"github.com/mr-dadong/Xift/internal/storage"
)

// TestContentKeysMapToNodes verifies L2 document keys are mapped back to live
// L1 NodeIDs and stale keys are skipped.
func TestContentKeysMapToNodes(t *testing.T) {
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
	root, err := b.AppendDense(1, 1, storage.RootParentIdx, "proj", storage.FlagDir, 0, 0, 1_700_000_000)
	if err != nil {
		t.Fatal(err)
	}
	id10, err := b.AppendDense(1, 10, root, "map_a.txt", 0, 0, 8, 1_700_000_000)
	if err != nil {
		t.Fatal(err)
	}
	id11, err := b.AppendDense(1, 11, root, "map_b.txt", 0, 0, 8, 1_700_000_000)
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

	rootDir := t.TempDir()
	writeL2File(t, filepath.Join(rootDir, "map_a.txt"), "mapmarker alpha")
	writeL2File(t, filepath.Join(rootDir, "map_b.txt"), "mapmarker beta")
	writeL2File(t, filepath.Join(rootDir, "stale.txt"), "mapmarker stale")

	scanner := fixedScanner{recs: []l2search.ScanRecord{
		{VolID: 1, NodeKey: 1, ParentKey: 1, Name: filepath.Base(rootDir), IsDir: true},
		{VolID: 1, NodeKey: 10, ParentKey: 1, Name: "map_a.txt"},
		{VolID: 1, NodeKey: 11, ParentKey: 1, Name: "map_b.txt"},
		{VolID: 1, NodeKey: 99, ParentKey: 1, Name: "stale.txt"},
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p, err := OpenL2Pipeline(ctx, dataDir, store, L2Config{
		VolRoots:  map[uint16]string{1: rootDir},
		Scanner:   scanner,
		Debounce:  20 * time.Millisecond,
		BatchDocs: 10,
	})
	if err != nil {
		t.Fatalf("OpenL2Pipeline: %v", err)
	}
	defer p.Close()
	if _, err := p.AddRoot(rootDir); err != nil {
		t.Fatalf("AddRoot: %v", err)
	}

	waitForL2(t, 20*time.Second, func() bool {
		ids, _, serr := p.Search([]byte("mapmarker"), 0)
		return serr == nil && len(ids) == 2
	})
	ids, _, err := p.Search([]byte("mapmarker"), 0)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if len(ids) != 2 || ids[0] != id10 || ids[1] != id11 {
		t.Fatalf("mapped ids = %v, want [%d %d]", ids, id10, id11)
	}

	// Deleting the L1 node makes its content key unresolvable.
	if err := store.ApplyBatch([]storage.Mutation{{Op: storage.OpDelete, VolID: 1, NodeKey: 10}}); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
	ids, _, err = p.Search([]byte("mapmarker"), 0)
	if err != nil {
		t.Fatalf("Search after delete: %v", err)
	}
	if len(ids) != 1 || ids[0] != id11 {
		t.Fatalf("mapped ids after L1 delete = %v, want [%d]", ids, id11)
	}
}
