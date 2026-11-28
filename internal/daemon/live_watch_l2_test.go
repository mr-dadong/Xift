package daemon

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/mr-dadong/Xift/internal/l2search"
	"github.com/mr-dadong/Xift/internal/provider"
	"github.com/mr-dadong/Xift/internal/query"
	"github.com/mr-dadong/Xift/internal/storage"
)

// TestLiveWatchL2Fanout verifies batched events update both L1 and the L2
// whitelist index, while paths outside the whitelist only reach L1.
func TestLiveWatchL2Fanout(t *testing.T) {
	dataDir := t.TempDir()
	store, err := storage.Open(dataDir, storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	defer store.Close()

	root := t.TempDir()
	writeL2File(t, filepath.Join(root, "a.txt"), "alpha original")
	outside := filepath.Join(t.TempDir(), "outside.txt")
	writeL2File(t, outside, "outsideword content")

	b, err := store.BeginDenseSegment()
	if err != nil {
		t.Fatal(err)
	}
	rootID, err := b.AppendDense(1, 1, storage.RootParentIdx, filepath.Base(root), storage.FlagDir, 0, 0, 1_700_000_000)
	if err != nil {
		t.Fatal(err)
	}
	fileID, err := b.AppendDense(1, 2, rootID, "a.txt", 0, 0, 16, 1_700_000_000)
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l2p, err := OpenL2Pipeline(ctx, dataDir, store, L2Config{
		VolRoots: map[uint16]string{1: root},
		Scanner: fixedScanner{recs: []l2search.ScanRecord{
			{VolID: 1, NodeKey: 1, ParentKey: 1, Name: filepath.Base(root), IsDir: true},
			{VolID: 1, NodeKey: 2, ParentKey: 1, Name: "a.txt"},
		}},
		Debounce:  20 * time.Millisecond,
		BatchDocs: 10,
	})
	if err != nil {
		t.Fatalf("OpenL2Pipeline: %v", err)
	}
	defer l2p.Close()
	if _, err := l2p.AddRoot(root); err != nil {
		t.Fatalf("AddRoot: %v", err)
	}

	prov := newFakeWatchProvider()
	w, err := startLiveWatch(ctx, store, WatchConfig{
		Provider: prov,
		Volumes:  []provider.VolumeInfo{{VolID: 1, RootPath: root}},
		Enabled:  true,
		Window:   20 * time.Millisecond,
	}, l2p)
	if err != nil {
		t.Fatalf("startLiveWatch: %v", err)
	}
	defer w.Close()

	writeL2File(t, filepath.Join(root, "a.txt"), "alpha liveupdate")
	prov.events <- provider.FileEvent{
		Type: provider.EventModifyMeta, NodeKey: 2, ParentKey: 1, RelPath: "a.txt", Size: 16, ModTime: 1_700_000_002, Cursor: 20,
	}
	waitForL2(t, 5*time.Second, func() bool {
		ids, _, serr := l2p.Search([]byte("liveupdate"), 0)
		return serr == nil && len(ids) == 1 && ids[0] == fileID
	})

	prov.events <- provider.FileEvent{
		Type: provider.EventCreate, NodeKey: 99, ParentKey: 1, NewName: "outside.txt", OldPath: outside, Size: 16, ModTime: 1_700_000_003, Cursor: 21,
	}
	qe := query.NewEngine(store)
	waitForL2(t, 2*time.Second, func() bool {
		res, qerr := qe.Search(ctx, "outside", query.SearchOptions{Limit: 10})
		return qerr == nil && res.Total == 1
	})
	if ids, _, _ := l2p.Search([]byte("outsideword"), 0); len(ids) != 0 {
		t.Fatalf("content index accepted a path outside the whitelist: %v", ids)
	}
}
