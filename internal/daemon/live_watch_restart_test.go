package daemon

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/mr-dadong/Xift/internal/l2search"
	"github.com/mr-dadong/Xift/internal/provider"
	"github.com/mr-dadong/Xift/internal/storage"
)

// TestLiveWatchRestartCatchUp verifies a restarted watcher resumes from the
// persisted cursor, catches up changes made while it was stopped, and does not
// trigger a full rescan.
func TestLiveWatchRestartCatchUp(t *testing.T) {
	dataDir := t.TempDir()
	store, err := storage.Open(dataDir, storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	defer store.Close()

	root := t.TempDir()
	aPath := filepath.Join(root, "a.txt")
	writeL2File(t, aPath, "oldword catchup")

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
	if err := store.ApplyBatch([]storage.Mutation{{Op: storage.OpCursorUpdate, VolID: 1, Cursor: 42}}); err != nil {
		t.Fatalf("seed cursor: %v", err)
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
	waitForL2(t, 5*time.Second, func() bool {
		ids, _, serr := l2p.Search([]byte("oldword"), 0)
		return serr == nil && len(ids) == 1 && ids[0] == fileID
	})

	// Daemon stops, the file changes, then it restarts from cursor 42.
	writeL2File(t, aPath, "newword catchup")
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
	waitForL2(t, 2*time.Second, func() bool {
		cursor, ok := prov.sinceFor(1)
		return ok && cursor == 42
	})

	prov.events <- provider.FileEvent{
		Type: provider.EventModifyMeta, NodeKey: 2, ParentKey: 1, RelPath: "a.txt", Size: 16, ModTime: 1_700_000_100, Cursor: 50,
	}
	waitForL2(t, 5*time.Second, func() bool {
		ids, _, serr := l2p.Search([]byte("newword"), 0)
		return serr == nil && len(ids) == 1 && ids[0] == fileID
	})
	if ids, _, _ := l2p.Search([]byte("oldword"), 0); len(ids) != 0 {
		t.Fatalf("stale content still visible after catch-up: %v", ids)
	}
	if n := prov.fullScanCount(); n != 0 {
		t.Fatalf("restart performed %d full scans, want 0", n)
	}
}
