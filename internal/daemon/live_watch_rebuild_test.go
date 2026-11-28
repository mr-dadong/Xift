package daemon

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/mr-dadong/Xift/internal/provider"
	"github.com/mr-dadong/Xift/internal/query"
	"github.com/mr-dadong/Xift/internal/storage"
)

// TestLiveWatchRebuildOnCursorExpired verifies an expired cursor triggers a
// background volume rebuild (stale rows removed, deleted files gone, cursor
// advanced) and that watching resumes afterwards.
func TestLiveWatchRebuildOnCursorExpired(t *testing.T) {
	dataDir := t.TempDir()
	store, err := storage.Open(dataDir, storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	defer store.Close()

	root := t.TempDir()
	writeL2File(t, filepath.Join(root, "a.txt"), "keep")

	b, err := store.BeginDenseSegment()
	if err != nil {
		t.Fatal(err)
	}
	rootID, err := b.AppendDense(1, 1, storage.RootParentIdx, filepath.Base(root), storage.FlagDir, 0, 0, 1_700_000_000)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.AppendDense(1, 2, rootID, "a.txt", 0, 0, 4, 1_700_000_000); err != nil {
		t.Fatal(err)
	}
	if _, err := b.AppendDense(1, 3, rootID, "gone.txt", 0, 0, 4, 1_700_000_000); err != nil {
		t.Fatal(err)
	}
	meta, err := b.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CommitSegment(meta, nil); err != nil {
		t.Fatal(err)
	}

	prov := newFakeWatchProvider()
	prov.watchErrs = []error{provider.ErrCursorExpired}
	prov.scanCursor = 100
	prov.scanRecs = []provider.ScanRecord{
		{VolID: 1, NodeKey: 1, ParentKey: 1, Name: filepath.Base(root), Flags: storage.FlagDir, ModTime: 1_700_000_000_000_000_000},
		{VolID: 1, NodeKey: 2, ParentKey: 1, Name: "a.txt", Size: 4, ModTime: 1_700_000_000_000_000_000},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w, err := startLiveWatch(ctx, store, WatchConfig{
		Provider:   prov,
		Volumes:    []provider.VolumeInfo{{VolID: 1, RootPath: root}},
		Enabled:    true,
		Window:     20 * time.Millisecond,
		RetryDelay: 10 * time.Millisecond,
	}, nil)
	if err != nil {
		t.Fatalf("startLiveWatch: %v", err)
	}
	defer w.Close()

	qe := query.NewEngine(store)
	waitForL2(t, 5*time.Second, func() bool {
		gone, gerr := qe.Search(ctx, "gone", query.SearchOptions{Limit: 10})
		keep, kerr := qe.Search(ctx, "a.txt", query.SearchOptions{Limit: 10})
		return gerr == nil && kerr == nil && gone.Total == 0 && keep.Total == 1
	})
	if n := prov.fullScanCount(); n != 1 {
		t.Fatalf("full scans = %d, want 1", n)
	}
	waitForL2(t, 2*time.Second, func() bool {
		vc, ok := store.VolumeCursor(1)
		states := w.volumeSnapshots()
		return ok && vc.Cursor == 100 && len(states) == 1 && states[0].State == string(watchStateWatching) && states[0].Rebuilt == 1
	})

	// Watching resumes from the rebuilt cursor.
	prov.events <- provider.FileEvent{
		Type: provider.EventCreate, NodeKey: 4, ParentKey: 1, NewName: "after.txt", Size: 4, ModTime: 1_700_000_200, Cursor: 101,
	}
	waitForL2(t, 2*time.Second, func() bool {
		res, qerr := qe.Search(ctx, "after", query.SearchOptions{Limit: 10})
		return qerr == nil && res.Total == 1
	})
}
