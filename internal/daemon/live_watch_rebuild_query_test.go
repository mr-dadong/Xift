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

// TestLiveWatchRebuildKeepsQueriesServing verifies name queries keep working
// while a volume rebuild is in flight.
func TestLiveWatchRebuildKeepsQueriesServing(t *testing.T) {
	dataDir := t.TempDir()
	store, err := storage.Open(dataDir, storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	defer store.Close()

	root := t.TempDir()
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
	meta, err := b.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CommitSegment(meta, nil); err != nil {
		t.Fatal(err)
	}

	gate := make(chan struct{})
	prov := newFakeWatchProvider()
	prov.watchErrs = []error{provider.ErrCursorExpired}
	prov.scanGate = gate
	prov.scanCursor = 100
	prov.scanRecs = []provider.ScanRecord{
		{VolID: 1, NodeKey: 1, ParentKey: 1, Name: filepath.Base(root), Flags: storage.FlagDir},
		{VolID: 1, NodeKey: 2, ParentKey: 1, Name: "a.txt", Size: 4},
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

	waitForL2(t, 5*time.Second, func() bool {
		states := w.volumeSnapshots()
		return len(states) == 1 && states[0].State == string(watchStateRebuilding)
	})

	qe := query.NewEngine(store)
	done := make(chan error, 1)
	go func() {
		_, qerr := qe.Search(ctx, "a.txt", query.SearchOptions{Limit: 10})
		done <- qerr
	}()
	select {
	case qerr := <-done:
		if qerr != nil {
			t.Fatalf("query during rebuild: %v", qerr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("query blocked during rebuild")
	}

	close(gate)
	waitForL2(t, 5*time.Second, func() bool {
		res, qerr := qe.Search(ctx, "a.txt", query.SearchOptions{Limit: 10})
		return qerr == nil && res.Total == 1
	})
}
