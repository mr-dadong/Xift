package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mr-dadong/Xift/internal/provider"
	"github.com/mr-dadong/Xift/internal/query"
	"github.com/mr-dadong/Xift/internal/storage"
)

// TestLiveWatchIsolatesVolumeFailure verifies one failing volume retries with
// backoff while another volume keeps delivering events.
func TestLiveWatchIsolatesVolumeFailure(t *testing.T) {
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
	if _, err := b.AppendDense(1, 1, storage.RootParentIdx, "vol1", storage.FlagDir, 0, 0, 1_700_000_000); err != nil {
		t.Fatal(err)
	}
	if _, err := b.AppendDense(2, 10, storage.RootParentIdx, "vol2", storage.FlagDir, 0, 0, 1_700_000_000); err != nil {
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
	prov.errVols[1] = errors.New("watch permission denied")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w, err := startLiveWatch(ctx, store, WatchConfig{
		Provider: prov,
		Volumes: []provider.VolumeInfo{
			{VolID: 1, RootPath: t.TempDir()},
			{VolID: 2, RootPath: t.TempDir()},
		},
		Enabled:    true,
		Window:     20 * time.Millisecond,
		RetryDelay: 10 * time.Millisecond,
	}, nil)
	if err != nil {
		t.Fatalf("startLiveWatch: %v", err)
	}
	defer w.Close()

	prov.events <- provider.FileEvent{
		Type: provider.EventCreate, NodeKey: 11, ParentKey: 10, NewName: "v2.txt", Size: 4, ModTime: 1_700_000_001, Cursor: 7,
	}
	qe := query.NewEngine(store)
	waitForL2(t, 2*time.Second, func() bool {
		res, qerr := qe.Search(ctx, "v2.txt", query.SearchOptions{Limit: 10})
		return qerr == nil && res.Total == 1
	})

	waitForL2(t, 2*time.Second, func() bool {
		if prov.attemptsFor(1) < 2 {
			return false
		}
		for _, st := range w.volumeSnapshots() {
			if st.VolID == 1 {
				return st.State == string(watchStateRetrying) && st.LastErr != ""
			}
		}
		return false
	})
	if vc, ok := store.VolumeCursor(2); !ok || vc.Cursor != 7 {
		t.Fatalf("volume 2 cursor = (%+v, %v), want 7", vc, ok)
	}
}
