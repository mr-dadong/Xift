package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/mr-dadong/Xift/internal/provider"
	"github.com/mr-dadong/Xift/internal/query"
	"github.com/mr-dadong/Xift/internal/storage"
)

// TestLiveWatchShutdownFlushes verifies Close drains the event queue and
// flushes the final batch before returning.
func TestLiveWatchShutdownFlushes(t *testing.T) {
	dataDir := t.TempDir()
	store, err := storage.Open(dataDir, storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	defer store.Close()
	seedLiveRoot(t, store)

	prov := newFakeWatchProvider()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w, err := startLiveWatch(ctx, store, WatchConfig{
		Provider: prov,
		Volumes:  []provider.VolumeInfo{{VolID: 1, RootPath: t.TempDir()}},
		Enabled:  true,
		Window:   time.Second,
	}, nil)
	if err != nil {
		t.Fatalf("startLiveWatch: %v", err)
	}

	prov.events <- provider.FileEvent{
		Type: provider.EventCreate, NodeKey: 2, ParentKey: 1, NewName: "final.txt", Size: 4, ModTime: 1_700_000_001, Cursor: 10,
	}
	waitForL2(t, 2*time.Second, func() bool {
		for _, st := range w.volumeSnapshots() {
			if st.VolID == 1 && st.Cursor >= 10 {
				return true
			}
		}
		return false
	})

	w.Close()

	qe := query.NewEngine(store)
	res, err := qe.Search(context.Background(), "final", query.SearchOptions{Limit: 10})
	if err != nil {
		t.Fatalf("search after close: %v", err)
	}
	if res.Total != 1 {
		t.Fatalf("final batch not flushed before Close: total=%d", res.Total)
	}
}
