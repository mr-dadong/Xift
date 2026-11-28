package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/mr-dadong/Xift/internal/provider"
	"github.com/mr-dadong/Xift/internal/storage"
)

// TestLiveWatchPersistsCursor verifies a volume subscribes from its persisted
// cursor and that applied batches advance the engine cursor.
func TestLiveWatchPersistsCursor(t *testing.T) {
	dataDir := t.TempDir()
	store, err := storage.Open(dataDir, storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	defer store.Close()
	seedLiveRoot(t, store)
	if err := store.ApplyBatch([]storage.Mutation{{Op: storage.OpCursorUpdate, VolID: 1, Cursor: 42}}); err != nil {
		t.Fatalf("seed cursor: %v", err)
	}

	prov := newFakeWatchProvider()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w, err := startLiveWatch(ctx, store, WatchConfig{
		Provider: prov,
		Volumes:  []provider.VolumeInfo{{VolID: 1, RootPath: t.TempDir()}},
		Enabled:  true,
		Window:   20 * time.Millisecond,
	}, nil)
	if err != nil {
		t.Fatalf("startLiveWatch: %v", err)
	}
	defer w.Close()

	waitForL2(t, 2*time.Second, func() bool {
		cursor, ok := prov.sinceFor(1)
		return ok && cursor == 42
	})

	prov.events <- provider.FileEvent{
		Type: provider.EventCreate, NodeKey: 2, ParentKey: 1, NewName: "cursor.txt", Size: 4, ModTime: 1_700_000_001, Cursor: 43,
	}
	waitForL2(t, 2*time.Second, func() bool {
		vc, ok := store.VolumeCursor(1)
		return ok && vc.Cursor == 43
	})
}
