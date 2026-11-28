package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/mr-dadong/Xift/internal/provider"
	"github.com/mr-dadong/Xift/internal/storage"
)

// TestDaemonRunStopsWatch verifies daemon.Run owns the watch supervisor and
// stops every volume stream before returning.
func TestDaemonRunStopsWatch(t *testing.T) {
	dataDir := t.TempDir()
	store, err := storage.Open(dataDir, storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	prov := newFakeWatchProvider()
	ep := uniqueDaemonEndpoint(t)
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Config{
			DataDir:  dataDir,
			Endpoint: ep,
			Watch: &WatchConfig{
				Provider: prov,
				Volumes:  []provider.VolumeInfo{{VolID: 1, RootPath: t.TempDir()}},
				Enabled:  true,
			},
			OnReady: func(string) { close(ready) },
		})
	}()
	select {
	case <-ready:
	case err := <-done:
		t.Fatalf("daemon exited early: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("daemon not ready")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("daemon shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("daemon did not shut down")
	}
	select {
	case volID := <-prov.watchDone:
		if volID != 1 {
			t.Fatalf("watch stopped for volume %d, want 1", volID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("watch goroutine was not stopped")
	}
}
