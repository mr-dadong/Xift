package cli

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/mr-dadong/Xift/internal/daemon"
	"github.com/mr-dadong/Xift/internal/storage"
)

// TestStatusJSONIncludesWatch verifies `xift status --daemon --json` carries
// the watch state and that a disabled supervisor reports enabled=false.
func TestStatusJSONIncludesWatch(t *testing.T) {
	dataDir := t.TempDir()
	store, err := storage.Open(dataDir, storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	ep := uniqueCLIEndpoint(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- daemon.Run(ctx, daemon.Config{
			DataDir:  dataDir,
			Endpoint: ep,
			Watch:    &daemon.WatchConfig{Enabled: false},
			OnReady:  func(string) { close(ready) },
		})
	}()
	select {
	case <-ready:
	case err := <-done:
		t.Fatalf("daemon exited early: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("daemon not ready")
	}
	t.Cleanup(func() {
		cancel()
		<-done
	})

	code, out, errStr := runCLI(t, "status", "--daemon", "--endpoint", ep, "--json")
	if code != 0 {
		t.Fatalf("status exited %d: %s", code, errStr)
	}
	var status DaemonStatusOutput
	if err := json.Unmarshal([]byte(out), &status); err != nil {
		t.Fatalf("decode status: %v (%s)", err, out)
	}
	if status.Watch == nil {
		t.Fatalf("status JSON has no watch field: %s", out)
	}
	if status.Watch.Enabled {
		t.Fatalf("watch enabled = true, want false")
	}
}
