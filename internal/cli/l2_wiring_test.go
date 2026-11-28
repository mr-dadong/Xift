package cli

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mr-dadong/Xift/internal/daemon"
	"github.com/mr-dadong/Xift/internal/l2search"
	"github.com/mr-dadong/Xift/internal/storage"
)

func wiringKey(path string) uint64 {
	var h uint64 = 1469598103934665603
	for i := 0; i < len(path); i++ {
		h ^= uint64(path[i])
		h *= 1099511628211
	}
	return h
}

type wiringScanner struct{}

func (wiringScanner) ScanSubtreeWithRoot(ctx context.Context, root string, fn func(l2search.ScanRecord) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := fn(l2search.ScanRecord{
		VolID: 1, NodeKey: wiringKey(root), ParentKey: wiringKey(root), Name: filepath.Base(root), IsDir: true,
	}); err != nil {
		return err
	}
	return filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || path == root {
			return err
		}
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		return fn(l2search.ScanRecord{
			VolID:     1,
			NodeKey:   wiringKey(path),
			ParentKey: wiringKey(filepath.Dir(path)),
			Name:      d.Name(),
			IsDir:     d.IsDir(),
		})
	})
}

// TestL2DaemonWiring drives the production path end to end: a daemon with L2
// enabled indexes a directory added through `xift l2 add --daemon`, and the
// CLI sees both the content hit and the growing status counters.
func TestL2DaemonWiring(t *testing.T) {
	dataDir := t.TempDir()
	root := t.TempDir()
	file := filepath.Join(root, "wired.txt")
	if err := os.WriteFile(file, []byte("cli wiring needle"), 0o644); err != nil {
		t.Fatal(err)
	}

	store, err := storage.Open(dataDir, storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	b, err := store.BeginDenseSegment()
	if err != nil {
		t.Fatal(err)
	}
	rootID, err := b.AppendDense(1, wiringKey(root), storage.RootParentIdx, filepath.Base(root), storage.FlagDir, 0, 0, 1_700_000_000)
	if err != nil {
		t.Fatal(err)
	}
	fileID, err := b.AppendDense(1, wiringKey(file), rootID, "wired.txt", 0, 0, 16, 1_700_000_000)
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
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	ep := uniqueCLIEndpoint(t)
	dCtx, dCancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	dDone := make(chan error, 1)
	go func() {
		dDone <- daemon.Run(dCtx, daemon.Config{
			DataDir:  dataDir,
			Endpoint: ep,
			L2: &daemon.L2Config{
				VolRoots:  map[uint16]string{1: filepath.Dir(root)},
				Scanner:   wiringScanner{},
				Debounce:  30 * time.Millisecond,
				BatchDocs: 10,
			},
			OnReady: func(string) { close(ready) },
		})
	}()
	select {
	case <-ready:
	case err := <-dDone:
		t.Fatalf("daemon exited early: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("daemon not ready")
	}
	t.Cleanup(func() {
		dCancel()
		<-dDone
	})

	if code, _, errStr := runCLI(t, "l2", "add", "--daemon", "--endpoint", ep, "--json", root); code != 0 {
		t.Fatalf("l2 add exited %d: %s", code, errStr)
	}

	found := false
	deadline := time.Now().Add(20 * time.Second)
	for !found && time.Now().Before(deadline) {
		code, out, errStr := runCLI(t, "--daemon", "--endpoint", ep, "--json", "--limit", "10", "content:\"needle\"")
		if code != 0 {
			t.Fatalf("content query exited %d: %s", code, errStr)
		}
		var payload struct {
			Results []struct {
				NodeID uint32 `json:"node_id"`
			} `json:"results"`
		}
		if err := json.Unmarshal([]byte(out), &payload); err == nil {
			for _, row := range payload.Results {
				if row.NodeID == fileID {
					found = true
					break
				}
			}
		}
		if !found {
			time.Sleep(20 * time.Millisecond)
		}
	}
	if !found {
		t.Fatalf("content hit for node %d not visible within 20s", fileID)
	}

	code, statusOut, errStr := runCLI(t, "l2", "status", "--data-dir", dataDir, "--daemon", "--endpoint", ep, "--json")
	if code != 0 {
		t.Fatalf("l2 status exited %d: %s", code, errStr)
	}
	var status L2StatusOutput
	if err := json.Unmarshal([]byte(statusOut), &status); err != nil {
		t.Fatalf("decode status: %v (%s)", err, statusOut)
	}
	if status.Indexed == 0 {
		t.Fatalf("status = %+v, want indexed documents", status)
	}
	if len(status.Roots) != 1 || status.Roots[0] != root {
		t.Fatalf("roots = %v, want [%s]", status.Roots, root)
	}
}
