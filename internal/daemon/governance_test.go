package daemon

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mr-dadong/Xift/internal/ipc"
	"github.com/mr-dadong/Xift/internal/query"
	"github.com/mr-dadong/Xift/internal/storage"
)

func TestDaemonFrecencyPersistenceAndRuleGovernance(t *testing.T) {
	dataDir := t.TempDir()
	store, err := storage.Open(dataDir, storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	ops := []storage.Mutation{
		{Op: storage.OpCreate, VolID: 1, NodeKey: 1, ParentKey: 0, Name: "root", Flags: storage.FlagDir},
		{Op: storage.OpCreate, VolID: 1, NodeKey: 2, ParentKey: 1, Name: "a", Flags: storage.FlagDir},
		{Op: storage.OpCreate, VolID: 1, NodeKey: 3, ParentKey: 1, Name: "b", Flags: storage.FlagDir},
		{Op: storage.OpCreate, VolID: 1, NodeKey: 4, ParentKey: 2, Name: "design.md"},
		{Op: storage.OpCreate, VolID: 1, NodeKey: 5, ParentKey: 3, Name: "design.md"},
	}
	if err := store.ApplyBatch(ops); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("store.Close: %v", err)
	}

	startDaemon := func() (string, context.CancelFunc, chan error) {
		ep := uniqueDaemonEndpoint(t)
		ctx, cancel := context.WithCancel(context.Background())
		ready := make(chan string, 1)
		done := make(chan error, 1)
		go func() {
			done <- Run(ctx, Config{
				DataDir:  dataDir,
				Endpoint: ep,
				OnReady:  func(addr string) { ready <- addr },
			})
		}()
		select {
		case addr := <-ready:
			return addr, cancel, done
		case err := <-done:
			t.Fatalf("daemon exited early: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatalf("daemon startup timeout")
		}
		return "", nil, nil
	}

	// Run 1: RecordOpen for root/b/design.md and Fold root/a.
	ep1, cancel1, done1 := startDaemon()
	ctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer ccancel()
	cli1, err := ipc.Dial(ctx, ep1)
	if err != nil {
		t.Fatalf("Dial 1: %v", err)
	}
	if err := cli1.RecordOpen(ctx, "root/b/design.md"); err != nil {
		t.Fatalf("RecordOpen: %v", err)
	}
	if _, err := cli1.ApplyFilterRule(ctx, ipc.FilterActionFold, "root/a", 0, 1); err != nil {
		t.Fatalf("ApplyFilterRule: %v", err)
	}
	_ = cli1.Close()
	cancel1()
	<-done1

	// Verify frecency.db and rules.json were persisted under <dataDir>/index/.
	if _, err := os.Stat(filepath.Join(dataDir, "index", "frecency.db")); err != nil {
		t.Fatalf("expected frecency.db persisted: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "index", "rules.json")); err != nil {
		t.Fatalf("expected rules.json persisted: %v", err)
	}

	// Run 2: Restart daemon and confirm root/b/design.md is still the only design.md and frecency loaded.
	ep2, cancel2, done2 := startDaemon()
	defer func() {
		cancel2()
		<-done2
	}()
	cli2, err := ipc.Dial(ctx, ep2)
	if err != nil {
		t.Fatalf("Dial 2: %v", err)
	}
	defer cli2.Close()

	meta, err := cli2.QueryStart(ctx, "design.md", uint8(query.SortByRelevance), false, '/')
	if err != nil {
		t.Fatalf("QueryStart 2: %v", err)
	}
	if meta.Total != 1 {
		t.Fatalf("expected 1 design.md after Fold persisted, got %d", meta.Total)
	}
}
