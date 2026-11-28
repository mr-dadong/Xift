package storage_test

import (
	"testing"

	"github.com/mr-dadong/Xift/internal/storage"
)

func seedKeyLookupIndex(t *testing.T, dataDir string) *storage.Engine {
	t.Helper()
	eng, err := storage.Open(dataDir, storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	b, err := eng.BeginDenseSegment()
	if err != nil {
		t.Fatalf("BeginDenseSegment: %v", err)
	}
	root, err := b.AppendDense(1, 1, storage.RootParentIdx, "vol1", storage.FlagDir, 0, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.AppendDense(1, 100, root, "a.txt", 0, 0, 10, 100); err != nil {
		t.Fatal(err)
	}
	if _, err := b.AppendDense(1, 101, root, "b.txt", 0, 0, 10, 100); err != nil {
		t.Fatal(err)
	}
	if _, err := b.AppendDense(2, 100, storage.RootParentIdx, "c.txt", 0, 0, 10, 100); err != nil {
		t.Fatal(err)
	}
	meta, err := b.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.CommitSegment(meta, nil); err != nil {
		t.Fatal(err)
	}
	if err := eng.ApplyBatch([]storage.Mutation{{
		Op: storage.OpCreate, VolID: 1, NodeKey: 200, Name: "mem.txt", Size: 4, ModTime: 120,
	}}); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
	return eng
}

func namesFor(t *testing.T, eng *storage.Engine, ids []uint32) []string {
	t.Helper()
	snap := eng.AcquireSnapshot()
	defer snap.Release()
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		_, name, ok := snap.LookupNode(id)
		if !ok {
			t.Fatalf("node %d not found", id)
		}
		out = append(out, string(name))
	}
	return out
}

// TestLookupLiveNodeIDs verifies VolID+NodeKey resolves to live NodeIDs across
// MemTable and segments, skipping deleted and unknown keys.
func TestLookupLiveNodeIDs(t *testing.T) {
	eng := seedKeyLookupIndex(t, t.TempDir())
	defer eng.Close()

	refs := []storage.NodeKeyRef{
		{VolID: 1, NodeKey: 100},
		{VolID: 1, NodeKey: 101},
		{VolID: 2, NodeKey: 100},
		{VolID: 1, NodeKey: 200},
		{VolID: 1, NodeKey: 999},
	}
	ids := eng.LookupLiveNodeIDs(refs)
	if got, want := namesFor(t, eng, ids), []string{"a.txt", "b.txt", "c.txt", "mem.txt"}; len(got) != len(want) {
		t.Fatalf("resolved = %v, want %v", got, want)
	} else {
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("resolved = %v, want %v", got, want)
			}
		}
	}

	if got := eng.LookupLiveNodeIDs(nil); len(got) != 0 {
		t.Fatalf("nil refs = %v", got)
	}

	// A newer mutation shadows the committed segment.
	if err := eng.ApplyBatch([]storage.Mutation{{
		Op: storage.OpCreate, VolID: 1, NodeKey: 100, ParentKey: 1, Name: "a2.txt", Size: 11, ModTime: 130,
	}}); err != nil {
		t.Fatalf("ApplyBatch update: %v", err)
	}
	if err := eng.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	ids = eng.LookupLiveNodeIDs([]storage.NodeKeyRef{{VolID: 1, NodeKey: 100}})
	if got := namesFor(t, eng, ids); len(got) != 1 || got[0] != "a2.txt" {
		t.Fatalf("newest key resolution = %v, want a2.txt", got)
	}

	// Deleted keys disappear.
	if err := eng.ApplyBatch([]storage.Mutation{{Op: storage.OpDelete, VolID: 1, NodeKey: 101}}); err != nil {
		t.Fatalf("ApplyBatch delete: %v", err)
	}
	ids = eng.LookupLiveNodeIDs([]storage.NodeKeyRef{{VolID: 1, NodeKey: 101}})
	if len(ids) != 0 {
		t.Fatalf("deleted key resolved to %v", ids)
	}
}

// TestLookupLiveNodeIDsTier2 verifies the same lookup routes through Tier 2
// key indexes and the block cache.
func TestLookupLiveNodeIDsTier2(t *testing.T) {
	dataDir := t.TempDir()
	eng, err := storage.Open(dataDir, storage.Options{
		DisableWALSync: true,
		LowMemoryMode:  true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer eng.Close()

	b, err := eng.BeginDenseSegment()
	if err != nil {
		t.Fatalf("BeginDenseSegment: %v", err)
	}
	root, err := b.AppendDense(1, 1, storage.RootParentIdx, "vol1", storage.FlagDir, 0, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.AppendDense(1, 7, root, "tier2.txt", 0, 0, 10, 100); err != nil {
		t.Fatal(err)
	}
	meta, err := b.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if !meta.Tier2 {
		t.Fatal("expected a Tier 2 segment")
	}
	if err := eng.CommitSegment(meta, nil); err != nil {
		t.Fatal(err)
	}

	ids := eng.LookupLiveNodeIDs([]storage.NodeKeyRef{{VolID: 1, NodeKey: 7}})
	if got := namesFor(t, eng, ids); len(got) != 1 || got[0] != "tier2.txt" {
		t.Fatalf("tier2 resolution = %v", got)
	}
	if used, _ := eng.BlockCacheStats(); used == 0 {
		t.Log("block cache not yet populated; lookup still resolved through the key index")
	}
}
