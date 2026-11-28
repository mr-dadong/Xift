package storage

import (
	"errors"
	"fmt"
	"testing"
)

// dirLookupFixture builds one Tier 2 segment: root -> dirA, dirB -> file.
func dirLookupFixture(t *testing.T) *Engine {
	t.Helper()
	eng, err := Open(t.TempDir(), Options{
		DisableWALSync:               true,
		DisableBackgroundMaintenance: true,
		LowMemoryMode:                true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	err = eng.ApplyBatch([]Mutation{
		{Op: OpCreate, VolID: 1, NodeKey: 1, ParentKey: 0, Name: "root", Flags: FlagDir},
		{Op: OpCreate, VolID: 1, NodeKey: 2, ParentKey: 1, Name: "dirA", Flags: FlagDir},
		{Op: OpCreate, VolID: 1, NodeKey: 3, ParentKey: 1, Name: "dirB", Flags: FlagDir},
		{Op: OpCreate, VolID: 1, NodeKey: 4, ParentKey: 2, Name: "file.txt", Size: 10},
	})
	if err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
	if err := eng.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	return eng
}

func mustNodeID(t *testing.T, eng *Engine, volID uint16, key uint64) uint32 {
	t.Helper()
	nodeID, ok := eng.findLiveByKeyLocked(volID, key)
	if !ok {
		t.Fatalf("live lookup missing %d", key)
	}
	return nodeID
}

// TestDirLookup covers the sorted binary-search path, the unsorted fallback, and
// tombstone filtering for directory resolution.
func TestDirLookup(t *testing.T) {
	eng := dirLookupFixture(t)
	if len(eng.segments) != 1 {
		t.Fatalf("segments = %d, want 1", len(eng.segments))
	}
	seg := eng.segments[0]
	if !seg.meta.DirSorted {
		t.Fatal("built segment must record DirSorted")
	}
	wantA := mustNodeID(t, eng, 1, 2)
	wantB := mustNodeID(t, eng, 1, 3)

	got, ok, err := seg.DirID(1, 2)
	if err != nil || !ok || got != wantA {
		t.Fatalf("DirID(1,2) = (%d,%v,%v), want (%d,true,nil)", got, ok, err, wantA)
	}
	if got, ok, err := seg.DirID(1, 3); err != nil || !ok || got != wantB {
		t.Fatalf("DirID(1,3) = (%d,%v,%v), want (%d,true,nil)", got, ok, err, wantB)
	}
	if _, ok, err := seg.DirID(1, 999); err != nil || ok {
		t.Fatalf("DirID(1,999) = (_,%v,%v), want (false,nil)", ok, err)
	}
	if _, ok, err := seg.DirID(2, 2); err != nil || ok {
		t.Fatalf("DirID(2,2) = (_,%v,%v), want (false,nil) for unknown volume", ok, err)
	}

	// Unsorted fallback (older on-disk format) must agree with the binary path.
	seg.meta.DirSorted = false
	if got, ok, err := seg.DirID(1, 2); err != nil || !ok || got != wantA {
		t.Fatalf("fallback DirID(1,2) = (%d,%v,%v), want (%d,true,nil)", got, ok, err, wantA)
	}
	seg.meta.DirSorted = true

	// Deleted directories must not resolve.
	if err := eng.ApplyBatch([]Mutation{{Op: OpDelete, VolID: 1, NodeKey: 3, ParentKey: 1}}); err != nil {
		t.Fatalf("delete dirB: %v", err)
	}
	if _, ok, err := seg.DirID(1, 3); err != nil || ok {
		t.Fatalf("DirID(1,3) after delete = (_,%v,%v), want (false,nil)", ok, err)
	}
}

// TestDirMapBudget verifies the engine holds no process-wide directory map:
// opening a Tier 2 index keeps DirMapBytes at zero, and a fallback map only
// materializes for unsorted legacy segments.
func TestDirMapBudget(t *testing.T) {
	const dirs = 2000
	dir := t.TempDir()
	opts := Options{DisableWALSync: true, DisableBackgroundMaintenance: true, LowMemoryMode: true}
	eng, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	batch := make([]Mutation, 0, dirs+1)
	batch = append(batch, Mutation{Op: OpCreate, VolID: 1, NodeKey: 1, ParentKey: 0, Name: "root", Flags: FlagDir})
	for i := 0; i < dirs; i++ {
		batch = append(batch, Mutation{
			Op: OpCreate, VolID: 1, NodeKey: uint64(i + 2), ParentKey: 1,
			Name: fmt.Sprintf("dir_%04d", i), Flags: FlagDir,
		})
	}
	if err := eng.ApplyBatch(batch); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
	if err := eng.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := eng.DirMapBytes(); got != 0 {
		t.Fatalf("DirMapBytes after build = %d, want 0", got)
	}
	if err := eng.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	eng, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer eng.Close()
	if got := eng.DirMapBytes(); got != 0 {
		t.Fatalf("DirMapBytes after reopen = %d, want 0", got)
	}
	if got := eng.DirMapBytes(); got > DirMapLimit {
		t.Fatalf("DirMapBytes %d exceeds limit %d", got, DirMapLimit)
	}

	// The sorted lookup must still resolve entries without building a map.
	seg := eng.segments[0]
	if _, ok, err := seg.DirID(1, dirs+1); err != nil || !ok {
		t.Fatalf("DirID(last) = (_,%v,%v), want found", ok, err)
	}
	if got := eng.DirMapBytes(); got != 0 {
		t.Fatalf("DirMapBytes after sorted lookup = %d, want 0", got)
	}
}

// TestIncrementalParentResolution verifies mutations resolve parents across
// segments and volumes after reopen, and fail loudly for unknown parents.
func TestIncrementalParentResolution(t *testing.T) {
	dir := t.TempDir()
	opts := Options{DisableWALSync: true, DisableBackgroundMaintenance: true, LowMemoryMode: true}
	eng, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	err = eng.ApplyBatch([]Mutation{
		{Op: OpCreate, VolID: 1, NodeKey: 1, ParentKey: 0, Name: "root", Flags: FlagDir},
		{Op: OpCreate, VolID: 1, NodeKey: 2, ParentKey: 1, Name: "a", Flags: FlagDir},
		{Op: OpCreate, VolID: 1, NodeKey: 3, ParentKey: 2, Name: "b", Flags: FlagDir},
		{Op: OpCreate, VolID: 2, NodeKey: 1, ParentKey: 0, Name: "root2", Flags: FlagDir},
	})
	if err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
	if err := eng.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := eng.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	eng, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer eng.Close()
	depth2, ok := eng.findLiveByKeyLocked(1, 3)
	if !ok {
		t.Fatal("deep directory missing after reopen")
	}
	err = eng.ApplyBatch([]Mutation{
		{Op: OpCreate, VolID: 1, NodeKey: 4, ParentKey: 3, Name: "deep.txt", Size: 1},
		{Op: OpCreate, VolID: 2, NodeKey: 9, ParentKey: 1, Name: "other-vol.txt", Size: 1},
	})
	if err != nil {
		t.Fatalf("ApplyBatch(incremental): %v", err)
	}
	snap := eng.AcquireSnapshot()
	defer snap.Release()
	deepID, _ := eng.findLiveByKeyLocked(1, 4)
	node, _, ok := snap.LookupNode(deepID)
	if !ok || node.ParentIdx != depth2 {
		t.Fatalf("deep.txt parent = %d, want %d", node.ParentIdx, depth2)
	}
	otherID, _ := eng.findLiveByKeyLocked(2, 9)
	other, _, ok := snap.LookupNode(otherID)
	vol2Root, _ := eng.findLiveByKeyLocked(2, 1)
	if !ok || other.ParentIdx != vol2Root {
		t.Fatalf("other-vol.txt parent = %d, want vol2 root %d", other.ParentIdx, vol2Root)
	}

	err = eng.ApplyBatch([]Mutation{{Op: OpCreate, VolID: 1, NodeKey: 5, ParentKey: 999, Name: "orphan.txt"}})
	if !errors.Is(err, ErrParentNotFound) {
		t.Fatalf("unknown parent err = %v, want ErrParentNotFound", err)
	}
}
