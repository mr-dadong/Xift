package storage

import (
	"sync"
	"testing"
)

func TestSnapshotSegmentViewAndGenerationMVCC(t *testing.T) {
	dir := t.TempDir()
	eng, err := Open(dir, Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer eng.Close()

	b, err := eng.BeginDenseSegment()
	if err != nil {
		t.Fatalf("BeginDenseSegment: %v", err)
	}
	rootID, err := b.AppendDense(1, 1, RootParentIdx, "C:", FlagDir, 0, 0, 100)
	if err != nil {
		t.Fatalf("AppendDense root: %v", err)
	}
	if _, err := b.AppendDense(1, 2, rootID, "alpha.go", 0, 0, 128, 200); err != nil {
		t.Fatalf("AppendDense alpha: %v", err)
	}
	if _, err := b.AppendDense(1, 3, rootID, "beta.go", 0, 0, 256, 300); err != nil {
		t.Fatalf("AppendDense beta: %v", err)
	}
	meta, err := b.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if err := eng.CommitSegment(meta, nil); err != nil {
		t.Fatalf("CommitSegment: %v", err)
	}

	snap1 := eng.AcquireSnapshot()
	defer snap1.Release()
	gen1 := snap1.Generation()
	if gen1 == 0 {
		t.Fatalf("expected non-zero generation after CommitSegment")
	}
	if snap1.SegmentCount() != 1 {
		t.Fatalf("expected 1 segment, got %d", snap1.SegmentCount())
	}

	sv1 := snap1.SegmentSnapshotAt(0)
	if sv1.Count() != 3 || sv1.LiveCount() != 3 {
		t.Fatalf("unexpected counts: count=%d live=%d", sv1.Count(), sv1.LiveCount())
	}
	if len(sv1.HotNodes()) != 3 {
		t.Fatalf("expected 3 hot nodes, got %d", len(sv1.HotNodes()))
	}
	if string(sv1.NameBytesAt(1)) != "alpha.go" {
		t.Fatalf("expected alpha.go, got %q", string(sv1.NameBytesAt(1)))
	}

	// Mutate concurrently: delete alpha.go and add gamma.go.
	if err := eng.ApplyBatch([]Mutation{
		{Op: OpDelete, VolID: 1, NodeKey: 2},
		{Op: OpCreate, VolID: 1, NodeKey: 4, ParentKey: 1, Name: "gamma.go", Size: 512, ModTime: 400},
	}); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}

	// snap1 MUST remain completely isolated at point-in-time gen1.
	if sv1.IsDeleted(1) {
		t.Fatalf("snap1 should still see alpha.go as live")
	}

	snap2 := eng.AcquireSnapshot()
	defer snap2.Release()
	if snap2.Generation() <= gen1 {
		t.Fatalf("expected generation to advance: gen1=%d gen2=%d", gen1, snap2.Generation())
	}
	sv2 := snap2.SegmentSnapshotAt(0)
	if !sv2.IsDeleted(1) {
		t.Fatalf("snap2 should see alpha.go as deleted")
	}
	if sv2.IsDeleted(2) {
		t.Fatalf("snap2 should still see beta.go as live")
	}

	// Concurrent reads on snap1 while Compact runs.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for iter := 0; iter < 50; iter++ {
			nodes := sv1.HotNodes()
			names := sv1.NamesBytes()
			for i := uint32(0); i < sv1.Count(); i++ {
				if sv1.IsDeleted(i) {
					t.Errorf("unexpected deleted in snap1 idx %d", i)
					return
				}
				n := nodes[i]
				_ = names[n.NameOff : n.NameOff+uint32(n.NameLen)]
				_ = sv1.NodeIDAt(i)
			}
		}
	}()
	if err := eng.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	wg.Wait()

	snap3 := eng.AcquireSnapshot()
	defer snap3.Release()
	if snap3.Generation() <= snap2.Generation() {
		t.Fatalf("expected generation to advance after Compact: gen2=%d gen3=%d", snap2.Generation(), snap3.Generation())
	}
}
