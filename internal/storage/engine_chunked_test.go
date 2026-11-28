package storage

import (
	"fmt"
	"testing"
)

// buildChunkedCorpus writes root + 10 dirs + files through a DenseWriter with
// the given per-segment node cap.
func buildChunkedCorpus(t *testing.T, files, segMax int) (*Engine, map[uint32]string) {
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

	dw, err := eng.NewDenseWriter(segMax)
	if err != nil {
		t.Fatalf("NewDenseWriter: %v", err)
	}
	if _, err := dw.Append(1, 1, RootParentIdx, "root", FlagDir, 0, 0, 1); err != nil {
		t.Fatalf("append root: %v", err)
	}
	const dirs = 10
	dirIDs := make([]uint32, dirs)
	for i := 0; i < dirs; i++ {
		name := fmt.Sprintf("dir_%02d", i)
		id, err := dw.Append(1, uint64(i+2), RootParentIdx, name, FlagDir, 0, 0, int64(i+2))
		if err != nil {
			t.Fatalf("append %s: %v", name, err)
		}
		dirIDs[i] = id
	}
	for i := 0; i < files; i++ {
		name := fmt.Sprintf("report_%05d.txt", i)
		if _, err := dw.Append(1, uint64(2+dirs+i), dirIDs[i%dirs], name, 0, 0, int64(i), int64(2+dirs+i)); err != nil {
			t.Fatalf("append %s: %v", name, err)
		}
	}
	if _, err := dw.Commit(nil); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	snap := eng.AcquireSnapshot()
	defer snap.Release()
	names := make(map[uint32]string)
	for seg := 0; seg < snap.SegmentCount(); seg++ {
		ss := snap.SegmentSnapshotAt(seg)
		for i := uint32(0); i < ss.Count(); i++ {
			if ss.IsDeleted(i) {
				continue
			}
			names[ss.NodeIDAt(i)] = string(ss.NameBytesAt(i))
		}
	}
	return eng, names
}

// TestChunkedBuildEquivalence verifies splitting a volume across segments
// preserves node identity, parent links, and names.
func TestChunkedBuildEquivalence(t *testing.T) {
	const files = 3000
	single, singleNames := buildChunkedCorpus(t, files, 1_000_000)
	chunked, chunkedNames := buildChunkedCorpus(t, files, 1000)

	if got := len(single.segments); got != 1 {
		t.Fatalf("single build segments = %d, want 1", got)
	}
	if got := len(chunked.segments); got < 3 {
		t.Fatalf("chunked build segments = %d, want >= 3", got)
	}
	if len(singleNames) != len(chunkedNames) {
		t.Fatalf("node count mismatch: %d vs %d", len(singleNames), len(chunkedNames))
	}
	for id, want := range singleNames {
		if got := chunkedNames[id]; got != want {
			t.Fatalf("node %d name = %q, want %q", id, got, want)
		}
	}

	// Cross-segment parent links must resolve to the directory's global NodeID.
	snap := chunked.AcquireSnapshot()
	defer snap.Release()
	dirIDs := make(map[string]uint32)
	for id, name := range chunkedNames {
		if len(name) == 6 && name[:4] == "dir_" {
			dirIDs[name] = id
		}
	}
	for id, name := range chunkedNames {
		if len(name) < 6 || name[:6] != "report" {
			continue
		}
		node, _, ok := snap.LookupNode(id)
		if !ok {
			t.Fatalf("node %d missing", id)
		}
		// report_NNNNN.txt is placed in dir_(N % 10).
		var n int
		if _, err := fmt.Sscanf(name, "report_%05d.txt", &n); err != nil {
			t.Fatalf("parse %q: %v", name, err)
		}
		wantParent := dirIDs[fmt.Sprintf("dir_%02d", n%10)]
		if node.ParentIdx != wantParent {
			t.Fatalf("%s parent = %d, want %d", name, node.ParentIdx, wantParent)
		}
	}
}

// TestChunkedBuildCrashRecovery verifies an interrupted chunked build only
// exposes committed segments after reopen and leaves no stray temp files.
func TestChunkedBuildCrashRecovery(t *testing.T) {
	dir := t.TempDir()
	opts := Options{DisableWALSync: true, DisableBackgroundMaintenance: true, LowMemoryMode: true}
	eng, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	dw, err := eng.NewDenseWriter(100)
	if err != nil {
		t.Fatalf("NewDenseWriter: %v", err)
	}
	for i := 0; i < 250; i++ {
		if _, err := dw.Append(1, uint64(i+1), RootParentIdx, fmt.Sprintf("f_%04d.txt", i), 0, 0, 1, int64(i)); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	// Simulate a crash mid-build: the third chunk never reaches MANIFEST.
	if err := eng.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// A real crash leaves file handles behind; release them so Windows can clean
	// up the temp dir. The chunk was never committed either way.
	dw.Abort()

	eng, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer eng.Close()
	committed := 0
	for i := range eng.segments {
		committed += int(eng.segments[i].Count())
	}
	if committed != 200 {
		t.Fatalf("committed nodes = %d, want 200 (two chunks)", committed)
	}
	if err := eng.ApplyBatch([]Mutation{{Op: OpCursorUpdate, VolID: 1, Cursor: 1}}); err != nil {
		t.Fatalf("ApplyBatch after recovery: %v", err)
	}
}

// TestCompactionThreshold verifies tiered compaction triggers on the segment
// count threshold (32) rather than the old 8-segment cutoff.
func TestCompactionThreshold(t *testing.T) {
	eng, err := Open(t.TempDir(), Options{
		DisableWALSync:               true,
		DisableBackgroundMaintenance: true,
		LowMemoryMode:                true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer eng.Close()

	appendN := func(dw *DenseWriter, from, to int) {
		t.Helper()
		for i := from; i < to; i++ {
			if _, err := dw.Append(1, uint64(i+1), RootParentIdx, fmt.Sprintf("f_%04d.txt", i), 0, 0, 1, int64(i)); err != nil {
				t.Fatalf("append %d: %v", i, err)
			}
		}
	}

	eng.mu.Lock()
	threshold := tier2CompactionSegmentThreshold
	eng.mu.Unlock()

	// One node per segment: 8 segments stay below the new threshold.
	dw, err := eng.NewDenseWriter(1)
	if err != nil {
		t.Fatalf("NewDenseWriter: %v", err)
	}
	appendN(dw, 0, 4)
	if _, err := dw.Commit(nil); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if got := len(eng.segments); got >= 8 {
		t.Fatalf("segments = %d, want < 8 for this assertion", got)
	}
	if eng.needsTieredCompactionLocked() {
		t.Fatalf("compaction must not trigger at %d segments", len(eng.segments))
	}

	// Cross the count threshold.
	dw, err = eng.NewDenseWriter(1)
	if err != nil {
		t.Fatalf("NewDenseWriter: %v", err)
	}
	appendN(dw, 4, threshold+2)
	if _, err := dw.Commit(nil); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if !eng.needsTieredCompactionLocked() {
		t.Fatalf("compaction must trigger at %d segments", len(eng.segments))
	}
}
