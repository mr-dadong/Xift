package storage

import (
	"os"
	"path/filepath"
	"testing"
)

type snapshotFixture struct {
	ids  map[string]uint32
	keys map[string]uint64
}

// buildSnapshotIndex creates a two-volume Tier 1 index:
//
//	vol1: vol1/ (key 1) -> docs/ (key 2) -> alpha_mutex.go (10), beta.md (11)
//	vol2: vol2/ (key 3) -> gamma.txt (20)
func buildSnapshotIndex(t *testing.T, dataDir string) snapshotFixture {
	t.Helper()
	eng, err := Open(dataDir, Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	b, err := eng.BeginDenseSegment()
	if err != nil {
		t.Fatalf("BeginDenseSegment: %v", err)
	}
	fx := snapshotFixture{ids: map[string]uint32{}, keys: map[string]uint64{}}
	add := func(vol uint16, key uint64, parent uint32, name string, flags uint16, size int64) {
		id, err := b.AppendDense(vol, key, parent, name, flags, 0, size, 1_700_000_000)
		if err != nil {
			t.Fatalf("AppendDense %s: %v", name, err)
		}
		fx.ids[name] = id
		fx.keys[name] = key
	}
	add(1, 1, RootParentIdx, "vol1", FlagDir, 0)
	add(1, 2, fx.ids["vol1"], "docs", FlagDir, 0)
	add(1, 10, fx.ids["docs"], "alpha_mutex.go", 0, 4096)
	add(1, 11, fx.ids["docs"], "beta.md", 0, 2048)
	add(2, 3, RootParentIdx, "vol2", FlagDir, 0)
	add(2, 20, fx.ids["vol2"], "gamma.txt", 0, 512)

	meta, err := b.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if err := eng.CommitSegment(meta, nil); err != nil {
		t.Fatalf("CommitSegment: %v", err)
	}
	if err := eng.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return fx
}

func openSnapshotIndex(t *testing.T, dataDir string) *Engine {
	t.Helper()
	eng, err := Open(dataDir, Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	return eng
}

func liveNames(t *testing.T, snap *Snapshot, label string) map[string]uint32 {
	t.Helper()
	out := make(map[string]uint32)
	snap.ForEachLive(func(id uint32, node HotNode, name []byte) bool {
		if label != "" {
			if got, _, ok := snap.SnapshotSource(id); !ok || got != label {
				return true
			}
		}
		out[string(name)] = id
		return true
	})
	return out
}

// TestSnapshotExport verifies a volume exports as a clean, self-contained
// package and the source index is left untouched.
func TestSnapshotExport(t *testing.T) {
	dataDir := t.TempDir()
	buildSnapshotIndex(t, dataDir)
	eng := openSnapshotIndex(t, dataDir)

	manifestBefore, err := os.ReadFile(filepath.Join(dataDir, "index", "MANIFEST"))
	if err != nil {
		t.Fatalf("read MANIFEST: %v", err)
	}
	liveBefore := eng.LiveCount()

	out := t.TempDir()
	pkg, err := eng.ExportVolumeSnapshot(1, out, ExportOptions{VolumeLabel: "data", RootPath: `D:\vol1`})
	if err != nil {
		t.Fatalf("ExportVolumeSnapshot: %v", err)
	}
	if pkg.Nodes != 4 || pkg.VolumeID != 1 || pkg.VolumeLabel != "data" || pkg.RootPath != `D:\vol1` {
		t.Fatalf("package = %+v", pkg)
	}
	if len(pkg.Files) == 0 {
		t.Fatal("package manifest lists no files")
	}
	if _, err := os.Stat(filepath.Join(out, SnapshotManifestName)); err != nil {
		t.Fatalf("snapshot.json: %v", err)
	}

	manifestAfter, err := os.ReadFile(filepath.Join(dataDir, "index", "MANIFEST"))
	if err != nil {
		t.Fatalf("read MANIFEST after: %v", err)
	}
	if string(manifestBefore) != string(manifestAfter) {
		t.Fatal("export modified the source MANIFEST")
	}
	if got := eng.LiveCount(); got != liveBefore {
		t.Fatalf("export changed live count: %d -> %d", liveBefore, got)
	}

	// The package only contains volume 1 nodes with consistent parents.
	seg, err := OpenSegmentWithCache(filepath.Join(out, pkg.Segment), 0, nil)
	if err != nil {
		t.Fatalf("open exported segment: %v", err)
	}
	defer seg.Release()
	if seg.meta.NodeCount != 4 {
		t.Fatalf("exported node count = %d", seg.meta.NodeCount)
	}
	names := make(map[string]bool)
	for i := uint32(0); i < seg.meta.NodeCount; i++ {
		node := seg.NodeAt(i)
		if node.VolID != 1 {
			t.Fatalf("exported node %d volID = %d", i, node.VolID)
		}
		names[string(seg.NameBytesAt(i))] = true
		if string(seg.NameBytesAt(i)) == "vol1" && node.ParentIdx != RootParentIdx {
			t.Fatalf("volume root parent = %d", node.ParentIdx)
		}
		if string(seg.NameBytesAt(i)) == "alpha_mutex.go" && node.ParentIdx == RootParentIdx {
			t.Fatal("exported child lost its parent link")
		}
	}
	if len(names) != 4 || names["gamma.txt"] {
		t.Fatalf("exported names = %v", names)
	}
}

// TestSnapshotImportValidates verifies corruption is rejected before mounting.
func TestSnapshotImportValidates(t *testing.T) {
	dataDir := t.TempDir()
	buildSnapshotIndex(t, dataDir)
	eng := openSnapshotIndex(t, dataDir)

	out := t.TempDir()
	if _, err := eng.ExportVolumeSnapshot(1, out, ExportOptions{VolumeLabel: "data"}); err != nil {
		t.Fatalf("ExportVolumeSnapshot: %v", err)
	}

	segFile := filepath.Join(out, snapshotSegDirName, "hot.col")
	raw, err := os.ReadFile(segFile)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)/2] ^= 0xFF
	if err := os.WriteFile(segFile, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := eng.ImportVolumeSnapshot(out); err == nil {
		t.Fatal("corrupt package must be rejected")
	}
	if got := len(eng.Snapshots()); got != 0 {
		t.Fatalf("corrupt package was mounted (%d mounts)", got)
	}
	if entries, err := os.ReadDir(filepath.Join(dataDir, "index", "snapshots")); err == nil && len(entries) != 0 {
		t.Fatalf("failed import left %d directories behind", len(entries))
	}
}

// TestOfflineVolumeStillSearchable verifies snapshot rows survive deletion of
// the online volume's nodes and that filesystem deletes never touch packages.
func TestOfflineVolumeStillSearchable(t *testing.T) {
	dataDir := t.TempDir()
	fx := buildSnapshotIndex(t, dataDir)
	eng := openSnapshotIndex(t, dataDir)

	out := t.TempDir()
	if _, err := eng.ExportVolumeSnapshot(1, out, ExportOptions{VolumeLabel: "archive"}); err != nil {
		t.Fatalf("ExportVolumeSnapshot: %v", err)
	}
	info, err := eng.ImportVolumeSnapshot(out)
	if err != nil {
		t.Fatalf("ImportVolumeSnapshot: %v", err)
	}
	if info.BaseNodeID == 0 || info.Nodes != 4 {
		t.Fatalf("mount info = %+v", info)
	}

	// Take the source volume offline by tombstoning its live nodes.
	muts := make([]Mutation, 0, 4)
	for _, name := range []string{"alpha_mutex.go", "beta.md", "docs", "vol1"} {
		muts = append(muts, Mutation{Op: OpDelete, VolID: 1, NodeKey: fx.keys[name]})
	}
	if err := eng.ApplyBatch(muts); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}

	snap := eng.AcquireSnapshot()
	defer snap.Release()
	if _, _, ok := snap.LookupNode(fx.ids["alpha_mutex.go"]); ok {
		t.Fatal("online node still present after delete")
	}
	names := liveNames(t, snap, "archive")
	if _, ok := names["alpha_mutex.go"]; !ok {
		t.Fatalf("snapshot rows lost after source volume went offline: %v", names)
	}
	if _, ok := names["beta.md"]; !ok {
		t.Fatalf("snapshot rows incomplete: %v", names)
	}
	if got := eng.LiveCount(); got != 2 {
		t.Fatalf("engine live count = %d, want only vol2 root and its file", got)
	}

	// The package directory is a copy: deleting online files never touches it.
	if _, err := os.Stat(filepath.Join(dataDir, "index", "snapshots")); err != nil {
		t.Fatalf("snapshot directory missing: %v", err)
	}
}

// TestSnapshotRemoveIsolated verifies removal drops only the mount and its copy.
func TestSnapshotRemoveIsolated(t *testing.T) {
	dataDir := t.TempDir()
	buildSnapshotIndex(t, dataDir)
	eng := openSnapshotIndex(t, dataDir)

	out := t.TempDir()
	if _, err := eng.ExportVolumeSnapshot(1, out, ExportOptions{VolumeLabel: "data"}); err != nil {
		t.Fatalf("ExportVolumeSnapshot: %v", err)
	}
	info, err := eng.ImportVolumeSnapshot(out)
	if err != nil {
		t.Fatalf("ImportVolumeSnapshot: %v", err)
	}
	liveBefore := eng.LiveCount()

	if err := eng.RemoveSnapshot(info.ID); err != nil {
		t.Fatalf("RemoveSnapshot: %v", err)
	}
	if got := len(eng.Snapshots()); got != 0 {
		t.Fatalf("snapshots after remove = %d", got)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "index", filepath.FromSlash(info.Package))); !os.IsNotExist(err) {
		t.Fatalf("snapshot copy still present: %v", err)
	}
	if got := eng.LiveCount(); got != liveBefore {
		t.Fatalf("remove changed online live count: %d -> %d", liveBefore, got)
	}

	snap := eng.AcquireSnapshot()
	defer snap.Release()
	names := liveNames(t, snap, "")
	for _, want := range []string{"alpha_mutex.go", "beta.md", "gamma.txt"} {
		if _, ok := names[want]; !ok {
			t.Fatalf("online L1 row %q lost after snapshot remove: %v", want, names)
		}
	}

	// Reopen: the registry no longer lists the removed package.
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}
	again := openSnapshotIndex(t, dataDir)
	if got := len(again.Snapshots()); got != 0 {
		t.Fatalf("removed snapshot reappeared after reopen: %d", got)
	}
}

// TestSnapshotMountPersists verifies an imported package is remounted on reopen.
func TestSnapshotMountPersists(t *testing.T) {
	dataDir := t.TempDir()
	buildSnapshotIndex(t, dataDir)
	eng := openSnapshotIndex(t, dataDir)

	out := t.TempDir()
	if _, err := eng.ExportVolumeSnapshot(1, out, ExportOptions{VolumeLabel: "keep"}); err != nil {
		t.Fatalf("ExportVolumeSnapshot: %v", err)
	}
	info, err := eng.ImportVolumeSnapshot(out)
	if err != nil {
		t.Fatalf("ImportVolumeSnapshot: %v", err)
	}
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}

	again := openSnapshotIndex(t, dataDir)
	mounts := again.Snapshots()
	if len(mounts) != 1 || mounts[0].ID != info.ID || mounts[0].BaseNodeID != info.BaseNodeID {
		t.Fatalf("reopened mounts = %+v, want %+v", mounts, info)
	}
	snap := again.AcquireSnapshot()
	defer snap.Release()
	names := liveNames(t, snap, "keep")
	if _, ok := names["alpha_mutex.go"]; !ok {
		t.Fatalf("remounted snapshot rows missing: %v", names)
	}
}
