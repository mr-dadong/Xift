package storage_test

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mr-dadong/Xift/internal/query"
	"github.com/mr-dadong/Xift/internal/storage"
)

func buildQuerySnapshotIndex(t *testing.T, dataDir string) {
	t.Helper()
	eng, err := storage.Open(dataDir, storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	b, err := eng.BeginDenseSegment()
	if err != nil {
		t.Fatalf("BeginDenseSegment: %v", err)
	}
	root, err := b.AppendDense(1, 1, storage.RootParentIdx, "vol1", storage.FlagDir, 0, 0, 1_700_000_000)
	if err != nil {
		t.Fatal(err)
	}
	docs, err := b.AppendDense(1, 2, root, "docs", storage.FlagDir, 0, 0, 1_700_000_000)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.AppendDense(1, 10, docs, "alpha_mutex.go", 0, 0, 4096, 1_700_000_000); err != nil {
		t.Fatal(err)
	}
	if _, err := b.AppendDense(1, 11, docs, "beta.md", 0, 0, 2048, 1_700_000_000); err != nil {
		t.Fatal(err)
	}
	meta, err := b.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.CommitSegment(meta, nil); err != nil {
		t.Fatal(err)
	}
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestSnapshotReadOnlyMount verifies mounted packages take part in queries,
// report their snapshot origin, survive compaction, and never affect online
// live counts.
func TestSnapshotReadOnlyMount(t *testing.T) {
	dataDir := t.TempDir()
	buildQuerySnapshotIndex(t, dataDir)

	eng, err := storage.Open(dataDir, storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer eng.Close()

	pkgDir := t.TempDir()
	if _, err := eng.ExportVolumeSnapshot(1, pkgDir, storage.ExportOptions{VolumeLabel: "archive"}); err != nil {
		t.Fatalf("export: %v", err)
	}
	info, err := eng.ImportVolumeSnapshot(pkgDir)
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	liveBefore := eng.LiveCount()

	qe := query.NewEngine(eng)
	ctx := context.Background()
	res, err := qe.Search(ctx, "alpha_mutex", query.SearchOptions{Limit: 10})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	var online, snapshotRow int
	for _, item := range res.Items {
		if item.Name != "alpha_mutex.go" {
			t.Fatalf("unexpected row %+v", item)
		}
		if item.SnapshotVol == "archive" {
			snapshotRow++
			if !item.Offline {
				t.Fatalf("snapshot row must be marked offline: %+v", item)
			}
			if !strings.HasSuffix(strings.ReplaceAll(item.Path, `\`, "/"), "vol1/docs/alpha_mutex.go") {
				t.Fatalf("snapshot row path = %q", item.Path)
			}
			if item.NodeID < info.BaseNodeID {
				t.Fatalf("snapshot row nodeID %d below mount base %d", item.NodeID, info.BaseNodeID)
			}
		} else {
			online++
		}
	}
	if online != 1 || snapshotRow != 1 {
		t.Fatalf("rows online=%d snapshot=%d, want one of each: %+v", online, snapshotRow, res.Items)
	}

	// Compaction must leave the read-only mount untouched.
	if err := eng.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if got := eng.LiveCount(); got != liveBefore {
		t.Fatalf("compact changed online live count: %d -> %d", liveBefore, got)
	}
	res, err = qe.Search(ctx, "alpha_mutex", query.SearchOptions{Limit: 10})
	if err != nil {
		t.Fatalf("Search after compact: %v", err)
	}
	foundSnapshot := false
	for _, item := range res.Items {
		if item.SnapshotVol == "archive" {
			foundSnapshot = true
		}
	}
	if !foundSnapshot {
		t.Fatal("compaction dropped the mounted snapshot segment")
	}

	// A tombstone on an online node never reaches the mount.
	if err := eng.ApplyBatch([]storage.Mutation{{Op: storage.OpDelete, VolID: 1, NodeKey: 10}}); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
	res, err = qe.Search(ctx, "alpha_mutex", query.SearchOptions{Limit: 10})
	if err != nil {
		t.Fatalf("Search after delete: %v", err)
	}
	if len(res.Items) != 1 || res.Items[0].SnapshotVol != "archive" {
		t.Fatalf("after deleting the online node rows = %+v, want only the snapshot row", res.Items)
	}
}

func copyTree(t *testing.T, src, dst string) {
	t.Helper()
	if err := filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, raw, 0o644)
	}); err != nil {
		t.Fatalf("copyTree: %v", err)
	}
}

// TestSnapshotEndToEnd moves a package to another directory before importing,
// then confirms it outlives the source volume.
func TestSnapshotEndToEnd(t *testing.T) {
	dataDir := t.TempDir()
	buildQuerySnapshotIndex(t, dataDir)

	eng, err := storage.Open(dataDir, storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer eng.Close()

	pkgDir := t.TempDir()
	if _, err := eng.ExportVolumeSnapshot(1, pkgDir, storage.ExportOptions{VolumeLabel: "backups"}); err != nil {
		t.Fatalf("export: %v", err)
	}
	moved := t.TempDir()
	copyTree(t, pkgDir, moved)

	info, err := eng.ImportVolumeSnapshot(moved)
	if err != nil {
		t.Fatalf("import from moved package: %v", err)
	}
	if info.VolumeLabel != "backups" || info.Nodes != 4 {
		t.Fatalf("info = %+v", info)
	}

	qe := query.NewEngine(eng)
	ctx := context.Background()
	res, err := qe.Search(ctx, "alpha_mutex", query.SearchOptions{Limit: 10})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(res.Items) != 2 {
		t.Fatalf("rows before offline = %+v, want online + snapshot", res.Items)
	}

	if err := eng.ApplyBatch([]storage.Mutation{
		{Op: storage.OpDelete, VolID: 1, NodeKey: 10},
		{Op: storage.OpDelete, VolID: 1, NodeKey: 2},
		{Op: storage.OpDelete, VolID: 1, NodeKey: 1},
	}); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}

	res, err = qe.Search(ctx, "alpha_mutex", query.SearchOptions{Limit: 10})
	if err != nil {
		t.Fatalf("Search after offline: %v", err)
	}
	if len(res.Items) != 1 {
		t.Fatalf("rows after offline = %+v", res.Items)
	}
	row := res.Items[0]
	if row.SnapshotVol != "backups" || !row.Offline {
		t.Fatalf("offline row not labelled: %+v", row)
	}
	if !strings.HasSuffix(strings.ReplaceAll(row.Path, `\`, "/"), "vol1/docs/alpha_mutex.go") {
		t.Fatalf("offline row path = %q", row.Path)
	}
}
