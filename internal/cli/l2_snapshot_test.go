package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/mr-dadong/Xift/internal/l2search"
	"github.com/mr-dadong/Xift/internal/storage"
)

func seedCLISnapshotIndex(t *testing.T, dataDir string) {
	t.Helper()
	eng, err := storage.Open(dataDir, storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	b, err := eng.BeginDenseSegment()
	if err != nil {
		t.Fatalf("BeginDenseSegment: %v", err)
	}
	root, err := b.AppendDense(1, 1, storage.RootParentIdx, "proj", storage.FlagDir, 0, 0, 1_700_000_000)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.AppendDense(1, 2, root, "main.go", 0, 0, 128, 1_700_000_000); err != nil {
		t.Fatal(err)
	}
	if _, err := b.AppendDense(1, 3, root, "notes.txt", 0, 0, 64, 1_700_000_000); err != nil {
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

func runCLI(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var out, errBuf bytes.Buffer
	code := Run(context.Background(), args, &out, &errBuf)
	return code, out.String(), errBuf.String()
}

// TestSnapshotAndL2Commands exercises the snapshot package lifecycle and the
// L2 whitelist CLI, including non-zero exits for bad arguments.
func TestSnapshotAndL2Commands(t *testing.T) {
	dataDir := t.TempDir()
	seedCLISnapshotIndex(t, dataDir)
	pkgDir := t.TempDir()

	code, out, errStr := runCLI(t, "snapshot", "export", "--data-dir", dataDir, "--volume", "1", "--out", pkgDir, "--label", "proj", "--json")
	if code != 0 {
		t.Fatalf("snapshot export exited %d: %s", code, errStr)
	}
	var pkg storage.SnapshotPackage
	if err := json.Unmarshal([]byte(out), &pkg); err != nil {
		t.Fatalf("export json: %v (%s)", err, out)
	}
	if pkg.VolumeID != 1 || pkg.Nodes != 3 || pkg.VolumeLabel != "proj" {
		t.Fatalf("export package = %+v", pkg)
	}

	code, out, errStr = runCLI(t, "snapshot", "import", "--data-dir", dataDir, "--json", pkgDir)
	if code != 0 {
		t.Fatalf("snapshot import exited %d: %s", code, errStr)
	}
	var info storage.SnapshotInfo
	if err := json.Unmarshal([]byte(out), &info); err != nil {
		t.Fatalf("import json: %v (%s)", err, out)
	}
	if info.ID != 1 || info.Nodes != 3 || info.VolumeLabel != "proj" {
		t.Fatalf("import info = %+v", info)
	}

	code, out, errStr = runCLI(t, "snapshot", "list", "--data-dir", dataDir, "--json")
	if code != 0 {
		t.Fatalf("snapshot list exited %d: %s", code, errStr)
	}
	var list SnapshotListOutput
	if err := json.Unmarshal([]byte(out), &list); err != nil {
		t.Fatalf("list json: %v (%s)", err, out)
	}
	if len(list.Snapshots) != 1 || list.Snapshots[0].ID != 1 {
		t.Fatalf("list = %+v", list)
	}

	code, _, errStr = runCLI(t, "snapshot", "remove", "--data-dir", dataDir, "1")
	if code != 0 {
		t.Fatalf("snapshot remove exited %d: %s", code, errStr)
	}
	code, out, errStr = runCLI(t, "snapshot", "list", "--data-dir", dataDir, "--json")
	if code != 0 {
		t.Fatalf("snapshot list after remove exited %d: %s", code, errStr)
	}
	if err := json.Unmarshal([]byte(out), &list); err != nil {
		t.Fatal(err)
	}
	if len(list.Snapshots) != 0 {
		t.Fatalf("snapshot still mounted: %+v", list.Snapshots)
	}

	// Bad arguments are usage errors.
	if code, _, _ := runCLI(t, "snapshot", "export", "--data-dir", dataDir); code != 2 {
		t.Fatalf("missing --volume/--out exit = %d, want 2", code)
	}
	if code, _, _ := runCLI(t, "snapshot", "remove", "--data-dir", dataDir, "not-a-number"); code != 2 {
		t.Fatalf("bad snapshot id exit = %d, want 2", code)
	}

	// L2 whitelist add / status / remove.
	root := t.TempDir()
	code, out, errStr = runCLI(t, "l2", "add", "--data-dir", dataDir, "--json", root)
	if code != 0 {
		t.Fatalf("l2 add exited %d: %s", code, errStr)
	}
	var rootsResp struct {
		Roots []string `json:"roots"`
	}
	if err := json.Unmarshal([]byte(out), &rootsResp); err != nil {
		t.Fatalf("l2 add json: %v (%s)", err, out)
	}
	if len(rootsResp.Roots) != 1 || rootsResp.Roots[0] != root {
		t.Fatalf("roots = %v, want [%s]", rootsResp.Roots, root)
	}

	code, out, errStr = runCLI(t, "l2", "status", "--data-dir", dataDir, "--json")
	if code != 0 {
		t.Fatalf("l2 status exited %d: %s", code, errStr)
	}
	var st L2StatusOutput
	if err := json.Unmarshal([]byte(out), &st); err != nil {
		t.Fatalf("l2 status json: %v (%s)", err, out)
	}
	if !st.Enabled || len(st.Roots) != 1 || st.Roots[0] != root {
		t.Fatalf("l2 status = %+v", st)
	}

	code, out, errStr = runCLI(t, "l2", "remove", "--data-dir", dataDir, "--json", root)
	if code != 0 {
		t.Fatalf("l2 remove exited %d: %s", code, errStr)
	}
	if err := json.Unmarshal([]byte(out), &rootsResp); err != nil {
		t.Fatal(err)
	}
	if len(rootsResp.Roots) != 0 {
		t.Fatalf("roots after remove = %v", rootsResp.Roots)
	}

	if code, _, _ := runCLI(t, "l2", "add", "--data-dir", dataDir, filepath.Join(root, "missing")); code != 1 {
		t.Fatalf("l2 add missing dir exit = %d, want 1", code)
	}
	if code, _, _ := runCLI(t, "l2", "bogus"); code != 2 {
		t.Fatalf("l2 bogus exit = %d, want 2", code)
	}
}

// TestL2StatusJSON verifies `l2 status --json` reports the whitelist, indexed
// counts, per-reason skip counters, disk usage/cap and the paused flag.
func TestL2StatusJSON(t *testing.T) {
	dataDir := t.TempDir()
	dir := filepath.Join(dataDir, "l2")
	root := t.TempDir()

	st, err := l2search.OpenStore(dir, nil)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	if err := st.AddSegment([]l2search.Doc{
		{Key: l2search.DocKey{VolID: 1, NodeKey: 1}, Text: []byte("alpha mutex")},
		{Key: l2search.DocKey{VolID: 1, NodeKey: 2}, Text: []byte("beta mutex")},
	}); err != nil {
		t.Fatalf("AddSegment: %v", err)
	}
	_ = st.Close()

	if err := l2search.SaveRoots(dir, []string{root}); err != nil {
		t.Fatal(err)
	}
	if err := l2search.SaveStats(dir, l2search.PersistedStats{
		Indexed: 2,
		Deleted: 1,
		Batches: 1,
		Skipped: map[string]int{"binary": 3, "oversized": 1},
		Paused:  true,
		DiskCap: 1 << 20,
	}); err != nil {
		t.Fatal(err)
	}

	code, out, errStr := runCLI(t, "l2", "status", "--data-dir", dataDir, "--json")
	if code != 0 {
		t.Fatalf("l2 status exited %d: %s", code, errStr)
	}
	var got L2StatusOutput
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("json: %v (%s)", err, out)
	}
	if !got.Enabled || len(got.Roots) != 1 || got.Roots[0] != root {
		t.Fatalf("roots = %+v", got)
	}
	if got.Indexed != 2 || got.Deleted != 1 || got.Batches != 1 {
		t.Fatalf("counters = %+v", got)
	}
	if got.Docs != 2 || got.Segments != 1 {
		t.Fatalf("store counters = %+v", got)
	}
	if got.Skipped["binary"] != 3 || got.Skipped["oversized"] != 1 {
		t.Fatalf("skipped = %+v", got.Skipped)
	}
	if !got.Paused {
		t.Fatal("paused flag not reported")
	}
	if got.DiskUsed <= 0 || got.DiskCap != 1<<20 {
		t.Fatalf("disk = %d / %d", got.DiskUsed, got.DiskCap)
	}
}
