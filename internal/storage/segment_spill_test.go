package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// buildSpillCorpus writes n files through a Tier 2 flush with the given build
// budget and returns the segment directory.
func buildSpillCorpus(t *testing.T, n int, budget int64) string {
	t.Helper()
	dir := t.TempDir()
	eng, err := Open(dir, Options{
		DisableWALSync:               true,
		DisableBackgroundMaintenance: true,
		LowMemoryMode:                true,
		BuildBudgetBytes:             budget,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer eng.Close()

	batch := make([]Mutation, 0, n+12)
	batch = append(batch, Mutation{Op: OpCreate, VolID: 1, NodeKey: 1, ParentKey: 0, Name: "root", Flags: FlagDir})
	const dirs = 8
	for i := 0; i < dirs; i++ {
		batch = append(batch, Mutation{
			Op: OpCreate, VolID: 1, NodeKey: uint64(i + 2), ParentKey: 1,
			Name: fmt.Sprintf("dir_%02d", i), Flags: FlagDir,
		})
	}
	for i := 0; i < n; i++ {
		batch = append(batch, Mutation{
			Op: OpCreate, VolID: 1, NodeKey: uint64(2 + dirs + i), ParentKey: uint64(2 + i%dirs),
			Name: fmt.Sprintf("report_%05d.txt", i), Size: int64(i),
		})
	}
	if err := eng.ApplyBatch(batch); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
	if err := eng.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	return filepath.Join(dir, "index", "segments", "seg_000001")
}

// TestNameExternalSortEquivalence verifies a spill-forced build produces the
// same segment files as an in-memory build.
func TestNameExternalSortEquivalence(t *testing.T) {
	const files = 2000
	memSeg := buildSpillCorpus(t, files, 1<<30)
	spillSeg := buildSpillCorpus(t, files, 4096) // forces name + posting runs

	for _, name := range []string{"names.blk", "names.idx", "trigram.idx", "bigram.idx", "hot.col", "keys.idx"} {
		memBytes, err := os.ReadFile(filepath.Join(memSeg, name))
		if err != nil {
			t.Fatalf("read mem %s: %v", name, err)
		}
		spillBytes, err := os.ReadFile(filepath.Join(spillSeg, name))
		if err != nil {
			t.Fatalf("read spill %s: %v", name, err)
		}
		if !bytes.Equal(memBytes, spillBytes) {
			t.Fatalf("%s differs between in-memory and spilled builds (%d vs %d bytes)", name, len(memBytes), len(spillBytes))
		}
	}
	entries, err := os.ReadDir(spillSeg)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, e := range entries {
		if ext := filepath.Ext(e.Name()); ext == ".tmp" || ext == ".post" {
			t.Fatalf("leftover build temp file %q", e.Name())
		}
	}
}
