package lsm

import (
	"bytes"
	"math"
	"os"
	"path/filepath"
	"testing"
)

// spillGramKeys produces a realistic mix: a constant dense gram (crosses the
// array/bitmap threshold), a medium-frequency gram, and sparse grams.
func spillGramKeys(i int) []uint64 {
	return []uint64{
		7,                       // constant: cardinality == name count
		uint64(100 + i%97),      // medium frequency
		uint64(10_000 + i%5000), // sparse buckets
	}
}

// TestInvertedSpillEquivalence verifies that spilling and merging produces a
// byte-identical inverted index file to the fully in-memory build.
func TestInvertedSpillEquivalence(t *testing.T) {
	const names = 7000
	dir := t.TempDir()

	build := func(spill bool, out string) InvertedStats {
		b := NewInvertedBuilder()
		if spill {
			b.ConfigureSpill(dir, 1024) // tiny budget: force many runs
		}
		for i := 0; i < names; i++ {
			if err := b.AddName(spillGramKeys(i), uint32(i)); err != nil {
				t.Fatalf("AddName(%d): %v", i, err)
			}
		}
		st, err := b.Finish(out, math.MaxInt32)
		if err != nil {
			t.Fatalf("Finish(spill=%v): %v", spill, err)
		}
		return st
	}

	memPath := filepath.Join(dir, "mem.idx")
	spillPath := filepath.Join(dir, "spill.idx")
	memStats := build(false, memPath)
	spillStats := build(true, spillPath)
	if memStats.GramCount < 2 || memStats.GramCount != spillStats.GramCount {
		t.Fatalf("gram counts: mem=%d spill=%d", memStats.GramCount, spillStats.GramCount)
	}
	// Dense gram 7 must be a bitmap container, exercising the non-array path.
	if memStats.MaxDF < 4096 {
		t.Fatalf("MaxDF = %d, want >= 4096", memStats.MaxDF)
	}

	memBytes, err := os.ReadFile(memPath)
	if err != nil {
		t.Fatalf("read mem: %v", err)
	}
	spillBytes, err := os.ReadFile(spillPath)
	if err != nil {
		t.Fatalf("read spill: %v", err)
	}
	if !bytes.Equal(memBytes, spillBytes) {
		t.Fatalf("spilled index differs: %d vs %d bytes", len(spillBytes), len(memBytes))
	}

	// Spill temporaries must be gone.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".tmp" || filepath.Ext(e.Name()) == ".post" {
			t.Fatalf("leftover temp file %q", e.Name())
		}
	}
}

// TestInvertedSpillLookup verifies the merged index answers lookups exactly
// like the in-memory build at the reader level.
func TestInvertedSpillLookup(t *testing.T) {
	const names = 3000
	dir := t.TempDir()
	b := NewInvertedBuilder()
	b.ConfigureSpill(dir, 256)
	for i := 0; i < names; i++ {
		if err := b.AddName(spillGramKeys(i), uint32(i)); err != nil {
			t.Fatalf("AddName: %v", err)
		}
	}
	path := filepath.Join(dir, "spill.idx")
	if _, err := b.Finish(path, math.MaxInt32); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	ix, err := OpenInvertedIndex(path, 1, nil)
	if err != nil {
		t.Fatalf("OpenInvertedIndex: %v", err)
	}
	defer ix.Close()
	df, ok := ix.Df(1, 0, 7)
	if !ok || df != names {
		t.Fatalf("Df(7) = (%d,%v), want (%d,true)", df, ok, names)
	}
	raw, ok, err := ix.LookupPosting(1, 0, 7)
	if err != nil || !ok {
		t.Fatalf("LookupPosting(7) = (%v,%v)", ok, err)
	}
	v, err := ParseRoaring(raw)
	if err != nil {
		t.Fatalf("ParseRoaring: %v", err)
	}
	if got := v.Cardinality(); got != names {
		t.Fatalf("cardinality = %d, want %d", got, names)
	}
	for i := uint32(0); i < names; i++ {
		if !v.Contains(i) {
			t.Fatalf("posting missing %d", i)
		}
	}
}
