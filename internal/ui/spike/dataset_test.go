package spike

import (
	"runtime"
	"strings"
	"testing"
)

func TestNewVirtualDataset_ThreeMillionLowHeap(t *testing.T) {
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	ds := NewVirtualDataset(3_000_000)
	if ds.Count() != 3_000_000 {
		t.Fatalf("Count() = %d, want 3000000", ds.Count())
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	var delta uint64
	if after.HeapAlloc > before.HeapAlloc {
		delta = after.HeapAlloc - before.HeapAlloc
	}
	const maxDelta = 32 << 20 // 32 MiB hard ceiling from spec
	if delta >= maxDelta {
		t.Fatalf("heap delta = %d bytes, want < %d bytes", delta, maxDelta)
	}
}

func TestVirtualDataset_DeterministicRandomAccessAndBounds(t *testing.T) {
	ds := NewVirtualDataset(3_000_000)

	for _, idx := range []int{0, 1_500_000, 2_999_999} {
		row1, ok := ds.Row(idx)
		if !ok {
			t.Fatalf("Row(%d) ok = false, want true", idx)
		}
		if row1.ID != idx {
			t.Fatalf("Row(%d).ID = %d, want %d", idx, row1.ID, idx)
		}
		if row1.Name == "" || row1.Dir == "" || row1.Ext == "" || row1.SizeText == "" {
			t.Fatalf("Row(%d) has empty fields: %+v", idx, row1)
		}
		row2, ok := ds.Row(idx)
		if !ok || row1 != row2 {
			t.Fatalf("Row(%d) not deterministic: %+v vs %+v", idx, row1, row2)
		}
	}

	if _, ok := ds.Row(-1); ok {
		t.Fatalf("Row(-1) ok = true, want false")
	}
	if _, ok := ds.Row(3_000_000); ok {
		t.Fatalf("Row(3000000) ok = true, want false")
	}
}

func TestVirtualDataset_QueryFilterCJKAndASCII(t *testing.T) {
	ds := NewVirtualDataset(3_000_000)

	// Chinese IME keyword filter
	ds.SetQuery("设计文档")
	if ds.Query() != "设计文档" {
		t.Fatalf("Query() = %q, want %q", ds.Query(), "设计文档")
	}
	cjkCount := ds.Count()
	if cjkCount <= 0 || cjkCount >= 3_000_000 {
		t.Fatalf("CJK filtered Count() = %d, want 0 < count < 3000000", cjkCount)
	}
	for _, idx := range []int{0, cjkCount / 2, cjkCount - 1} {
		row, ok := ds.Row(idx)
		if !ok {
			t.Fatalf("filtered Row(%d) ok = false", idx)
		}
		if !strings.Contains(row.Name, "设计文档") && !strings.Contains(row.Dir, "设计文档") {
			t.Fatalf("filtered Row(%d) does not match query '设计文档': %+v", idx, row)
		}
	}

	// Case-insensitive ASCII filter
	ds.SetQuery("REPORT")
	asciiCount := ds.Count()
	if asciiCount <= 0 || asciiCount >= 3_000_000 {
		t.Fatalf("ASCII filtered Count() = %d, want 0 < count < 3000000", asciiCount)
	}
	for _, idx := range []int{0, asciiCount / 2, asciiCount - 1} {
		row, ok := ds.Row(idx)
		if !ok {
			t.Fatalf("ASCII filtered Row(%d) ok = false", idx)
		}
		full := strings.ToLower(row.Name + " " + row.Dir)
		if !strings.Contains(full, "report") {
			t.Fatalf("ASCII filtered Row(%d) does not match 'report': %+v", idx, row)
		}
	}

	// Unmatched query
	ds.SetQuery("不存在的关键字xyz999")
	if ds.Count() != 0 {
		t.Fatalf("unmatched Count() = %d, want 0", ds.Count())
	}
	if _, ok := ds.Row(0); ok {
		t.Fatalf("Row(0) on empty filtered result ok = true, want false")
	}

	// Clear query restores full 3,000,000 rows
	ds.SetQuery("")
	if ds.Count() != 3_000_000 {
		t.Fatalf("after clearing query, Count() = %d, want 3000000", ds.Count())
	}
}
