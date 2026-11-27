package mftspike

import (
	"os"
	"testing"
	"unsafe"
)

func TestHotNode_SizeIs16Bytes(t *testing.T) {
	if sz := unsafe.Sizeof(HotNode{}); sz != 16 {
		t.Fatalf("unsafe.Sizeof(HotNode{}) = %d, want 16", sz)
	}
}

func TestHotColumns_WriteMmapSearchAndClose(t *testing.T) {
	dir := t.TempDir()

	w, err := NewHotColumnWriter(dir)
	if err != nil {
		t.Fatalf("NewHotColumnWriter: %v", err)
	}

	inputs := []struct {
		name   string
		parent uint32
		flags  uint16
	}{
		{name: "main.go", parent: 0, flags: 0},
		{name: "系统设计文档.md", parent: 0, flags: FlagNonASCII},
		{name: "node_modules", parent: 0, flags: FlagDir | FlagFolded},
		{name: "kernel32.dll", parent: 1, flags: FlagSystem},
	}

	for _, in := range inputs {
		if err := w.Append(in.name, in.parent, in.flags, 1); err != nil {
			t.Fatalf("Append(%q): %v", in.name, err)
		}
	}
	stats, err := w.Close()
	if err != nil {
		t.Fatalf("writer Close: %v", err)
	}
	if stats.NodeCount != 4 || stats.HotColBytes != 64 {
		t.Fatalf("unexpected writer stats: %+v", stats)
	}

	mm, err := OpenMmapHotColumns(dir)
	if err != nil {
		t.Fatalf("OpenMmapHotColumns: %v", err)
	}

	if mm.Count() != 4 {
		_ = mm.Close()
		t.Fatalf("mm.Count() = %d, want 4", mm.Count())
	}
	for i, in := range inputs {
		if got := mm.Name(i); got != in.name {
			_ = mm.Close()
			t.Fatalf("Name(%d) = %q, want %q", i, got, in.name)
		}
		node := mm.Node(i)
		if node.Flags != in.flags {
			_ = mm.Close()
			t.Fatalf("Node(%d).Flags = %#x, want %#x", i, node.Flags, in.flags)
		}
	}

	cjkHits := mm.SearchSubstring("设计文档", 10)
	if len(cjkHits) != 1 || cjkHits[0] != 1 {
		_ = mm.Close()
		t.Fatalf("SearchSubstring('设计文档') = %v, want [1]", cjkHits)
	}

	asciiHits := mm.SearchSubstring("KERNEL32", 10)
	if len(asciiHits) != 1 || asciiHits[0] != 3 {
		_ = mm.Close()
		t.Fatalf("SearchSubstring('KERNEL32') = %v, want [3]", asciiHits)
	}

	if err := mm.Close(); err != nil {
		t.Fatalf("mm.Close(): %v", err)
	}

	// Ensure Windows mmap handles are completely released so directory can be removed cleanly.
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("os.RemoveAll(%q) after Close failed (leaked mmap handle?): %v", dir, err)
	}
}
