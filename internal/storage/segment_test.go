package storage

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"unsafe"
)

func TestSegmentDenseAndDeltaLifecycle(t *testing.T) {
	if got := unsafe.Sizeof(keyIndexEntry{}); got != 16 {
		t.Fatalf("unsafe.Sizeof(keyIndexEntry) = %d, want 16", got)
	}
	assertNoPointers(t, reflect.TypeOf(keyIndexEntry{}))

	dir := t.TempDir()
	extPool := NewExtPool()

	// 1. Build a Dense base segment.
	denseDir := filepath.Join(dir, "seg_000001")
	b, err := NewSegmentBuilder(denseDir, 1, 0, true, extPool)
	if err != nil {
		t.Fatalf("NewSegmentBuilder dense: %v", err)
	}
	if err := b.Append(0, 1, 100, RootParentIdx, "C:", FlagDir, 0, 0, 1700000000); err != nil {
		t.Fatalf("append root: %v", err)
	}
	if err := b.Append(1, 1, 200, 0, "node_modules", FlagDir|FlagFolded, 0, 0, 1700000001); err != nil {
		t.Fatalf("append folded dir: %v", err)
	}
	if err := b.Append(2, 1, 300, 0, "设计文档.md", 0, extPool.InternFromName("设计文档.md", false), 4096, 1700000002); err != nil {
		t.Fatalf("append file: %v", err)
	}
	meta, err := b.Finish()
	if err != nil {
		t.Fatalf("Finish dense: %v", err)
	}
	if meta.NodeCount != 3 || !meta.Dense || meta.BaseNodeID != 0 {
		t.Fatalf("unexpected dense meta: %+v", meta)
	}

	seg, err := OpenSegment(denseDir)
	if err != nil {
		t.Fatalf("OpenSegment dense: %v", err)
	}
	defer seg.Release()

	if seg.Count() != 3 || seg.LiveCount() != 3 {
		t.Fatalf("count=%d live=%d, want 3/3", seg.Count(), seg.LiveCount())
	}
	if local, ok := seg.LookupNodeID(2); !ok || local != 2 {
		t.Fatalf("LookupNodeID(2) = (%d,%v), want (2,true)", local, ok)
	}
	if got := string(seg.NameBytesAt(2)); got != "设计文档.md" {
		t.Fatalf("NameBytesAt(2) = %q", got)
	}
	if (seg.NodeAt(2).Flags & FlagNonASCII) == 0 {
		t.Fatalf("expected FlagNonASCII on UTF-8 filename")
	}
	if (seg.NodeAt(1).Flags & FlagFolded) == 0 {
		t.Fatalf("expected FlagFolded on node_modules")
	}
	// Cold metadata point read via ReadAt.
	cm, err := seg.ReadColdMeta(2)
	if err != nil || cm.Size != 4096 || cm.ModTime != 1700000002 {
		t.Fatalf("ReadColdMeta(2) = (%+v, %v)", cm, err)
	}
	// Key lookup via sorted keys.idx.
	if nid, local, ok := seg.LookupByKey(1, 300); !ok || nid != 2 || local != 2 {
		t.Fatalf("LookupByKey(1,300) = (%d,%d,%v), want (2,2,true)", nid, local, ok)
	}

	// Mark node 1 deleted, persist del.bit, reload, verify tombstone persists.
	if !seg.MarkDeleted(1) {
		t.Fatalf("MarkDeleted(1) = false, want true")
	}
	if seg.MarkDeleted(1) {
		t.Fatalf("second MarkDeleted(1) should return false")
	}
	if seg.LiveCount() != 2 || seg.DeadCount() != 1 {
		t.Fatalf("live=%d dead=%d, want 2/1", seg.LiveCount(), seg.DeadCount())
	}
	if _, ok := seg.LookupNodeID(1); ok {
		t.Fatalf("deleted node 1 must not be returned by LookupNodeID")
	}
	if _, _, ok := seg.LookupByKey(1, 200); ok {
		t.Fatalf("deleted key 200 must not be returned by LookupByKey")
	}
	if err := seg.FlushTombstones(); err != nil {
		t.Fatalf("FlushTombstones: %v", err)
	}

	segReload, err := OpenSegment(denseDir)
	if err != nil {
		t.Fatalf("reopen dense segment: %v", err)
	}
	if !segReload.IsDeleted(1) || segReload.LiveCount() != 2 {
		t.Fatalf("tombstone did not persist across reopen: isDel=%v live=%d", segReload.IsDeleted(1), segReload.LiveCount())
	}
	segReload.Release()

	// 2. Build a Delta (non-dense) segment with sparse ascending NodeIDs.
	deltaDir := filepath.Join(dir, "seg_000002")
	db, err := NewSegmentBuilder(deltaDir, 2, 0, false, extPool)
	if err != nil {
		t.Fatalf("NewSegmentBuilder delta: %v", err)
	}
	if err := db.Append(1, 1, 200, 0, "node_modules_moved", FlagDir, 0, 0, 1700000010); err != nil {
		t.Fatalf("delta append 1: %v", err)
	}
	if err := db.Append(50, 2, 999, 1, "delta.go", 0, extPool.Intern("go"), 8192, 1700000050); err != nil {
		t.Fatalf("delta append 50: %v", err)
	}
	dmeta, err := db.Finish()
	if err != nil {
		t.Fatalf("Finish delta: %v", err)
	}
	if dmeta.Dense || dmeta.NodeCount != 2 {
		t.Fatalf("unexpected delta meta: %+v", dmeta)
	}

	dseg, err := OpenSegment(deltaDir)
	if err != nil {
		t.Fatalf("OpenSegment delta: %v", err)
	}
	defer dseg.Release()

	if local, ok := dseg.LookupNodeID(50); !ok || local != 1 {
		t.Fatalf("delta LookupNodeID(50) = (%d,%v), want (1,true)", local, ok)
	}
	if _, ok := dseg.LookupNodeID(25); ok {
		t.Fatalf("delta LookupNodeID(25) should miss")
	}
	if nid, local, ok := dseg.LookupByKey(2, 999); !ok || nid != 50 || local != 1 {
		t.Fatalf("delta LookupByKey(2,999) = (%d,%d,%v), want (50,1,true)", nid, local, ok)
	}
	if cm, err := dseg.ReadColdMeta(1); err != nil || cm.Size != 8192 {
		t.Fatalf("delta ReadColdMeta(1) = (%+v,%v)", cm, err)
	}

	// 3. Corrupt meta.hdr must return ErrCorruptSegment.
	badDir := filepath.Join(dir, "seg_bad")
	if err := os.MkdirAll(badDir, 0o755); err != nil {
		t.Fatalf("mkdir badDir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(badDir, "meta.hdr"), []byte("bad-header"), 0o644); err != nil {
		t.Fatalf("write bad meta.hdr: %v", err)
	}
	if _, err := OpenSegment(badDir); !errors.Is(err, ErrCorruptSegment) {
		t.Fatalf("expected ErrCorruptSegment, got %v", err)
	}
}
