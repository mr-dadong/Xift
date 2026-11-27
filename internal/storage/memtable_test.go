package storage

import (
	"testing"
)

func TestMemTableUpsertRenameDeleteAndColdOverride(t *testing.T) {
	mt := NewMemTable(16)

	// Upsert new file.
	slot, existed := mt.Upsert(10, 1, 1001, 0, "alpha.txt", 0, 1, 128, 1700000001)
	if existed {
		t.Fatalf("expected new entry")
	}
	if got := string(mt.NameBytesAt(slot)); got != "alpha.txt" {
		t.Fatalf("Name = %q, want alpha.txt", got)
	}
	if mt.LiveCount() != 1 {
		t.Fatalf("LiveCount = %d, want 1", mt.LiveCount())
	}

	// Lookup by key and nodeID.
	if s, ok := mt.GetByKey(1, 1001); !ok || s != slot {
		t.Fatalf("GetByKey = (%d,%v), want (%d,true)", s, ok, slot)
	}
	if s, ok := mt.GetByNodeID(10); !ok || s != slot {
		t.Fatalf("GetByNodeID = (%d,%v), want (%d,true)", s, ok, slot)
	}

	// Rename to shorter name (in-place) then longer name (arena append).
	slot2, existed := mt.Upsert(10, 1, 1001, 0, "a.go", 0, 2, 256, 1700000002)
	if !existed || slot2 != slot {
		t.Fatalf("expected in-place update on same slot: slot2=%d existed=%v", slot2, existed)
	}
	if got := string(mt.NameBytesAt(slot)); got != "a.go" {
		t.Fatalf("short rename = %q, want a.go", got)
	}

	slot3, existed := mt.Upsert(10, 1, 1001, 5, "very_long_renamed_file_设计文档.md", FlagNonASCII, 3, 512, 1700000003)
	if !existed || slot3 != slot {
		t.Fatalf("expected update on same slot after long rename")
	}
	if got := string(mt.NameBytesAt(slot)); got != "very_long_renamed_file_设计文档.md" {
		t.Fatalf("long rename = %q", got)
	}
	if mt.NodeAt(slot).ParentIdx != 5 || mt.NodeAt(slot).ExtID != 3 {
		t.Fatalf("unexpected updated HotNode: %+v", mt.NodeAt(slot))
	}

	// Cold override on existing MemTable node updates in place.
	mt.SetColdOverride(10, ColdMeta{Size: 999, ModTime: 1700000999})
	if meta, ok := mt.GetColdOverride(10); !ok || meta.Size != 999 || meta.ModTime != 1700000999 {
		t.Fatalf("GetColdOverride(10) = (%+v,%v)", meta, ok)
	}
	if mt.ColdOverrideCount() != 0 {
		t.Fatalf("in-place cold update should not populate separate cold override table, got %d", mt.ColdOverrideCount())
	}

	// Cold override for segment-resident node (not in MemTable).
	mt.SetColdOverride(99, ColdMeta{Size: 4096, ModTime: 1700004096})
	if meta, ok := mt.GetColdOverride(99); !ok || meta.Size != 4096 || meta.ModTime != 1700004096 {
		t.Fatalf("GetColdOverride(99) = (%+v,%v)", meta, ok)
	}
	if mt.ColdOverrideCount() != 1 {
		t.Fatalf("ColdOverrideCount = %d, want 1", mt.ColdOverrideCount())
	}

	// Delete node 10.
	if !mt.MarkDeletedByNodeID(10) {
		t.Fatalf("MarkDeletedByNodeID(10) = false, want true")
	}
	if mt.LiveCount() != 0 {
		t.Fatalf("LiveCount after delete = %d, want 0", mt.LiveCount())
	}
	if _, ok := mt.GetByKey(1, 1001); ok {
		t.Fatalf("deleted key must not be found via GetByKey")
	}
	liveVisited := 0
	mt.ForEachLive(func(nodeID uint32, node HotNode, name []byte) bool {
		liveVisited++
		return true
	})
	if liveVisited != 0 {
		t.Fatalf("ForEachLive visited %d, want 0", liveVisited)
	}
}
