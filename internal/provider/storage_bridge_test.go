package provider

import (
	"testing"
	"time"

	"github.com/mr-dadong/Xift/internal/filter"
	"github.com/mr-dadong/Xift/internal/storage"
)

func TestStorageSegmentWriterAndReopenPersistence(t *testing.T) {
	dataDir := t.TempDir()
	eng, err := storage.Open(dataDir, storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}

	vol := VolumeInfo{
		VolID:           1,
		RootPath:        "D:",
		FSType:          "NTFS",
		RootKey:         5,
		CaseInsensitive: true,
		SupportsJournal: true,
	}

	w, err := NewStorageSegmentWriter(eng, vol)
	if err != nil {
		t.Fatalf("NewStorageSegmentWriter: %v", err)
	}

	// Write in topological order: root -> dirs -> files.
	records := []ScanRecord{
		{VolID: 1, NodeKey: 5, ParentKey: 5, Name: "D:", Flags: storage.FlagDir},
		{VolID: 1, NodeKey: 10, ParentKey: 5, Name: "code", Flags: storage.FlagDir},
		{VolID: 1, NodeKey: 11, ParentKey: 10, Name: "xift", Flags: storage.FlagDir},
		{VolID: 1, NodeKey: 12, ParentKey: 11, Name: "node_modules", Flags: storage.FlagDir | storage.FlagFolded},
		{VolID: 1, NodeKey: 100, ParentKey: 11, Name: "设计文档.md", Flags: 0, Size: 4096, ModTime: 1700000000},
	}
	for _, r := range records {
		if err := w.WriteRecord(r); err != nil {
			t.Fatalf("WriteRecord(%+v): %v", r, err)
		}
	}

	meta, err := w.Commit(88888)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if meta.NodeCount != uint32(len(records)) {
		t.Fatalf("meta.NodeCount = %d, want %d", meta.NodeCount, len(records))
	}

	// Verify path resolution & cold metadata & cursor before and after reopen.
	checkEngine := func(e *storage.Engine) {
		t.Helper()
		vc, ok := e.VolumeCursor(1)
		if !ok || vc.Cursor != 88888 || vc.RootPath != "D:" {
			t.Fatalf("VolumeCursor(1) = %+v, ok=%v; want cursor=88888 root=D:", vc, ok)
		}
		snap := e.AcquireSnapshot()
		defer snap.Release()

		var foundID uint32
		var foundFlags uint16
		snap.ForEachLive(func(nodeID uint32, node storage.HotNode, name []byte) bool {
			if string(name) == "设计文档.md" {
				foundID = nodeID
				foundFlags = node.Flags
				return false
			}
			return true
		})
		if foundID == 0 {
			t.Fatal("file 设计文档.md not found in snapshot")
		}
		if foundFlags&storage.FlagNonASCII == 0 {
			t.Fatalf("expected FlagNonASCII on 设计文档.md, got flags=%b", foundFlags)
		}
		p, err := snap.ResolvePath(foundID)
		if err != nil {
			t.Fatalf("ResolvePath: %v", err)
		}
		if p != "D:/code/xift/设计文档.md" {
			t.Fatalf("ResolvePath = %q, want %q", p, "D:/code/xift/设计文档.md")
		}
		cm, err := snap.ReadColdMeta(foundID)
		if err != nil || cm.Size != 4096 || cm.ModTime != 1700000000 {
			t.Fatalf("ReadColdMeta = %+v, err=%v", cm, err)
		}
	}

	checkEngine(eng)
	if err := eng.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := storage.Open(dataDir, storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("reopen storage.Open: %v", err)
	}
	defer reopened.Close()
	checkEngine(reopened)
}

func TestStorageEventSinkWithBatcherEndToEnd(t *testing.T) {
	dataDir := t.TempDir()
	eng, err := storage.Open(dataDir, storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	defer eng.Close()

	vol := VolumeInfo{VolID: 1, RootPath: "/work", RootKey: 1}
	w, err := NewStorageSegmentWriter(eng, vol)
	if err != nil {
		t.Fatalf("NewStorageSegmentWriter: %v", err)
	}
	_ = w.WriteRecord(ScanRecord{VolID: 1, NodeKey: 1, ParentKey: 1, Name: "work", Flags: storage.FlagDir})
	_ = w.WriteRecord(ScanRecord{VolID: 1, NodeKey: 2, ParentKey: 1, Name: "src", Flags: storage.FlagDir})
	_ = w.WriteRecord(ScanRecord{VolID: 1, NodeKey: 3, ParentKey: 1, Name: "archive", Flags: storage.FlagDir})
	if _, err := w.Commit(10); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	var rescans []FileEvent
	sink := NewStorageEventSink(eng, map[uint16]string{1: "/work"}, func(ev FileEvent) error {
		rescans = append(rescans, ev)
		return nil
	})

	pruner := filter.NewPruner(filter.DefaultRuleEngine(), nil)
	batcher := NewBatcher(BatcherOptions{
		Window:  20 * time.Millisecond,
		Pruner:  pruner,
		OnBatch: sink.HandleBatch,
	})

	// Create + Modify -> persists with latest size
	batcher.Submit(FileEvent{VolID: 1, Type: EventCreate, NodeKey: 20, ParentKey: 2, NewName: "main.go", Size: 10, ModTime: 100, Cursor: 11})
	batcher.Submit(FileEvent{VolID: 1, Type: EventModifyMeta, NodeKey: 20, ParentKey: 2, Size: 512, ModTime: 200, Cursor: 12})

	// Create + Delete -> cancelled (never written to storage!)
	batcher.Submit(FileEvent{VolID: 1, Type: EventCreate, NodeKey: 21, ParentKey: 2, NewName: "ephemeral.go", Size: 1, ModTime: 10, Cursor: 13})
	batcher.Submit(FileEvent{VolID: 1, Type: EventDelete, NodeKey: 21, ParentKey: 2, Cursor: 14})

	// Directory created then moved to archive/
	batcher.Submit(FileEvent{VolID: 1, Type: EventCreate, NodeKey: 30, ParentKey: 2, NewName: "sub", IsDir: true, Cursor: 15})
	batcher.FlushNow()

	// Move dir 30 from src(2) -> archive(3) without renaming (OpMoveDir)
	batcher.Submit(FileEvent{VolID: 1, Type: EventRename, NodeKey: 30, ParentKey: 2, NewParentKey: 3, IsDir: true, Cursor: 16})
	// Explicit rescan event
	batcher.Submit(FileEvent{VolID: 1, Type: EventRescanDir, NodeKey: 3, OldPath: "/work/archive", IsDir: true, Cursor: 17})
	batcher.FlushNow()

	if sinkErr := sink.LastError(); sinkErr != nil {
		t.Fatalf("sink LastError: %v", sinkErr)
	}
	if len(rescans) != 1 || rescans[0].NodeKey != 3 {
		t.Fatalf("rescans = %+v, want 1 rescan for dir 3", rescans)
	}

	snap := eng.AcquireSnapshot()
	defer snap.Release()

	paths := make(map[string]uint32)
	snap.ForEachLive(func(nodeID uint32, node storage.HotNode, name []byte) bool {
		p, err := snap.ResolvePath(nodeID)
		if err != nil {
			t.Errorf("ResolvePath(%d): %v", nodeID, err)
			return true
		}
		paths[p] = nodeID
		return true
	})

	if _, ok := paths["work/src/ephemeral.go"]; ok {
		t.Fatal("ephemeral.go should have been cancelled and never indexed")
	}
	mainID, ok := paths["work/src/main.go"]
	if !ok {
		t.Fatalf("work/src/main.go missing; paths=%+v", paths)
	}
	if cm, err := snap.ReadColdMeta(mainID); err != nil || cm.Size != 512 || cm.ModTime != 200 {
		t.Fatalf("main.go cold meta = %+v, err=%v", cm, err)
	}
	if _, ok := paths["work/archive/sub"]; !ok {
		t.Fatalf("moved dir work/archive/sub missing; paths=%+v", paths)
	}
	vc, ok := eng.VolumeCursor(1)
	if !ok || vc.Cursor != 17 {
		t.Fatalf("VolumeCursor(1) = %+v, want 17", vc)
	}
}
