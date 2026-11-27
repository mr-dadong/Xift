package provider

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mr-dadong/Xift/internal/filter"
	"github.com/mr-dadong/Xift/internal/storage"
)

func TestRegressionC11ZeroSizeTruncationAndOSFlagPreservation(t *testing.T) {
	var batches []EventBatch
	pruner := filter.NewPruner(filter.DefaultRuleEngine(), nil)
	b := NewBatcher(BatcherOptions{
		Window:  150 * time.Millisecond,
		Pruner:  pruner,
		OnBatch: func(batch EventBatch) { batches = append(batches, batch) },
	})

	// 1) File created with Size=4096 then truncated to Size=0 within same window MUST persist Size=0.
	b.Submit(FileEvent{VolID: 1, Type: EventCreate, NodeKey: 5001, ParentKey: 10, NewName: "trunc.log", Size: 4096, ModTime: 100, Cursor: 1})
	b.Submit(FileEvent{VolID: 1, Type: EventModifyMeta, NodeKey: 5001, ParentKey: 10, NewName: "trunc.log", Size: 0, ModTime: 200, Cursor: 2})

	// 2) File created with Size=2048 then modified with SizeUnknown (-1, like USN) MUST keep Size=2048.
	b.Submit(FileEvent{VolID: 1, Type: EventCreate, NodeKey: 5002, ParentKey: 10, NewName: "usn.bin", Size: 2048, ModTime: 100, Cursor: 3})
	b.Submit(FileEvent{VolID: 1, Type: EventModifyMeta, NodeKey: 5002, ParentKey: 10, NewName: "usn.bin", Size: SizeUnknown, ModTime: 300, Cursor: 4})

	// 3) Hidden directory created with FlagHidden then ModifyMeta (Flags: 0) MUST NOT clobber FlagHidden.
	b.Submit(FileEvent{VolID: 1, Type: EventCreate, NodeKey: 5003, ParentKey: 10, NewName: ".secret", IsDir: true, Flags: storage.FlagDir | storage.FlagHidden, Cursor: 5})
	b.Submit(FileEvent{VolID: 1, Type: EventModifyMeta, NodeKey: 5003, ParentKey: 10, NewName: ".secret", IsDir: true, Flags: 0, ModTime: 400, Cursor: 6})

	b.FlushNow()

	if len(batches) != 1 || len(batches[0].Events) != 3 {
		t.Fatalf("expected 1 batch with 3 events, got %+v", batches)
	}
	byKey := make(map[uint64]FileEvent)
	for _, ev := range batches[0].Events {
		byKey[ev.NodeKey] = ev
	}
	if got := byKey[5001]; got.Size != 0 || got.ModTime != 200 {
		t.Fatalf("trunc.log = %+v, want Size=0 ModTime=200", got)
	}
	if got := byKey[5002]; got.Size != 2048 || got.ModTime != 300 {
		t.Fatalf("usn.bin = %+v, want preserved Size=2048 ModTime=300", got)
	}
	if got := byKey[5003]; got.Flags&storage.FlagHidden == 0 || got.Flags&storage.FlagDir == 0 {
		t.Fatalf(".secret flags = %b, want FlagDir|FlagHidden preserved", got.Flags)
	}
}

func TestRegressionC11PrunedSubtreeTombstonesDescendantsAndI1PreservesSize(t *testing.T) {
	eng, err := storage.Open(t.TempDir(), storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	vol := VolumeInfo{VolID: 1, RootPath: "/proj", RootKey: 1}
	w, err := NewStorageSegmentWriter(eng, vol)
	if err != nil {
		t.Fatal(err)
	}
	_ = w.WriteRecord(ScanRecord{VolID: 1, NodeKey: 1, ParentKey: 1, Name: "proj", Flags: storage.FlagDir})
	_ = w.WriteRecord(ScanRecord{VolID: 1, NodeKey: 2, ParentKey: 1, Name: "deps", Flags: storage.FlagDir})
	_ = w.WriteRecord(ScanRecord{VolID: 1, NodeKey: 3, ParentKey: 2, Name: "pkg", Flags: storage.FlagDir})
	_ = w.WriteRecord(ScanRecord{VolID: 1, NodeKey: 4, ParentKey: 3, Name: "index.js", Size: 777, ModTime: 100})
	_ = w.WriteRecord(ScanRecord{VolID: 1, NodeKey: 5, ParentKey: 1, Name: "keep.go", Size: 999, ModTime: 100})
	if _, err := w.Commit(10); err != nil {
		t.Fatal(err)
	}

	pruner := filter.NewPruner(filter.DefaultRuleEngine(), nil)
	sink := NewStorageEventSink(eng, map[uint16]string{1: "/proj"}, nil)
	batcher := NewBatcher(BatcherOptions{
		Window:  50 * time.Millisecond,
		Pruner:  pruner,
		OnBatch: sink.HandleBatch,
	})

	// Rename visible dir "deps" (NodeKey=2) -> "node_modules" (becomes Level 1 Fold -> PrunedSubtree=true).
	batcher.Submit(FileEvent{
		VolID:        1,
		Type:         EventRename,
		NodeKey:      2,
		ParentKey:    1,
		NewParentKey: 1,
		OldPath:      "deps",
		NewName:      "node_modules",
		IsDir:        true,
		Cursor:       11,
	})
	// ModifyMeta with SizeUnknown on keep.go (NodeKey=5) MUST preserve existing Size=999.
	batcher.Submit(FileEvent{
		VolID:     1,
		Type:      EventModifyMeta,
		NodeKey:   5,
		ParentKey: 1,
		NewName:   "keep.go",
		Size:      SizeUnknown,
		ModTime:   500,
		Cursor:    12,
	})
	batcher.FlushNow()

	if err := sink.LastError(); err != nil {
		t.Fatalf("sink error: %v", err)
	}

	snap := eng.AcquireSnapshot()
	defer snap.Release()

	names := make(map[string]uint32)
	snap.ForEachLive(func(nodeID uint32, node storage.HotNode, name []byte) bool {
		names[string(name)] = nodeID
		return true
	})

	if _, ok := names["node_modules"]; !ok {
		t.Fatalf("expected folded shell node_modules retained, got %+v", names)
	}
	if _, ok := names["pkg"]; ok {
		t.Fatal("descendant dir pkg should have been tombstoned on PrunedSubtree")
	}
	if _, ok := names["index.js"]; ok {
		t.Fatal("descendant file index.js should have been tombstoned on PrunedSubtree")
	}
	keepID, ok := names["keep.go"]
	if !ok {
		t.Fatal("keep.go missing")
	}
	cm, err := snap.ReadColdMeta(keepID)
	if err != nil || cm.Size != 999 || cm.ModTime != 500 {
		t.Fatalf("keep.go cold meta = %+v (err=%v), want Size=999 preserved and ModTime=500", cm, err)
	}
}

func TestRegressionI2BoundedRescanDirsAndI3CursorOrderAndNamelessFoldedMove(t *testing.T) {
	// I2: Bounded rescanDirs collapses to a single volume-root EventRescanDir when cap exceeded.
	var batches []EventBatch
	b := NewBatcher(BatcherOptions{
		RingCapacity:  10,
		HighWaterPct:  50,
		MaxRescanDirs: 4,
		OnBatch:       func(batch EventBatch) { batches = append(batches, batch) },
	})
	for i := uint64(1); i <= 30; i++ {
		b.Submit(FileEvent{VolID: 1, Type: EventCreate, NodeKey: 100 + i, ParentKey: i, NewName: "x", RelPath: "d/x", Cursor: i})
	}
	b.FlushNow()
	if len(batches) != 1 {
		t.Fatalf("batches = %d, want 1", len(batches))
	}
	rescanCount := 0
	collapsedRoot := false
	for _, ev := range batches[0].Events {
		if ev.Type == EventRescanDir {
			rescanCount++
			if ev.NodeKey == 0 {
				collapsedRoot = true
			}
		}
	}
	if rescanCount != 1 || !collapsedRoot {
		t.Fatalf("expected collapsed single volume-root EventRescanDir, got count=%d collapsedRoot=%v", rescanCount, collapsedRoot)
	}

	// I3: Cursor MUST NOT advance when onRescanDir fails, and nameless move of folded dir MUST NOT un-fold it.
	eng, err := storage.Open(t.TempDir(), storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	vol := VolumeInfo{VolID: 1, RootPath: "/v", RootKey: 1}
	sw, _ := NewStorageSegmentWriter(eng, vol)
	_ = sw.WriteRecord(ScanRecord{VolID: 1, NodeKey: 1, ParentKey: 1, Name: "v", Flags: storage.FlagDir})
	_ = sw.WriteRecord(ScanRecord{VolID: 1, NodeKey: 2, ParentKey: 1, Name: "node_modules", Flags: storage.FlagDir | storage.FlagFolded})
	_, _ = sw.Commit(50)

	pruner := filter.NewPruner(filter.DefaultRuleEngine(), nil)
	pruner.PrunedDirSet().Set(1, 2, filter.DirFlagFoldedShell)

	// Nameless directory move of folded node_modules MUST preserve FlagFolded and NOT trigger DecisionRescanDir.
	dec := pruner.InterceptEvent(filter.EventInput{VolID: 1, Op: filter.EventRename, NodeKey: 2, ParentKey: 1, NewParentKey: 1, IsDir: true}, nil)
	if dec.Action != filter.DecisionPass || dec.Flags&filter.FlagFolded == 0 {
		t.Fatalf("nameless move of folded dir got %+v, want DecisionPass with FlagFolded", dec)
	}

	sink := NewStorageEventSink(eng, map[uint16]string{1: "/v"}, func(FileEvent) error {
		return errors.New("simulated rescan failure")
	})
	sink.HandleBatch(EventBatch{
		Events:  []FileEvent{{VolID: 1, Type: EventRescanDir, NodeKey: 1, IsDir: true, Cursor: 60}},
		Cursors: map[uint16]uint64{1: 60},
	})
	vc, ok := eng.VolumeCursor(1)
	if !ok || vc.Cursor != 50 {
		t.Fatalf("cursor advanced to %d despite failed onRescanDir; want 50", vc.Cursor)
	}
}

func TestRegressionM1ConcurrentStartCloseNoRace(t *testing.T) {
	for i := 0; i < 20; i++ {
		b := NewBatcher(BatcherOptions{Window: 10 * time.Millisecond, DisableLowPriority: true})
		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			b.Start(ctx)
		}()
		go func() {
			defer wg.Done()
			b.Close()
		}()
		wg.Wait()
		cancel()
	}
}
