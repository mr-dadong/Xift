package provider

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/mr-dadong/Xift/internal/filter"
)

func TestBatcherCancellationMatrix(t *testing.T) {
	var batches []EventBatch
	b := NewBatcher(BatcherOptions{
		Window:       150 * time.Millisecond,
		RingCapacity: 1024,
		OnBatch: func(batch EventBatch) {
			batches = append(batches, batch)
		},
	})

	const vol uint16 = 1

	// 1) Create + N*Modify + Delete -> cancelled (0 events)
	b.Submit(FileEvent{VolID: vol, Type: EventCreate, NodeKey: 1001, ParentKey: 10, NewName: "tmp1.o", Size: 10, ModTime: 100, Cursor: 1})
	b.Submit(FileEvent{VolID: vol, Type: EventModifyMeta, NodeKey: 1001, ParentKey: 10, Size: 20, ModTime: 200, Cursor: 2})
	b.Submit(FileEvent{VolID: vol, Type: EventModifyMeta, NodeKey: 1001, ParentKey: 10, Size: 30, ModTime: 300, Cursor: 3})
	b.Submit(FileEvent{VolID: vol, Type: EventDelete, NodeKey: 1001, ParentKey: 10, Cursor: 4})

	// 2) Create + Rename + Delete -> cancelled (0 events)
	b.Submit(FileEvent{VolID: vol, Type: EventCreate, NodeKey: 1002, ParentKey: 10, NewName: "a.tmp", Cursor: 5})
	b.Submit(FileEvent{VolID: vol, Type: EventRename, NodeKey: 1002, ParentKey: 10, NewParentKey: 11, NewName: "b.tmp", Cursor: 6})
	b.Submit(FileEvent{VolID: vol, Type: EventDelete, NodeKey: 1002, ParentKey: 11, Cursor: 7})

	// 3) Create + 3*Modify -> single Create with latest Size & ModTime
	b.Submit(FileEvent{VolID: vol, Type: EventCreate, NodeKey: 1003, ParentKey: 10, NewName: "report.md", Size: 1, ModTime: 10, Cursor: 8})
	b.Submit(FileEvent{VolID: vol, Type: EventModifyMeta, NodeKey: 1003, ParentKey: 10, Size: 50, ModTime: 50, Cursor: 9})
	b.Submit(FileEvent{VolID: vol, Type: EventModifyMeta, NodeKey: 1003, ParentKey: 10, Size: 99, ModTime: 90, Cursor: 10})

	// 4) Create + Rename -> single Create at final name and parent
	b.Submit(FileEvent{VolID: vol, Type: EventCreate, NodeKey: 1004, ParentKey: 10, NewName: "draft.txt", Size: 12, ModTime: 120, Cursor: 11})
	b.Submit(FileEvent{VolID: vol, Type: EventRename, NodeKey: 1004, ParentKey: 10, NewParentKey: 20, NewName: "final.txt", Cursor: 12})

	// 5) N*Modify -> single ModifyMeta with latest Size & ModTime
	b.Submit(FileEvent{VolID: vol, Type: EventModifyMeta, NodeKey: 1005, ParentKey: 10, Size: 100, ModTime: 1000, Cursor: 13})
	b.Submit(FileEvent{VolID: vol, Type: EventModifyMeta, NodeKey: 1005, ParentKey: 10, Size: 200, ModTime: 2000, Cursor: 14})
	b.Submit(FileEvent{VolID: vol, Type: EventModifyMeta, NodeKey: 1005, ParentKey: 10, Size: 300, ModTime: 3000, Cursor: 15})

	// 6) Modify + Rename -> single Rename keeping latest metadata
	b.Submit(FileEvent{VolID: vol, Type: EventModifyMeta, NodeKey: 1006, ParentKey: 10, Size: 400, ModTime: 4000, Cursor: 16})
	b.Submit(FileEvent{VolID: vol, Type: EventRename, NodeKey: 1006, ParentKey: 10, NewParentKey: 22, NewName: "renamed.go", Cursor: 17})

	// 7) Modify + Delete -> single Delete
	b.Submit(FileEvent{VolID: vol, Type: EventModifyMeta, NodeKey: 1007, ParentKey: 10, Size: 55, ModTime: 555, Cursor: 18})
	b.Submit(FileEvent{VolID: vol, Type: EventDelete, NodeKey: 1007, ParentKey: 10, Cursor: 19})

	// 8) Rename + Rename -> single Rename preserving original OldPath and final name/parent
	b.Submit(FileEvent{VolID: vol, Type: EventRename, NodeKey: 1008, ParentKey: 10, NewParentKey: 11, OldPath: "/a/x.go", NewName: "y.go", Cursor: 20})
	b.Submit(FileEvent{VolID: vol, Type: EventRename, NodeKey: 1008, ParentKey: 11, NewParentKey: 12, OldPath: "/b/y.go", NewName: "z.go", Cursor: 21})

	// 9) Rename + Delete -> single Delete
	b.Submit(FileEvent{VolID: vol, Type: EventRename, NodeKey: 1009, ParentKey: 10, NewParentKey: 11, NewName: "gone.txt", Cursor: 22})
	b.Submit(FileEvent{VolID: vol, Type: EventDelete, NodeKey: 1009, ParentKey: 11, Cursor: 23})

	// 10) Delete + Create (same NodeKey reused) -> single Create
	b.Submit(FileEvent{VolID: vol, Type: EventDelete, NodeKey: 1010, ParentKey: 10, Cursor: 24})
	b.Submit(FileEvent{VolID: vol, Type: EventCreate, NodeKey: 1010, ParentKey: 10, NewName: "reborn.go", Size: 77, ModTime: 777, Cursor: 25})

	b.FlushNow()

	if len(batches) != 1 {
		t.Fatalf("flushed batches = %d, want 1", len(batches))
	}
	got := make(map[uint64]FileEvent, len(batches[0].Events))
	for _, ev := range batches[0].Events {
		got[ev.NodeKey] = ev
	}

	if _, ok := got[1001]; ok {
		t.Fatalf("node 1001 (Create+Modify+Delete) should be cancelled, got %+v", got[1001])
	}
	if _, ok := got[1002]; ok {
		t.Fatalf("node 1002 (Create+Rename+Delete) should be cancelled, got %+v", got[1002])
	}
	if ev := got[1003]; ev.Type != EventCreate || ev.Size != 99 || ev.ModTime != 90 || ev.NewName != "report.md" {
		t.Fatalf("node 1003 = %+v, want Create size=99 mtime=90", ev)
	}
	if ev := got[1004]; ev.Type != EventCreate || ev.ParentKey != 20 || ev.NewName != "final.txt" || ev.Size != 12 {
		t.Fatalf("node 1004 = %+v, want Create parent=20 name=final.txt size=12", ev)
	}
	if ev := got[1005]; ev.Type != EventModifyMeta || ev.Size != 300 || ev.ModTime != 3000 {
		t.Fatalf("node 1005 = %+v, want ModifyMeta size=300 mtime=3000", ev)
	}
	if ev := got[1006]; ev.Type != EventRename || ev.NewParentKey != 22 || ev.NewName != "renamed.go" || ev.Size != 400 {
		t.Fatalf("node 1006 = %+v, want Rename parent=22 name=renamed.go size=400", ev)
	}
	if ev := got[1007]; ev.Type != EventDelete {
		t.Fatalf("node 1007 = %+v, want Delete", ev)
	}
	if ev := got[1008]; ev.Type != EventRename || ev.OldPath != "/a/x.go" || ev.NewParentKey != 12 || ev.NewName != "z.go" {
		t.Fatalf("node 1008 = %+v, want Rename old=/a/x.go dst=12 name=z.go", ev)
	}
	if ev := got[1009]; ev.Type != EventDelete {
		t.Fatalf("node 1009 = %+v, want Delete", ev)
	}
	if ev := got[1010]; ev.Type != EventCreate || ev.NewName != "reborn.go" || ev.Size != 77 {
		t.Fatalf("node 1010 = %+v, want Create reborn.go size=77", ev)
	}
	if batches[0].Cursors[vol] != 25 {
		t.Fatalf("cursor = %d, want 25", batches[0].Cursors[vol])
	}
	st := b.Stats()
	if st.CancelledPairs != 2 {
		t.Fatalf("CancelledPairs = %d, want 2", st.CancelledPairs)
	}
}

func TestBatcherCursorAdvancesWhenAllEventsCancelled(t *testing.T) {
	var batches []EventBatch
	b := NewBatcher(BatcherOptions{
		Window: 150 * time.Millisecond,
		OnBatch: func(batch EventBatch) {
			batches = append(batches, batch)
		},
	})

	b.Submit(FileEvent{VolID: 2, Type: EventCreate, NodeKey: 500, ParentKey: 1, NewName: "ephemeral.txt", Cursor: 100})
	b.Submit(FileEvent{VolID: 2, Type: EventDelete, NodeKey: 500, ParentKey: 1, Cursor: 105})
	b.FlushNow()

	if len(batches) != 1 {
		t.Fatalf("expected 1 cursor-only batch, got %d", len(batches))
	}
	if len(batches[0].Events) != 0 {
		t.Fatalf("expected 0 events, got %d", len(batches[0].Events))
	}
	if batches[0].Cursors[2] != 105 {
		t.Fatalf("cursor[2] = %d, want 105", batches[0].Cursors[2])
	}
}

func TestBatcherPrunerSourceIntercept80KStorm(t *testing.T) {
	rules := filter.DefaultRuleEngine()
	pruner := filter.NewPruner(rules, nil)

	var batches []EventBatch
	b := NewBatcher(BatcherOptions{
		Window: 150 * time.Millisecond,
		Pruner: pruner,
		OnBatch: func(batch EventBatch) {
			batches = append(batches, batch)
		},
	})

	const (
		volID      uint16 = 1
		projectKey uint64 = 100
		nodeModKey uint64 = 101
	)

	// Top-level node_modules shell passes once with FlagDir|FlagFolded.
	res := b.Submit(FileEvent{
		VolID:     volID,
		Type:      EventCreate,
		NodeKey:   nodeModKey,
		ParentKey: projectKey,
		NewName:   "node_modules",
		IsDir:     true,
		Cursor:    1,
	})
	if res != SubmitQueued {
		t.Fatalf("top-level node_modules submit = %v, want SubmitQueued", res)
	}

	// 1,000 nested dirs + 78,999 nested files must all be dropped at the source.
	for i := uint64(0); i < 1000; i++ {
		if r := b.Submit(FileEvent{
			VolID:     volID,
			Type:      EventCreate,
			NodeKey:   1000 + i,
			ParentKey: nodeModKey,
			NewName:   "pkg",
			IsDir:     true,
			Cursor:    2 + i,
		}); r != SubmitDroppedPruned {
			t.Fatalf("nested dir %d = %v, want SubmitDroppedPruned", i, r)
		}
	}
	for i := uint64(0); i < 78_999; i++ {
		if r := b.Submit(FileEvent{
			VolID:     volID,
			Type:      EventCreate,
			NodeKey:   100_000 + i,
			ParentKey: 1000 + (i % 1000),
			NewName:   "index.js",
			IsDir:     false,
			Cursor:    2000 + i,
		}); r != SubmitDroppedPruned {
			t.Fatalf("nested file %d = %v, want SubmitDroppedPruned", i, r)
		}
	}

	b.FlushNow()

	if len(batches) != 1 || len(batches[0].Events) != 1 {
		t.Fatalf("expected exactly 1 flushed event (node_modules shell), got batches=%d events=%d", len(batches), func() int {
			if len(batches) == 0 {
				return 0
			}
			return len(batches[0].Events)
		}())
	}
	ev := batches[0].Events[0]
	if ev.NodeKey != nodeModKey || ev.Flags&(filter.FlagDir|filter.FlagFolded) != (filter.FlagDir|filter.FlagFolded) {
		t.Fatalf("unexpected shell event: %+v", ev)
	}
	st := b.Stats()
	if st.PrunedDropped != 79_999 {
		t.Fatalf("PrunedDropped = %d, want 79999", st.PrunedDropped)
	}
	if batches[0].Cursors[volID] != 2000+78_998 {
		t.Fatalf("cursor = %d, want %d", batches[0].Cursors[volID], 2000+78_998)
	}
}

func TestBatcherPrunerMoveIntoAndOutOfPrunedSubtree(t *testing.T) {
	pruner := filter.NewPruner(filter.DefaultRuleEngine(), nil)
	pruner.PrunedDirSet().Set(1, 50, filter.DirFlagFoldedShell)

	var batches []EventBatch
	b := NewBatcher(BatcherOptions{
		Window: 150 * time.Millisecond,
		Pruner: pruner,
		OnBatch: func(batch EventBatch) {
			batches = append(batches, batch)
		},
	})

	// File moved INTO folded dir 50 -> DecisionDeleteFromIndex -> EventDelete
	b.Submit(FileEvent{
		VolID:        1,
		Type:         EventRename,
		NodeKey:      601,
		ParentKey:    10,
		NewParentKey: 50,
		NewName:      "moved_in.go",
		Cursor:       10,
	})

	// Dir moved OUT of folded dir 50 into visible dir 10 -> DecisionRescanDir -> EventRename + EventRescanDir
	b.Submit(FileEvent{
		VolID:        1,
		Type:         EventRename,
		NodeKey:      602,
		ParentKey:    50,
		NewParentKey: 10,
		NewName:      "extracted_pkg",
		IsDir:        true,
		Cursor:       11,
	})

	b.FlushNow()
	if len(batches) != 1 {
		t.Fatalf("batches = %d, want 1", len(batches))
	}

	var sawDelete, sawRename, sawRescan bool
	for _, ev := range batches[0].Events {
		switch {
		case ev.NodeKey == 601 && ev.Type == EventDelete:
			sawDelete = true
		case ev.NodeKey == 602 && ev.Type == EventRename:
			sawRename = true
		case ev.NodeKey == 602 && ev.Type == EventRescanDir:
			sawRescan = true
		}
	}
	if !sawDelete || !sawRename || !sawRescan {
		t.Fatalf("missing expected transitions: sawDelete=%v sawRename=%v sawRescan=%v events=%+v", sawDelete, sawRename, sawRescan, batches[0].Events)
	}
}

func TestBatcherOver80PercentDegradesToRescanDir(t *testing.T) {
	var batches []EventBatch
	b := NewBatcher(BatcherOptions{
		Window:       150 * time.Millisecond,
		RingCapacity: 100, // 80% threshold = 80
		HighWaterPct: 80,
		OnBatch: func(batch EventBatch) {
			batches = append(batches, batch)
		},
	})

	degradedCount := 0
	for i := uint64(1); i <= 150; i++ {
		parent := uint64(10)
		if i%2 == 0 {
			parent = 20
		}
		res := b.Submit(FileEvent{
			VolID:     1,
			Type:      EventCreate,
			NodeKey:   1000 + i,
			ParentKey: parent,
			NewName:   "storm.dat",
			Cursor:    i,
		})
		if res == SubmitDegradedRescan {
			degradedCount++
		}
	}

	if degradedCount == 0 {
		t.Fatal("expected events above 80% ring capacity to degrade to EventRescanDir")
	}

	b.FlushNow()
	if len(batches) != 1 {
		t.Fatalf("batches = %d, want 1", len(batches))
	}

	rescans := make(map[uint64]int)
	for _, ev := range batches[0].Events {
		if ev.Type == EventRescanDir {
			rescans[ev.NodeKey]++
		}
	}
	if rescans[10] != 1 || rescans[20] != 1 {
		t.Fatalf("expected deduplicated EventRescanDir for dirs 10 and 20, got %+v", rescans)
	}
	if batches[0].Cursors[1] != 150 {
		t.Fatalf("cursor = %d, want 150", batches[0].Cursors[1])
	}
}

func TestBatcherAsync150msWindowAndConcurrentRace(t *testing.T) {
	var (
		mu       sync.Mutex
		received []FileEvent
		notifyCh = make(chan struct{}, 16)
	)

	b := NewBatcher(BatcherOptions{
		Window:       150 * time.Millisecond,
		RingCapacity: 4096,
		OnBatch: func(batch EventBatch) {
			mu.Lock()
			received = append(received, batch.Events...)
			mu.Unlock()
			select {
			case notifyCh <- struct{}{}:
			default:
			}
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b.Start(ctx)

	start := time.Now()
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			base := uint64(gid * 100)
			for i := uint64(1); i <= 25; i++ {
				b.Submit(FileEvent{
					VolID:     1,
					Type:      EventCreate,
					NodeKey:   base + i,
					ParentKey: 1,
					NewName:   "file.go",
					Cursor:    base + i,
				})
			}
		}(g)
	}
	wg.Wait()

	// Wait for the 150ms window to fire naturally (must arrive well under 300ms SLA).
	select {
	case <-notifyCh:
		elapsed := time.Since(start)
		if elapsed > 450*time.Millisecond {
			t.Fatalf("async batch latency too slow: %v", elapsed)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for 150ms async batch flush")
	}

	b.Close()
	mu.Lock()
	total := len(received)
	mu.Unlock()
	if total != 100 {
		t.Fatalf("total flushed events = %d, want 100", total)
	}
}

func BenchmarkBatcherPrunedStorm80K(b *testing.B) {
	pruner := filter.NewPruner(filter.DefaultRuleEngine(), nil)
	pruner.PrunedDirSet().Set(1, 101, filter.DirFlagFoldedShell)
	batcher := NewBatcher(BatcherOptions{
		Window: 150 * time.Millisecond,
		Pruner: pruner,
	})

	ev := FileEvent{
		VolID:     1,
		Type:      EventCreate,
		NodeKey:   99999,
		ParentKey: 101,
		NewName:   "index.js",
		IsDir:     false,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batcher.Submit(ev)
	}
}
