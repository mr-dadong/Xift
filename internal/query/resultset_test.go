package query

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/mr-dadong/Xift/internal/storage"
)

func buildResultSetTestEngine(t *testing.T, n int) (*storage.Engine, *Engine) {
	t.Helper()
	dir := t.TempDir()
	store, err := storage.Open(dir, storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	b, err := store.BeginDenseSegment()
	if err != nil {
		t.Fatalf("BeginDenseSegment: %v", err)
	}
	rootID, err := b.AppendDense(1, 1, storage.RootParentIdx, "root", storage.FlagDir, 0, 0, 1_700_000_000)
	if err != nil {
		t.Fatalf("AppendDense root: %v", err)
	}
	for i := 0; i < n; i++ {
		if _, err := b.AppendDense(
			1,
			uint64(i+2),
			rootID,
			fmt.Sprintf("item_%05d.txt", i),
			0,
			0,
			int64((i+1)*100),
			int64(1_700_000_000+i),
		); err != nil {
			t.Fatalf("AppendDense %d: %v", i, err)
		}
	}
	meta, err := b.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if err := store.CommitSegment(meta, nil); err != nil {
		t.Fatalf("CommitSegment: %v", err)
	}
	return store, NewEngine(store)
}

func TestResultSetViewportAndColdReads(t *testing.T) {
	_, eng := buildResultSetTestEngine(t, 10_000)
	sess := eng.NewSession()

	rs, err := sess.Start(context.Background(), "item_", SearchOptions{
		Sort:    SortByRelevance,
		PathSep: '/',
	})
	if err != nil {
		t.Fatalf("Session.Start: %v", err)
	}
	defer rs.Close()

	if rs.Total() != 10_000 {
		t.Fatalf("Total() = %d, want 10000", rs.Total())
	}
	if rs.Retained() != 10_000 {
		t.Fatalf("Retained() = %d, want 10000", rs.Retained())
	}
	if rs.Truncated() {
		t.Fatalf("Truncated() = true, want false")
	}
	if rs.ColdReads() != 0 {
		t.Fatalf("initial ColdReads() = %d, want 0 for relevance sort", rs.ColdReads())
	}

	beforeCold := rs.ColdReads()
	rows, err := rs.Viewport(context.Background(), 9000, 60)
	if err != nil {
		t.Fatalf("Viewport(9000, 60): %v", err)
	}
	if len(rows) != 60 {
		t.Fatalf("len(rows) = %d, want 60", len(rows))
	}
	deltaCold := rs.ColdReads() - beforeCold
	if deltaCold > 60 {
		t.Fatalf("ColdReads delta = %d, want <= 60", deltaCold)
	}

	// Verify exact slice matches full Search at offset 9000, limit 60.
	full, err := eng.Search(context.Background(), "item_", SearchOptions{
		Offset:  9000,
		Limit:   60,
		Sort:    SortByRelevance,
		PathSep: '/',
	})
	if err != nil {
		t.Fatalf("Engine.Search: %v", err)
	}
	if len(full.Items) != len(rows) {
		t.Fatalf("len mismatch: viewport=%d, search=%d", len(rows), len(full.Items))
	}
	for i := range rows {
		if rows[i].NodeID != full.Items[i].NodeID || rows[i].Path != full.Items[i].Path || rows[i].Size != full.Items[i].Size {
			t.Fatalf("row %d mismatch: got %+v, want %+v", i, rows[i], full.Items[i])
		}
	}

	// Out-of-bounds offset returns empty slice without error.
	empty, err := rs.Viewport(context.Background(), 20_000, 60)
	if err != nil {
		t.Fatalf("Viewport OOB: %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("expected 0 rows for OOB offset, got %d", len(empty))
	}
}

func TestResultSetSnapshotConsistencyAcrossDeletes(t *testing.T) {
	store, eng := buildResultSetTestEngine(t, 500)
	sess := eng.NewSession()

	rs, err := sess.Start(context.Background(), "item_", SearchOptions{
		Sort:    SortByName,
		PathSep: '/',
	})
	if err != nil {
		t.Fatalf("Session.Start: %v", err)
	}
	defer rs.Close()

	before, err := rs.Viewport(context.Background(), 100, 50)
	if err != nil {
		t.Fatalf("Viewport before delete: %v", err)
	}
	if len(before) != 50 {
		t.Fatalf("len(before) = %d, want 50", len(before))
	}

	// Delete 200 nodes covering the viewport range and compact.
	delOps := make([]storage.Mutation, 0, 200)
	for i := 50; i < 250; i++ {
		delOps = append(delOps, storage.Mutation{
			Op:      storage.OpDelete,
			VolID:   1,
			NodeKey: uint64(i + 2),
		})
	}
	if err := store.ApplyBatch(delOps); err != nil {
		t.Fatalf("ApplyBatch delete: %v", err)
	}
	if err := store.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	after, err := rs.Viewport(context.Background(), 100, 50)
	if err != nil {
		t.Fatalf("Viewport after delete: %v", err)
	}
	if len(after) != len(before) {
		t.Fatalf("len(after) = %d, want %d", len(after), len(before))
	}
	for i := range before {
		if after[i].NodeID != before[i].NodeID || after[i].Path != before[i].Path || after[i].Size != before[i].Size {
			t.Fatalf("row %d changed after delete: got %+v, want %+v", i, after[i], before[i])
		}
	}
}

func TestResultSetMaxRetainedTopK(t *testing.T) {
	_, eng := buildResultSetTestEngine(t, 200)
	sess := eng.NewSession()

	rs, err := sess.Start(context.Background(), "item_", SearchOptions{
		Sort:        SortBySize,
		Desc:        true,
		MaxRetained: 25,
	})
	if err != nil {
		t.Fatalf("Session.Start: %v", err)
	}
	defer rs.Close()

	if rs.Total() != 200 {
		t.Fatalf("Total() = %d, want 200", rs.Total())
	}
	if rs.Retained() != 25 {
		t.Fatalf("Retained() = %d, want 25", rs.Retained())
	}
	if !rs.Truncated() {
		t.Fatalf("Truncated() = false, want true")
	}

	rows, err := rs.Viewport(context.Background(), 0, 25)
	if err != nil {
		t.Fatalf("Viewport: %v", err)
	}
	if len(rows) != 25 {
		t.Fatalf("len(rows) = %d, want 25", len(rows))
	}
	// Largest file is item_00199.txt with size 200*100 = 20000
	if rows[0].Name != "item_00199.txt" || rows[0].Size != 20000 {
		t.Fatalf("top-0 = %+v, want item_00199.txt size 20000", rows[0])
	}
	// 25th largest is item_00175.txt with size 176*100 = 17600
	if rows[24].Name != "item_00175.txt" || rows[24].Size != 17600 {
		t.Fatalf("top-24 = %+v, want item_00175.txt size 17600", rows[24])
	}
	for i := 1; i < len(rows); i++ {
		if rows[i].Size > rows[i-1].Size {
			t.Fatalf("not descending at %d: %d > %d", i, rows[i].Size, rows[i-1].Size)
		}
	}
}

func TestResultSetCloseIdempotentAndRejectsViewport(t *testing.T) {
	_, eng := buildResultSetTestEngine(t, 50)
	sess := eng.NewSession()

	rs, err := sess.Start(context.Background(), "item_", SearchOptions{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	rs.Close()
	rs.Close() // must be idempotent

	if _, err := rs.Viewport(context.Background(), 0, 10); err == nil {
		t.Fatalf("Viewport after Close should return error")
	}
}

func TestResultSetFoldedInPaging(t *testing.T) {
	diskRoot := t.TempDir()
	nmDir := filepath.Join(diskRoot, "node_modules")
	if err := os.MkdirAll(nmDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		p := filepath.Join(nmDir, fmt.Sprintf("pkg_%02d.js", i))
		if err := os.WriteFile(p, []byte("module.exports = {};"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	indexDir := t.TempDir()
	store, err := storage.Open(indexDir, storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	b, err := store.BeginDenseSegment()
	if err != nil {
		t.Fatal(err)
	}
	parts := splitPathParts(diskRoot)
	parentIdx := storage.RootParentIdx
	var nextKey uint64 = 1
	for _, part := range parts {
		id, err := b.AppendDense(1, nextKey, parentIdx, part, storage.FlagDir, 0, 0, 1_700_000_000)
		if err != nil {
			t.Fatal(err)
		}
		parentIdx = id
		nextKey++
	}
	if _, err := b.AppendDense(1, nextKey, parentIdx, "node_modules", storage.FlagDir|storage.FlagFolded, 0, 0, 1_700_000_000); err != nil {
		t.Fatal(err)
	}
	meta, err := b.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CommitSegment(meta, nil); err != nil {
		t.Fatal(err)
	}

	eng := NewEngine(store)
	sess := eng.NewSession()
	rs, err := sess.Start(context.Background(), "in:node_modules pkg_", SearchOptions{
		Sort: SortByName,
	})
	if err != nil {
		t.Fatalf("Start in:node_modules: %v", err)
	}
	defer rs.Close()

	if rs.Total() != 30 || rs.Retained() != 30 {
		t.Fatalf("got total=%d retained=%d, want 30/30", rs.Total(), rs.Retained())
	}
	p1, err := rs.Viewport(context.Background(), 0, 10)
	if err != nil {
		t.Fatalf("Viewport page 1: %v", err)
	}
	p2, err := rs.Viewport(context.Background(), 10, 10)
	if err != nil {
		t.Fatalf("Viewport page 2: %v", err)
	}
	if len(p1) != 10 || len(p2) != 10 {
		t.Fatalf("page lens = %d, %d, want 10, 10", len(p1), len(p2))
	}
	if p1[0].Name != "pkg_00.js" || p2[0].Name != "pkg_10.js" {
		t.Fatalf("unexpected paging names: p1[0]=%s p2[0]=%s", p1[0].Name, p2[0].Name)
	}
}

func TestResultSetRankedRowSizeAndNoPointers(t *testing.T) {
	if sz := unsafe.Sizeof(rankedRow{}); sz != 8 {
		t.Fatalf("sizeof(rankedRow) = %d, want 8", sz)
	}
}

func TestResultSetConcurrentMutationsAndCompactRace(t *testing.T) {
	store, eng := buildResultSetTestEngine(t, 1000)
	sess := eng.NewSession()

	rs, err := sess.Start(context.Background(), "item_", SearchOptions{Sort: SortByRelevance})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer rs.Close()

	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 40; i++ {
				rows, err := rs.Viewport(context.Background(), (worker*50+i*10)%900, 20)
				if err != nil {
					t.Errorf("Viewport: %v", err)
					return
				}
				if len(rows) != 20 {
					t.Errorf("expected 20 rows, got %d", len(rows))
					return
				}
			}
		}(r)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			_ = store.ApplyBatch([]storage.Mutation{
				{
					Op:        storage.OpCreate,
					VolID:     1,
					NodeKey:   uint64(5000 + i),
					ParentKey: 1,
					Name:      fmt.Sprintf("new_%d.txt", i),
					Size:      123,
					ModTime:   time.Now().UnixNano(),
				},
				{
					Op:      storage.OpDelete,
					VolID:   1,
					NodeKey: uint64(i + 2),
				},
			})
			if i%3 == 0 {
				_ = store.Compact()
			}
		}
	}()

	wg.Wait()
}

func splitPathParts(p string) []string {
	clean := filepath.Clean(p)
	vol := filepath.VolumeName(clean)
	rest := clean[len(vol):]
	var parts []string
	if vol != "" {
		parts = append(parts, vol)
	} else if len(rest) > 0 && rest[0] == '/' {
		parts = append(parts, "/")
	}
	cur := rest
	for len(cur) > 0 {
		for len(cur) > 0 && (cur[0] == '\\' || cur[0] == '/') {
			cur = cur[1:]
		}
		if len(cur) == 0 {
			break
		}
		idx := 0
		for idx < len(cur) && cur[idx] != '\\' && cur[idx] != '/' {
			idx++
		}
		parts = append(parts, cur[:idx])
		cur = cur[idx:]
	}
	return parts
}
