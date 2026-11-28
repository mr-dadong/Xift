package l2search

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// TestSegmentMergeEquivalence verifies merging preserves the exact live answer
// set and leaves one segment behind.
func TestSegmentMergeEquivalence(t *testing.T) {
	st, err := OpenStore(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	defer st.Close()

	// Nine segments: node 500 is updated each time (newest must win), and the
	// others are stable.
	for i := 0; i < 9; i++ {
		docs := []Doc{
			{NodeID: uint32(100 + i), MTime: int64(i), Text: []byte("stable mutex entry")},
			{NodeID: 500, MTime: int64(i), Text: []byte("mutex generation")},
		}
		if err := st.AddSegment(docs); err != nil {
			t.Fatalf("AddSegment %d: %v", i, err)
		}
		if i < 8 {
			// Supersede the previous node-500 document.
			prevSeg := uint32(i + 1)
			if err := st.MarkDead(prevSeg, []uint32{1}); err != nil {
				t.Fatalf("MarkDead: %v", err)
			}
		}
	}
	before, _, err := st.Search([]byte("mutex"), 0)
	if err != nil {
		t.Fatalf("Search before: %v", err)
	}
	if !st.NeedsMerge() {
		t.Fatal("nine segments must require a merge")
	}
	if err := st.Merge(); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if got := st.SegmentCount(); got != 1 {
		t.Fatalf("after merge segments = %d, want 1", got)
	}
	after, _, err := st.Search([]byte("mutex"), 0)
	if err != nil {
		t.Fatalf("Search after: %v", err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("merge changed results: before %v, after %v", before, after)
	}
	if got := st.DocCount(); got != 10 {
		t.Fatalf("after merge docs = %d, want 10 live nodes", got)
	}
}

// TestDiskCapPauses verifies the disk budget stops ingestion and reports it.
func TestDiskCapPauses(t *testing.T) {
	st, err := OpenStore(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	defer st.Close()

	st.SetDiskCap(100)
	err = st.AddSegment([]Doc{{NodeID: 1, Text: []byte("some mutex content that exceeds the tiny cap")}})
	if !errors.Is(err, ErrDiskCap) {
		t.Fatalf("AddSegment err = %v, want ErrDiskCap", err)
	}
	if !st.Paused() {
		t.Fatal("store must report paused after hitting the disk cap")
	}
	if st.DocCount() != 0 {
		t.Fatalf("paused store must not index new docs, got %d", st.DocCount())
	}

	st.SetDiskCap(1 << 20)
	if err := st.AddSegment([]Doc{{NodeID: 1, Text: []byte("fits now")}}); err != nil {
		t.Fatalf("AddSegment after raising cap: %v", err)
	}
	if st.Paused() {
		t.Fatal("store must unpause after a successful add")
	}
}

// TestAutoMergeAfterIndex verifies the workspace compacts segments on its own
// once an indexing pass leaves more than the merge threshold behind.
func TestAutoMergeAfterIndex(t *testing.T) {
	root := t.TempDir()
	for i := 0; i < 12; i++ {
		writeFile(t, filepath.Join(root, "f", pathName(i)), "merge marker text")
	}
	res := newFakeResolver()
	ws, st := openWorkspace(t, WSOptions{Resolver: res, BatchDocs: 1})
	ws.AddRoot(root)
	if err := ws.IndexRoot(context.Background(), root); err != nil {
		t.Fatalf("IndexRoot: %v", err)
	}
	if got := st.SegmentCount(); got != 1 {
		t.Fatalf("segments after auto-merge = %d, want 1", got)
	}
	ids, _, err := ws.Search([]byte("merge marker"), 0)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(ids) != 12 {
		t.Fatalf("search ids = %d, want 12", len(ids))
	}
}

// TestDeleteAfterAutoMerge verifies per-path references survive compaction, so
// a later delete still invalidates the merged document.
func TestDeleteAfterAutoMerge(t *testing.T) {
	root := t.TempDir()
	paths := make([]string, 12)
	for i := range paths {
		paths[i] = filepath.Join(root, "f", pathName(i))
		writeFile(t, paths[i], "survivor marker")
	}
	res := newFakeResolver()
	ws, st := openWorkspace(t, WSOptions{Resolver: res, BatchDocs: 1, Debounce: time.Millisecond})
	ws.AddRoot(root)
	if err := ws.IndexRoot(context.Background(), root); err != nil {
		t.Fatalf("IndexRoot: %v", err)
	}
	if got := st.SegmentCount(); got != 1 {
		t.Fatalf("segments = %d, want 1 after auto-merge", got)
	}

	ws.EnqueueDelete(paths[3])
	time.Sleep(5 * time.Millisecond)
	ws.flushIfDue(context.Background())

	ids, _, err := ws.Search([]byte("survivor marker"), 0)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(ids) != 11 {
		t.Fatalf("search ids after delete = %d, want 11", len(ids))
	}
	for _, id := range ids {
		if id == res.idOf(paths[3]) {
			t.Fatal("deleted document survived auto-merge")
		}
	}
}
