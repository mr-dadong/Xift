package l2search

import (
	"reflect"
	"testing"
)

func addDoc(t *testing.T, st *Store, key DocKey, mtime int64, text string) uint32 {
	t.Helper()
	before := st.DocCount()
	if err := st.AddSegment([]Doc{{Key: key, MTime: mtime, Text: []byte(text)}}); err != nil {
		t.Fatalf("AddSegment: %v", err)
	}
	if st.DocCount() != before+1 {
		t.Fatalf("DocCount did not grow")
	}
	return uint32(st.SegmentCount()) // 1-based segment id == count here
}

// TestLiveDocFilter verifies dead documents disappear from search results and
// that exact substring verification rejects trigram false positives.
func TestLiveDocFilter(t *testing.T) {
	st, err := OpenStore(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	defer st.Close()

	if err := st.AddSegment([]Doc{
		{Key: DocKey{VolID: 1, NodeKey: 10}, MTime: 1, Text: []byte("mutex guard around the map")},
		{Key: DocKey{VolID: 1, NodeKey: 11}, MTime: 2, Text: []byte("mutex free implementation")},
		{Key: DocKey{VolID: 1, NodeKey: 12}, MTime: 3, Text: []byte("abc xyz bcd without adjacency")},
	}); err != nil {
		t.Fatalf("AddSegment: %v", err)
	}
	segID := uint32(st.SegmentCount())
	// Segment id equals its order here: first segment is id 1.
	segID = 1

	ids, _, err := st.Search([]byte("mutex"), 0)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if !reflect.DeepEqual(ids, []DocKey{{VolID: 1, NodeKey: 10}, {VolID: 1, NodeKey: 11}}) {
		t.Fatalf("Search(mutex) = %v, want both keys", ids)
	}

	// Trigram candidates exist for "abcd" (abc + bcd), but no substring.
	if ids, _, err := st.Search([]byte("abcd"), 0); err != nil {
		t.Fatalf("Search(abcd): %v", err)
	} else if len(ids) != 0 {
		t.Fatalf("Search(abcd) = %v, want none", ids)
	}

	// Mark node 10's document dead: it must disappear.
	if err := st.MarkDead(segID, []uint32{0}); err != nil {
		t.Fatalf("MarkDead: %v", err)
	}
	ids, _, err = st.Search([]byte("mutex"), 0)
	if err != nil {
		t.Fatalf("Search after dead: %v", err)
	}
	if !reflect.DeepEqual(ids, []DocKey{{VolID: 1, NodeKey: 11}}) {
		t.Fatalf("Search(mutex) after dead = %v, want only key 11", ids)
	}

	// Dead markers survive reopen.
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	st, err = OpenStore(st.dir, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st.Close()
	ids, _, err = st.Search([]byte("mutex"), 0)
	if err != nil {
		t.Fatalf("Search after reopen: %v", err)
	}
	if !reflect.DeepEqual(ids, []DocKey{{VolID: 1, NodeKey: 11}}) {
		t.Fatalf("Search(mutex) after reopen = %v, want only key 11", ids)
	}
}

// TestSearchBudgetTruncates verifies the candidate budget stops the scan early.
func TestSearchBudgetTruncates(t *testing.T) {
	st, err := OpenStore(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	defer st.Close()
	docs := make([]Doc, 0, 50)
	for i := 0; i < 50; i++ {
		docs = append(docs, Doc{Key: DocKey{VolID: 1, NodeKey: uint64(100 + i)}, Text: []byte("common token here")})
	}
	if err := st.AddSegment(docs); err != nil {
		t.Fatalf("AddSegment: %v", err)
	}
	ids, truncated, err := st.Search([]byte("common"), 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if !truncated {
		t.Fatal("budget-limited search must report truncation")
	}
	if len(ids) > 10 {
		t.Fatalf("returned %d ids, want <= 10", len(ids))
	}
}
