package l2search

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir, nil)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	docs := []Doc{
		{NodeID: 10, MTime: 100, Text: []byte("mutex guard around the map")},
		{NodeID: 11, MTime: 200, Text: []byte("golang implementation notes")},
		{NodeID: 12, MTime: 300, Text: []byte("设计文档 mutex 说明")},
	}
	if err := st.AddSegment(docs); err != nil {
		t.Fatalf("AddSegment: %v", err)
	}
	if got := st.SegmentCount(); got != 1 {
		t.Fatalf("SegmentCount = %d, want 1", got)
	}
	if got := st.DocCount(); got != len(docs) {
		t.Fatalf("DocCount = %d, want %d", got, len(docs))
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	st, err = OpenStore(dir, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st.Close()
	if got := st.SegmentCount(); got != 1 {
		t.Fatalf("reopened SegmentCount = %d, want 1", got)
	}
	if got := st.DocCount(); got != len(docs) {
		t.Fatalf("reopened DocCount = %d, want %d", got, len(docs))
	}
	for i, want := range docs {
		rec, ok := st.Doc(0, uint32(i))
		if !ok {
			t.Fatalf("Doc(0,%d) missing", i)
		}
		if rec.NodeID != want.NodeID || rec.MTime != want.MTime {
			t.Fatalf("Doc(0,%d) = %+v, want node %d mtime %d", i, rec, want.NodeID, want.MTime)
		}
	}
	if _, ok := st.Doc(0, 99); ok {
		t.Fatal("out-of-range Doc must not resolve")
	}

	if _, err := os.Stat(filepath.Join(dir, "MANIFEST")); err != nil {
		t.Fatalf("MANIFEST missing: %v", err)
	}
}
