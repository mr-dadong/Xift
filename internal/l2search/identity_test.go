package l2search

import (
	"os"
	"path/filepath"
	"testing"
)

// TestStoreIdentityV2 verifies documents are keyed by (VolID, NodeKey) and an
// incompatible v1 index is archived and rebuilt.
func TestStoreIdentityV2(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir, nil)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	docs := []Doc{
		{Key: DocKey{VolID: 2, NodeKey: 10}, MTime: 1, Text: []byte("mutex alpha")},
		{Key: DocKey{VolID: 1, NodeKey: 11}, MTime: 2, Text: []byte("mutex beta")},
		{Key: DocKey{VolID: 1, NodeKey: 10}, MTime: 3, Text: []byte("mutex gamma")},
	}
	if err := st.AddSegment(docs); err != nil {
		t.Fatalf("AddSegment: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st, err = OpenStore(dir, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	keys, _, err := st.Search([]byte("mutex"), 0)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	want := []DocKey{{1, 10}, {1, 11}, {2, 10}}
	if len(keys) != len(want) {
		t.Fatalf("keys = %v, want %v", keys, want)
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Fatalf("keys = %v, want %v", keys, want)
		}
	}
	for i, d := range docs {
		rec, ok := st.Doc(0, uint32(i))
		if !ok || rec.Key != d.Key {
			t.Fatalf("Doc(0,%d) = %+v, want key %+v", i, rec, d.Key)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	// A v1 MANIFEST must be archived aside and replaced with an empty index.
	v1Dir := filepath.Join(t.TempDir(), "l2")
	if err := os.MkdirAll(v1Dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(v1Dir, "MANIFEST"),
		[]byte(`{"version":1,"next_segment_id":1,"segments":[],"paused":false}`), 0o644); err != nil {
		t.Fatal(err)
	}
	st, err = OpenStore(v1Dir, nil)
	if err != nil {
		t.Fatalf("open v1: %v", err)
	}
	if st.DocCount() != 0 {
		t.Fatalf("v2 rebuild docs = %d, want 0", st.DocCount())
	}
	if _, err := os.Stat(v1Dir + ".v1.bak"); err != nil {
		t.Fatalf("v1 backup missing: %v", err)
	}
	if err := st.AddSegment([]Doc{{Key: DocKey{VolID: 1, NodeKey: 1}, Text: []byte("fresh mutex")}}); err != nil {
		t.Fatalf("AddSegment after migration: %v", err)
	}
	if keys, _, _ := st.Search([]byte("fresh"), 0); len(keys) != 1 {
		t.Fatalf("search after migration = %v", keys)
	}
	_ = st.Close()
}

// TestSearchReturnsDocKeys verifies newest generation wins per composite key
// and keys from different volumes stay distinct.
func TestSearchReturnsDocKeys(t *testing.T) {
	st, err := OpenStore(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	defer st.Close()

	if err := st.AddSegment([]Doc{
		{Key: DocKey{VolID: 1, NodeKey: 7}, Text: []byte("mutex old")},
		{Key: DocKey{VolID: 2, NodeKey: 7}, Text: []byte("mutex other volume")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.AddSegment([]Doc{
		{Key: DocKey{VolID: 1, NodeKey: 7}, Text: []byte("mutex new")},
	}); err != nil {
		t.Fatal(err)
	}
	keys, _, err := st.Search([]byte("mutex"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 {
		t.Fatalf("keys = %v, want two distinct composite keys", keys)
	}
	if keys[0] != (DocKey{VolID: 1, NodeKey: 7}) || keys[1] != (DocKey{VolID: 2, NodeKey: 7}) {
		t.Fatalf("keys = %v", keys)
	}
}
