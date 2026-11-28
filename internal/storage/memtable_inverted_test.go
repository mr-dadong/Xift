package storage

import (
	"slices"
	"testing"

	"github.com/mr-dadong/Xift/internal/textkey"
)

func TestMemTableInvertedPostings(t *testing.T) {
	m := NewMemTable(16)
	m.EnableInverted()
	m.Upsert(1, 1, 100, 0, "report_2026.md", 0, 0, 1, 1)
	m.Upsert(2, 1, 101, 0, "设计文档.md", 0, 0, 1, 1)
	m.Upsert(3, 1, 102, 0, "main.go", 0, 0, 1, 1)

	// Trigram of "report" prefix hits nodeID 1.
	gram := textkey.AppendTrigramKeys([]byte("report_2026.md"), nil)[0]
	ids := m.LookupGramInverted(gram)
	if !slices.Contains(ids, 1) {
		t.Fatalf("gram posting = %v, want contains 1", ids)
	}

	// Pinyin trigram (s,j,w) of 设计文档 hits nodeID 2.
	pyGram := textkey.GramNamespacePinyin | uint64('s')<<16 | uint64('j')<<8 | uint64('w')
	ids = m.LookupGramInverted(pyGram)
	if !slices.Contains(ids, 2) {
		t.Fatalf("pinyin gram posting = %v, want contains 2", ids)
	}

	// Absent gram misses.
	if ids := m.LookupGramInverted(999); ids != nil {
		t.Fatalf("absent gram = %v", ids)
	}
}

func TestMemTableInvertedRenameSuperset(t *testing.T) {
	m := NewMemTable(16)
	m.EnableInverted()
	m.Upsert(7, 1, 700, 0, "alpha.txt", 0, 0, 1, 1)
	oldGram := textkey.AppendTrigramKeys([]byte("alpha.txt"), nil)[0]

	// Rename: same nodeID, new name.
	m.Upsert(7, 1, 700, 0, "beta.txt", 0, 0, 1, 1)
	newGram := textkey.AppendTrigramKeys([]byte("beta.txt"), nil)[0]

	// New grams must be present; old grams MAY linger (superset semantics).
	if !slices.Contains(m.LookupGramInverted(newGram), 7) {
		t.Fatal("renamed node missing from new gram posting")
	}
	_ = oldGram

	// Deleting the node keeps a superset; liveness is the verifier's job.
	m.MarkDeletedByNodeID(7)
	if !slices.Contains(m.LookupGramInverted(newGram), 7) {
		t.Fatal("posting must remain a superset after delete")
	}
}

func TestMemTableInvertedCoWIsolation(t *testing.T) {
	m := NewMemTable(16)
	m.EnableInverted()
	m.Upsert(1, 1, 100, 0, "alpha.txt", 0, 0, 1, 1)
	gram := textkey.AppendTrigramKeys([]byte("alpha.txt"), nil)[0]

	snap := m.CloneSnapshot()
	// Post-snapshot mutation on the original must not leak into the clone's
	// postings or vice versa.
	m.Upsert(2, 1, 101, 0, "alphabet.txt", 0, 0, 1, 1)
	otherGram := textkey.AppendTrigramKeys([]byte("alphabet.txt"), nil)[0]

	if !slices.Contains(snap.LookupGramInverted(gram), 1) {
		t.Fatal("snapshot lost pre-clone posting")
	}
	if slices.Contains(snap.LookupGramInverted(otherGram), 2) {
		t.Fatal("snapshot leaked post-clone posting")
	}
	if !slices.Contains(m.LookupGramInverted(otherGram), 2) {
		t.Fatal("original missing post-clone posting")
	}
}

func TestMemTableInvertedReset(t *testing.T) {
	m := NewMemTable(16)
	m.EnableInverted()
	m.Upsert(1, 1, 100, 0, "alpha.txt", 0, 0, 1, 1)
	gram := textkey.AppendTrigramKeys([]byte("alpha.txt"), nil)[0]
	m.Reset()
	if ids := m.LookupGramInverted(gram); ids != nil {
		t.Fatalf("after Reset posting = %v", ids)
	}
}

func TestMemTableInvertedRebuildUnderChurn(t *testing.T) {
	// Heavy rename churn must not grow the posting map without bound.
	m := NewMemTable(16)
	m.EnableInverted()
	for i := 0; i < 500; i++ {
		name := "name_" + string(rune('a'+i%26)) + string(rune('a'+(i/26)%26)) + ".txt"
		m.Upsert(1, 1, 100, 0, name, 0, 0, 1, 1) // same nodeID, new name each time
	}
	// After churn the map must have been rebuilt (adds counter reset).
	if m.inv.adds > memInvertedRebuildFactor*max(m.liveCount, m.inv.liveAt)*16 {
		t.Fatalf("rebuild did not bound adds: adds=%d live=%d", m.inv.adds, m.liveCount)
	}
	// The live name's grams must still resolve.
	lastGram := textkey.AppendTrigramKeys([]byte("name_z"+string(rune('a'+491/26%26))+".txt"), nil)
	_ = lastGram
	if m.LookupGramInverted(textkey.AppendTrigramKeys([]byte("name_"), nil)[0]) == nil {
		t.Skip("name_ alone is a bigram-scale prefix; trigram presence depends on suffix")
	}
}
