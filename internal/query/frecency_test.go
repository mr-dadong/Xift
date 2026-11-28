package query

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/mr-dadong/Xift/internal/storage"
)

func TestFrecencyStoreDecayEvictionAndPersistence(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "frecency.db")

	base := time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)
	fs, err := OpenFrecencyStore(dbPath, FrecencyOptions{MaxEntries: 3})
	if err != nil {
		t.Fatalf("OpenFrecencyStore: %v", err)
	}

	// File A opened 60 days ago; File B opened 1 hour ago.
	fs.RecordAt("/work/fileA.md", base.Add(-60*24*time.Hour))
	fs.RecordAt("/work/fileB.md", base.Add(-1*time.Hour))
	if err := fs.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Reload into a fresh store instance.
	fs2, err := OpenFrecencyStore(dbPath, FrecencyOptions{MaxEntries: 3})
	if err != nil {
		t.Fatalf("reopen OpenFrecencyStore: %v", err)
	}
	scoreA := fs2.ScoreAt("/work/fileA.md", base)
	scoreB := fs2.ScoreAt("/work/fileB.md", base)
	if scoreB <= scoreA {
		t.Fatalf("expected recent fileB (%v) > stale fileA (%v)", scoreB, scoreA)
	}

	// Capacity eviction: add 2 more recent files (total 4 > MaxEntries 3), oldest/lowest (fileA) must be evicted.
	fs2.RecordAt("/work/fileC.md", base)
	fs2.RecordAt("/work/fileD.md", base)
	if fs2.Len() != 3 {
		t.Fatalf("expected Len=3 after eviction, got %d", fs2.Len())
	}
	if fs2.ScoreAt("/work/fileA.md", base) != 0 {
		t.Fatalf("expected lowest-score fileA to be evicted, got score %v", fs2.ScoreAt("/work/fileA.md", base))
	}
}

func TestEngineRankingBoostedByFrecencyStore(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.Open(dir, storage.Options{})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	defer store.Close()

	ops := []storage.Mutation{
		{Op: storage.OpCreate, VolID: 1, NodeKey: 1, ParentKey: 0, Name: "root", Flags: storage.FlagDir},
		{Op: storage.OpCreate, VolID: 1, NodeKey: 2, ParentKey: 1, Name: "archive", Flags: storage.FlagDir},
		{Op: storage.OpCreate, VolID: 1, NodeKey: 3, ParentKey: 1, Name: "notes", Flags: storage.FlagDir},
		{Op: storage.OpCreate, VolID: 1, NodeKey: 4, ParentKey: 2, Name: "spec.md"},
		{Op: storage.OpCreate, VolID: 1, NodeKey: 5, ParentKey: 3, Name: "spec.md"},
	}
	if err := store.ApplyBatch(ops); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}

	fs, err := OpenFrecencyStore(filepath.Join(dir, "frecency.db"), FrecencyOptions{})
	if err != nil {
		t.Fatalf("OpenFrecencyStore: %v", err)
	}
	fs.Record("root/notes/spec.md")
	fs.Record("root/notes/spec.md")

	eng := NewEngine(store, WithFrecency(fs))
	res, err := eng.Search(context.Background(), "spec.md", SearchOptions{Sort: SortByRelevance, Limit: 10})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(res.Items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(res.Items))
	}
	if res.Items[0].Path != "root/notes/spec.md" {
		t.Fatalf("expected boosted root/notes/spec.md first, got %q (scores: %v vs %v)",
			res.Items[0].Path, res.Items[0].Score, res.Items[1].Score)
	}
}

func TestFrecencyStoreConcurrentSaveAndTierOrder(t *testing.T) {
	if !(TierWildcard < TierFuzzy && TierFuzzy < TierPinyin && TierPinyin < TierSubstring) {
		t.Fatalf("unexpected tier order: wildcard=%d fuzzy=%d pinyin=%d substring=%d",
			TierWildcard, TierFuzzy, TierPinyin, TierSubstring)
	}
	dbPath := filepath.Join(t.TempDir(), "frecency.db")
	fs, err := OpenFrecencyStore(dbPath, FrecencyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 8)
	for g := 0; g < 8; g++ {
		go func(id int) {
			for i := 0; i < 10; i++ {
				fs.Record("/work/file_" + string(rune('a'+id)) + ".go")
				if err := fs.Save(); err != nil {
					done <- err
					return
				}
			}
			done <- nil
		}(g)
	}
	for g := 0; g < 8; g++ {
		if err := <-done; err != nil {
			t.Fatalf("concurrent Save failed: %v", err)
		}
	}
}
