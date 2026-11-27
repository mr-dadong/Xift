package query

import (
	"context"
	"testing"

	"github.com/mr-dadong/Xift/internal/storage"
)

func TestFuzzySubsequenceMatchingAndHighlights(t *testing.T) {
	// Multi-char abbreviation matches and produces accurate highlight ranges.
	tier, ranges := MatchFuzzy([]byte("alpha_beta_gamma.go"), false, []byte("abg"), "abg")
	if tier != TierFuzzy {
		t.Fatalf("expected TierFuzzy, got %v", tier)
	}
	var got string
	for _, r := range ranges {
		got += "alpha_beta_gamma.go"[r.Start:r.End]
	}
	if got != "abg" {
		t.Fatalf("expected highlighted chars 'abg', got %q (ranges=%+v)", got, ranges)
	}

	// Single-char terms MUST NOT trigger Fuzzy fallback.
	if tier, _ := MatchFuzzy([]byte("readme.md"), false, []byte("z"), "z"); tier != TierNone {
		t.Fatalf("single char must not match fuzzy, got %v", tier)
	}
	if tier, _ := MatchFuzzy([]byte("query_engine_ranker.go"), false, []byte("q"), "q"); tier != TierNone {
		t.Fatalf("single char must not trigger MatchFuzzy, got %v", tier)
	}

	// Non-matching subsequence.
	if tier, _ := MatchFuzzy([]byte("readme.md"), false, []byte("qer"), "qer"); tier != TierNone {
		t.Fatalf("expected TierNone for readme.md vs qer, got %v", tier)
	}
}

func TestMatchTierOrderingExactSubstringPinyinFuzzy(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.Open(dir, storage.Options{})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	defer store.Close()

	ops := []storage.Mutation{
		{Op: storage.OpCreate, VolID: 1, NodeKey: 1, ParentKey: 0, Name: "root", Flags: storage.FlagDir},
		{Op: storage.OpCreate, VolID: 1, NodeKey: 2, ParentKey: 1, Name: "wdjg.txt"},
		{Op: storage.OpCreate, VolID: 1, NodeKey: 3, ParentKey: 1, Name: "文档架构.md", Flags: storage.FlagNonASCII},
		{Op: storage.OpCreate, VolID: 1, NodeKey: 4, ParentKey: 1, Name: "world_design_job_guide.txt"},
	}
	if err := store.ApplyBatch(ops); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}

	eng := NewEngine(store)
	res, err := eng.Search(context.Background(), "wdjg", SearchOptions{Sort: SortByRelevance, Limit: 10})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(res.Items) != 3 {
		t.Fatalf("expected 3 items, got %d: %+v", len(res.Items), res.Items)
	}
	if res.Items[0].Name != "wdjg.txt" {
		t.Errorf("rank 0 = %q, want wdjg.txt", res.Items[0].Name)
	}
	if res.Items[1].Name != "文档架构.md" {
		t.Errorf("rank 1 = %q, want 文档架构.md", res.Items[1].Name)
	}
	if res.Items[2].Name != "world_design_job_guide.txt" {
		t.Errorf("rank 2 = %q, want world_design_job_guide.txt", res.Items[2].Name)
	}

	// Verify highlights on pinyin and fuzzy items.
	pyItem := res.Items[1]
	var pyHL string
	for _, r := range pyItem.Highlights {
		pyHL += pyItem.Name[r.Start:r.End]
	}
	if pyHL != "文档架构" {
		t.Errorf("pinyin highlights sliced %q, want 文档架构", pyHL)
	}

	fzItem := res.Items[2]
	if len(fzItem.Highlights) == 0 {
		t.Errorf("fuzzy item missing highlights")
	}
}
