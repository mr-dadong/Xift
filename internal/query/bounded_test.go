package query

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/mr-dadong/Xift/internal/storage"
)

// openStoreN builds an index containing n files named
// report_0000.txt .. report_NNNN.txt plus a volume root.
func openStoreN(t *testing.T, n int, tier2 bool) (*storage.Engine, *Engine) {
	t.Helper()
	opts := storage.Options{DisableWALSync: true, DisableBackgroundMaintenance: true, LowMemoryMode: tier2}
	store, err := storage.Open(t.TempDir(), opts)
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	mt := time.Date(2026, 11, 27, 15, 0, 0, 0, time.UTC).UnixNano()
	batch := make([]storage.Mutation, 0, n+2)
	batch = append(batch, storage.Mutation{
		Op: storage.OpCreate, VolID: 1, NodeKey: 1, ParentKey: 0,
		Name: "root", Flags: storage.FlagDir, ModTime: mt,
	})
	for i := 0; i < n; i++ {
		batch = append(batch, storage.Mutation{
			Op: storage.OpCreate, VolID: 1, NodeKey: uint64(i + 2), ParentKey: 1,
			Name: fmt.Sprintf("report_%04d.txt", i), Size: int64(i), ModTime: mt + int64(i),
		})
	}
	if err := store.ApplyBatch(batch); err != nil {
		_ = store.Close()
		t.Fatalf("ApplyBatch: %v", err)
	}
	if err := store.Flush(); err != nil {
		_ = store.Close()
		t.Fatalf("Flush: %v", err)
	}
	return store, NewEngine(store)
}

// TestTier2ScanBounded verifies the Tier 2 scan stops at the candidate budget
// and reports both truncation and the verified-count lower bound.
func TestTier2ScanBounded(t *testing.T) {
	const nodes = 200
	store, _ := openStoreN(t, nodes, true)
	defer store.Close()

	q, err := ParseAt("report", tier2TestNow)
	if err != nil {
		t.Fatalf("ParseAt: %v", err)
	}
	plan := compileQuery(q, store.ExtPool())
	snap := store.AcquireSnapshot()
	defer snap.Release()

	out, err := scanSnapshot(context.Background(), snap, plan, 1, 50)
	if err != nil {
		t.Fatalf("scanSnapshot: %v", err)
	}
	if got := len(out.cands); got != 50 {
		t.Fatalf("retained %d candidates, want 50", got)
	}
	if out.verified != 50 {
		t.Fatalf("verified = %d, want 50", out.verified)
	}
	if !out.truncated {
		t.Fatal("scan must report truncation once the budget is exhausted")
	}
}

// TestBoundedSearchTruncationReporting verifies the engine-level contract:
// the lower bound is the verified count and the viewport still fills.
func TestBoundedSearchTruncationReporting(t *testing.T) {
	const nodes = 200
	store, qe := openStoreN(t, nodes, true)
	defer store.Close()

	res, err := qe.Search(context.Background(), "report", SearchOptions{Limit: 10, CandidateBudget: 50})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if !res.Truncated {
		t.Fatal("Search must report truncation for a budget-limited broad query")
	}
	if res.Total != 50 {
		t.Fatalf("Total = %d, want lower bound 50", res.Total)
	}
	if len(res.Items) != 10 {
		t.Fatalf("returned %d items, want 10", len(res.Items))
	}
	for _, it := range res.Items {
		if it.Name == "" {
			t.Fatalf("item %+v missing materialized name", it)
		}
	}
}

// TestExactCount verifies Count reports the true match total regardless of the
// interactive budget, and that its result is consistent with what Search returned.
func TestExactCount(t *testing.T) {
	const nodes = 200
	store, qe := openStoreN(t, nodes, true)
	defer store.Close()

	broad, err := qe.Search(context.Background(), "report", SearchOptions{Limit: 10, CandidateBudget: 20})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if !broad.Truncated {
		t.Fatal("precondition: broad query must be truncated")
	}

	total, err := qe.Count(context.Background(), "report", SearchOptions{})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if total != nodes {
		t.Fatalf("Count = %d, want %d", total, nodes)
	}
	if total < broad.TotalLowerBound {
		t.Fatalf("Count %d < lower bound %d", total, broad.TotalLowerBound)
	}

	// Every returned item must be part of the exact count.
	again, err := qe.Count(context.Background(), "report_0199", SearchOptions{})
	if err != nil {
		t.Fatalf("Count(narrow): %v", err)
	}
	if again != 1 {
		t.Fatalf("Count(report_0199) = %d, want 1", again)
	}
}

// TestTruncationReporting verifies the distinction between an exact result and
// a truncated one: narrow queries stay exact, broad ones carry a lower bound.
func TestTruncationReporting(t *testing.T) {
	const nodes = 200
	store, qe := openStoreN(t, nodes, true)
	defer store.Close()

	narrow, err := qe.Search(context.Background(), "report_0199", SearchOptions{Limit: 10, CandidateBudget: 50})
	if err != nil {
		t.Fatalf("Search(narrow): %v", err)
	}
	if narrow.Truncated {
		t.Fatal("narrow query must not be truncated")
	}
	if narrow.Total != 1 || narrow.TotalLowerBound != 0 {
		t.Fatalf("narrow Total=%d lower=%d, want 1/0", narrow.Total, narrow.TotalLowerBound)
	}

	broad, err := qe.Search(context.Background(), "report", SearchOptions{Limit: 10, CandidateBudget: 50})
	if err != nil {
		t.Fatalf("Search(broad): %v", err)
	}
	if !broad.Truncated {
		t.Fatal("broad query must be truncated")
	}
	if broad.TotalLowerBound < len(broad.Items) {
		t.Fatalf("lower bound %d < returned %d", broad.TotalLowerBound, len(broad.Items))
	}
	if broad.TotalLowerBound != 50 {
		t.Fatalf("lower bound = %d, want verified count 50", broad.TotalLowerBound)
	}
}

// TestBoundedEqualsFullRank verifies that when every match fits the budget the
// result set is identical to the unbounded scan.
func TestBoundedEqualsFullRank(t *testing.T) {
	const nodes = 120
	tier2Store, tier2Engine := openStoreN(t, nodes, true)
	defer tier2Store.Close()
	tier1Store, tier1Engine := openStoreN(t, nodes, false)
	defer tier1Store.Close()

	// Tier 1 never applies a budget, so its ranking is the unbounded reference.
	full, err := tier1Engine.Search(context.Background(), "report_00", SearchOptions{Limit: 20})
	if err != nil {
		t.Fatalf("Search(tier1): %v", err)
	}
	bounded, err := tier2Engine.Search(context.Background(), "report_00", SearchOptions{Limit: 20, CandidateBudget: 100000})
	if err != nil {
		t.Fatalf("Search(tier2): %v", err)
	}
	if full.Truncated || bounded.Truncated {
		t.Fatal("queries fitting the budget must not report truncation")
	}
	if full.Total != bounded.Total {
		t.Fatalf("Total mismatch: %d vs %d", full.Total, bounded.Total)
	}
	if want := 100; full.Total != want {
		t.Fatalf("Total = %d, want %d", full.Total, want)
	}
	if len(full.Items) != len(bounded.Items) {
		t.Fatalf("item count mismatch: %d vs %d", len(full.Items), len(bounded.Items))
	}
	for i := range full.Items {
		if full.Items[i].Name != bounded.Items[i].Name {
			t.Fatalf("item %d mismatch: %q vs %q", i, full.Items[i].Name, bounded.Items[i].Name)
		}
	}
}

// TestTruncatedSessionNotCached verifies a truncated result never seeds the
// session's incremental narrowing, so a later keystroke still sees matches that
// fell outside the earlier budget window.
func TestTruncatedSessionNotCached(t *testing.T) {
	const nodes = 200
	store, qe := openStoreN(t, nodes, true)
	defer store.Close()

	sess := qe.NewSession()
	defer sess.Reset()

	first, err := sess.Start(context.Background(), "report", SearchOptions{Limit: 10, CandidateBudget: 20})
	if err != nil {
		t.Fatalf("Start(report): %v", err)
	}
	if !first.Truncated() {
		t.Fatal("first query must be truncated")
	}
	first.Close()

	// report_0199 lies beyond the 20-candidate window; a narrowing that reused
	// truncated candidates would miss it.
	next, err := sess.Start(context.Background(), "report_0199", SearchOptions{Limit: 10, CandidateBudget: 20})
	if err != nil {
		t.Fatalf("Start(report_0199): %v", err)
	}
	defer next.Close()
	if next.Incremental() {
		t.Fatal("truncated result must not be reused for incremental narrowing")
	}
	items, err := next.Viewport(context.Background(), 0, 10)
	if err != nil {
		t.Fatalf("Viewport: %v", err)
	}
	if len(items) != 1 || items[0].Name != "report_0199.txt" {
		t.Fatalf("items = %v, want report_0199.txt", itemNames(&SearchResult{Items: items}))
	}
}
