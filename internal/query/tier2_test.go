package query

import (
	"context"
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/mr-dadong/Xift/internal/storage"
)

var tier2TestNow = time.Date(2026, 11, 27, 15, 0, 0, 0, time.UTC)

func tier2TestCorpus() []storage.Mutation {
	mt := func(daysAgo int) int64 { return tier2TestNow.AddDate(0, 0, -daysAgo).UnixNano() }
	return []storage.Mutation{
		{Op: storage.OpCreate, VolID: 1, NodeKey: 1, ParentKey: 0, Name: "root", Flags: storage.FlagDir, ModTime: mt(400)},
		{Op: storage.OpCreate, VolID: 1, NodeKey: 2, ParentKey: 1, Name: "dirA", Flags: storage.FlagDir, ModTime: mt(390)},
		{Op: storage.OpCreate, VolID: 1, NodeKey: 3, ParentKey: 2, Name: "report_2026.md", Size: 2048, ModTime: mt(10)},
		{Op: storage.OpCreate, VolID: 1, NodeKey: 4, ParentKey: 2, Name: "report_2025.md", Size: 1500, ModTime: mt(200)},
		{Op: storage.OpCreate, VolID: 1, NodeKey: 5, ParentKey: 2, Name: "main.go", Size: 300, ModTime: mt(5)},
		{Op: storage.OpCreate, VolID: 1, NodeKey: 6, ParentKey: 2, Name: "README.md", Size: 900, ModTime: mt(30)},
		{Op: storage.OpCreate, VolID: 1, NodeKey: 7, ParentKey: 2, Name: "设计文档.md", Size: 700, ModTime: mt(1)},
		{Op: storage.OpCreate, VolID: 1, NodeKey: 8, ParentKey: 2, Name: "年度报告.txt", Size: 1800, ModTime: mt(60)},
		{Op: storage.OpCreate, VolID: 1, NodeKey: 9, ParentKey: 2, Name: "img_0001.png", Size: 5000, ModTime: mt(90)},
		{Op: storage.OpCreate, VolID: 1, NodeKey: 10, ParentKey: 2, Name: "notes.txt", Size: 100, ModTime: mt(3)},
		{Op: storage.OpCreate, VolID: 1, NodeKey: 11, ParentKey: 2, Name: "world_design_job_guide.txt", Size: 600, ModTime: mt(20)},
	}
}

func buildTierQueryEngine(t *testing.T, tier2 bool) (*storage.Engine, *Engine) {
	t.Helper()
	opts := storage.Options{DisableWALSync: true, DisableBackgroundMaintenance: true}
	if tier2 {
		opts.LowMemoryMode = true
	}
	store, err := storage.Open(t.TempDir(), opts)
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	if err := store.ApplyBatch(tier2TestCorpus()); err != nil {
		_ = store.Close()
		t.Fatalf("ApplyBatch: %v", err)
	}
	if err := store.Flush(); err != nil {
		_ = store.Close()
		t.Fatalf("Flush: %v", err)
	}
	return store, NewEngine(store)
}

func mustCompilePlan(t *testing.T, store *storage.Engine, raw string) *compiledPlan {
	t.Helper()
	q, err := ParseAt(raw, tier2TestNow)
	if err != nil {
		t.Fatalf("ParseAt(%q): %v", raw, err)
	}
	return compileQuery(q, store.ExtPool())
}

func itemNames(res *SearchResult) []string {
	out := make([]string, len(res.Items))
	for i, it := range res.Items {
		out[i] = it.Name
	}
	return out
}

func TestRouter(t *testing.T) {
	tier2Store, _ := buildTierQueryEngine(t, true)
	defer tier2Store.Close()
	tier1Store, _ := buildTierQueryEngine(t, false)
	defer tier1Store.Close()

	tier2Snap := tier2Store.AcquireSnapshot()
	defer tier2Snap.Release()
	ss2 := tier2Snap.SegmentSnapshotAt(0)
	if !ss2.IsTier2() {
		t.Fatal("expected a Tier 2 segment")
	}

	tier1Snap := tier1Store.AcquireSnapshot()
	defer tier1Snap.Release()
	ss1 := tier1Snap.SegmentSnapshotAt(0)
	if ss1.IsTier2() {
		t.Fatal("expected a Tier 1 segment")
	}

	tier2Cases := []struct {
		query string
		want  routePlan
	}{
		{"report", routeInverted},
		{"report 2026", routeInverted},
		{"设计文档", routeInverted},
		{"sjw", routeInverted},
		{"ab", routeShortWord},
		{"a", routeShortWord},
		{"ext:md", routeScan},
		{"report | README", routeScan},
		{"!report", routeScan},
		{"*.go", routeScan},
		{"", routeScan},
	}
	for _, tc := range tier2Cases {
		plan := mustCompilePlan(t, tier2Store, tc.query)
		if got := routeQuery(plan, ss2, false); got != tc.want {
			t.Errorf("routeQuery(%q) = %s, want %s", tc.query, got, tc.want)
		}
	}

	// Tier 1 keeps the unchanged full-scan plan.
	for _, raw := range []string{"report", "report 2026", "ab"} {
		plan := mustCompilePlan(t, tier1Store, raw)
		if got := routeQuery(plan, ss1, false); got != routeScan {
			t.Errorf("Tier 1 routeQuery(%q) = %s, want scan", raw, got)
		}
	}

	// Session incremental narrowing has the highest priority.
	plan := mustCompilePlan(t, tier2Store, "report")
	if got := routeQuery(plan, ss2, true); got != routeIncremental {
		t.Errorf("incremental routeQuery = %s, want incremental", got)
	}
}

func TestInvertedScanSemantics(t *testing.T) {
	store, eng := buildTierQueryEngine(t, true)
	defer store.Close()
	ctx := context.Background()
	opts := SearchOptions{Now: tier2TestNow, Limit: 50}

	check := func(q string, want ...string) *SearchResult {
		t.Helper()
		res, err := eng.Search(ctx, q, opts)
		if err != nil {
			t.Fatalf("Search(%q): %v", q, err)
		}
		got := itemNames(res)
		if len(got) != len(want) {
			t.Fatalf("Search(%q) = %v, want %v", q, got, want)
		}
		for _, w := range want {
			found := false
			for _, g := range got {
				if g == w {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("Search(%q) = %v, missing %q", q, got, w)
			}
		}
		return res
	}

	check("report", "report_2026.md", "report_2025.md")
	check("report 2026", "report_2026.md")
	check("report | README", "report_2026.md", "report_2025.md", "README.md")
	check("report ext:md size:>1000b", "report_2026.md", "report_2025.md")
	check("sjw", "设计文档.md")
	check("设计", "设计文档.md")
	check("zzzz")

	// Tombstones are filtered before verification.
	if err := store.ApplyBatch([]storage.Mutation{
		{Op: storage.OpDelete, VolID: 1, NodeKey: 4},
	}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	check("report", "report_2026.md")

	// MemTable entries merge into the inverted path.
	if err := store.ApplyBatch([]storage.Mutation{
		{Op: storage.OpCreate, VolID: 1, NodeKey: 100, ParentKey: 2, Name: "report_live.md", Size: 10, ModTime: tier2TestNow.UnixNano()},
	}); err != nil {
		t.Fatalf("memtable create: %v", err)
	}
	check("report", "report_2026.md", "report_live.md")
}

// TestInvertedNoFalseNegative ties the inverted candidate set to the matcher:
// for every term, the scan must return exactly the live names the Tier 2
// matcher accepts.
func TestInvertedNoFalseNegative(t *testing.T) {
	store, _ := buildTierQueryEngine(t, true)
	defer store.Close()
	snap := store.AcquireSnapshot()
	defer snap.Release()
	ss := snap.SegmentSnapshotAt(0)

	terms := []string{"report", "2026", "main", "readme", "设计文", "sjw", "note", "img", "world", "guide"}
	for _, term := range terms {
		plan := mustCompilePlan(t, store, term)
		var cold int64
		cands, err := scanInvertedSegment(context.Background(), snap, ss, plan, &cold)
		if err != nil {
			t.Fatalf("scanInvertedSegment(%q): %v", term, err)
		}
		got := make(map[uint32]string, len(cands))
		for _, c := range cands {
			if _, name, ok := snap.LookupNode(c.nodeID); ok {
				got[c.nodeID] = string(name)
			}
		}

		want := make(map[uint32]string)
		for i := uint32(0); i < ss.Count(); i++ {
			if ss.IsDeleted(i) {
				continue
			}
			name := ss.NameBytesAt(i)
			if len(name) == 0 {
				continue
			}
			nonASCII := (ss.NodeAt(i).Flags & storage.FlagNonASCII) != 0
			if MatchNameNoFuzzy(name, nonASCII, []byte(term), term) != TierNone {
				want[ss.NodeIDAt(i)] = string(name)
			}
		}
		if len(got) != len(want) {
			t.Fatalf("term %q: inverted = %v, matcher = %v", term, got, want)
		}
		for id, name := range want {
			if got[id] != name {
				t.Fatalf("term %q: inverted missing %q (id %d); got %v", term, name, id, got)
			}
		}
	}
}

func TestTier2FuzzyDisabled(t *testing.T) {
	ctx := context.Background()
	opts := SearchOptions{Now: tier2TestNow, Limit: 50}

	tier1Store, tier1Engine := buildTierQueryEngine(t, false)
	defer tier1Store.Close()
	tier2Store, tier2Engine := buildTierQueryEngine(t, true)
	defer tier2Store.Close()

	// "wdjg" is a fuzzy-only hit for world_design_job_guide.txt in Tier 1.
	res1, err := tier1Engine.Search(ctx, "wdjg", opts)
	if err != nil {
		t.Fatal(err)
	}
	names1 := itemNames(res1)
	if len(names1) != 1 || names1[0] != "world_design_job_guide.txt" {
		t.Fatalf("Tier 1 wdjg = %v, want [world_design_job_guide.txt]", names1)
	}

	res2, err := tier2Engine.Search(ctx, "wdjg", opts)
	if err != nil {
		t.Fatal(err)
	}
	if names2 := itemNames(res2); len(names2) != 0 {
		t.Fatalf("Tier 2 must not return fuzzy-only hits: %v", names2)
	}

	// Pinyin is indexed and must still match in Tier 2 (设计文档 -> sjwd).
	res3, err := tier2Engine.Search(ctx, "sjwd", opts)
	if err != nil {
		t.Fatal(err)
	}
	if names3 := itemNames(res3); len(names3) != 1 || names3[0] != "设计文档.md" {
		t.Fatalf("Tier 2 sjwd = %v, want [设计文档.md]", names3)
	}
}

func TestTier2Parity(t *testing.T) {
	ctx := context.Background()
	tier1Store, tier1Engine := buildTierQueryEngine(t, false)
	defer tier1Store.Close()
	tier2Store, tier2Engine := buildTierQueryEngine(t, true)
	defer tier2Store.Close()

	queries := []string{
		"report",
		"report 2026",
		"report | README",
		"设计文件",
		"设计文档",
		"sjw",
		"report ext:md",
		"report size:>1k",
		"report dm:>2023-01-01",
		"path:dirA",
		"notes",
		"main",
		"REPORT",
		"*.go",
		"!report",
		"img png",
		"",
	}
	for _, q := range queries {
		opts := SearchOptions{Now: tier2TestNow, Limit: 50}
		r1, err := tier1Engine.Search(ctx, q, opts)
		if err != nil {
			t.Fatalf("Tier1 Search(%q): %v", q, err)
		}
		r2, err := tier2Engine.Search(ctx, q, opts)
		if err != nil {
			t.Fatalf("Tier2 Search(%q): %v", q, err)
		}
		if r1.Total != r2.Total {
			t.Fatalf("query %q: total Tier1=%d Tier2=%d (%v vs %v)", q, r1.Total, r2.Total, itemNames(r1), itemNames(r2))
		}
		n1, n2 := itemNames(r1), itemNames(r2)
		if len(n1) != len(n2) {
			t.Fatalf("query %q: items Tier1=%v Tier2=%v", q, n1, n2)
		}
		for i := range n1 {
			if n1[i] != n2[i] {
				t.Fatalf("query %q: rank %d Tier1=%q Tier2=%q", q, i, n1[i], n2[i])
			}
			if math.Abs(r1.Items[i].Score-r2.Items[i].Score) > 1e-9 {
				t.Fatalf("query %q: rank %d score Tier1=%f Tier2=%f", q, i, r1.Items[i].Score, r2.Items[i].Score)
			}
		}
	}
}

func TestShortWordBigramTruncation(t *testing.T) {
	store, eng := buildTierQueryEngine(t, true)
	defer store.Close()
	ctx := context.Background()
	opts := SearchOptions{Now: tier2TestNow, Limit: 50}

	// Sanity: two-rune query resolves through the bigram table.
	res, err := eng.Search(ctx, "re", opts)
	if err != nil {
		t.Fatalf("Search(re): %v", err)
	}
	if res.Total != 3 {
		t.Fatalf("Search(re) total = %d, want 3 (%v)", res.Total, itemNames(res))
	}

	// Truncation bounds the candidate stream: only the first K postings are
	// verified and ranked, never the full hit set.
	orig := shortWordPostingLimit
	shortWordPostingLimit = 2
	defer func() { shortWordPostingLimit = orig }()
	res, err = eng.Search(ctx, "re", opts)
	if err != nil {
		t.Fatalf("Search(re) truncated: %v", err)
	}
	if res.Total > 2 {
		t.Fatalf("truncated short-word search returned %d items, want <= 2", res.Total)
	}

	// A bigram absent from the index falls back to a correct sequential scan.
	res, err = eng.Search(ctx, "zz", opts)
	if err != nil {
		t.Fatalf("Search(zz): %v", err)
	}
	if res.Total != 0 {
		t.Fatalf("Search(zz) = %v, want none", itemNames(res))
	}
}

func TestShortWordGrow(t *testing.T) {
	store, eng := buildTierQueryEngine(t, true)
	defer store.Close()
	ctx := context.Background()
	opts := SearchOptions{Now: tier2TestNow, Limit: 50}
	sess := eng.NewSession()

	first, err := sess.Search(ctx, "re", opts)
	if err != nil {
		t.Fatalf("search re: %v", err)
	}
	if first.Total != 3 {
		t.Fatalf("two-rune search total = %d, want 3", first.Total)
	}

	// Third rune switches to the full inverted intersection and refines the
	// cached candidate set instead of re-scanning.
	grown, err := sess.Search(ctx, "rep", opts)
	if err != nil {
		t.Fatalf("search rep: %v", err)
	}
	if grown.Total != 2 {
		t.Fatalf("three-rune search = %v, want report_2026.md/report_2025.md", itemNames(grown))
	}
	if !grown.Incremental {
		t.Fatal("2->3 rune continuation should reuse the session candidate set")
	}

	grownAgain, err := sess.Search(ctx, "repo", opts)
	if err != nil {
		t.Fatalf("search repo: %v", err)
	}
	if !grownAgain.Incremental {
		t.Fatal("continued typing should keep refining the cached candidate set")
	}
	if grownAgain.Total != 2 {
		t.Fatalf("four-rune search = %v", itemNames(grownAgain))
	}
}

// TestShortWordBigramCutFallback covers the case where the normal-namespace
// bigram was dropped by the df budget while the Pinyin namespace still has it:
// the short-word path must fall back to a scan instead of returning the Pinyin
// subset only.
func TestShortWordBigramCutFallback(t *testing.T) {
	store, err := storage.Open(t.TempDir(), storage.Options{
		DisableWALSync:               true,
		LowMemoryMode:                true,
		DisableBackgroundMaintenance: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	muts := []storage.Mutation{
		{Op: storage.OpCreate, VolID: 1, NodeKey: 1, ParentKey: 0, Name: "root", Flags: storage.FlagDir},
	}
	for i := 0; i < 65; i++ {
		muts = append(muts, storage.Mutation{
			Op:        storage.OpCreate,
			VolID:     1,
			NodeKey:   uint64(2 + i),
			ParentKey: 1,
			Name:      fmt.Sprintf("ab_file_%02d.txt", i),
		})
	}
	// Pinyin initials "ab" keep the Pinyin-namespace bigram alive.
	muts = append(muts, storage.Mutation{
		Op: storage.OpCreate, VolID: 1, NodeKey: 100, ParentKey: 1, Name: "阿波.txt",
	})
	if err := store.ApplyBatch(muts); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
	if err := store.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	res, err := NewEngine(store).Search(context.Background(), "ab", SearchOptions{Limit: 200})
	if err != nil {
		t.Fatalf("Search(ab): %v", err)
	}
	if res.Total != 66 {
		t.Fatalf("Search(ab) total = %d, want 66 (direct matches must not be lost)", res.Total)
	}
	names := itemNames(res)
	if !slices.Contains(names, "ab_file_00.txt") || !slices.Contains(names, "阿波.txt") {
		t.Fatalf("Search(ab) names = %v", names)
	}
}

// TestRefineCandidatesPreservesTier2 keeps the no-fuzzy Tier 2 flag across
// incremental session narrowing.
func TestRefineCandidatesPreservesTier2(t *testing.T) {
	store, _ := buildTierQueryEngine(t, true)
	defer store.Close()
	snap := store.AcquireSnapshot()
	defer snap.Release()

	plan := mustCompilePlan(t, store, "report")
	// report_2026.md is node 2 in the fixture's creation order.
	prev := []candidate{{nodeID: 2, tier2: true}}
	out, _, err := refineCandidates(context.Background(), snap, prev, plan)
	if err != nil {
		t.Fatalf("refineCandidates: %v", err)
	}
	if len(out) != 1 || !out[0].tier2 {
		t.Fatalf("refined candidates = %+v, want one tier2 candidate", out)
	}
}

// TestShortWordTruncatedEmptyNotCached guards the case where the truncated
// short-word scan yields zero matches: the empty set is still partial and must
// not seed the next keystroke's incremental narrowing.
func TestShortWordTruncatedEmptyNotCached(t *testing.T) {
	store, err := storage.Open(t.TempDir(), storage.Options{
		DisableWALSync:               true,
		LowMemoryMode:                true,
		DisableBackgroundMaintenance: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	if err := store.ApplyBatch([]storage.Mutation{
		{Op: storage.OpCreate, VolID: 1, NodeKey: 1, ParentKey: 0, Name: "root", Flags: storage.FlagDir},
		{Op: storage.OpCreate, VolID: 1, NodeKey: 2, ParentKey: 1, Name: "abx1.txt"},
		{Op: storage.OpCreate, VolID: 1, NodeKey: 3, ParentKey: 1, Name: "abx2.txt"},
		{Op: storage.OpCreate, VolID: 1, NodeKey: 4, ParentKey: 1, Name: "abx3.pdf"},
	}); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
	if err := store.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	orig := shortWordPostingLimit
	shortWordPostingLimit = 2 // first two postings are the .txt files
	defer func() { shortWordPostingLimit = orig }()

	sess := NewEngine(store).NewSession()
	ctx := context.Background()
	opts := SearchOptions{Now: tier2TestNow, Limit: 20}

	first, err := sess.Search(ctx, "ab ext:pdf", opts)
	if err != nil {
		t.Fatalf("first search: %v", err)
	}
	if first.Total != 0 {
		t.Fatalf("first search = %v, want no .txt hits and the .pdf beyond the truncation", itemNames(first))
	}

	second, err := sess.Search(ctx, "abx ext:pdf", opts)
	if err != nil {
		t.Fatalf("second search: %v", err)
	}
	if second.Total != 1 || itemNames(second)[0] != "abx3.pdf" {
		t.Fatalf("second search = %v, want [abx3.pdf]", itemNames(second))
	}
	if second.Incremental {
		t.Fatal("an empty truncated result must not seed the incremental cache")
	}
}
