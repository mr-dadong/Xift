package query

import "testing"

// TestBoundedCollector verifies the collector stops accepting once the candidate
// budget is exhausted and never retains more than the budget.
func TestBoundedCollector(t *testing.T) {
	const budget = 100
	c := newCandidateCollector(budget)
	for i := 0; i < 100_000; i++ {
		if !c.add(candidate{nodeID: uint32(i), bestTier: TierSubstring, nameLen: 20}) {
			break
		}
	}
	if got := c.verified(); got != budget {
		t.Fatalf("verified = %d, want %d", got, budget)
	}
	if !c.truncated() {
		t.Fatal("collector must report truncation after exceeding budget")
	}
	if !c.done() {
		t.Fatal("collector must stop scanning once the budget is exhausted")
	}
	if got := c.len(); got > budget {
		t.Fatalf("retained %d candidates, want <= %d", got, budget)
	}
}

// TestBoundedCollectorNotTruncated verifies a result set that fits the budget is
// reported as complete and keeps every match.
func TestBoundedCollectorNotTruncated(t *testing.T) {
	c := newCandidateCollector(100)
	for i := 0; i < 7; i++ {
		c.add(candidate{nodeID: uint32(i), bestTier: TierSubstring, nameLen: 20})
	}
	if c.truncated() {
		t.Fatal("collector must not report truncation when every match fits the budget")
	}
	if c.done() {
		t.Fatal("collector must keep scanning while budget remains")
	}
	if got := c.len(); got != 7 {
		t.Fatalf("retained %d candidates, want 7", got)
	}
}

// TestBoundedCollectorUnbounded verifies a non-positive budget keeps every match,
// which preserves Tier 1's exact in-memory result semantics.
func TestBoundedCollectorUnbounded(t *testing.T) {
	c := newCandidateCollector(0)
	for i := 0; i < 50_000; i++ {
		c.add(candidate{nodeID: uint32(i), bestTier: TierSubstring, nameLen: 20})
	}
	if c.done() || c.truncated() {
		t.Fatal("unbounded collector must never stop or truncate")
	}
	if got := c.len(); got != 50_000 {
		t.Fatalf("retained %d candidates, want 50000", got)
	}
}

// TestCollectorOrdering verifies retained results are returned best-first and
// that equal pre-scores fall back to ascending nodeID.
func TestCollectorOrdering(t *testing.T) {
	c := newCandidateCollector(64)
	c.add(candidate{nodeID: 9, bestTier: TierPinyin, nameLen: 10})
	c.add(candidate{nodeID: 8, bestTier: TierPinyin, nameLen: 10})
	c.add(candidate{nodeID: 7, bestTier: TierWildcard, nameLen: 4})
	c.add(candidate{nodeID: 6, bestTier: TierExact, nameLen: 30})
	c.add(candidate{nodeID: 5, bestTier: TierPrefix, nameLen: 6})

	got := c.results()
	want := []uint32{6, 5, 8, 9, 7}
	if len(got) != len(want) {
		t.Fatalf("retained %d candidates, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].nodeID != want[i] {
			t.Fatalf("order = %v, want %v", nodeIDs(got), want)
		}
	}
}

func nodeIDs(cands []candidate) []uint32 {
	out := make([]uint32, len(cands))
	for i := range cands {
		out[i] = cands[i].nodeID
	}
	return out
}
