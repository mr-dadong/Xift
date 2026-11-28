package query

import "slices"

// DefaultCandidateBudget bounds how many matched candidates one interactive
// query may verify and retain before declaring its result truncated. It keeps
// scan work and query-time memory independent of the total hit count.
const DefaultCandidateBudget = 8192

// candidateCollector accumulates matches while bounding the number of verified
// candidates. A non-positive budget means unbounded (Tier 1's in-memory scan
// keeps its exact result semantics).
type candidateCollector struct {
	budget  int
	cands   []candidate
	count   int
	stopped bool
}

// newCandidateCollector creates a collector. A non-positive budget retains
// every match.
func newCandidateCollector(budget int) *candidateCollector {
	return &candidateCollector{budget: budget}
}

// add records one verified match. It reports false once the budget is
// exhausted, signalling callers to stop scanning.
func (c *candidateCollector) add(cand candidate) bool {
	if c.stopped {
		return false
	}
	c.count++
	c.cands = append(c.cands, cand)
	if c.budget > 0 && c.count >= c.budget {
		c.stopped = true
	}
	return !c.stopped
}

// done reports whether scanning must stop.
func (c *candidateCollector) done() bool { return c.stopped }

// truncated reports whether the match set may hold more entries than were
// verified, so callers must not claim a complete result.
func (c *candidateCollector) truncated() bool { return c.budget > 0 && c.count >= c.budget }

// verified returns the number of matches accepted so far.
func (c *candidateCollector) verified() int { return c.count }

// len returns the number of retained candidates.
func (c *candidateCollector) len() int { return len(c.cands) }

// results returns the retained candidates ordered best-first by the cheap
// pre-score; the ranker recomputes exact scores over this bounded set.
func (c *candidateCollector) results() []candidate {
	if len(c.cands) == 0 {
		return nil
	}
	out := make([]candidate, len(c.cands))
	copy(out, c.cands)
	if c.budget > 0 {
		slices.SortFunc(out, func(a, b candidate) int {
			switch {
			case candidateBetter(a, b):
				return -1
			case candidateBetter(b, a):
				return 1
			default:
				return 0
			}
		})
	}
	return out
}

// candidatePreScore is the cheap ranking key available during scanning: match
// tier plus a short-name tie-breaker, matching the ranker's fallback pre-score.
func candidatePreScore(c candidate) float64 {
	return c.bestTier.Score() + 10.0/float64(int(c.nameLen)+1)
}

// candidateBetter reports whether a outranks b under candidatePreScore.
func candidateBetter(a, b candidate) bool {
	as, bs := candidatePreScore(a), candidatePreScore(b)
	if as != bs {
		return as > bs
	}
	return a.nodeID < b.nodeID
}
