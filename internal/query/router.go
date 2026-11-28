package query

import (
	"github.com/mr-dadong/Xift/internal/storage"
)

// minInvertedRunes is the shortest term length the trigram inverted index can
// service. Shorter terms route to the bigram/Frecency short-word path.
const minInvertedRunes = 3

// routePlan selects how one segment is executed for a query.
type routePlan uint8

const (
	// routeScan reads every live record of the segment with the full matcher
	// (Tier 1 semantics, or Tier 2 when no indexable name term is required).
	routeScan routePlan = iota
	// routeInverted uses the trigram inverted index to produce candidates.
	routeInverted
	// routeShortWord uses the bigram prefix table plus Frecency Top-N for
	// one- and two-rune terms.
	routeShortWord
	// routeIncremental reuses the session's cached candidate set (highest
	// priority, decided before any segment routing).
	routeIncremental
)

func (r routePlan) String() string {
	switch r {
	case routeInverted:
		return "inverted"
	case routeShortWord:
		return "short-word"
	case routeIncremental:
		return "incremental"
	default:
		return "scan"
	}
}

// routeQuery returns the highest-priority execution plan. Incremental session
// narrowing always wins; otherwise the per-segment router decides.
func routeQuery(plan *compiledPlan, ss storage.SegmentSnapshot, sessionIncremental bool) routePlan {
	if sessionIncremental {
		return routeIncremental
	}
	return routeSegment(plan, ss)
}

// routeSegment picks the execution plan for a single segment. Tier 1 segments
// keep the existing full-scan plan; Tier 2 segments use the inverted index when
// a required name term is long enough for trigrams, the short-word path for
// one/two-rune terms, and a sequential point-read scan otherwise.
func routeSegment(plan *compiledPlan, ss storage.SegmentSnapshot) routePlan {
	if !ss.IsTier2() || plan == nil || plan.root == nil {
		return routeScan
	}
	long, short := false, false
	for _, atom := range plan.requiredNames {
		switch {
		case atom.needleRunes >= minInvertedRunes:
			long = true
		case atom.needleRunes > 0:
			short = true
		}
	}
	if long {
		return routeInverted
	}
	if short {
		return routeShortWord
	}
	return routeScan
}
