package query

import (
	"context"
	"strings"
	"time"

	"github.com/mr-dadong/Xift/internal/storage"
)

const (
	systemDemotePenalty = 500.0
	depthWeight         = 60.0
	exactDepthCutoff    = 4096
)

func (e *Engine) rankCandidates(
	ctx context.Context,
	snap *storage.Snapshot,
	q *Query,
	plan *compiledPlan,
	cands []candidate,
	coldReads int,
	incremental bool,
	opts SearchOptions,
	start time.Time,
) (*ResultSet, error) {
	total := len(cands)
	if total == 0 {
		snap.Release()
		rs := &ResultSet{
			query:       q.Raw,
			plan:        plan,
			pathSep:     opts.PathSep,
			total:       0,
			cost:        time.Since(start),
			incremental: incremental,
			warnings:    q.Warnings,
		}
		rs.coldReads.Store(int64(coldReads))
		return rs, nil
	}

	// Compute initial scores cheaply without cold reads or full path resolution.
	computeDepth := total <= exactDepthCutoff && opts.Sort == SortByRelevance
	for i := range cands {
		c := &cands[i]
		score := c.bestTier.Score()
		if !plan.includeSystem && (c.flags&storage.FlagSystem) != 0 {
			score -= systemDemotePenalty
		}
		if computeDepth {
			depth := nodeDepth(snap, c.nodeID)
			score += depthWeight / float64(depth+1)
		} else {
			// Lightweight tie-breaker favoring shorter basenames before Top-K refinement.
			score += 10.0 / float64(int(c.nameLen)+1)
		}
		if e.frecency != nil {
			score += e.frecency.Score(c.nodeID, "")
		}
		c.score = score
	}

	maxRetained := opts.MaxRetained
	if maxRetained <= 0 {
		maxRetained = DefaultMaxRetained
	}

	var kept []candidate
	switch opts.Sort {
	case SortByRelevance:
		kept = selectTopK(cands, maxRetained, func(a, b candidate) int {
			if a.score > b.score {
				return -1
			}
			if a.score < b.score {
				return 1
			}
			if a.nodeID < b.nodeID {
				return -1
			}
			if a.nodeID > b.nodeID {
				return 1
			}
			return 0
		})
	case SortByName:
		kept = selectTopK(cands, maxRetained, func(a, b candidate) int {
			_, na, _ := snap.LookupNode(a.nodeID)
			_, nb, _ := snap.LookupNode(b.nodeID)
			cmp := compareASCIIFold(na, nb)
			if opts.Desc {
				cmp = -cmp
			}
			if cmp != 0 {
				return cmp
			}
			if a.nodeID < b.nodeID {
				return -1
			}
			if a.nodeID > b.nodeID {
				return 1
			}
			return 0
		})
	case SortBySize, SortByModTime:
		// Cold-column sort requires loading cold metadata for matched candidates.
		for i := range cands {
			if (i & ctxCheckIntervalMask) == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			if !cands[i].hasCold {
				coldReads++
				if cm, err := snap.ReadColdMeta(cands[i].nodeID); err == nil {
					cands[i].hasCold = true
					cands[i].size = cm.Size
					cands[i].modTime = cm.ModTime
				}
			}
		}
		kept = selectTopK(cands, maxRetained, func(a, b candidate) int {
			var av, bv int64
			if opts.Sort == SortBySize {
				av, bv = a.size, b.size
			} else {
				av, bv = a.modTime, b.modTime
			}
			var cmp int
			if av < bv {
				cmp = -1
			} else if av > bv {
				cmp = 1
			}
			if opts.Desc {
				cmp = -cmp
			}
			if cmp != 0 {
				return cmp
			}
			if a.nodeID < b.nodeID {
				return -1
			}
			if a.nodeID > b.nodeID {
				return 1
			}
			return 0
		})
	default:
		kept = cands
		if len(kept) > maxRetained {
			kept = kept[:maxRetained]
		}
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	rows := make([]rankedRow, len(kept))
	for i := range kept {
		rows[i] = rankedRow{
			nodeID: kept[i].nodeID,
			score:  float32(kept[i].score),
		}
	}

	rs := &ResultSet{
		snap:        snap,
		query:       q.Raw,
		plan:        plan,
		pathSep:     opts.PathSep,
		total:       total,
		rows:        rows,
		cost:        time.Since(start),
		incremental: incremental,
		warnings:    q.Warnings,
	}
	rs.coldReads.Store(int64(coldReads))
	return rs, nil
}

func computeHighlights(name []byte, nonASCII bool, posNames []compiledAtom) []HighlightRange {
	if len(posNames) == 0 || len(name) == 0 {
		return nil
	}
	var ranges []HighlightRange
	for i := range posNames {
		ca := &posNames[i]
		if tier, s, e := MatchName(name, nonASCII, ca.needleBytes, ca.atom.Value); tier != TierNone && e > s {
			ranges = append(ranges, HighlightRange{Start: s, End: e})
		}
	}
	return MergeHighlights(ranges)
}

func nodeDepth(snap *storage.Snapshot, nodeID uint32) int {
	cur := nodeID
	for depth := 0; depth < 64; depth++ {
		node, _, ok := snap.LookupNode(cur)
		if !ok || node.ParentIdx == storage.RootParentIdx || node.ParentIdx == storage.OrphanParentIdx {
			return depth
		}
		cur = node.ParentIdx
	}
	return 64
}

func compareASCIIFold(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		la := asciiLower(a[i])
		lb := asciiLower(b[i])
		if la < lb {
			return -1
		}
		if la > lb {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return strings.Compare(string(a), string(b))
}
