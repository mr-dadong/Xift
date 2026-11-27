package query

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/mr-dadong/Xift/internal/storage"
)

const (
	systemDemotePenalty = 500.0
	depthWeight         = 60.0
	exactDepthCutoff    = 4096
)

func (e *Engine) rankAndMaterialize(
	ctx context.Context,
	snap *storage.Snapshot,
	q *Query,
	plan *compiledPlan,
	cands []candidate,
	coldReads int,
	incremental bool,
	opts SearchOptions,
	start time.Time,
) (*SearchResult, error) {
	total := len(cands)
	if total == 0 {
		cost := time.Since(start)
		return &SearchResult{
			Query:       q.Raw,
			Total:       0,
			Returned:    0,
			Offset:      opts.Offset,
			Limit:       opts.Limit,
			Cost:        cost,
			CostMs:      float64(cost.Microseconds()) / 1000.0,
			Incremental: incremental,
			ColdReads:   coldReads,
			Warnings:    q.Warnings,
			Items:       []ResultItem{},
		}, nil
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

	switch opts.Sort {
	case SortByRelevance:
		slices.SortFunc(cands, func(a, b candidate) int {
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
		// Sort by lowercased basename using snapshot lookup.
		slices.SortFunc(cands, func(a, b candidate) int {
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
			if !cands[i].hasCold {
				coldReads++
				if cm, err := snap.ReadColdMeta(cands[i].nodeID); err == nil {
					cands[i].hasCold = true
					cands[i].size = cm.Size
					cands[i].modTime = cm.ModTime
				}
			}
		}
		slices.SortFunc(cands, func(a, b candidate) int {
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
	}

	// Slice viewport [Offset : Offset+Limit] and materialize paths + cold columns ONLY for viewport rows.
	if opts.Offset >= total {
		cost := time.Since(start)
		return &SearchResult{
			Query:       q.Raw,
			Total:       total,
			Returned:    0,
			Offset:      opts.Offset,
			Limit:       opts.Limit,
			Cost:        cost,
			CostMs:      float64(cost.Microseconds()) / 1000.0,
			Incremental: incremental,
			ColdReads:   coldReads,
			Warnings:    q.Warnings,
			Items:       []ResultItem{},
		}, nil
	}

	end := opts.Offset + opts.Limit
	if end > total {
		end = total
	}
	vp := cands[opts.Offset:end]
	items := make([]ResultItem, 0, len(vp))

	for i := range vp {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		c := vp[i]
		node, nameBytes, ok := snap.LookupNode(c.nodeID)
		if !ok {
			continue
		}
		pathStr, err := snap.ResolvePathWithSep(c.nodeID, opts.PathSep)
		if err != nil {
			pathStr = string(nameBytes)
		}
		size, mtime := c.size, c.modTime
		if !c.hasCold {
			coldReads++
			if cm, err := snap.ReadColdMeta(c.nodeID); err == nil {
				size = cm.Size
				mtime = cm.ModTime
			}
		}
		highlights := computeHighlights(nameBytes, (node.Flags&storage.FlagNonASCII) != 0, plan.positiveNames)
		items = append(items, ResultItem{
			NodeID:     c.nodeID,
			Path:       pathStr,
			Name:       string(nameBytes),
			Flags:      node.Flags,
			ExtID:      node.ExtID,
			VolID:      node.VolID,
			IsDir:      (node.Flags & storage.FlagDir) != 0,
			Folded:     (node.Flags & storage.FlagFolded) != 0,
			System:     (node.Flags & storage.FlagSystem) != 0,
			Size:       size,
			ModTime:    mtime,
			Score:      c.score,
			Highlights: highlights,
		})
	}

	cost := time.Since(start)
	return &SearchResult{
		Query:       q.Raw,
		Total:       total,
		Returned:    len(items),
		Offset:      opts.Offset,
		Limit:       opts.Limit,
		Cost:        cost,
		CostMs:      float64(cost.Microseconds()) / 1000.0,
		Incremental: incremental,
		ColdReads:   coldReads,
		Warnings:    q.Warnings,
		Items:       items,
	}, nil
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
