package query

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/mr-dadong/Xift/internal/storage"
)

const (
	contentDisabledWarning = "content: full-text search is not enabled (no L2 whitelist configured); content filters were ignored and the result is empty"
	contentInDirWarning    = "content: full-text search is not supported together with in: scoped queries; the result is empty"
)

// ContentSearcher resolves a body substring to the L1 node IDs that contain it.
// The L2 content index implements it; truncated reports an incomplete set.
type ContentSearcher interface {
	Search(term []byte, budget int) ([]uint32, bool, error)
}

// WithContentSearcher attaches the optional L2 content index used by content: atoms.
func WithContentSearcher(cs ContentSearcher) EngineOption {
	return func(e *Engine) {
		e.content = cs
	}
}

// prepareContent resolves every content: term to a node-ID set and reports
// whether the query must return empty because L2 is unavailable or the query
// combines content: with in: scoping. It never silently drops the condition.
func (e *Engine) prepareContent(ctx context.Context, q *Query, plan *compiledPlan, opts SearchOptions) (empty, truncated bool, err error) {
	if plan == nil || len(plan.contentAtoms) == 0 {
		return false, false, nil
	}
	if e.content == nil {
		q.Warnings = append(q.Warnings, contentDisabledWarning)
		return true, false, nil
	}
	if len(q.InDirs) > 0 {
		q.Warnings = append(q.Warnings, contentInDirWarning)
		return true, false, nil
	}

	sets := make(map[string]map[uint32]struct{}, len(plan.contentAtoms))
	for _, ca := range plan.contentAtoms {
		if ca.contentSet != nil {
			continue
		}
		term := ca.atom.Value
		if set, ok := sets[term]; ok {
			ca.contentSet = set
			continue
		}
		ids, trunc, serr := e.content.Search([]byte(term), opts.CandidateBudget)
		if serr != nil {
			return false, false, fmt.Errorf("query: content search %q: %w", term, serr)
		}
		set := make(map[uint32]struct{}, len(ids))
		for _, id := range ids {
			set[id] = struct{}{}
		}
		sets[term] = set
		ca.contentSet = set
		truncated = truncated || trunc
	}
	return false, truncated, nil
}

func (e *Engine) emptyResult(q *Query, plan *compiledPlan, opts SearchOptions, start time.Time) *ResultSet {
	return &ResultSet{
		query:    q.Raw,
		plan:     plan,
		pathSep:  opts.PathSep,
		total:    0,
		cost:     time.Since(start),
		warnings: q.Warnings,
	}
}

// contentCandidates evaluates a content-only expression over the L2 node set
// instead of scanning the whole L1 index, then applies the same candidate
// budget used by the Tier 2 scan path.
func contentCandidates(ctx context.Context, snap *storage.Snapshot, plan *compiledPlan, budget int) (scanOutcome, error) {
	union := make(map[uint32]struct{})
	for _, ca := range plan.contentAtoms {
		for id := range ca.contentSet {
			union[id] = struct{}{}
		}
	}
	ids := make([]uint32, 0, len(union))
	for id := range union {
		ids = append(ids, id)
	}
	slices.Sort(ids)

	tier2 := snap.Tier2Mode()
	out := scanOutcome{}
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return scanOutcome{}, err
		}
		if budget > 0 && out.verified >= budget {
			out.truncated = true
			break
		}
		node, name, ok := snap.LookupNode(id)
		if !ok || (node.Flags&storage.FlagDeleted) != 0 {
			continue
		}
		var localCold int64
		ec := evalContext{
			snap:      snap,
			nodeID:    id,
			node:      node,
			name:      name,
			nonASCII:  (node.Flags & storage.FlagNonASCII) != 0,
			noFuzzy:   tier2,
			coldReads: &localCold,
		}
		ok, tier := evalExpr(plan.root, &ec)
		if !ok {
			continue
		}
		cand := candidate{
			nodeID:   id,
			flags:    node.Flags,
			extID:    node.ExtID,
			volID:    node.VolID,
			nameLen:  node.NameLen,
			bestTier: tier,
			tier2:    tier2,
		}
		if ec.coldReady {
			cand.hasCold = true
			cand.size = ec.coldMeta.Size
			cand.modTime = ec.coldMeta.ModTime
		}
		out.cands = append(out.cands, cand)
		out.verified++
		out.coldReads += int(localCold)
	}
	return out, nil
}
