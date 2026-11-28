package query

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/mr-dadong/Xift/internal/storage"
)

const maxCachedSessionCandidates = 200_000

// Session maintains incremental refinement state for interactive as-you-type queries
// and automatically cancels any superseded in-flight query in the same session.
type Session struct {
	engine *Engine

	mu             sync.Mutex
	activeCancel   context.CancelFunc
	activeSeq      uint64
	lastGen        uint64
	lastQuery      *Query
	lastCandidates []candidate
	hasCache       bool
}

// Cancel aborts any in-flight query executing on this Session.
func (s *Session) Cancel() {
	s.mu.Lock()
	cancel := s.activeCancel
	s.activeCancel = nil
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// Reset clears any cached candidate set.
func (s *Session) Reset() {
	s.mu.Lock()
	s.hasCache = false
	s.lastQuery = nil
	s.lastCandidates = nil
	s.mu.Unlock()
}

// Start executes rawQuery on this Session, cancelling any previous in-flight query and
// reusing cached candidates when the new query strictly narrows the prior query against the
// same storage Snapshot generation. The returned ResultSet holds the snapshot until Close().
func (s *Session) Start(ctx context.Context, rawQuery string, opts SearchOptions) (*ResultSet, error) {
	if s == nil || s.engine == nil || s.engine.store == nil {
		return nil, fmt.Errorf("query: nil session engine")
	}
	opts.normalize()
	start := time.Now()

	q, err := ParseAt(rawQuery, opts.Now)
	if err != nil {
		return nil, err
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	s.mu.Lock()
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	if s.activeCancel != nil {
		s.activeCancel()
	}
	s.activeSeq++
	mySeq := s.activeSeq
	s.activeCancel = cancel
	cachedGen := s.lastGen
	cachedQuery := s.lastQuery
	cachedCands := s.lastCandidates
	hasCache := s.hasCache
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		if s.activeSeq == mySeq {
			s.activeCancel = nil
		}
		s.mu.Unlock()
	}()

	snap := s.engine.store.AcquireSnapshot()
	plan := compileQuery(q, s.engine.store.ExtPool())

	if len(q.InDirs) > 0 {
		rs, err := s.engine.startFoldedSearch(runCtx, snap, q, plan, opts, start)
		snap.Release()
		return rs, err
	}

	var cands []candidate
	var coldReads int
	partial := false
	truncated := false
	verified := 0
	incremental := false

	// Session incremental narrowing has the highest routing priority; the
	// per-segment router only runs when no reusable cache applies.
	sessionIncremental := hasCache && snap.Generation() == cachedGen && isNarrowingQuery(cachedQuery, q)
	if routeQuery(plan, storage.SegmentSnapshot{}, sessionIncremental) == routeIncremental {
		cands, coldReads, err = refineCandidates(runCtx, snap, cachedCands, plan)
		if err != nil {
			snap.Release()
			return nil, err
		}
		verified = len(cands)
		incremental = true
	} else {
		out, scanErr := scanSnapshot(runCtx, snap, plan, opts.Workers, opts.CandidateBudget)
		if scanErr != nil {
			snap.Release()
			return nil, scanErr
		}
		cands, coldReads, partial = out.cands, out.coldReads, out.partial
		truncated, verified = out.truncated, out.verified
	}

	// Store compact candidate snapshot for next incremental keystroke if within
	// budget. Truncated results (short-word path or candidate budget) are
	// incomplete and must not seed the next keystroke, or the 2->3 rune
	// transition could miss hits.
	if len(cands) <= maxCachedSessionCandidates && !partial && !truncated && !containsPartialCandidates(cands) {
		cacheCopy := make([]candidate, len(cands))
		copy(cacheCopy, cands)
		s.mu.Lock()
		if s.activeSeq == mySeq {
			s.lastGen = snap.Generation()
			s.lastQuery = q
			s.lastCandidates = cacheCopy
			s.hasCache = true
		}
		s.mu.Unlock()
	} else {
		s.mu.Lock()
		if s.activeSeq == mySeq {
			s.hasCache = false
			s.lastCandidates = nil
		}
		s.mu.Unlock()
	}

	rs, err := s.engine.rankCandidates(runCtx, snap, q, plan, cands, coldReads, incremental, opts, start, truncated, verified)
	if err != nil {
		snap.Release()
		return nil, err
	}
	return rs, nil
}

// Search executes rawQuery on this Session, cancelling any previous in-flight query
// and reusing cached candidates when the new query strictly narrows the prior query
// against the same storage Snapshot generation.
func (s *Session) Search(ctx context.Context, rawQuery string, opts SearchOptions) (*SearchResult, error) {
	opts.normalize()
	start := time.Now()

	rs, err := s.Start(ctx, rawQuery, opts)
	if err != nil {
		return nil, err
	}
	defer rs.Close()

	items, err := rs.Viewport(ctx, opts.Offset, opts.Limit)
	if err != nil {
		return nil, err
	}
	return rs.toSearchResult(items, opts.Offset, opts.Limit, start), nil
}

func refineCandidates(
	ctx context.Context,
	snap *storage.Snapshot,
	prev []candidate,
	plan *compiledPlan,
) ([]candidate, int, error) {
	var coldReads int64
	out := make([]candidate, 0, len(prev)/2+1)

	for i := range prev {
		if (i & ctxCheckIntervalMask) == 0 {
			if err := ctx.Err(); err != nil {
				return nil, int(coldReads), err
			}
		}
		c := prev[i]
		node, name, ok := snap.LookupNode(c.nodeID)
		if !ok {
			continue
		}
		ec := evalContext{
			snap:      snap,
			nodeID:    c.nodeID,
			node:      node,
			name:      name,
			nonASCII:  (node.Flags & storage.FlagNonASCII) != 0,
			noFuzzy:   c.tier2,
			coldReady: c.hasCold,
			coldMeta:  storage.ColdMeta{Size: c.size, ModTime: c.modTime},
			coldReads: &coldReads,
		}
		if ok, tier := evalExpr(plan.root, &ec); ok {
			next := candidate{
				nodeID:   c.nodeID,
				flags:    node.Flags,
				extID:    node.ExtID,
				volID:    node.VolID,
				nameLen:  node.NameLen,
				bestTier: tier,
				tier2:    c.tier2,
			}
			if ec.coldReady {
				next.hasCold = true
				next.size = ec.coldMeta.Size
				next.modTime = ec.coldMeta.ModTime
			}
			out = append(out, next)
		}
	}
	return out, int(coldReads), nil
}

// containsPartialCandidates reports whether any candidate came from a
// truncated (incomplete) scan path.
func containsPartialCandidates(cands []candidate) bool {
	for i := range cands {
		if cands[i].partial {
			return true
		}
	}
	return false
}

// isNarrowingQuery conservatively checks whether every node matching next is guaranteed
// to also match prev (so refining prev's candidate set is 100% sound and lossless).
func isNarrowingQuery(prev, next *Query) bool {
	if prev == nil || next == nil {
		return false
	}
	if len(prev.InDirs) > 0 || len(next.InDirs) > 0 {
		return false
	}
	if prev.IncludeSystem != next.IncludeSystem {
		return false
	}
	prevAtoms, okPrev := prev.SimpleAndAtoms()
	nextAtoms, okNext := next.SimpleAndAtoms()
	if !okPrev || !okNext {
		return false
	}
	// Every previous atom must be implied by at least one atom in next.
	for i := range prevAtoms {
		pa := &prevAtoms[i]
		covered := false
		for j := range nextAtoms {
			if atomNarrows(pa, &nextAtoms[j]) {
				covered = true
				break
			}
		}
		if !covered {
			return false
		}
	}
	return true
}

func atomNarrows(prev, next *Atom) bool {
	if prev.Kind != next.Kind {
		return false
	}
	switch prev.Kind {
	case AtomName:
		// If next's substring contains prev's substring (e.g., "rep" -> "report_2026"),
		// any filename matching next MUST also match prev.
		return strings.Contains(next.Value, prev.Value)
	case AtomPath:
		if prev.HasWildcard || next.HasWildcard {
			return prev.Value == next.Value
		}
		return strings.Contains(next.Value, prev.Value)
	case AtomType:
		return prev.WantDir == next.WantDir
	case AtomExt:
		// Every ext in next must be in prev's ext set.
		for _, ne := range next.Exts {
			found := false
			for _, pe := range prev.Exts {
				if ne == pe {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return len(next.Exts) > 0
	case AtomWildcard:
		return prev.Value == next.Value
	case AtomSize:
		if prev.Size.HasMin && (!next.Size.HasMin || next.Size.Min < prev.Size.Min) {
			return false
		}
		if prev.Size.HasMax && (!next.Size.HasMax || next.Size.Max > prev.Size.Max) {
			return false
		}
		return true
	case AtomModTime:
		if prev.ModTime.HasMin && (!next.ModTime.HasMin || next.ModTime.MinNano < prev.ModTime.MinNano) {
			return false
		}
		if prev.ModTime.HasMax && (!next.ModTime.HasMax || next.ModTime.MaxNano > prev.ModTime.MaxNano) {
			return false
		}
		return true
	default:
		return false
	}
}
