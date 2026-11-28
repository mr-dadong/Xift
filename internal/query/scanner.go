package query

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"

	"github.com/mr-dadong/Xift/internal/storage"
)

const (
	minParallelShardSize = 16384
	ctxCheckIntervalMask = 4095
)

// candidate is a compact internal match record kept between hot-column scan and ranking.
// Hot-only queries keep Size/ModTime unread (hasCold == false) until viewport materialization.
type candidate struct {
	nodeID   uint32
	flags    uint16
	extID    uint16
	volID    uint16
	nameLen  uint16
	bestTier MatchTier
	// tier2 records that this candidate came from a Tier 2 segment, whose
	// matcher deliberately omits the Fuzzy fallback.
	tier2 bool
	// partial marks a result of the truncated short-word path; such a set MUST
	// NOT be cached for incremental narrowing because it is not complete.
	partial bool
	hasCold bool
	size    int64
	modTime int64
	score   float64
}

type compiledAtom struct {
	atom          Atom
	needleBytes   []byte
	needleASCII   bool
	needleRunes   int
	extIDs        []uint16
	extImpossible bool
	// contentSet holds the L1 node IDs whose body contains this content term.
	// It is filled by Engine.Start before scanning and is nil when L2 is off.
	contentSet map[uint32]struct{}
}

type compiledExpr struct {
	kind      ExprKind
	atom      compiledAtom
	child     *compiledExpr
	children  []*compiledExpr
	needsPath bool
	needsCold bool
}

type compiledPlan struct {
	root          *compiledExpr
	includeSystem bool
	positiveNames []compiledAtom // Used only for final viewport HighlightRange extraction
	// requiredNames holds name atoms that any match of root must satisfy
	// (top-level conjunction only; OR branches and negations are excluded).
	// They drive Tier 2 inverted candidate generation.
	requiredNames []compiledAtom
	needsPath     bool
	needsCold     bool
	// contentAtoms points at every AtomContent leaf, so Engine.Start can attach
	// one L2 result set per distinct term before scanning.
	contentAtoms []*compiledAtom
	// contentOnly is true when every leaf of the expression is a content term,
	// so L2's node set can seed candidates instead of scanning the whole index.
	contentOnly bool
}

func compileQuery(q *Query, extPool *storage.ExtPool) *compiledPlan {
	p := &compiledPlan{
		includeSystem: q.IncludeSystem,
	}
	if q.Root == nil {
		return p
	}
	p.root = compileExpr(q.Root, extPool, false, &p.positiveNames)
	if p.root != nil {
		p.needsPath = p.root.needsPath
		p.needsCold = p.root.needsCold
		p.requiredNames = p.root.requiredNameAtoms(nil)
		p.contentAtoms = p.root.collectContentAtoms(nil)
		p.contentOnly = len(p.contentAtoms) > 0 && p.root.allAtomsContent()
	}
	return p
}

// collectContentAtoms gathers pointers to every AtomContent leaf.
func (ce *compiledExpr) collectContentAtoms(out []*compiledAtom) []*compiledAtom {
	if ce == nil {
		return out
	}
	switch ce.kind {
	case ExprAtom:
		if ce.atom.atom.Kind == AtomContent {
			out = append(out, &ce.atom)
		}
	case ExprAnd, ExprOr:
		for _, ch := range ce.children {
			out = ch.collectContentAtoms(out)
		}
	case ExprNot:
		out = ce.child.collectContentAtoms(out)
	}
	return out
}

// allAtomsContent reports whether every leaf is a content term.
func (ce *compiledExpr) allAtomsContent() bool {
	if ce == nil {
		return false
	}
	switch ce.kind {
	case ExprAtom:
		return ce.atom.atom.Kind == AtomContent
	case ExprAnd, ExprOr:
		if len(ce.children) == 0 {
			return false
		}
		for _, ch := range ce.children {
			if !ch.allAtomsContent() {
				return false
			}
		}
		return true
	case ExprNot:
		return ce.child.allAtomsContent()
	}
	return false
}

// requiredNameAtoms collects name atoms every match of ce must satisfy.
func (ce *compiledExpr) requiredNameAtoms(out []compiledAtom) []compiledAtom {
	if ce == nil {
		return out
	}
	switch ce.kind {
	case ExprAtom:
		if ce.atom.atom.Kind == AtomName {
			out = append(out, ce.atom)
		}
	case ExprAnd:
		for _, ch := range ce.children {
			out = ch.requiredNameAtoms(out)
		}
	}
	return out
}

func compileExpr(e *Expr, extPool *storage.ExtPool, negated bool, posNames *[]compiledAtom) *compiledExpr {
	if e == nil {
		return nil
	}
	ce := &compiledExpr{kind: e.Kind}
	switch e.Kind {
	case ExprAtom:
		ca := compiledAtom{atom: e.Atom}
		switch e.Atom.Kind {
		case AtomName:
			ca.needleBytes = []byte(e.Atom.Value)
			ca.needleASCII = isAllASCII(ca.needleBytes)
			ca.needleRunes = utf8.RuneCount(ca.needleBytes)
			if !negated {
				*posNames = append(*posNames, ca)
			}
		case AtomWildcard:
			if e.Atom.PureExt != "" && extPool != nil {
				if id, ok := extPool.Lookup(e.Atom.PureExt); ok {
					ca.extIDs = []uint16{id}
				} else {
					ca.extImpossible = true
				}
			}
		case AtomExt:
			if extPool != nil {
				ids := make([]uint16, 0, len(e.Atom.Exts))
				for _, ext := range e.Atom.Exts {
					if id, ok := extPool.Lookup(ext); ok {
						ids = append(ids, id)
					}
				}
				if len(ids) == 0 {
					ca.extImpossible = true
				} else {
					ca.extIDs = ids
				}
			}
		case AtomPath:
			ce.needsPath = true
		case AtomSize, AtomModTime:
			ce.needsCold = true
		case AtomContent:
			// The candidate set is attached by Engine.Start after L2 lookup.
		}
		ce.atom = ca
	case ExprNot:
		ce.child = compileExpr(e.Child, extPool, !negated, posNames)
		if ce.child != nil {
			ce.needsPath = ce.child.needsPath
			ce.needsCold = ce.child.needsCold
		}
	case ExprAnd, ExprOr:
		ce.children = make([]*compiledExpr, 0, len(e.Children))
		for _, ch := range e.Children {
			cch := compileExpr(ch, extPool, negated, posNames)
			if cch != nil {
				ce.children = append(ce.children, cch)
				if cch.needsPath {
					ce.needsPath = true
				}
				if cch.needsCold {
					ce.needsCold = true
				}
			}
		}
		// Order AND children cheapest-first: hot-only atoms -> path atoms -> cold atoms.
		if e.Kind == ExprAnd && len(ce.children) > 1 {
			orderAndChildren(ce.children)
		}
	}
	return ce
}

func exprCost(c *compiledExpr) int {
	if c.needsCold {
		return 30
	}
	if c.needsPath {
		return 20
	}
	if c.kind == ExprAtom {
		switch c.atom.atom.Kind {
		case AtomType:
			return 1
		case AtomExt:
			return 2
		case AtomContent:
			// Evaluate L1 conditions first; body membership runs last.
			return 40
		case AtomWildcard:
			if c.atom.atom.PureExt != "" {
				return 2
			}
			return 8
		case AtomName:
			return 5
		}
	}
	return 10
}

func orderAndChildren(children []*compiledExpr) {
	// Insertion sort (len is tiny, usually 2..8).
	for i := 1; i < len(children); i++ {
		cur := children[i]
		curCost := exprCost(cur)
		j := i - 1
		for j >= 0 && exprCost(children[j]) > curCost {
			children[j+1] = children[j]
			j--
		}
		children[j+1] = cur
	}
}

type evalContext struct {
	snap     *storage.Snapshot
	nodeID   uint32
	node     storage.HotNode
	name     []byte
	nonASCII bool
	// noFuzzy disables the Fuzzy fallback (Tier 2 segments).
	noFuzzy   bool
	pathReady bool
	pathLower string
	coldReady bool
	coldMeta  storage.ColdMeta
	coldReads *int64
}

func (ec *evalContext) getPathLower() string {
	if !ec.pathReady {
		ec.pathReady = true
		if p, err := ec.snap.ResolvePathWithSep(ec.nodeID, '/'); err == nil {
			ec.pathLower = strings.ToLower(p)
		}
	}
	return ec.pathLower
}

func (ec *evalContext) getCold() (storage.ColdMeta, bool) {
	if !ec.coldReady {
		ec.coldReady = true
		if ec.coldReads != nil {
			*ec.coldReads++
		}
		cm, err := ec.snap.ReadColdMeta(ec.nodeID)
		if err != nil {
			return storage.ColdMeta{}, false
		}
		ec.coldMeta = cm
	}
	return ec.coldMeta, true
}

func evalExpr(ce *compiledExpr, ec *evalContext) (bool, MatchTier) {
	if ce == nil {
		return true, TierNone
	}
	switch ce.kind {
	case ExprAtom:
		return evalAtom(&ce.atom, ec)
	case ExprNot:
		ok, _ := evalExpr(ce.child, ec)
		return !ok, TierNone
	case ExprAnd:
		best := TierNone
		for _, ch := range ce.children {
			ok, tier := evalExpr(ch, ec)
			if !ok {
				return false, TierNone
			}
			if tier > best {
				best = tier
			}
		}
		return true, best
	case ExprOr:
		matched := false
		best := TierNone
		for _, ch := range ce.children {
			ok, tier := evalExpr(ch, ec)
			if ok {
				matched = true
				if tier > best {
					best = tier
				}
				// Short-circuit when we already hit exact or no name tier is needed.
				if best == TierExact {
					return true, best
				}
			}
		}
		return matched, best
	default:
		return false, TierNone
	}
}

func evalAtom(ca *compiledAtom, ec *evalContext) (bool, MatchTier) {
	switch ca.atom.Kind {
	case AtomType:
		isDir := (ec.node.Flags & storage.FlagDir) != 0
		return isDir == ca.atom.WantDir, TierNone

	case AtomExt:
		if ca.extImpossible || (ec.node.Flags&storage.FlagDir) != 0 || ec.node.ExtID == 0 {
			return false, TierNone
		}
		for _, id := range ca.extIDs {
			if ec.node.ExtID == id {
				return true, TierNone
			}
		}
		return false, TierNone

	case AtomWildcard:
		if ca.atom.PureExt != "" {
			if ca.extImpossible || (ec.node.Flags&storage.FlagDir) != 0 || ec.node.ExtID == 0 {
				return false, TierNone
			}
			if ec.node.ExtID == ca.extIDs[0] {
				return true, TierWildcard
			}
			return false, TierNone
		}
		if MatchWildcard(ec.name, ec.nonASCII, ca.atom.Value) {
			return true, TierWildcard
		}
		return false, TierNone

	case AtomName:
		var tier MatchTier
		if ec.noFuzzy {
			tier = MatchNameNoFuzzy(ec.name, ec.nonASCII, ca.needleBytes, ca.atom.Value)
		} else {
			tier = MatchNameWithFallback(ec.name, ec.nonASCII, ca.needleBytes, ca.atom.Value)
		}
		if tier == TierNone {
			return false, TierNone
		}
		return true, tier

	case AtomPath:
		pl := ec.getPathLower()
		if pl == "" {
			return false, TierNone
		}
		if ca.atom.HasWildcard {
			return MatchWildcard([]byte(pl), ec.nonASCII || !isAllASCII([]byte(pl)), ca.atom.Value), TierNone
		}
		return strings.Contains(pl, ca.atom.Value), TierNone

	case AtomSize:
		cm, ok := ec.getCold()
		if !ok {
			return false, TierNone
		}
		return ca.atom.Size.Matches(cm.Size), TierNone

	case AtomModTime:
		cm, ok := ec.getCold()
		if !ok {
			return false, TierNone
		}
		return ca.atom.ModTime.Matches(cm.ModTime), TierNone

	case AtomContent:
		if ca.contentSet == nil {
			return false, TierNone
		}
		_, ok := ca.contentSet[ec.nodeID]
		return ok, TierNone

	default:
		return false, TierNone
	}
}

// scanOutcome is what one snapshot scan produced under a candidate budget.
type scanOutcome struct {
	cands     []candidate
	coldReads int
	// partial marks results from a truncated short-word path (incomplete low-df
	// bigram coverage); such sets must not seed session narrowing.
	partial bool
	// truncated marks that the candidate budget was exhausted, so the match set
	// may contain more entries than were verified.
	truncated bool
	// verified counts accepted matches. When truncated it is a lower bound.
	verified int
}

// lockedCollector serializes candidate collection across parallel scan shards.
type lockedCollector struct {
	mu  sync.Mutex
	col *candidateCollector
}

func newLockedCollector(budget int) *lockedCollector {
	return &lockedCollector{col: newCandidateCollector(budget)}
}

func (l *lockedCollector) add(c candidate) bool {
	l.mu.Lock()
	ok := l.col.add(c)
	l.mu.Unlock()
	return ok
}

func (l *lockedCollector) done() bool {
	l.mu.Lock()
	d := l.col.done()
	l.mu.Unlock()
	return d
}

// scanSnapshot performs lock-free multi-core sharded hot-column scanning over all segments
// and MemTable in snap, verifying at most budget matches (Tier 2 only; Tier 1 keeps
// exact in-memory semantics with an unbounded budget).
func scanSnapshot(ctx context.Context, snap *storage.Snapshot, plan *compiledPlan, workers, budget int) (scanOutcome, error) {
	if err := ctx.Err(); err != nil {
		return scanOutcome{}, err
	}

	// Tier 2 mode deliberately omits the Fuzzy fallback: trigram candidates are
	// complete for substring and Pinyin hits but cannot cover Fuzzy-only hits,
	// so matching semantics are held constant across the whole query.
	noFuzzy := snapshotTier2Mode(snap)

	// Tier 1 scans immutable in-memory or mmap'd columns, so full materialization
	// stays cheap and bounded by the 5M-node tier ceiling; the budget exists to
	// bound Tier 2's per-match block reads.
	if !noFuzzy {
		budget = 0
	}
	col := newLockedCollector(budget)
	var totalCold atomic.Int64
	partial := false

	segCount := snap.SegmentCount()
	for si := 0; si < segCount; si++ {
		if err := ctx.Err(); err != nil {
			return scanOutcome{}, err
		}
		if col.done() {
			break
		}
		ss := snap.SegmentSnapshotAt(si)
		n := ss.Count()
		if n == 0 || ss.LiveCount() == 0 {
			continue
		}

		if ss.IsTier2() {
			var localCold int64
			var err error
			switch routeSegment(plan, ss) {
			case routeInverted:
				err = scanInvertedSegment(ctx, snap, ss, plan, &localCold, col)
			case routeShortWord:
				var ok, segPartial bool
				ok, segPartial, err = scanShortWordSegment(ctx, snap, ss, plan, &localCold, col)
				if err == nil && !ok {
					err = scanSegmentRangeTier2(ctx, snap, ss, plan, 0, n, &localCold, col)
				} else if segPartial {
					partial = true
				}
			default:
				err = scanSegmentRangeTier2(ctx, snap, ss, plan, 0, n, &localCold, col)
			}
			totalCold.Add(localCold)
			if err != nil {
				return scanOutcome{}, err
			}
			continue
		}

		if workers <= 1 || n < minParallelShardSize {
			var localCold int64
			err := scanSegmentRange(ctx, snap, ss, plan, 0, n, noFuzzy, &localCold, col)
			totalCold.Add(localCold)
			if err != nil {
				return scanOutcome{}, err
			}
			continue
		}

		wCount := workers
		shardSize := (n + uint32(wCount) - 1) / uint32(wCount)
		if shardSize < minParallelShardSize/2 {
			shardSize = minParallelShardSize / 2
			wCount = int((n + shardSize - 1) / shardSize)
		}

		errs := make([]error, wCount)
		var wg sync.WaitGroup
		for w := 0; w < wCount; w++ {
			start := uint32(w) * shardSize
			if start >= n {
				break
			}
			end := start + shardSize
			if end > n {
				end = n
			}
			wg.Add(1)
			go func(idx int, lo, hi uint32) {
				defer wg.Done()
				var localCold int64
				errs[idx] = scanSegmentRange(ctx, snap, ss, plan, lo, hi, noFuzzy, &localCold, col)
				totalCold.Add(localCold)
			}(w, start, end)
		}
		wg.Wait()

		for i := 0; i < wCount; i++ {
			if errs[i] != nil {
				return scanOutcome{}, errs[i]
			}
		}
	}

	// Scan MemTable (typically small, <= 64K entries). In Tier 2 mode the
	// in-memory inverted map prunes candidates first; otherwise every live
	// slot is evaluated.
	if mem := snap.MemTable(); mem != nil && mem.LiveCount() > 0 && !col.done() {
		if err := ctx.Err(); err != nil {
			return scanOutcome{}, err
		}
		var localCold int64
		emit := func(slot uint32) bool {
			node := mem.NodeAt(slot)
			if (node.Flags & storage.FlagDeleted) != 0 {
				return true
			}
			name := mem.NameBytesAt(slot)
			nodeID := mem.NodeIDAt(slot)
			ec := evalContext{
				snap:      snap,
				nodeID:    nodeID,
				node:      node,
				name:      name,
				nonASCII:  (node.Flags & storage.FlagNonASCII) != 0,
				noFuzzy:   noFuzzy,
				coldReads: &localCold,
			}
			if ok, tier := evalExpr(plan.root, &ec); ok {
				cand := candidate{
					nodeID:   nodeID,
					flags:    node.Flags,
					extID:    node.ExtID,
					volID:    node.VolID,
					nameLen:  node.NameLen,
					bestTier: tier,
					tier2:    noFuzzy,
				}
				if ec.coldReady {
					cand.hasCold = true
					cand.size = ec.coldMeta.Size
					cand.modTime = ec.coldMeta.ModTime
				}
				col.add(cand)
			}
			return true
		}

		// The in-memory inverted map is only a sound superset when Fuzzy is
		// disabled (Tier 2 mode); Tier 1 keeps the full scan for fuzzy hits.
		if ids, ok := func() ([]uint32, bool) {
			if !noFuzzy {
				return nil, false
			}
			return memInvertedCandidates(mem, plan.requiredNames)
		}(); ok {
			for i, nodeID := range ids {
				if (i & ctxCheckIntervalMask) == 0 {
					if err := ctx.Err(); err != nil {
						totalCold.Add(localCold)
						return scanOutcome{}, err
					}
					if col.done() {
						break
					}
				}
				if slot, ok := mem.GetByNodeID(nodeID); ok {
					emit(slot)
				}
			}
		} else {
			memLen := mem.Len()
			for i := 0; i < memLen; i++ {
				if (i & ctxCheckIntervalMask) == 0 {
					if err := ctx.Err(); err != nil {
						totalCold.Add(localCold)
						return scanOutcome{}, err
					}
					if col.done() {
						break
					}
				}
				emit(uint32(i))
			}
		}
		totalCold.Add(localCold)
	}

	return scanOutcome{
		cands:     col.col.results(),
		coldReads: int(totalCold.Load()),
		partial:   partial,
		truncated: col.col.truncated(),
		verified:  col.col.verified(),
	}, nil
}

// snapshotTier2Mode reports whether the engine was in Tier 2 mode for snap.
func snapshotTier2Mode(snap *storage.Snapshot) bool {
	return snap.Tier2Mode()
}

// scanSegmentRangeTier2 scans a Tier 2 segment sequentially through the block
// cache (no whole-column slices). Used when no trigram-prunable term exists.
func scanSegmentRangeTier2(
	ctx context.Context,
	snap *storage.Snapshot,
	ss storage.SegmentSnapshot,
	plan *compiledPlan,
	start, end uint32,
	localCold *int64,
	col *lockedCollector,
) error {
	for i := start; i < end; i++ {
		if (i & ctxCheckIntervalMask) == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
			if col.done() {
				return nil
			}
		}
		if ss.IsDeleted(i) {
			continue
		}
		node := ss.NodeAt(i)
		name := ss.NameBytesAt(i)
		if name == nil && node.NameLen > 0 {
			continue
		}
		nodeID := ss.NodeIDAt(i)
		ec := evalContext{
			snap:      snap,
			nodeID:    nodeID,
			node:      node,
			name:      name,
			nonASCII:  (node.Flags & storage.FlagNonASCII) != 0,
			noFuzzy:   true,
			coldReads: localCold,
		}
		if ok, tier := evalExpr(plan.root, &ec); ok {
			cand := candidate{
				nodeID:   nodeID,
				flags:    node.Flags,
				extID:    node.ExtID,
				volID:    node.VolID,
				nameLen:  node.NameLen,
				bestTier: tier,
				tier2:    true,
			}
			if ec.coldReady {
				cand.hasCold = true
				cand.size = ec.coldMeta.Size
				cand.modTime = ec.coldMeta.ModTime
			}
			col.add(cand)
		}
	}
	return nil
}

func scanSegmentRange(
	ctx context.Context,
	snap *storage.Snapshot,
	ss storage.SegmentSnapshot,
	plan *compiledPlan,
	start, end uint32,
	noFuzzy bool,
	localCold *int64,
	col *lockedCollector,
) error {
	nodes := ss.HotNodes()
	names := ss.NamesBytes()
	namesLen := uint64(len(names))
	root := plan.root

	for i := start; i < end; i++ {
		if (i & ctxCheckIntervalMask) == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
			if col.done() {
				return nil
			}
		}
		if ss.IsDeleted(i) {
			continue
		}
		node := nodes[i]
		nameStart := uint64(node.NameOff)
		nameEnd := nameStart + uint64(node.NameLen)
		if nameEnd > namesLen {
			continue
		}
		name := names[nameStart:nameEnd]
		nodeID := ss.NodeIDAt(i)

		ec := evalContext{
			snap:      snap,
			nodeID:    nodeID,
			node:      node,
			name:      name,
			nonASCII:  (node.Flags & storage.FlagNonASCII) != 0,
			noFuzzy:   noFuzzy,
			coldReads: localCold,
		}
		if ok, tier := evalExpr(root, &ec); ok {
			cand := candidate{
				nodeID:   nodeID,
				flags:    node.Flags,
				extID:    node.ExtID,
				volID:    node.VolID,
				nameLen:  node.NameLen,
				bestTier: tier,
				tier2:    noFuzzy,
			}
			if ec.coldReady {
				cand.hasCold = true
				cand.size = ec.coldMeta.Size
				cand.modTime = ec.coldMeta.ModTime
			}
			col.add(cand)
		}
	}
	return nil
}
