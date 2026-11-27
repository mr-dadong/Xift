package query

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"

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
	hasCold  bool
	size     int64
	modTime  int64
	score    float64
}

type compiledAtom struct {
	atom          Atom
	needleBytes   []byte
	needleASCII   bool
	extIDs        []uint16
	extImpossible bool
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
	needsPath     bool
	needsCold     bool
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
	}
	return p
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
	snap      *storage.Snapshot
	nodeID    uint32
	node      storage.HotNode
	name      []byte
	nonASCII  bool
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
		tier := MatchNameWithFallback(ec.name, ec.nonASCII, ca.needleBytes, ca.atom.Value)
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

	default:
		return false, TierNone
	}
}

// scanSnapshot performs lock-free multi-core sharded hot-column scanning over all segments
// and MemTable in snap.
func scanSnapshot(ctx context.Context, snap *storage.Snapshot, plan *compiledPlan, workers int) ([]candidate, int, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}

	var allCands []candidate
	var totalCold atomic.Int64

	segCount := snap.SegmentCount()
	for si := 0; si < segCount; si++ {
		if err := ctx.Err(); err != nil {
			return nil, int(totalCold.Load()), err
		}
		ss := snap.SegmentSnapshotAt(si)
		n := ss.Count()
		if n == 0 || ss.LiveCount() == 0 {
			continue
		}

		if workers <= 1 || n < minParallelShardSize {
			var localCold int64
			cands, err := scanSegmentRange(ctx, snap, ss, plan, 0, n, &localCold)
			totalCold.Add(localCold)
			if err != nil {
				return nil, int(totalCold.Load()), err
			}
			allCands = append(allCands, cands...)
			continue
		}

		wCount := workers
		shardSize := (n + uint32(wCount) - 1) / uint32(wCount)
		if shardSize < minParallelShardSize/2 {
			shardSize = minParallelShardSize / 2
			wCount = int((n + shardSize - 1) / shardSize)
		}

		results := make([][]candidate, wCount)
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
				c, err := scanSegmentRange(ctx, snap, ss, plan, lo, hi, &localCold)
				totalCold.Add(localCold)
				results[idx] = c
				errs[idx] = err
			}(w, start, end)
		}
		wg.Wait()

		for i := 0; i < wCount; i++ {
			if errs[i] != nil {
				return nil, int(totalCold.Load()), errs[i]
			}
		}
		totalLen := len(allCands)
		for i := 0; i < wCount; i++ {
			totalLen += len(results[i])
		}
		if cap(allCands) < totalLen {
			grown := make([]candidate, len(allCands), totalLen)
			copy(grown, allCands)
			allCands = grown
		}
		for i := 0; i < wCount; i++ {
			allCands = append(allCands, results[i]...)
		}
	}

	// Scan MemTable (typically small, <= 64K entries).
	if mem := snap.MemTable(); mem != nil && mem.LiveCount() > 0 {
		if err := ctx.Err(); err != nil {
			return nil, int(totalCold.Load()), err
		}
		var localCold int64
		memLen := mem.Len()
		for i := 0; i < memLen; i++ {
			if (i & ctxCheckIntervalMask) == 0 {
				if err := ctx.Err(); err != nil {
					totalCold.Add(localCold)
					return nil, int(totalCold.Load()), err
				}
			}
			slot := uint32(i)
			node := mem.NodeAt(slot)
			if (node.Flags & storage.FlagDeleted) != 0 {
				continue
			}
			name := mem.NameBytesAt(slot)
			nodeID := mem.NodeIDAt(slot)
			ec := evalContext{
				snap:      snap,
				nodeID:    nodeID,
				node:      node,
				name:      name,
				nonASCII:  (node.Flags & storage.FlagNonASCII) != 0,
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
				}
				if ec.coldReady {
					cand.hasCold = true
					cand.size = ec.coldMeta.Size
					cand.modTime = ec.coldMeta.ModTime
				}
				allCands = append(allCands, cand)
			}
		}
		totalCold.Add(localCold)
	}

	return allCands, int(totalCold.Load()), nil
}

func scanSegmentRange(
	ctx context.Context,
	snap *storage.Snapshot,
	ss storage.SegmentSnapshot,
	plan *compiledPlan,
	start, end uint32,
	localCold *int64,
) ([]candidate, error) {
	nodes := ss.HotNodes()
	names := ss.NamesBytes()
	namesLen := uint64(len(names))
	root := plan.root

	var out []candidate
	for i := start; i < end; i++ {
		if (i & ctxCheckIntervalMask) == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
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
			}
			if ec.coldReady {
				cand.hasCold = true
				cand.size = ec.coldMeta.Size
				cand.modTime = ec.coldMeta.ModTime
			}
			out = append(out, cand)
		}
	}
	return out, nil
}
