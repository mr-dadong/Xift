package query

import (
	"cmp"
	"context"
	"slices"

	"github.com/mr-dadong/Xift/internal/storage"
	"github.com/mr-dadong/Xift/internal/storage/lsm"
	"github.com/mr-dadong/Xift/internal/textkey"
)

const (
	invertedFileTrigram = uint64(0)
	invertedFileBigram  = uint64(1)
)

// gramRef pairs one query gram with its document frequency in a segment.
type gramRef struct {
	key uint64
	df  uint32
}

// atomViews holds the loaded postings for one required name atom, split by
// namespace. normalOK/pinyinOK report whether every gram of that path exists
// in the segment dictionary; a missing gram means no name can match via that
// path, so the path contributes an empty candidate set.
type atomViews struct {
	normal    []*lsm.RoaringView
	normalOK  bool
	normalMin uint32
	pinyin    []*lsm.RoaringView
	pinyinOK  bool
	pinyinMin uint32
}

func (a *atomViews) plausible() bool { return a.normalOK || a.pinyinOK }

func (a *atomViews) minCard() uint32 {
	switch {
	case a.normalOK && a.pinyinOK:
		return min(a.normalMin, a.pinyinMin)
	case a.normalOK:
		return a.normalMin
	default:
		return a.pinyinMin
	}
}

// mostSelective returns the path with the smallest leading document frequency
// so the intersection streams from the rarest posting available.
func (a *atomViews) mostSelective() []*lsm.RoaringView {
	switch {
	case a.normalOK && a.pinyinOK && a.pinyinMin < a.normalMin:
		return a.pinyin
	case a.normalOK:
		return a.normal
	default:
		return a.pinyin
	}
}

func (a *atomViews) contains(localIdx uint32) bool {
	if a.normalOK && allViewsContain(a.normal, localIdx) {
		return true
	}
	if a.pinyinOK {
		return allViewsContain(a.pinyin, localIdx)
	}
	return false
}

func allViewsContain(views []*lsm.RoaringView, localIdx uint32) bool {
	for _, v := range views {
		if !v.Contains(localIdx) {
			return false
		}
	}
	return true
}

// buildAtomViews loads the trigram postings for one required name atom,
// df-ascending so the intersection streams from the most selective gram first.
func buildAtomViews(ss storage.SegmentSnapshot, ca compiledAtom) (*atomViews, error) {
	tri, _ := ss.Tier2Indexes()
	if tri == nil {
		return nil, nil
	}
	segID := ss.SegmentID()

	normal := textkey.AppendTrigramKeys(ca.needleBytes, nil)
	slices.Sort(normal)
	normal = slices.Compact(normal)
	if len(normal) == 0 {
		return nil, nil
	}

	av := &atomViews{}
	views, ok, minDF, err := loadPostings(tri, segID, normal)
	if err != nil {
		return nil, err
	}
	av.normal, av.normalOK, av.normalMin = views, ok, minDF

	// A pure-ASCII term can also match through the Pinyin-initial namespace,
	// whose keys pack three lowercase letters into 8-bit lanes.
	if ca.needleASCII {
		var pinyin []uint64
		for i := 0; i+3 <= len(ca.needleBytes); i++ {
			pinyin = append(pinyin, textkey.GramNamespacePinyin|
				uint64(ca.needleBytes[i])<<16|
				uint64(ca.needleBytes[i+1])<<8|
				uint64(ca.needleBytes[i+2]))
		}
		slices.Sort(pinyin)
		pinyin = slices.Compact(pinyin)
		views, ok, minDF, err = loadPostings(tri, segID, pinyin)
		if err != nil {
			return nil, err
		}
		av.pinyin, av.pinyinOK, av.pinyinMin = views, ok, minDF
	}
	return av, nil
}

// loadPostings resolves every gram to its posting view, sorted by document
// frequency ascending. ok=false means at least one gram is absent, which makes
// the whole path impossible to match.
func loadPostings(ix *lsm.InvertedIndex, segID uint32, grams []uint64) (views []*lsm.RoaringView, ok bool, minDF uint32, err error) {
	refs := make([]gramRef, 0, len(grams))
	for _, g := range grams {
		df, hit := ix.Df(segID, invertedFileTrigram, g)
		if !hit {
			return nil, false, 0, nil
		}
		refs = append(refs, gramRef{key: g, df: df})
	}
	slices.SortFunc(refs, func(a, b gramRef) int { return cmp.Compare(a.df, b.df) })
	views = make([]*lsm.RoaringView, 0, len(refs))
	for _, r := range refs {
		raw, hit, lookupErr := ix.LookupPosting(segID, invertedFileTrigram, r.key)
		if lookupErr != nil {
			return nil, false, 0, lookupErr
		}
		if !hit {
			return nil, false, 0, nil
		}
		v, parseErr := lsm.ParseRoaring(raw)
		if parseErr != nil {
			return nil, false, 0, parseErr
		}
		views = append(views, v)
	}
	return views, true, refs[0].df, nil
}

// scanInvertedSegment streams the intersection of the required atoms' trigram
// postings (df-ascending, short-circuit on an empty posting) and verifies each
// surviving local index with the full expression. Tombstones are filtered
// before verification and Fuzzy is disabled (Tier 2 semantics).
func scanInvertedSegment(
	ctx context.Context,
	snap *storage.Snapshot,
	ss storage.SegmentSnapshot,
	plan *compiledPlan,
	localCold *int64,
) ([]candidate, error) {
	views := make([]*atomViews, 0, len(plan.requiredNames))
	for _, ca := range plan.requiredNames {
		if ca.needleRunes < minInvertedRunes {
			continue
		}
		av, err := buildAtomViews(ss, ca)
		if err != nil {
			return nil, err
		}
		if av == nil {
			continue
		}
		if !av.plausible() {
			return nil, nil // no name in this segment can satisfy the atom
		}
		views = append(views, av)
	}
	if len(views) == 0 {
		return nil, nil
	}

	src := 0
	for i := 1; i < len(views); i++ {
		if views[i].minCard() < views[src].minCard() {
			src = i
		}
	}
	srcViews := views[src].mostSelective()

	var out []candidate
	var ctxErr error
	lsm.IntersectAll(srcViews, func(localIdx uint32) bool {
		if len(out)&ctxCheckIntervalMask == 0 {
			if err := ctx.Err(); err != nil {
				ctxErr = err
				return false
			}
		}
		if localIdx >= ss.Count() {
			return true
		}
		for i, av := range views {
			if i == src {
				continue
			}
			if !av.contains(localIdx) {
				return true
			}
		}
		if ss.IsDeleted(localIdx) {
			return true
		}
		node := ss.NodeAt(localIdx)
		name := ss.NameBytesAt(localIdx)
		if name == nil && node.NameLen > 0 {
			return true
		}
		nodeID := ss.NodeIDAt(localIdx)
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
			out = append(out, cand)
		}
		return true
	})
	if ctxErr != nil {
		return nil, ctxErr
	}
	return out, nil
}

// memInvertedCandidates returns MemTable nodeIDs that satisfy every long name
// atom per the in-memory inverted map. ok=false means no long atom allows
// pruning and the caller should scan the MemTable directly.
func memInvertedCandidates(mem *storage.MemTable, atoms []compiledAtom) ([]uint32, bool) {
	if !mem.InvertedEnabled() {
		// The gram index may be off (e.g. a Tier 1 index opened before the
		// switch); scanning the MemTable is the only sound fallback.
		return nil, false
	}
	var sets [][]uint32
	for _, ca := range atoms {
		if ca.needleRunes < minInvertedRunes {
			continue
		}
		ids := memAtomCandidates(mem, ca)
		sets = append(sets, ids)
	}
	if len(sets) == 0 {
		return nil, false
	}

	src := 0
	for i := 1; i < len(sets); i++ {
		if len(sets[i]) < len(sets[src]) {
			src = i
		}
	}
	out := make([]uint32, 0, len(sets[src]))
	for _, id := range sets[src] {
		keep := true
		for i, set := range sets {
			if i == src {
				continue
			}
			if _, found := slices.BinarySearch(set, id); !found {
				keep = false
				break
			}
		}
		if keep {
			out = append(out, id)
		}
	}
	return out, true
}

// memAtomCandidates intersects the gram postings of one atom in the MemTable's
// in-memory inverted map, unioning the normal and Pinyin namespaces per gram.
func memAtomCandidates(mem *storage.MemTable, ca compiledAtom) []uint32 {
	normal := textkey.AppendTrigramKeys(ca.needleBytes, nil)
	// Window-aligned Pinyin-namespace keys (8-bit lanes, unlike the 21-bit
	// original-rune lanes). Unioning both namespaces per window yields a
	// superset of either path's real intersection, which verification trims.
	var pinyin []uint64
	if ca.needleASCII {
		for i := 0; i+3 <= len(ca.needleBytes); i++ {
			pinyin = append(pinyin, textkey.GramNamespacePinyin|
				uint64(ca.needleBytes[i])<<16|
				uint64(ca.needleBytes[i+1])<<8|
				uint64(ca.needleBytes[i+2]))
		}
	}

	sets := make([][]uint32, 0, len(normal))
	for i, g := range normal {
		ids := append([]uint32(nil), mem.LookupGramInverted(g)...)
		if i < len(pinyin) {
			ids = append(ids, mem.LookupGramInverted(pinyin[i])...)
		}
		slices.Sort(ids)
		ids = slices.Compact(ids)
		if len(ids) == 0 {
			return nil
		}
		sets = append(sets, ids)
	}

	cur := sets[0]
	for _, next := range sets[1:] {
		cur = intersectSortedIDs(cur, next)
		if len(cur) == 0 {
			return nil
		}
	}
	return cur
}

// intersectSortedIDs intersects two ascending, duplicate-free nodeID slices.
func intersectSortedIDs(a, b []uint32) []uint32 {
	out := make([]uint32, 0, min(len(a), len(b)))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			i++
		case a[i] > b[j]:
			j++
		default:
			out = append(out, a[i])
			i++
			j++
		}
	}
	return out
}
