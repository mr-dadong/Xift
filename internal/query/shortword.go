package query

import (
	"context"
	"slices"

	"github.com/mr-dadong/Xift/internal/storage"
	"github.com/mr-dadong/Xift/internal/storage/lsm"
	"github.com/mr-dadong/Xift/internal/textkey"
)

// shortWordPostingLimit caps how many bigram-posting candidates a one/two-rune
// term may pull from one segment before verification. It is a variable so
// tests can exercise truncation at a small scale.
var shortWordPostingLimit = 10_000

// shortPathState describes one bigram namespace for a short term.
type shortPathState uint8

const (
	// shortAbsent: the segment dictionary has no such gram at all, so this
	// namespace cannot contribute matches.
	shortAbsent shortPathState = iota
	// shortCut: the gram exists but was dropped by the document-frequency
	// budget, so the hit set cannot be bounded by the posting.
	shortCut
	// shortOK: every gram has a usable posting.
	shortOK
)

// loadShortPath resolves a set of bigram grams by state.
func loadShortPath(bi *lsm.InvertedIndex, segID uint32, grams []uint64) ([]*lsm.RoaringView, shortPathState, error) {
	for _, g := range grams {
		df, hit := bi.Df(segID, invertedFileBigram, g)
		if !hit {
			return nil, shortAbsent, nil
		}
		if df == 0 {
			return nil, shortCut, nil
		}
	}
	views := make([]*lsm.RoaringView, 0, len(grams))
	for _, g := range grams {
		raw, hit, err := bi.LookupPosting(segID, invertedFileBigram, g)
		if err != nil {
			return nil, shortAbsent, err
		}
		if !hit {
			return nil, shortAbsent, nil
		}
		v, err := lsm.ParseRoaring(raw)
		if err != nil {
			return nil, shortAbsent, err
		}
		views = append(views, v)
	}
	return views, shortOK, nil
}

// scanShortWordSegment evaluates a 1-2 rune required term on a Tier 2 segment.
// Two-rune terms stream the bigram posting truncated to shortWordPostingLimit;
// one-rune terms (and missing/super-high-df bigrams) report ok=false so the
// caller falls back to the sequential point-read scan. Only the truncated
// candidate set is verified and ranked; the full hit set is never sorted.
func scanShortWordSegment(
	ctx context.Context,
	snap *storage.Snapshot,
	ss storage.SegmentSnapshot,
	plan *compiledPlan,
	localCold *int64,
) (cands []candidate, ok bool, partial bool, err error) {
	var atom *compiledAtom
	for i := range plan.requiredNames {
		ca := &plan.requiredNames[i]
		if ca.needleRunes == 2 && ca.needleASCII {
			atom = ca
			break
		}
	}
	if atom == nil {
		return nil, false, false, nil // no 2-rune ASCII term: sequential fallback
	}

	_, bi := ss.Tier2Indexes()
	if bi == nil {
		return nil, false, false, nil
	}
	segID := ss.SegmentID()

	normal := textkey.AppendBigramKeys(atom.needleBytes, nil)
	slices.Sort(normal)
	normal = slices.Compact(normal)

	var pinyin []uint64
	for i := 0; i+2 <= len(atom.needleBytes); i++ {
		pinyin = append(pinyin, textkey.GramNamespacePinyin|
			uint64(atom.needleBytes[i])<<8|
			uint64(atom.needleBytes[i+1]))
	}

	normalViews, normalState, err := loadShortPath(bi, segID, normal)
	if err != nil {
		return nil, false, false, err
	}
	var pinyinState shortPathState
	var pinyinViews []*lsm.RoaringView
	if len(pinyin) > 0 {
		pinyinViews, pinyinState, err = loadShortPath(bi, segID, pinyin)
		if err != nil {
			return nil, false, false, err
		}
	}

	// A cut path (df budget) cannot prove the hit set is complete, so fall back
	// to the sequential scan. With both paths absent, no name can match.
	if normalState == shortCut || pinyinState == shortCut {
		return nil, false, false, nil
	}
	if normalState != shortOK && pinyinState != shortOK {
		return nil, true, false, nil
	}
	views := make([]*lsm.RoaringView, 0, len(normalViews)+len(pinyinViews))
	if normalState == shortOK {
		views = append(views, normalViews...)
	}
	if pinyinState == shortOK {
		views = append(views, pinyinViews...)
	}

	var out []candidate
	var ctxErr error
	truncated := unionIterate(views, shortWordPostingLimit, func(localIdx uint32) bool {
		if len(out)&ctxCheckIntervalMask == 0 {
			if err := ctx.Err(); err != nil {
				ctxErr = err
				return false
			}
		}
		if localIdx >= ss.Count() || ss.IsDeleted(localIdx) {
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
		if matched, tier := evalExpr(plan.root, &ec); matched {
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
		return nil, false, false, ctxErr
	}
	if truncated {
		for i := range out {
			out[i].partial = true
		}
	}
	return out, true, truncated, nil
}

// unionIterate visits the union of the given views in ascending local-index
// order, stopping after limit distinct values. It reports whether the iteration
// was cut short (the candidate set is then partial). Heap allocation is bounded
// by limit, never by the posting cardinality.
func unionIterate(views []*lsm.RoaringView, limit int, fn func(uint32) bool) bool {
	if len(views) == 0 || limit <= 0 {
		return false
	}
	// Cheap common case: a single view streams directly.
	if len(views) == 1 {
		n := 0
		stopped := views[0].ForEach(func(v uint32) bool {
			n++
			if !fn(v) {
				return false
			}
			return n < limit
		})
		return stopped && n >= limit
	}
	seen := make(map[uint32]struct{}, min(limit, 1024))
	for _, v := range views {
		stopped := v.ForEach(func(x uint32) bool {
			if _, dup := seen[x]; dup {
				return true
			}
			seen[x] = struct{}{}
			if !fn(x) {
				return false
			}
			return len(seen) < limit
		})
		if stopped && len(seen) >= limit {
			return true
		}
	}
	return false
}
