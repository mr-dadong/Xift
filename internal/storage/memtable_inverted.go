package storage

import (
	"slices"

	"github.com/mr-dadong/Xift/internal/textkey"
)

// memInverted is a bounded in-memory gram -> nodeID posting map over the
// MemTable's live entries (superset semantics: renames leave stale postings
// that candidate verification filters). Memory stays proportional to live
// entries * grams: the map is rebuilt once stale additions dominate.
type memInverted struct {
	postings map[uint64][]uint32
	adds     int // total gram-node additions since last rebuild
	liveAt   int // liveCount snapshot at last rebuild
}

func newMemInverted() *memInverted {
	return &memInverted{postings: make(map[uint64][]uint32, 64)}
}

// gramBudget is the multiplier over live entries beyond which the map is
// rebuilt from live state (rename-heavy churn bound).
const memInvertedRebuildFactor = 8

func (mi *memInverted) addName(name []byte, nodeID uint32, gramBuf []uint64) []uint64 {
	gramBuf = textkey.AppendTrigramKeys(name, gramBuf[:0])
	gramBuf = textkey.AppendPinyinTrigramKeys(name, gramBuf)
	slices.Sort(gramBuf)
	gramBuf = slices.Compact(gramBuf)
	for _, g := range gramBuf {
		mi.postings[g] = append(mi.postings[g], nodeID)
	}
	mi.adds += len(gramBuf)
	return gramBuf
}

// rebuild reconstructs postings from live MemTable entries, dropping stale
// gram-node pairs left behind by renames.
func (mi *memInverted) rebuild(m *MemTable) {
	mi.postings = make(map[uint64][]uint32, len(mi.postings))
	mi.adds = 0
	mi.liveAt = m.liveCount
	var buf []uint64
	m.ForEachLive(func(nodeID uint32, _ HotNode, name []byte) bool {
		buf = mi.addName(name, nodeID, buf)
		return true
	})
}

// maybeRebuild keeps memory bounded under rename churn.
func (mi *memInverted) maybeRebuild(m *MemTable) {
	if m.liveCount == 0 {
		return
	}
	if mi.adds > memInvertedRebuildFactor*max(m.liveCount, mi.liveAt)*16 {
		mi.rebuild(m)
	}
}

// lookup returns the candidate nodeIDs for gram (shared slice; MUST NOT be
// mutated by callers). The result is a superset of live matching nodes.
func (mi *memInverted) lookup(gram uint64) []uint32 {
	return mi.postings[gram]
}

// clone deep-copies the inverted map for CoW snapshot isolation.
func (mi *memInverted) clone() *memInverted {
	cp := &memInverted{adds: mi.adds, liveAt: mi.liveAt}
	cp.postings = make(map[uint64][]uint32, len(mi.postings))
	for g, ids := range mi.postings {
		cp.postings[g] = append([]uint32(nil), ids...)
	}
	return cp
}

// clear drops all postings.
func (mi *memInverted) clear() {
	mi.postings = make(map[uint64][]uint32, 64)
	mi.adds = 0
	mi.liveAt = 0
}

// LookupGramInverted returns MemTable candidate nodeIDs for a trigram gram
// (original or pinyin namespace). The result is a shared superset slice;
// callers MUST verify liveness and name match.
func (m *MemTable) LookupGramInverted(gram uint64) []uint32 {
	if m.inv == nil {
		return nil
	}
	return m.inv.lookup(gram)
}
