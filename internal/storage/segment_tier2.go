package storage

import (
	"math"
	"path/filepath"
	"slices"
	"unicode"

	"github.com/mr-dadong/Xift/internal/storage/lsm"
	"github.com/mr-dadong/Xift/internal/textkey"
)

// appendTier2 feeds one appended name into the Tier 2 build state: inverted
// trigram/bigram postings (streaming, localIdx-ascending) plus the buffered
// fold-sorted name list emitted at Finish.
func (b *SegmentBuilder) appendTier2(name string, localIdx uint32) error {
	fold := foldNameRunes(name)
	b.t2names = append(b.t2names, t2NameEntry{fold: fold, localIdx: localIdx, name: name})

	b.gramBuf = textkey.AppendTrigramKeys([]byte(name), b.gramBuf[:0])
	b.gramBuf = textkey.AppendPinyinTrigramKeys([]byte(name), b.gramBuf)
	if err := b.invTri.AddName(b.gramBuf, localIdx); err != nil {
		return err
	}

	b.gramBuf = textkey.AppendBigramKeys([]byte(name), b.gramBuf[:0])
	b.gramBuf = textkey.AppendPinyinBigramKeys([]byte(name), b.gramBuf)
	if err := b.invBi.AddName(b.gramBuf, localIdx); err != nil {
		return err
	}
	return nil
}

// foldNameRunes lowercases every rune, matching textkey/query folding.
func foldNameRunes(name string) string {
	var b []rune
	for _, r := range name {
		b = append(b, unicode.ToLower(r))
	}
	return string(b)
}

// finishTier2 writes names.blk/names.idx (fold-sorted, Front-Coded),
// trigram.idx, and bigram.idx. Called from Finish before meta.hdr is written.
func (b *SegmentBuilder) finishTier2() error {
	slices.SortStableFunc(b.t2names, func(a, c t2NameEntry) int {
		if a.fold < c.fold {
			return -1
		}
		if a.fold > c.fold {
			return 1
		}
		if a.localIdx < c.localIdx {
			return -1
		}
		if a.localIdx > c.localIdx {
			return 1
		}
		return 0
	})

	nbw, err := lsm.NewNameBlocksWriter(filepath.Join(b.dir, "names.blk"), filepath.Join(b.dir, "names.idx"))
	if err != nil {
		return err
	}
	for _, e := range b.t2names {
		if _, _, err := nbw.Append([]byte(e.name), e.localIdx); err != nil {
			nbw.Abort()
			return err
		}
	}
	if err := nbw.Finish(); err != nil {
		nbw.Abort()
		return err
	}

	// Keep every trigram; cut bigrams above the 2% document-frequency cutoff.
	if _, err := b.invTri.Finish(filepath.Join(b.dir, "trigram.idx"), math.MaxInt32); err != nil {
		return err
	}
	// 2% document-frequency cutoff, with a floor so small segments still keep a
	// usable prefix table for the short-word query path.
	maxBigramDF := max(int(b.count)/50+1, 64)
	if _, err := b.invBi.Finish(filepath.Join(b.dir, "bigram.idx"), maxBigramDF); err != nil {
		return err
	}
	b.t2names = nil
	b.invTri = nil
	b.invBi = nil
	return nil
}

// Tier2Inverted exposes the segment's trigram and bigram inverted indexes for
// Tier 2 query routing. Both are nil for Tier 1 segments.
func (s *Segment) Tier2Inverted() (*lsm.InvertedIndex, *lsm.InvertedIndex) {
	return s.triIdx, s.biIdx
}

// IsTier2 reports whether this segment carries Tier 2 inverted sidecars.
func (s *Segment) IsTier2() bool { return s.tier2 }
