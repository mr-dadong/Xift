package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
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
	b.nameBytes += int64(len(fold)+len(name)) + 48
	if b.nameSpillBudget > 0 && b.nameBytes >= b.nameSpillBudget {
		if err := b.spillNames(); err != nil {
			return err
		}
	}

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
// spillNames sorts the buffered names and writes them to a run file so build
// memory stays bounded; finishTier2 merges the runs back in fold order.
func (b *SegmentBuilder) spillNames() error {
	sortNameEntries(b.t2names)
	path := filepath.Join(b.dir, invTempName("names-run-%03d.tmp", len(b.nameRuns)))
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	bw := bufio.NewWriterSize(f, 256*1024)
	var hdr [12]byte
	for i := range b.t2names {
		e := &b.t2names[i]
		binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(e.fold)))
		binary.LittleEndian.PutUint32(hdr[4:8], uint32(len(e.name)))
		binary.LittleEndian.PutUint32(hdr[8:12], e.localIdx)
		if _, err := bw.Write(hdr[:]); err != nil {
			_ = f.Close()
			_ = os.Remove(path)
			return err
		}
		if _, err := bw.WriteString(e.fold); err != nil {
			_ = f.Close()
			_ = os.Remove(path)
			return err
		}
		if _, err := bw.WriteString(e.name); err != nil {
			_ = f.Close()
			_ = os.Remove(path)
			return err
		}
	}
	if err := bw.Flush(); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(path)
		return err
	}
	b.nameRuns = append(b.nameRuns, path)
	b.t2names = b.t2names[:0]
	b.nameBytes = 0
	return nil
}

func invTempName(pattern string, n int) string {
	return fmt.Sprintf(pattern, n)
}

// sortNameEntries orders entries by (fold, localIdx), matching name-block order.
func sortNameEntries(entries []t2NameEntry) {
	slices.SortStableFunc(entries, func(a, c t2NameEntry) int {
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
}

func (b *SegmentBuilder) finishTier2() error {
	if len(b.nameRuns) > 0 {
		return b.finishTier2Spilled()
	}
	sortNameEntries(b.t2names)
	return b.writeTier2Tail()
}

// writeTier2Tail emits names and inverted indexes once names are in order.
func (b *SegmentBuilder) writeTier2Tail() error {
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
	b.t2names = nil
	return b.finishInverted()
}

// finishInverted writes trigram.idx and bigram.idx.
func (b *SegmentBuilder) finishInverted() error {
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
	b.invTri = nil
	b.invBi = nil
	return nil
}

// nameRunCursor streams one spilled name run in ascending (fold, localIdx) order.
type nameRunCursor struct {
	f        *os.File
	r        *bufio.Reader
	fold     string
	name     string
	localIdx uint32
	ok       bool
}

func openNameRun(path string) (*nameRunCursor, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	c := &nameRunCursor{f: f, r: bufio.NewReaderSize(f, 256*1024)}
	if err := c.advance(); err != nil {
		f.Close()
		return nil, err
	}
	return c, nil
}

func (c *nameRunCursor) advance() error {
	var hdr [12]byte
	if _, err := io.ReadFull(c.r, hdr[:]); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			c.ok = false
			return nil
		}
		return err
	}
	foldLen := int(binary.LittleEndian.Uint32(hdr[0:4]))
	nameLen := int(binary.LittleEndian.Uint32(hdr[4:8]))
	c.localIdx = binary.LittleEndian.Uint32(hdr[8:12])
	buf := make([]byte, foldLen+nameLen)
	if _, err := io.ReadFull(c.r, buf); err != nil {
		return err
	}
	c.fold = string(buf[:foldLen])
	c.name = string(buf[foldLen:])
	c.ok = true
	return nil
}

// finishTier2Spilled merges name runs with any remaining buffered names and
// writes the name blocks without ever holding the full name list in memory.
func (b *SegmentBuilder) finishTier2Spilled() error {
	defer func() {
		for _, p := range b.nameRuns {
			_ = os.Remove(p)
		}
		b.nameRuns = nil
	}()

	var cursors []*nameRunCursor
	defer func() {
		for _, c := range cursors {
			_ = c.f.Close()
		}
	}()
	for _, p := range b.nameRuns {
		c, err := openNameRun(p)
		if err != nil {
			return err
		}
		cursors = append(cursors, c)
	}

	sortNameEntries(b.t2names)
	liveIdx := 0

	nbw, err := lsm.NewNameBlocksWriter(filepath.Join(b.dir, "names.blk"), filepath.Join(b.dir, "names.idx"))
	if err != nil {
		return err
	}
	less := func(f1 string, i1 uint32, f2 string, i2 uint32) bool {
		if f1 != f2 {
			return f1 < f2
		}
		return i1 < i2
	}
	for {
		var (
			bestFold string
			bestIdx  uint32
			bestCur  *nameRunCursor
			found    bool
		)
		if liveIdx < len(b.t2names) {
			e := &b.t2names[liveIdx]
			bestFold, bestIdx, found = e.fold, e.localIdx, true
		}
		for _, c := range cursors {
			if !c.ok {
				continue
			}
			if !found || less(c.fold, c.localIdx, bestFold, bestIdx) {
				bestFold, bestIdx, bestCur, found = c.fold, c.localIdx, c, true
			}
		}
		if !found {
			break
		}
		name := ""
		if bestCur != nil {
			name = bestCur.name
			if err := bestCur.advance(); err != nil {
				nbw.Abort()
				return err
			}
		} else {
			name = b.t2names[liveIdx].name
			liveIdx++
		}
		if _, _, err := nbw.Append([]byte(name), bestIdx); err != nil {
			nbw.Abort()
			return err
		}
	}
	if err := nbw.Finish(); err != nil {
		nbw.Abort()
		return err
	}
	b.t2names = nil
	return b.finishInverted()
}

// Tier2Inverted exposes the segment's trigram and bigram inverted indexes for
// Tier 2 query routing. Both are nil for Tier 1 segments.
func (s *Segment) Tier2Inverted() (*lsm.InvertedIndex, *lsm.InvertedIndex) {
	return s.triIdx, s.biIdx
}

// IsTier2 reports whether this segment carries Tier 2 inverted sidecars.
func (s *Segment) IsTier2() bool { return s.tier2 }
