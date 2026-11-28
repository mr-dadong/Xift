package lsm

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
)

// Inverted index file layout (trigram.idx / bigram.idx):
//
//	header 36B: magic(8) version(4) gramCount(4) dictBytes(4) dfBytes(4)
//	            postingsBytes(8) crc32c(4 over header bytes 0..32)
//	grams region:     gramCount × u64 (strictly ascending gram keys)
//	dfs region:       gramCount × u32 (document frequency per gram)
//	offsets region:   gramCount × u64 (posting record start, relative to the
//	                  postings region; gramCount-th sentinel == postingsBytes)
//	postings region:  per gram: [crc32c u32][Roaring portable stream]
//
// Dictionary, df, and offset arrays are never resident: readers binary-search
// the grams region through 4 KiB cache blocks, so per-segment metadata stays
// bounded regardless of gram count.
const (
	invertedMagic    = "XIFTINV1"
	invertedVersion  = uint32(1)
	invertedHdrSize  = 36
	invertedGramsPer = 512  // u64 keys per 4 KiB dictionary block
	invertedDfsPer   = 1024 // u32 dfs per 4 KiB df block
	invertedOffsPer  = 512  // u64 offsets per 4 KiB offsets block
)

// ErrInvertedFormat reports a structurally invalid or corrupt inverted file.
var ErrInvertedFormat = errors.New("lsm: corrupt inverted index")

// invBlockID composes a cache blockID for inverted-file regions:
// file(4b) << 60 | region(2b) << 58 | block index.
func invBlockID(fileID, region, block uint64) uint64 {
	return fileID<<60 | region<<58 | block
}

// InvertedStats summarizes a finished inverted index file.
type InvertedStats struct {
	GramCount     int
	PostingsBytes int64
	MaxDF         uint32
}

// InvertedBuilder accumulates per-gram posting lists in ascending localIdx
// order and serializes them to an inverted index file.
//
// AddName MUST be called with strictly ascending localIdx values (the segment
// build order guarantees this); each call contributes one name's deduplicated
// gram keys.
type InvertedBuilder struct {
	postings map[uint64]*RoaringBuilder
	lastIdx  uint32
	hasLast  bool

	// Optional spilling: when the estimated resident posting bytes exceed the
	// budget, completed containers are written to sorted run files and merged
	// back at Finish, keeping build memory bounded.
	spillDir string
	budget   int64
	estBytes int64
	runs     []string
	runSeq   uint64
}

// invBuilderSeq gives each builder a unique run-file prefix so trigram and
// bigram spilling into one segment directory never collide.
var invBuilderSeq atomic.Uint64

// NewInvertedBuilder creates an empty builder.
func NewInvertedBuilder() *InvertedBuilder {
	return &InvertedBuilder{postings: make(map[uint64]*RoaringBuilder, 1024), runSeq: invBuilderSeq.Add(1)}
}

// AddName contributes one name's gram keys for localIdx.
func (b *InvertedBuilder) AddName(keys []uint64, localIdx uint32) error {
	if b.hasLast && localIdx <= b.lastIdx {
		return fmt.Errorf("lsm: inverted AddName localIdx %d out of order after %d", localIdx, b.lastIdx)
	}
	sorted := slices.Clone(keys)
	slices.Sort(sorted)
	for i, k := range sorted {
		if i > 0 && k == sorted[i-1] {
			continue // dedup within one name
		}
		rb, ok := b.postings[k]
		if !ok {
			rb = &RoaringBuilder{}
			b.postings[k] = rb
		}
		if err := rb.Add(localIdx); err != nil {
			return err
		}
	}
	b.lastIdx = localIdx
	b.hasLast = true
	b.estBytes += int64(len(sorted)) * 4
	if b.budget > 0 && b.spillDir != "" && b.estBytes >= b.budget {
		return b.spill()
	}
	return nil
}

// ConfigureSpill enables posting spilling into dir once the estimated resident
// posting bytes reach budget. budget <= 0 disables spilling.
func (b *InvertedBuilder) ConfigureSpill(dir string, budget int64) {
	b.spillDir = dir
	b.budget = budget
}

// spill writes every gram's completed containers to a sorted run file.
func (b *InvertedBuilder) spill() error {
	keys := make([]uint64, 0, len(b.postings))
	for k := range b.postings {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	path := filepath.Join(b.spillDir, fmt.Sprintf("inv-%d-run-%03d.tmp", b.runSeq, len(b.runs)))
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	bw := bufio.NewWriterSize(f, 256*1024)
	var hdr [12]byte
	for _, k := range keys {
		blob := b.postings[k].Spill()
		if len(blob) == 0 {
			continue
		}
		binary.LittleEndian.PutUint64(hdr[0:8], k)
		binary.LittleEndian.PutUint32(hdr[8:12], uint32(len(blob)))
		if _, err := bw.Write(hdr[:]); err != nil {
			_ = f.Close()
			_ = os.Remove(path)
			return err
		}
		if _, err := bw.Write(blob); err != nil {
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
	b.runs = append(b.runs, path)
	b.estBytes = 0
	return nil
}

func (b *InvertedBuilder) removeRuns() {
	for _, p := range b.runs {
		_ = os.Remove(p)
	}
	b.runs = nil
}

// Finish serializes the index to path. Grams whose document frequency exceeds
// maxDF are excluded (bigram high-frequency cutoff); pass math.MaxInt32 to
// keep everything.
func (b *InvertedBuilder) Finish(path string, maxDF int) (InvertedStats, error) {
	if len(b.runs) > 0 {
		return b.finishSpilled(path, maxDF)
	}
	keys := make([]uint64, 0, len(b.postings))
	for k := range b.postings {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	// Serialize every posting and collect sizes.
	type gramEntry struct {
		key   uint64
		buf   []byte
		card  uint64
		start uint64
	}
	entries := make([]gramEntry, 0, len(keys))
	var postingsBytes uint64
	maxKeptDF := uint32(0)
	for _, k := range keys {
		rb := b.postings[k]
		buf, card := rb.Finish()
		if int(card) > maxDF {
			// Keep a zero-cardinality dictionary sentinel: readers can then
			// distinguish "dropped by the df budget" (df == 0) from "no name in
			// this segment has the gram" (key absent), which the short-word
			// query path needs to decide between truncation and a full scan.
			var empty RoaringBuilder
			buf, card = empty.Finish()
		} else if uint32(card) > maxKeptDF {
			maxKeptDF = uint32(card)
		}
		entries = append(entries, gramEntry{key: k, buf: buf, card: card, start: postingsBytes})
		postingsBytes += uint64(4 + len(buf)) // crc + stream
	}

	f, err := os.Create(path)
	if err != nil {
		return InvertedStats{}, err
	}
	defer func() {
		if f != nil {
			f.Close()
			_ = os.Remove(path)
		}
	}()

	var hdr [invertedHdrSize]byte
	copy(hdr[0:], invertedMagic)
	binary.LittleEndian.PutUint32(hdr[8:], invertedVersion)
	binary.LittleEndian.PutUint32(hdr[12:], uint32(len(entries)))
	binary.LittleEndian.PutUint32(hdr[16:], uint32(len(entries)*8))
	binary.LittleEndian.PutUint32(hdr[20:], uint32(len(entries)*4))
	binary.LittleEndian.PutUint64(hdr[24:], postingsBytes)
	binary.LittleEndian.PutUint32(hdr[32:], lsmCRC32C(hdr[:32]))
	if _, err := f.Write(hdr[:]); err != nil {
		return InvertedStats{}, err
	}

	var u64 [8]byte
	for _, e := range entries {
		binary.LittleEndian.PutUint64(u64[:], e.key)
		if _, err := f.Write(u64[:]); err != nil {
			return InvertedStats{}, err
		}
	}
	var u32 [4]byte
	for _, e := range entries {
		binary.LittleEndian.PutUint32(u32[:], uint32(e.card))
		if _, err := f.Write(u32[:]); err != nil {
			return InvertedStats{}, err
		}
	}
	for _, e := range entries {
		binary.LittleEndian.PutUint64(u64[:], e.start)
		if _, err := f.Write(u64[:]); err != nil {
			return InvertedStats{}, err
		}
	}
	// Sentinel offset after the last gram: end of the postings region.
	binary.LittleEndian.PutUint64(u64[:], postingsBytes)
	if _, err := f.Write(u64[:]); err != nil {
		return InvertedStats{}, err
	}
	for _, e := range entries {
		binary.LittleEndian.PutUint32(u32[:], lsmCRC32C(e.buf))
		if _, err := f.Write(u32[:]); err != nil {
			return InvertedStats{}, err
		}
		if _, err := f.Write(e.buf); err != nil {
			return InvertedStats{}, err
		}
	}
	if err := f.Sync(); err != nil {
		return InvertedStats{}, err
	}
	if err := f.Close(); err != nil {
		return InvertedStats{}, err
	}
	f = nil

	b.postings = nil
	return InvertedStats{GramCount: len(entries), PostingsBytes: int64(postingsBytes), MaxDF: maxKeptDF}, nil
}

// invRun is one spilled posting run, read sequentially in ascending gram order.
type invRun struct {
	f    *os.File
	r    *bufio.Reader
	key  uint64
	blob []byte
	ok   bool
}

func openInvRun(path string) (*invRun, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	c := &invRun{f: f, r: bufio.NewReaderSize(f, 256*1024)}
	if err := c.advance(); err != nil {
		f.Close()
		return nil, err
	}
	return c, nil
}

func (c *invRun) advance() error {
	var hdr [12]byte
	if _, err := io.ReadFull(c.r, hdr[:]); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			c.ok = false
			c.blob = nil
			return nil
		}
		return err
	}
	n := binary.LittleEndian.Uint32(hdr[8:12])
	if cap(c.blob) < int(n) {
		c.blob = make([]byte, n)
	} else {
		c.blob = c.blob[:n]
	}
	if _, err := io.ReadFull(c.r, c.blob); err != nil {
		return err
	}
	c.key = binary.LittleEndian.Uint64(hdr[0:8])
	c.ok = true
	return nil
}

// finishSpilled merges the spill runs with the live postings and writes the
// same on-disk layout the in-memory path produces. Postings go to a temporary
// file first because the header needs their total size before they are copied.
func (b *InvertedBuilder) finishSpilled(path string, maxDF int) (InvertedStats, error) {
	defer b.removeRuns()

	var cursors []*invRun
	defer func() {
		for _, c := range cursors {
			_ = c.f.Close()
		}
	}()
	for _, p := range b.runs {
		c, err := openInvRun(p)
		if err != nil {
			return InvertedStats{}, err
		}
		cursors = append(cursors, c)
	}

	liveKeys := make([]uint64, 0, len(b.postings))
	for k := range b.postings {
		liveKeys = append(liveKeys, k)
	}
	slices.Sort(liveKeys)

	postPath := path + ".post"
	pf, err := os.Create(postPath)
	if err != nil {
		return InvertedStats{}, err
	}
	cleanupPost := func() {
		_ = pf.Close()
		_ = os.Remove(postPath)
	}
	bw := bufio.NewWriterSize(pf, 256*1024)

	var keys []uint64
	var cards []uint32
	var offs []uint64
	var postingsBytes uint64
	maxKeptDF := uint32(0)
	var u32 [4]byte
	li := 0
	var blob []byte

	for {
		min := ^uint64(0)
		found := false
		if li < len(liveKeys) {
			min = liveKeys[li]
			found = true
		}
		for _, c := range cursors {
			if c.ok && c.key < min {
				min = c.key
				found = true
			}
		}
		if !found {
			break
		}
		blob = blob[:0]
		for _, c := range cursors {
			if c.ok && c.key == min {
				blob = append(blob, c.blob...)
				if err := c.advance(); err != nil {
					cleanupPost()
					return InvertedStats{}, err
				}
			}
		}
		if li < len(liveKeys) && liveKeys[li] == min {
			rest, _ := b.postings[min].Finish()
			blob = append(blob, rest...)
			li++
		}

		card := uint64(0)
		if len(blob) > 0 {
			v, err := ParseRoaring(blob)
			if err != nil {
				cleanupPost()
				return InvertedStats{}, err
			}
			card = v.Cardinality()
		}
		if int(card) > maxDF {
			blob = nil
			card = 0
		} else if uint32(card) > maxKeptDF {
			maxKeptDF = uint32(card)
		}

		keys = append(keys, min)
		cards = append(cards, uint32(card))
		offs = append(offs, postingsBytes)
		binary.LittleEndian.PutUint32(u32[:], lsmCRC32C(blob))
		if _, err := bw.Write(u32[:]); err != nil {
			cleanupPost()
			return InvertedStats{}, err
		}
		if _, err := bw.Write(blob); err != nil {
			cleanupPost()
			return InvertedStats{}, err
		}
		postingsBytes += uint64(4 + len(blob))
	}
	if err := bw.Flush(); err != nil {
		cleanupPost()
		return InvertedStats{}, err
	}
	if err := pf.Close(); err != nil {
		_ = os.Remove(postPath)
		return InvertedStats{}, err
	}

	// Assemble the final file: header, dictionary, df, offsets, then postings.
	f, err := os.Create(path)
	if err != nil {
		_ = os.Remove(postPath)
		return InvertedStats{}, err
	}
	fail := func(err error) (InvertedStats, error) {
		_ = f.Close()
		_ = os.Remove(path)
		_ = os.Remove(postPath)
		return InvertedStats{}, err
	}
	var hdr [invertedHdrSize]byte
	copy(hdr[0:], invertedMagic)
	binary.LittleEndian.PutUint32(hdr[8:], invertedVersion)
	binary.LittleEndian.PutUint32(hdr[12:], uint32(len(keys)))
	binary.LittleEndian.PutUint32(hdr[16:], uint32(len(keys)*8))
	binary.LittleEndian.PutUint32(hdr[20:], uint32(len(keys)*4))
	binary.LittleEndian.PutUint64(hdr[24:], postingsBytes)
	binary.LittleEndian.PutUint32(hdr[32:], lsmCRC32C(hdr[:32]))
	if _, err := f.Write(hdr[:]); err != nil {
		return fail(err)
	}
	var u64 [8]byte
	for _, k := range keys {
		binary.LittleEndian.PutUint64(u64[:], k)
		if _, err := f.Write(u64[:]); err != nil {
			return fail(err)
		}
	}
	for _, c := range cards {
		binary.LittleEndian.PutUint32(u32[:], c)
		if _, err := f.Write(u32[:]); err != nil {
			return fail(err)
		}
	}
	for _, o := range offs {
		binary.LittleEndian.PutUint64(u64[:], o)
		if _, err := f.Write(u64[:]); err != nil {
			return fail(err)
		}
	}
	binary.LittleEndian.PutUint64(u64[:], postingsBytes)
	if _, err := f.Write(u64[:]); err != nil {
		return fail(err)
	}
	postFile, err := os.Open(postPath)
	if err != nil {
		return fail(err)
	}
	if _, err := io.Copy(f, postFile); err != nil {
		postFile.Close()
		return fail(err)
	}
	postFile.Close()
	_ = os.Remove(postPath)
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		return InvertedStats{}, err
	}

	b.postings = nil
	return InvertedStats{GramCount: len(keys), PostingsBytes: int64(postingsBytes), MaxDF: maxKeptDF}, nil
}

// InvertedIndex is a lazily-read inverted index file. Dictionary, df, and
// offset lookups stream through the shared BlockCache in 4 KiB blocks; only
// posting records are materialized.
type InvertedIndex struct {
	f            *os.File
	path         string
	cache        *BlockCache
	gramCount    int
	postingsBase int64
	fileSize     int64
}

// OpenInvertedIndex opens and validates an inverted index file. A nil cache
// disables block caching (every lookup hits the file directly).
func OpenInvertedIndex(path string, segID uint32, cache *BlockCache) (*InvertedIndex, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	var hdr [invertedHdrSize]byte
	if _, err := io.ReadFull(f, hdr[:]); err != nil {
		f.Close()
		return nil, fmt.Errorf("lsm: read inverted header: %w", err)
	}
	if string(hdr[0:8]) != invertedMagic {
		f.Close()
		return nil, ErrInvertedFormat
	}
	if binary.LittleEndian.Uint32(hdr[8:]) != invertedVersion {
		f.Close()
		return nil, ErrInvertedFormat
	}
	if lsmCRC32C(hdr[:32]) != binary.LittleEndian.Uint32(hdr[32:]) {
		f.Close()
		return nil, ErrInvertedFormat
	}
	gramCount := int(binary.LittleEndian.Uint32(hdr[12:]))
	dictBytes := int64(binary.LittleEndian.Uint32(hdr[16:]))
	dfBytes := int64(binary.LittleEndian.Uint32(hdr[20:]))
	base := int64(invertedHdrSize) + dictBytes + dfBytes + int64(gramCount+1)*8
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &InvertedIndex{
		f:            f,
		path:         path,
		cache:        cache,
		gramCount:    gramCount,
		postingsBase: base,
		fileSize:     fi.Size(),
	}, nil
}

// Close releases the file handle.
func (ix *InvertedIndex) Close() {
	if ix.f != nil {
		ix.f.Close()
		ix.f = nil
	}
}

// GramCount returns the number of grams stored in this index.
func (ix *InvertedIndex) GramCount() int { return ix.gramCount }

// readRange reads size bytes at absolute file offset, preferring the block
// cache (one block-sized read on miss). The returned slice is shared and MUST
// NOT be mutated.
func (ix *InvertedIndex) readRange(segID uint32, kind BlockKind, blockID uint64, fileOff int64, size int) ([]byte, error) {
	if remaining := ix.fileSize - fileOff; size > int(remaining) {
		size = int(remaining)
	}
	if size <= 0 {
		return nil, ErrInvertedFormat
	}
	if ix.cache != nil {
		if v, ok := ix.cache.Get(segID, kind, blockID); ok && len(v) >= size {
			return v, nil
		}
	}
	buf := make([]byte, size)
	if _, err := ix.f.ReadAt(buf, fileOff); err != nil {
		return nil, err
	}
	if ix.cache != nil {
		ix.cache.Put(segID, kind, blockID, buf)
	}
	return buf, nil
}

// readGram returns the gram key at rank i.
func (ix *InvertedIndex) readGram(segID uint32, fileID uint64, i int) (uint64, error) {
	block := i / invertedGramsPer
	off := invertedHdrSize + int64(block)*4096
	buf, err := ix.readRange(segID, BlockKindInverted, invBlockID(fileID, 0, uint64(block)), off, 4096)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(buf[8*(i%invertedGramsPer):]), nil
}

// readDf returns the document frequency at rank i.
func (ix *InvertedIndex) readDf(segID uint32, fileID uint64, i int) (uint32, error) {
	block := i / invertedDfsPer
	base := int64(invertedHdrSize) + int64(ix.gramCount)*8
	off := base + int64(block)*4096
	buf, err := ix.readRange(segID, BlockKindInverted, invBlockID(fileID, 1, uint64(block)), off, 4096)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(buf[4*(i%invertedDfsPer):]), nil
}

// readOffset returns the posting record offset (relative to the postings
// region) at rank i; the gramCount-th sentinel returns postingsBytes.
func (ix *InvertedIndex) readOffset(segID uint32, fileID uint64, i int) (uint64, error) {
	base := int64(invertedHdrSize) + int64(ix.gramCount)*12
	block := i / invertedOffsPer
	off := base + int64(block)*4096
	buf, err := ix.readRange(segID, BlockKindInverted, invBlockID(fileID, 2, uint64(block)), off, 4096)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(buf[8*(i%invertedOffsPer):]), nil
}

// rank returns the dictionary rank of gramKey, or -1 when absent.
func (ix *InvertedIndex) rank(segID uint32, fileID uint64, gramKey uint64) (int, error) {
	lo, hi := 0, ix.gramCount
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		k, err := ix.readGram(segID, fileID, mid)
		if err != nil {
			return 0, err
		}
		if k < gramKey {
			lo = mid + 1
		} else if k > gramKey {
			hi = mid
		} else {
			return mid, nil
		}
	}
	return -1, nil
}

// Df returns the document frequency of gramKey without loading its posting.
func (ix *InvertedIndex) Df(segID uint32, fileID uint64, gramKey uint64) (uint32, bool) {
	r, err := ix.rank(segID, fileID, gramKey)
	if err != nil || r < 0 {
		return 0, false
	}
	df, err := ix.readDf(segID, fileID, r)
	if err != nil {
		return 0, false
	}
	return df, true
}

// LookupPosting returns the Roaring posting stream bytes for gramKey. The
// returned slice is shared (cached) or freshly read (oversized postings) and
// MUST NOT be mutated.
func (ix *InvertedIndex) LookupPosting(segID uint32, fileID uint64, gramKey uint64) ([]byte, bool, error) {
	r, err := ix.rank(segID, fileID, gramKey)
	if err != nil {
		return nil, false, err
	}
	if r < 0 {
		return nil, false, nil
	}
	start, err := ix.readOffset(segID, fileID, r)
	if err != nil {
		return nil, false, err
	}
	end, err := ix.readOffset(segID, fileID, r+1)
	if err != nil {
		return nil, false, err
	}
	size := int(end - start) // crc(4) + roaring stream
	if size < 4 {
		return nil, false, ErrInvertedFormat
	}

	// Records within the shard budget are cached whole; giants stream direct.
	if ix.cache != nil && int64(size) <= ix.cache.limit/blockCacheShards {
		if v, ok := ix.cache.Get(segID, BlockKindInverted, invBlockID(fileID, 3, uint64(r))); ok {
			return ix.verifyRecord(v)
		}
	}
	buf := make([]byte, size)
	if _, err := ix.f.ReadAt(buf, ix.postingsBase+int64(start)); err != nil {
		return nil, false, err
	}
	if ix.cache != nil && int64(size) <= ix.cache.limit/blockCacheShards {
		ix.cache.Put(segID, BlockKindInverted, invBlockID(fileID, 3, uint64(r)), buf)
	}
	return ix.verifyRecord(buf)
}

// verifyRecord checks a posting record's CRC and strips the 4-byte prefix.
func (ix *InvertedIndex) verifyRecord(rec []byte) ([]byte, bool, error) {
	if len(rec) < 4 {
		return nil, false, ErrInvertedFormat
	}
	if lsmCRC32C(rec[4:]) != binary.LittleEndian.Uint32(rec[0:4]) {
		return nil, false, ErrInvertedFormat
	}
	return rec[4:], true, nil
}

var _ = math.MaxInt32
