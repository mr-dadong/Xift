package lsm

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

const (
	// nameBlockMaxBytes is the decoded-byte budget for one name block
	// (offsets table + concatenated names).
	nameBlockMaxBytes = 4096
	// nameBlockSlotBytes is the fixed on-disk slot occupied by every block:
	// [compLen u32][crc32c u32][payload][padding]. Fixed slots make block
	// offsets computable (blockID * slot) without a resident offset table.
	nameBlockSlotBytes = 4096
	// nameBlockHdrSize is the names.blk file header size:
	// magic(8) + version(4) + blockCount(4).
	nameBlockHdrSize = 16
	// namesIdxHdrSize is the names.idx file header size:
	// magic(8) + version(4) + entryCount(4).
	namesIdxHdrSize = 16

	nameBlocksMagic = "XIFTNBK1"
	nameIdxMagic    = "XIFTNID1"
	nameIdxVersion  = uint32(1)
)

var (
	ErrNameBlockFormat = errors.New("lsm: corrupt name block")
	ErrNameTooLong     = fmt.Errorf("lsm: name exceeds block budget (%d bytes)", nameBlockMaxBytes)

	crc32cTable = crc32.MakeTable(crc32.Castagnoli)
)

func lsmCRC32C(data []byte) uint32 {
	return crc32.Checksum(data, crc32cTable)
}

// nameBlocksScratch incrementally front-codes one block of names. Names SHOULD
// be appended in sorted order to maximize shared-prefix compression; the
// encoding itself is correct for any order.
//
// Encoded layout: [entryCount u16] then per entry [sharedLen u16][suffixLen
// u16][suffix bytes] where sharedLen is the prefix shared with the previous
// entry in the same block (0 for the first entry).
type nameBlocksScratch struct {
	enc     []byte
	nameSum int
	count   int
	prev    []byte
	prevBuf []byte
}

// decodedBytes returns the decoded footprint of entries accumulated so far:
// 4*(count+1) offset bytes plus concatenated name bytes.
func (s *nameBlocksScratch) decodedBytes() int {
	return 4*(s.count+1) + s.nameSum
}

func (s *nameBlocksScratch) empty() bool { return s.count == 0 }

// append adds one name, returning its entry index within the block.
func (s *nameBlocksScratch) append(name []byte) (int, error) {
	if s.count == 0 && 12+len(name) > nameBlockMaxBytes {
		return 0, ErrNameTooLong
	}
	if s.enc == nil {
		// Reserve the entryCount u16 prefix; patched by finish().
		s.enc = make([]byte, 2)
	}
	shared := 0
	if s.count > 0 {
		shared = commonPrefixLen(s.prev, name)
	}
	suffix := name[shared:]

	s.enc = binary.LittleEndian.AppendUint16(s.enc, uint16(shared))
	s.enc = binary.LittleEndian.AppendUint16(s.enc, uint16(len(suffix)))
	s.enc = append(s.enc, suffix...)

	s.prevBuf = append(s.prevBuf[:0], name...)
	s.prev = s.prevBuf
	s.nameSum += len(name)
	s.count++
	return s.count - 1, nil
}

// finish returns the encoded block payload and resets the scratch.
func (s *nameBlocksScratch) finish() []byte {
	enc := s.enc
	if enc != nil && s.count > 0 {
		binary.LittleEndian.PutUint16(enc[0:2], uint16(s.count))
	}
	s.enc = nil
	s.nameSum = 0
	s.count = 0
	s.prev = nil
	return enc
}

// encodeNameBlockRaw is a convenience wrapper front-coding a full name list.
func encodeNameBlockRaw(names [][]byte) []byte {
	var s nameBlocksScratch
	for _, n := range names {
		_, _ = s.append(n)
	}
	return s.finish()
}

func commonPrefixLen(a, b []byte) int {
	n := min(len(a), len(b))
	i := 0
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

// decodeNameBlock materializes an encoded block payload into a flat decoded
// block:
//
//	[entryCount u32][offsets (count+1) u32][concatenated name bytes]
//
// Name i spans decoded[hdr+off[i] : hdr+off[i+1]] where hdr = 4+4*(count+1).
func decodeNameBlock(enc []byte) ([]byte, error) {
	if len(enc) < 2 {
		return nil, ErrNameBlockFormat
	}
	count := int(binary.LittleEndian.Uint16(enc))
	if count == 0 {
		return nil, ErrNameBlockFormat
	}

	// Pass 1: validate entries and total name bytes.
	pos := 2
	prevLen := 0
	total := 0
	for i := 0; i < count; i++ {
		if len(enc)-pos < 4 {
			return nil, ErrNameBlockFormat
		}
		shared := int(binary.LittleEndian.Uint16(enc[pos:]))
		suffix := int(binary.LittleEndian.Uint16(enc[pos+2:]))
		pos += 4
		if i == 0 && shared != 0 {
			return nil, ErrNameBlockFormat
		}
		if shared > prevLen {
			return nil, ErrNameBlockFormat
		}
		if suffix < 0 || pos+suffix > len(enc) {
			return nil, ErrNameBlockFormat
		}
		pos += suffix
		prevLen = shared + suffix
		total += prevLen
	}
	if pos != len(enc) {
		return nil, ErrNameBlockFormat
	}

	// Pass 2: materialize names and offsets.
	dec := make([]byte, 4+4*(count+1)+total)
	binary.LittleEndian.PutUint32(dec, uint32(count))
	namesBase := 4 + 4*(count+1)

	pos = 2
	prevStart := 0 // start of the previous name within the names region
	writePos := 0  // end of the names written so far
	for i := 0; i < count; i++ {
		shared := int(binary.LittleEndian.Uint16(enc[pos:]))
		suffix := int(binary.LittleEndian.Uint16(enc[pos+2:]))
		pos += 4

		startI := writePos // name i starts where the names written so far end
		// Shared prefix bytes live at the start of the PREVIOUS name.
		copy(dec[namesBase+startI:namesBase+startI+shared], dec[namesBase+prevStart:namesBase+prevStart+shared])
		writePos += shared
		// Suffix bytes follow inline in the encoded payload.
		copy(dec[namesBase+writePos:], enc[pos:pos+suffix])
		pos += suffix
		writePos += suffix

		binary.LittleEndian.PutUint32(dec[4+4*i:], uint32(startI))
		prevStart = startI
	}
	binary.LittleEndian.PutUint32(dec[4+4*count:], uint32(total))
	return dec, nil
}

// decodedNameCount returns the entry count of a decoded block.
func decodedNameCount(dec []byte) int {
	if len(dec) < 4 {
		return 0
	}
	return int(binary.LittleEndian.Uint32(dec))
}

// decodedNameAt returns the name bytes of entry i within a decoded block.
func decodedNameAt(dec []byte, i int) []byte {
	count := decodedNameCount(dec)
	if i < 0 || i >= count {
		return nil
	}
	namesBase := 4 + 4*(count+1)
	start := namesBase + int(binary.LittleEndian.Uint32(dec[4+4*i:]))
	end := namesBase + int(binary.LittleEndian.Uint32(dec[4+4*(i+1):]))
	if start < namesBase || end < start || end > len(dec) {
		return nil
	}
	return dec[start:end]
}

// NameBlocksWriter streams front-coded name blocks and a parallel
// localIdx -> (blockID, entryIdx) index to names.blk / names.idx.
type NameBlocksWriter struct {
	blkPath string
	idxPath string
	blkFile *os.File
	idxFile *os.File
	blkBuf  *bufio.Writer
	idxBuf  *bufio.Writer

	blocks  uint32
	entries uint32
	scratch nameBlocksScratch

	// idxEntries[i] = (blockID, entryIdx) for localIdx i, recorded by the
	// caller out of emission order (names stream in fold-sorted order).
	idxEntries []nameIdxRecord
	idxFilled  []bool
}

type nameIdxRecord struct {
	blockID  uint32
	entryIdx uint32
}

// NewNameBlocksWriter creates names.blk and names.idx inside dir.
func NewNameBlocksWriter(blkPath, idxPath string) (*NameBlocksWriter, error) {
	w := &NameBlocksWriter{blkPath: blkPath, idxPath: idxPath}

	var err error
	if w.blkFile, err = os.Create(blkPath); err != nil {
		return nil, fmt.Errorf("create names.blk: %w", err)
	}
	if w.idxFile, err = os.Create(idxPath); err != nil {
		w.blkFile.Close()
		_ = os.Remove(blkPath)
		return nil, fmt.Errorf("create names.idx: %w", err)
	}
	w.blkBuf = bufio.NewWriterSize(w.blkFile, 64*1024)
	w.idxBuf = bufio.NewWriterSize(w.idxFile, 64*1024)

	// Placeholder headers patched with real counts at Finish.
	var hdr [nameBlockHdrSize]byte
	if _, err := w.blkBuf.Write(hdr[:]); err != nil {
		w.Abort()
		return nil, err
	}
	var idxHdr [namesIdxHdrSize]byte
	if _, err := w.idxBuf.Write(idxHdr[:]); err != nil {
		w.Abort()
		return nil, err
	}
	return w, nil
}

// Append adds one name associated with localIdx and returns its (blockID,
// entryIdx) location in the front-coded block stream. Names may arrive in any
// order (typically fold-sorted); localIdx keys the names.idx lookup table.
func (w *NameBlocksWriter) Append(name []byte, localIdx uint32) (blockID uint32, entryIdx uint32, err error) {
	need := w.scratch.decodedBytes() + 4 + len(name)
	if need > nameBlockMaxBytes && !w.scratch.empty() {
		if err := w.flushBlock(); err != nil {
			return 0, 0, err
		}
	}
	idx, err := w.scratch.append(name)
	if err != nil {
		return 0, 0, err
	}
	blockID = w.blocks
	entryIdx = uint32(idx)

	if int(localIdx) >= len(w.idxFilled) {
		grow := int(localIdx) + 1
		w.idxEntries = append(w.idxEntries, make([]nameIdxRecord, grow-len(w.idxEntries))...)
		w.idxFilled = append(w.idxFilled, make([]bool, grow-len(w.idxFilled))...)
	}
	if w.idxFilled[localIdx] {
		return 0, 0, fmt.Errorf("lsm: duplicate names.idx localIdx %d", localIdx)
	}
	w.idxEntries[localIdx] = nameIdxRecord{blockID: blockID, entryIdx: entryIdx}
	w.idxFilled[localIdx] = true
	w.entries++
	return blockID, entryIdx, nil
}

func (w *NameBlocksWriter) flushBlock() error {
	if w.scratch.empty() {
		return nil
	}
	enc := w.scratch.finish()
	if len(enc) > nameBlockSlotBytes-8 {
		// Cannot happen with the decoded budget, guard against regressions.
		return fmt.Errorf("lsm: encoded block %d exceeds slot budget", len(enc))
	}
	var slot [nameBlockSlotBytes]byte
	binary.LittleEndian.PutUint32(slot[0:], uint32(len(enc)))
	binary.LittleEndian.PutUint32(slot[4:], lsmCRC32C(enc))
	copy(slot[8:], enc)
	if _, err := w.blkBuf.Write(slot[:]); err != nil {
		return err
	}
	w.blocks++
	return nil
}

// Finish flushes the final block, patches both headers, syncs to disk, and
// closes both file handles. The writer is unusable afterwards.
// Finish seals both files. Any error also closes and removes the partial
// outputs so callers never leak handles on a failed segment build.
func (w *NameBlocksWriter) Finish() error {
	if err := w.finish(); err != nil {
		w.Abort()
		return err
	}
	return nil
}

func (w *NameBlocksWriter) finish() error {
	if err := w.flushBlock(); err != nil {
		return err
	}
	if err := w.blkBuf.Flush(); err != nil {
		return err
	}
	for i := range w.idxEntries {
		if !w.idxFilled[i] {
			return fmt.Errorf("lsm: names.idx hole at localIdx %d", i)
		}
		var entry [8]byte
		binary.LittleEndian.PutUint32(entry[0:], w.idxEntries[i].blockID)
		binary.LittleEndian.PutUint32(entry[4:], w.idxEntries[i].entryIdx)
		if _, err := w.idxBuf.Write(entry[:]); err != nil {
			return err
		}
	}
	if err := w.idxBuf.Flush(); err != nil {
		return err
	}
	var hdr [nameBlockHdrSize]byte
	copy(hdr[0:], nameBlocksMagic)
	binary.LittleEndian.PutUint32(hdr[8:], nameIdxVersion)
	binary.LittleEndian.PutUint32(hdr[12:], w.blocks)
	if _, err := w.blkFile.WriteAt(hdr[:], 0); err != nil {
		return err
	}
	var idxHdr [namesIdxHdrSize]byte
	copy(idxHdr[0:], nameIdxMagic)
	binary.LittleEndian.PutUint32(idxHdr[8:], nameIdxVersion)
	binary.LittleEndian.PutUint32(idxHdr[12:], w.entries)
	if _, err := w.idxFile.WriteAt(idxHdr[:], 0); err != nil {
		return err
	}
	if err := w.blkFile.Sync(); err != nil {
		return err
	}
	if err := w.idxFile.Sync(); err != nil {
		return err
	}
	firstErr := w.blkFile.Close()
	w.blkFile = nil
	if err := w.idxFile.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	w.idxFile = nil
	return firstErr
}

// Abort discards partially written files.
func (w *NameBlocksWriter) Abort() {
	if w.blkFile != nil {
		w.blkFile.Close()
		_ = os.Remove(w.blkPath)
	}
	if w.idxFile != nil {
		w.idxFile.Close()
		_ = os.Remove(w.idxPath)
	}
	w.blkFile = nil
	w.idxFile = nil
}

// NameBlocksReader provides point reads over names.blk / names.idx. When a
// cache is configured, decoded name blocks and index-entry chunks are served
// through the shared fixed-budget BlockCache; returned byte slices are shared
// and MUST NOT be mutated.
type NameBlocksReader struct {
	blkFile *os.File
	idxFile *os.File
	blocks  uint32
	entries uint32
	cache   *BlockCache
	segID   uint32
}

// OpenNameBlocksReader opens and validates names.blk and names.idx. A nil
// cache disables caching.
func OpenNameBlocksReader(blkPath, idxPath string, segID uint32, cache *BlockCache) (*NameBlocksReader, error) {
	blkFile, err := os.Open(blkPath)
	if err != nil {
		return nil, err
	}
	idxFile, err := os.Open(idxPath)
	if err != nil {
		blkFile.Close()
		return nil, err
	}
	r := &NameBlocksReader{blkFile: blkFile, idxFile: idxFile, cache: cache, segID: segID}

	var hdr [nameBlockHdrSize]byte
	if _, err := io.ReadFull(blkFile, hdr[:]); err != nil {
		r.Close()
		return nil, fmt.Errorf("read names.blk header: %w", err)
	}
	if string(hdr[0:8]) != nameBlocksMagic {
		r.Close()
		return nil, ErrNameBlockFormat
	}
	r.blocks = binary.LittleEndian.Uint32(hdr[12:])

	var idxHdr [namesIdxHdrSize]byte
	if _, err := io.ReadFull(idxFile, idxHdr[:]); err != nil {
		r.Close()
		return nil, fmt.Errorf("read names.idx header: %w", err)
	}
	if string(idxHdr[0:8]) != nameIdxMagic {
		r.Close()
		return nil, ErrNameBlockFormat
	}
	r.entries = binary.LittleEndian.Uint32(idxHdr[12:])
	return r, nil
}

func (r *NameBlocksReader) blockCount() uint32 { return r.blocks }
func (r *NameBlocksReader) entryCount() uint32 { return r.entries }

// readBlock loads, CRC-verifies, and decodes block blockID (cached when a
// cache is configured).
func (r *NameBlocksReader) readBlock(blockID uint32) ([]byte, error) {
	if blockID >= r.blocks {
		return nil, ErrNameBlockFormat
	}
	if r.cache != nil {
		if dec, ok := r.cache.Get(r.segID, BlockKindNames, uint64(blockID)); ok {
			return dec, nil
		}
	}
	buf := make([]byte, nameBlockSlotBytes)
	if _, err := r.blkFile.ReadAt(buf, int64(nameBlockHdrSize)+int64(blockID)*nameBlockSlotBytes); err != nil {
		return nil, err
	}
	compLen := binary.LittleEndian.Uint32(buf[0:])
	if compLen > nameBlockSlotBytes-8 {
		return nil, ErrNameBlockFormat
	}
	if lsmCRC32C(buf[8:8+compLen]) != binary.LittleEndian.Uint32(buf[4:]) {
		return nil, ErrNameBlockFormat
	}
	dec, err := decodeNameBlock(buf[8 : 8+compLen])
	if err != nil {
		return nil, err
	}
	if r.cache != nil {
		r.cache.Put(r.segID, BlockKindNames, uint64(blockID), dec)
	}
	return dec, nil
}

// NameBytesAt returns the name bytes for localIdx as a zero-copy slice into
// a (possibly cached) decoded block. The slice is shared and MUST NOT be
// mutated.
func (r *NameBlocksReader) NameBytesAt(localIdx uint32) ([]byte, error) {
	blockID, entryIdx, err := r.readIdxEntry(localIdx)
	if err != nil {
		return nil, err
	}
	dec, err := r.readBlock(blockID)
	if err != nil {
		return nil, err
	}
	name := decodedNameAt(dec, int(entryIdx))
	if name == nil {
		return nil, ErrNameBlockFormat
	}
	return name, nil
}

// ReadName returns the name stored for localIdx.
func (r *NameBlocksReader) ReadName(localIdx uint32) (string, error) {
	blockID, entryIdx, err := r.readIdxEntry(localIdx)
	if err != nil {
		return "", err
	}
	dec, err := r.readBlock(blockID)
	if err != nil {
		return "", err
	}
	name := decodedNameAt(dec, int(entryIdx))
	if name == nil {
		return "", ErrNameBlockFormat
	}
	return string(name), nil
}

// readIdxEntry reads the (blockID, entryIdx) pair for localIdx from names.idx
// (chunk-cached when a cache is configured: 512 entries per 4 KiB chunk).
func (r *NameBlocksReader) readIdxEntry(localIdx uint32) (uint32, uint32, error) {
	if localIdx >= r.entries {
		return 0, 0, ErrNameBlockFormat
	}
	chunk := uint64(localIdx / 512)
	if r.cache != nil {
		if v, ok := r.cache.Get(r.segID, BlockKindNameIdx, chunk); ok && len(v) >= 8 {
			off := int(localIdx%512) * 8
			blockID := binary.LittleEndian.Uint32(v[off:])
			entryIdx := binary.LittleEndian.Uint32(v[off+4:])
			if blockID >= r.blocks {
				return 0, 0, ErrNameBlockFormat
			}
			return blockID, entryIdx, nil
		}
	}
	var entry [8]byte
	if _, err := r.idxFile.ReadAt(entry[:], int64(namesIdxHdrSize)+int64(localIdx)*8); err != nil {
		return 0, 0, err
	}
	blockID := binary.LittleEndian.Uint32(entry[0:])
	entryIdx := binary.LittleEndian.Uint32(entry[4:])
	if r.cache != nil {
		// Load the full 4 KiB chunk so subsequent lookups hit the cache.
		chunkOff := int64(namesIdxHdrSize) + int64(chunk)*512*8
		chunkLen := int64(512 * 8)
		if max := int64(r.entries)*8 + namesIdxHdrSize - chunkOff; chunkLen > max {
			chunkLen = max
		}
		if chunkLen > 0 {
			buf := make([]byte, chunkLen)
			if _, err := r.idxFile.ReadAt(buf, chunkOff); err == nil {
				r.cache.Put(r.segID, BlockKindNameIdx, chunk, buf)
			}
		}
	}
	if blockID >= r.blocks {
		return 0, 0, ErrNameBlockFormat
	}
	return blockID, entryIdx, nil
}

// Close releases both file handles.
func (r *NameBlocksReader) Close() {
	if r.blkFile != nil {
		r.blkFile.Close()
		r.blkFile = nil
	}
	if r.idxFile != nil {
		r.idxFile.Close()
		r.idxFile = nil
	}
}
