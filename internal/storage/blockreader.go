package storage

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/mr-dadong/Xift/internal/storage/lsm"
)

// blockBytes is the fixed block size used for Tier 2 record-block reads.
const blockBytes = 4096

// blockFileReader reads fixed-size records from a Tier 2 column file
// (hot.col / keys.idx / nodeids.col) through the shared block cache. Record
// sizes must divide blockBytes so records never straddle blocks.
type blockFileReader struct {
	f        *os.File
	cache    *lsm.BlockCache
	segID    uint32
	kind     lsm.BlockKind
	recSize  int
	recsPer  int
	fileSize int64
}

// newBlockFileReader opens path and validates that its size is a multiple of
// recSize. A nil cache disables caching (direct reads).
func newBlockFileReader(path string, segID uint32, kind lsm.BlockKind, recSize int, cache *lsm.BlockCache) (*blockFileReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if fi.Size()%int64(recSize) != 0 {
		f.Close()
		return nil, fmt.Errorf("%w: %s size %d not a multiple of %d", ErrCorruptSegment, path, fi.Size(), recSize)
	}
	return &blockFileReader{
		f:        f,
		cache:    cache,
		segID:    segID,
		kind:     kind,
		recSize:  recSize,
		recsPer:  blockBytes / recSize,
		fileSize: fi.Size(),
	}, nil
}

// recordCount returns the number of records in the file.
func (r *blockFileReader) recordCount() int { return int(r.fileSize) / r.recSize }

// close releases the file handle.
func (r *blockFileReader) close() {
	if r.f != nil {
		r.f.Close()
		r.f = nil
	}
}

// block returns the cached-or-read block containing record i, together with
// the byte offset of record i within that block. The returned slice is shared
// and MUST NOT be mutated.
func (r *blockFileReader) block(i uint32) ([]byte, int, error) {
	blockID := uint64(i / uint32(r.recsPer))
	inOff := int(i%uint32(r.recsPer)) * r.recSize

	if r.cache != nil {
		if v, ok := r.cache.Get(r.segID, r.kind, blockID); ok && inOff+r.recSize <= len(v) {
			return v, inOff, nil
		}
	}
	fileOff := int64(blockID) * blockBytes
	size := blockBytes
	if rem := r.fileSize - fileOff; int64(size) > rem {
		size = int(rem)
	}
	if size <= 0 {
		return nil, 0, fmt.Errorf("%w: record %d out of range", ErrCorruptSegment, i)
	}
	buf := make([]byte, size)
	if _, err := r.f.ReadAt(buf, fileOff); err != nil {
		return nil, 0, err
	}
	if r.cache != nil {
		r.cache.Put(r.segID, r.kind, blockID, buf)
	}
	if inOff+r.recSize > len(buf) {
		return nil, 0, fmt.Errorf("%w: record %d beyond file end", ErrCorruptSegment, i)
	}
	return buf, inOff, nil
}

// forEachRecord invokes fn for every record in file order. fn receives the
// record bytes (shared slice; MUST NOT be mutated or retained past the call)
// and the record index. It stops when fn returns false.
func (r *blockFileReader) forEachRecord(fn func(rec []byte, idx int) bool) error {
	n := r.recordCount()
	for i := 0; i < n; {
		blockID := uint32(i / r.recsPer)
		buf, off, err := r.block(blockID * uint32(r.recsPer))
		if err != nil {
			return err
		}
		for off < len(buf) && i < n {
			if !fn(buf[off:off+r.recSize], i) {
				return nil
			}
			off += r.recSize
			i++
		}
	}
	return nil
}

// hotNodeAt decodes the HotNode at localIdx from hot.col.
func (r *blockFileReader) hotNodeAt(localIdx uint32) (HotNode, error) {
	buf, off, err := r.block(localIdx)
	if err != nil {
		return HotNode{}, err
	}
	return HotNode{
		NameOff:   binary.LittleEndian.Uint32(buf[off:]),
		ParentIdx: binary.LittleEndian.Uint32(buf[off+4:]),
		NameLen:   binary.LittleEndian.Uint16(buf[off+8:]),
		Flags:     binary.LittleEndian.Uint16(buf[off+10:]),
		ExtID:     binary.LittleEndian.Uint16(buf[off+12:]),
		VolID:     binary.LittleEndian.Uint16(buf[off+14:]),
	}, nil
}

// keyEntryAt decodes the keyIndexEntry at index i from keys.idx.
func (r *blockFileReader) keyEntryAt(i uint32) (keyIndexEntry, error) {
	buf, off, err := r.block(i)
	if err != nil {
		return keyIndexEntry{}, err
	}
	return keyIndexEntry{
		NodeKey:  binary.LittleEndian.Uint64(buf[off:]),
		LocalIdx: binary.LittleEndian.Uint32(buf[off+8:]),
		VolID:    binary.LittleEndian.Uint16(buf[off+12:]),
	}, nil
}

// nodeIDAt decodes the uint32 at index i from nodeids.col.
func (r *blockFileReader) nodeIDAt(i uint32) (uint32, error) {
	buf, off, err := r.block(i)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(buf[off:]), nil
}
