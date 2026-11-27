package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
)

const (
	walMagic         = "XIFTWAL2"
	walFileHdrSize   = 16 // magic(8) + generation(8)
	walFrameHdrSize  = 8  // payloadLen(4) + crc32c(4)
	walMinPayloadLen = 58 // fixed fields + ReplaceFlags(1) + NameLen(2) + RootPathLen(2)
	walMaxPayloadLen = 4 * 1024 * 1024
	walMaxScratchCap = 1 * 1024 * 1024
)

// WAL is an append-only CRC32C-framed write-ahead log with generation fencing for crash-safe checkpoints.
type WAL struct {
	path        string
	file        *os.File
	generation  uint64
	syncOnWrite bool
	scratch     []byte
}

func makeWALHeader(gen uint64) [walFileHdrSize]byte {
	var hdr [walFileHdrSize]byte
	copy(hdr[0:8], walMagic)
	binary.LittleEndian.PutUint64(hdr[8:16], gen)
	return hdr
}

// OpenWAL opens or creates wal.log for expectedGen.
// If an existing WAL has a lower generation than expectedGen (crash between MANIFEST commit and WAL reset),
// stale records are skipped and wal.log is atomically reset to expectedGen.
// Torn tail bytes (truncated, zero-filled, or torn final batch) are safely truncated; mid-log corruption returns ErrCorruptWAL.
func OpenWAL(path string, expectedGen uint64, syncOnWrite bool) (*WAL, []Mutation, int, error) {
	if expectedGen == 0 {
		expectedGen = 1
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, nil, 0, fmt.Errorf("mkdir wal dir: %w", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, nil, 0, fmt.Errorf("read wal: %w", err)
	}

	w := &WAL{
		path:        path,
		generation:  expectedGen,
		syncOnWrite: syncOnWrite,
	}

	if os.IsNotExist(err) || len(raw) == 0 {
		hdr := makeWALHeader(expectedGen)
		if err := atomicWriteFile(path, hdr[:]); err != nil {
			return nil, nil, 0, fmt.Errorf("init wal header: %w", err)
		}
		f, err := os.OpenFile(path, os.O_RDWR, 0o644)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("open new wal: %w", err)
		}
		if _, err := f.Seek(walFileHdrSize, io.SeekStart); err != nil {
			_ = f.Close()
			return nil, nil, 0, err
		}
		w.file = f
		return w, nil, 0, nil
	}

	if len(raw) < walFileHdrSize || string(raw[:8]) != walMagic {
		return nil, nil, 0, fmt.Errorf("%w: invalid wal header magic", ErrCorruptWAL)
	}

	fileGen := binary.LittleEndian.Uint64(raw[8:16])
	if fileGen < expectedGen {
		// Crash occurred after MANIFEST committed new generation and before WAL reset:
		// discard stale WAL atomically without replaying already-checkpointed frames.
		hdr := makeWALHeader(expectedGen)
		if err := atomicWriteFile(path, hdr[:]); err != nil {
			return nil, nil, 0, fmt.Errorf("reset stale wal: %w", err)
		}
		f, err := os.OpenFile(path, os.O_RDWR, 0o644)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("open reset wal: %w", err)
		}
		if _, err := f.Seek(walFileHdrSize, io.SeekStart); err != nil {
			_ = f.Close()
			return nil, nil, 0, err
		}
		w.file = f
		return w, nil, 0, nil
	}
	if fileGen > expectedGen {
		return nil, nil, 0, fmt.Errorf("%w: wal generation %d ahead of manifest %d", ErrCorruptWAL, fileGen, expectedGen)
	}

	var muts []Mutation
	off := walFileHdrSize
	truncatedBytes := 0
	for off < len(raw) {
		if off+walFrameHdrSize > len(raw) {
			truncatedBytes = len(raw) - off
			break
		}
		payloadLen := binary.LittleEndian.Uint32(raw[off : off+4])
		expectedCRC := binary.LittleEndian.Uint32(raw[off+4 : off+8])

		// Handle zero-filled preallocated tail left by NTFS/ext4 crash recovery.
		if payloadLen == 0 && allZero(raw[off:]) {
			truncatedBytes = len(raw) - off
			break
		}

		frameEnd := uint64(off) + walFrameHdrSize + uint64(payloadLen)
		if payloadLen < walMinPayloadLen || payloadLen > walMaxPayloadLen || frameEnd > uint64(len(raw)) {
			if frameEnd > uint64(len(raw)) || !hasValidSubsequentFrame(raw, off+walFrameHdrSize) {
				truncatedBytes = len(raw) - off
				break
			}
			return nil, nil, 0, fmt.Errorf("%w: invalid frame payload len %d at offset %d", ErrCorruptWAL, payloadLen, off)
		}

		payload := raw[off+walFrameHdrSize : int(frameEnd)]
		if crc32c(payload) != expectedCRC {
			if int(frameEnd) == len(raw) || !hasValidFrameAt(raw, int(frameEnd)) {
				// Final frame or torn final batch without any valid committed frame after it.
				truncatedBytes = len(raw) - off
				break
			}
			// A valid committed frame follows this corrupt frame: genuine mid-log corruption.
			return nil, nil, 0, fmt.Errorf("%w: crc32c mismatch at offset %d", ErrCorruptWAL, off)
		}

		m, err := decodeMutation(payload)
		if err != nil {
			if int(frameEnd) == len(raw) || !hasValidFrameAt(raw, int(frameEnd)) {
				truncatedBytes = len(raw) - off
				break
			}
			return nil, nil, 0, fmt.Errorf("%w: decode frame at %d: %v", ErrCorruptWAL, off, err)
		}
		muts = append(muts, m)
		off = int(frameEnd)
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("reopen wal rw: %w", err)
	}
	if truncatedBytes > 0 {
		if err := f.Truncate(int64(off)); err != nil {
			_ = f.Close()
			return nil, nil, 0, fmt.Errorf("truncate torn wal tail: %w", err)
		}
		if syncOnWrite {
			if err := f.Sync(); err != nil {
				_ = f.Close()
				return nil, nil, 0, fmt.Errorf("sync truncated wal: %w", err)
			}
		}
	}
	if _, err := f.Seek(int64(off), io.SeekStart); err != nil {
		_ = f.Close()
		return nil, nil, 0, fmt.Errorf("seek wal end: %w", err)
	}
	w.file = f
	return w, muts, truncatedBytes, nil
}

func allZero(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}

func hasValidFrameAt(raw []byte, pos int) bool {
	if pos < 0 || pos+walFrameHdrSize > len(raw) {
		return false
	}
	pLen := binary.LittleEndian.Uint32(raw[pos : pos+4])
	crc := binary.LittleEndian.Uint32(raw[pos+4 : pos+8])
	if pLen < walMinPayloadLen || pLen > walMaxPayloadLen {
		return false
	}
	end := uint64(pos) + walFrameHdrSize + uint64(pLen)
	if end > uint64(len(raw)) {
		return false
	}
	payload := raw[pos+walFrameHdrSize : int(end)]
	if crc32c(payload) != crc {
		return false
	}
	_, err := decodeMutation(payload)
	return err == nil
}

func hasValidSubsequentFrame(raw []byte, start int) bool {
	return hasValidFrameAt(raw, start)
}

// Generation returns the WAL's current checkpoint generation.
func (w *WAL) Generation() uint64 {
	return w.generation
}

// AppendBatch encodes and writes all mutations as a single sequential write.
// On partial write failure, the file is rolled back to its pre-write offset.
func (w *WAL) AppendBatch(muts []Mutation) error {
	if w.file == nil {
		return ErrClosed
	}
	if len(muts) == 0 {
		return nil
	}

	totalSize := 0
	for i := range muts {
		if len(muts[i].Name) > math.MaxUint16 || len(muts[i].RootPath) > math.MaxUint16 {
			return fmt.Errorf("wal string field exceeds uint16 max")
		}
		totalSize += walFrameHdrSize + walMinPayloadLen + len(muts[i].Name) + len(muts[i].RootPath)
	}
	if cap(w.scratch) < totalSize {
		w.scratch = make([]byte, totalSize)
	}
	buf := w.scratch[:totalSize]

	off := 0
	for i := range muts {
		m := muts[i]
		pLen := walMinPayloadLen + len(m.Name) + len(m.RootPath)
		payload := buf[off+walFrameHdrSize : off+walFrameHdrSize+pLen]
		encodeMutation(payload, m)
		binary.LittleEndian.PutUint32(buf[off:off+4], uint32(pLen))
		binary.LittleEndian.PutUint32(buf[off+4:off+8], crc32c(payload))
		off += walFrameHdrSize + pLen
	}

	startOff, seekErr := w.file.Seek(0, io.SeekCurrent)
	if _, err := w.file.Write(buf); err != nil {
		if seekErr == nil {
			_ = w.file.Truncate(startOff)
			_, _ = w.file.Seek(startOff, io.SeekStart)
		}
		return fmt.Errorf("write wal batch: %w", err)
	}
	if w.syncOnWrite {
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("sync wal batch: %w", err)
		}
	}
	return nil
}

// Reset atomically replaces wal.log with a fresh header stamped with newGen.
func (w *WAL) Reset(newGen uint64) error {
	if w.file == nil {
		return ErrClosed
	}
	if err := w.file.Close(); err != nil {
		w.file = nil
		return fmt.Errorf("close wal for reset: %w", err)
	}
	w.file = nil

	hdr := makeWALHeader(newGen)
	if err := atomicWriteFile(w.path, hdr[:]); err != nil {
		return fmt.Errorf("atomic reset wal: %w", err)
	}
	f, err := os.OpenFile(w.path, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("reopen reset wal: %w", err)
	}
	if _, err := f.Seek(walFileHdrSize, io.SeekStart); err != nil {
		_ = f.Close()
		return fmt.Errorf("seek reset wal: %w", err)
	}
	w.file = f
	w.generation = newGen
	if cap(w.scratch) > walMaxScratchCap {
		w.scratch = nil
	}
	return nil
}

// Close closes the WAL file handle.
func (w *WAL) Close() error {
	if w == nil || w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

func encodeMutation(dst []byte, m Mutation) {
	dst[0] = byte(m.Op)
	binary.LittleEndian.PutUint16(dst[1:3], m.VolID)
	binary.LittleEndian.PutUint16(dst[3:5], m.Flags)
	if m.ReplaceFlags {
		dst[5] = 1
	} else {
		dst[5] = 0
	}
	binary.LittleEndian.PutUint64(dst[6:14], m.NodeKey)
	binary.LittleEndian.PutUint64(dst[14:22], m.ParentKey)
	binary.LittleEndian.PutUint64(dst[22:30], m.NewParentKey)
	binary.LittleEndian.PutUint64(dst[30:38], uint64(m.Size))
	binary.LittleEndian.PutUint64(dst[38:46], uint64(m.ModTime))
	binary.LittleEndian.PutUint64(dst[46:54], m.Cursor)
	binary.LittleEndian.PutUint16(dst[54:56], uint16(len(m.Name)))
	off := 56
	copy(dst[off:off+len(m.Name)], m.Name)
	off += len(m.Name)
	binary.LittleEndian.PutUint16(dst[off:off+2], uint16(len(m.RootPath)))
	off += 2
	copy(dst[off:off+len(m.RootPath)], m.RootPath)
}

func decodeMutation(src []byte) (Mutation, error) {
	if len(src) < walMinPayloadLen {
		return Mutation{}, fmt.Errorf("payload too short: %d", len(src))
	}
	op := MutationOp(src[0])
	if op < OpCreate || op > OpCursorUpdate {
		return Mutation{}, fmt.Errorf("invalid mutation op %d", op)
	}
	m := Mutation{
		Op:           op,
		VolID:        binary.LittleEndian.Uint16(src[1:3]),
		Flags:        binary.LittleEndian.Uint16(src[3:5]),
		ReplaceFlags: src[5] != 0,
		NodeKey:      binary.LittleEndian.Uint64(src[6:14]),
		ParentKey:    binary.LittleEndian.Uint64(src[14:22]),
		NewParentKey: binary.LittleEndian.Uint64(src[22:30]),
		Size:         int64(binary.LittleEndian.Uint64(src[30:38])),
		ModTime:      int64(binary.LittleEndian.Uint64(src[38:46])),
		Cursor:       binary.LittleEndian.Uint64(src[46:54]),
	}
	nameLen := int(binary.LittleEndian.Uint16(src[54:56]))
	off := 56
	if off+nameLen+2 > len(src) {
		return Mutation{}, fmt.Errorf("truncated name field")
	}
	m.Name = string(src[off : off+nameLen])
	off += nameLen
	rpLen := int(binary.LittleEndian.Uint16(src[off : off+2]))
	off += 2
	if off+rpLen != len(src) {
		return Mutation{}, fmt.Errorf("root path length mismatch")
	}
	m.RootPath = string(src[off : off+rpLen])
	return m, nil
}
