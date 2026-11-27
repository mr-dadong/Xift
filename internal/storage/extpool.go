package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"sync"
)

const (
	extPoolMagic   = "XIFTEXT1"
	extPoolVersion = uint32(1)
	extPoolHdrSize = 8 + 4 + 4 + 4 // magic(8) + version(4) + count(4) + crc32c(4)
)

// ExtPool interns lowercase file extensions to compact uint16 IDs (1..65535; 0 means no extension).
type ExtPool struct {
	mu     sync.RWMutex
	saveMu sync.Mutex
	byExt  map[string]uint16
	byID   []string // index 0 unused (""), index id -> lowercase ext
	dirty  bool
}

// NewExtPool creates an empty extension intern pool.
func NewExtPool() *ExtPool {
	return &ExtPool{
		byExt: make(map[string]uint16, 256),
		byID:  []string{""},
	}
}

// LoadExtPool loads ext.pool from path. If path does not exist, an empty pool is returned.
func LoadExtPool(path string) (*ExtPool, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return NewExtPool(), nil
		}
		return nil, fmt.Errorf("read ext.pool: %w", err)
	}
	if len(raw) < extPoolHdrSize {
		return nil, fmt.Errorf("%w: truncated header (%d bytes)", ErrCorruptExtPool, len(raw))
	}
	if string(raw[0:8]) != extPoolMagic {
		return nil, fmt.Errorf("%w: invalid magic", ErrCorruptExtPool)
	}
	ver := binary.LittleEndian.Uint32(raw[8:12])
	if ver != extPoolVersion {
		return nil, fmt.Errorf("%w: unsupported version %d", ErrCorruptExtPool, ver)
	}
	count := binary.LittleEndian.Uint32(raw[12:16])
	expectedCRC := binary.LittleEndian.Uint32(raw[16:20])
	payload := raw[extPoolHdrSize:]
	if crc32c(payload) != expectedCRC {
		return nil, fmt.Errorf("%w: crc32c mismatch", ErrCorruptExtPool)
	}
	if count > math.MaxUint16 {
		return nil, fmt.Errorf("%w: count %d exceeds uint16", ErrCorruptExtPool, count)
	}

	pool := &ExtPool{
		byExt: make(map[string]uint16, count+64),
		byID:  make([]string, 1, count+1),
	}
	off := 0
	for i := uint32(0); i < count; i++ {
		if off+2 > len(payload) {
			return nil, fmt.Errorf("%w: truncated entry length at %d", ErrCorruptExtPool, i)
		}
		n := int(binary.LittleEndian.Uint16(payload[off : off+2]))
		off += 2
		if off+n > len(payload) {
			return nil, fmt.Errorf("%w: truncated entry bytes at %d", ErrCorruptExtPool, i)
		}
		ext := string(payload[off : off+n])
		off += n
		id := uint16(len(pool.byID))
		pool.byID = append(pool.byID, ext)
		pool.byExt[ext] = id
	}
	if off != len(payload) {
		return nil, fmt.Errorf("%w: trailing bytes (%d)", ErrCorruptExtPool, len(payload)-off)
	}
	return pool, nil
}

// Intern normalizes ext (trims leading dot, lowercases) and returns its stable uint16 ID.
func (p *ExtPool) Intern(ext string) uint16 {
	ext = strings.TrimPrefix(strings.ToLower(ext), ".")
	if ext == "" || len(ext) > math.MaxUint16 {
		return 0
	}

	p.mu.RLock()
	if id, ok := p.byExt[ext]; ok {
		p.mu.RUnlock()
		return id
	}
	p.mu.RUnlock()

	p.mu.Lock()
	defer p.mu.Unlock()
	if id, ok := p.byExt[ext]; ok {
		return id
	}
	if len(p.byID) > math.MaxUint16 {
		return 0
	}
	id := uint16(len(p.byID))
	p.byID = append(p.byID, ext)
	p.byExt[ext] = id
	p.dirty = true
	return id
}

// InternFromName extracts and interns the extension from a filename (returns 0 for directories or extensionless names).
func (p *ExtPool) InternFromName(name string, isDir bool) uint16 {
	if isDir {
		return 0
	}
	ext := ExtractLowerExt(name)
	if ext == "" {
		return 0
	}
	return p.Intern(ext)
}

// Lookup returns the interned ID for ext if present without allocating a new ID.
func (p *ExtPool) Lookup(ext string) (uint16, bool) {
	ext = strings.TrimPrefix(strings.ToLower(ext), ".")
	if ext == "" {
		return 0, false
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	id, ok := p.byExt[ext]
	return id, ok
}

// Ext returns the lowercase extension string for id, or "" if id is 0 or out of range.
func (p *ExtPool) Ext(id uint16) string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if int(id) >= len(p.byID) {
		return ""
	}
	return p.byID[id]
}

// Count returns the number of interned extensions (excluding 0).
func (p *ExtPool) Count() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.byID) - 1
}

// Dirty reports whether the pool has unsaved additions.
func (p *ExtPool) Dirty() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.dirty
}

// Save atomically persists the extension pool to path with CRC32C protection.
func (p *ExtPool) Save(path string) error {
	p.saveMu.Lock()
	defer p.saveMu.Unlock()

	p.mu.RLock()
	count := len(p.byID) - 1
	payloadSize := 0
	for i := 1; i < len(p.byID); i++ {
		payloadSize += 2 + len(p.byID[i])
	}
	buf := make([]byte, extPoolHdrSize+payloadSize)
	copy(buf[0:8], extPoolMagic)
	binary.LittleEndian.PutUint32(buf[8:12], extPoolVersion)
	binary.LittleEndian.PutUint32(buf[12:16], uint32(count))

	off := extPoolHdrSize
	for i := 1; i < len(p.byID); i++ {
		ext := p.byID[i]
		binary.LittleEndian.PutUint16(buf[off:off+2], uint16(len(ext)))
		off += 2
		copy(buf[off:off+len(ext)], ext)
		off += len(ext)
	}
	p.mu.RUnlock()

	binary.LittleEndian.PutUint32(buf[16:20], crc32c(buf[extPoolHdrSize:]))
	if err := atomicWriteFile(path, buf); err != nil {
		return err
	}

	p.mu.Lock()
	if len(p.byID)-1 == count {
		p.dirty = false
	}
	p.mu.Unlock()
	return nil
}
