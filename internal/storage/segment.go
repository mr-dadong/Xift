package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/bits"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"unsafe"
)

const (
	segMetaMagic    = "XIFTSEG1"
	segMetaVersion  = uint32(1)
	segMetaSize     = 48
	segMetaEndMagic = uint32(0x54464958) // "XIFT"

	segFlagDense uint32 = 1 << 0
)

// SegmentMeta describes an immutable on-disk columnar segment.
type SegmentMeta struct {
	ID           uint32
	BaseNodeID   uint32
	NodeCount    uint32
	NamesBytes   uint64
	Dense        bool
	TombstoneGen uint32
}

// SegmentDirName returns the canonical directory name for segID under segments/.
func SegmentDirName(segID uint32) string {
	return fmt.Sprintf("seg_%06d", segID)
}

func tombstoneFileName(gen uint32) string {
	if gen == 0 {
		return "del.bit"
	}
	return fmt.Sprintf("del_%06d.bit", gen)
}

// keyIndexEntry is a 16-byte pointer-free sorted index entry mapping (VolID, NodeKey) -> LocalIdx.
type keyIndexEntry struct {
	NodeKey  uint64
	LocalIdx uint32
	VolID    uint16
	_        uint16
}

var _ [16]byte = [unsafe.Sizeof(keyIndexEntry{})]byte{}

// SegmentBuilder streams hot and cold columns to disk for a new immutable Tier 1 segment.
type SegmentBuilder struct {
	dir        string
	segID      uint32
	baseNodeID uint32
	dense      bool
	extPool    *ExtPool
	onAbort    func()

	hotFile     *os.File
	namesFile   *os.File
	sizesFile   *os.File
	mtimesFile  *os.File
	nodeIDsFile *os.File

	hotBuf     *bufio.Writer
	namesBuf   *bufio.Writer
	sizesBuf   *bufio.Writer
	mtimesBuf  *bufio.Writer
	nodeIDsBuf *bufio.Writer

	nameOffset uint64
	count      uint32
	lastNodeID uint32
	hasLastID  bool
	keyEntries []keyIndexEntry
	dirEntries []keyIndexEntry

	hotScratch [HotNodeSize]byte
	u64Scratch [8]byte
	u32Scratch [4]byte
	closed     bool
}

// NewSegmentBuilder creates a new segment directory and opens column writers for streaming build.
func NewSegmentBuilder(segDir string, segID uint32, baseNodeID uint32, dense bool, extPool *ExtPool) (*SegmentBuilder, error) {
	if err := os.MkdirAll(segDir, 0o755); err != nil {
		return nil, fmt.Errorf("create segment dir %s: %w", segDir, err)
	}

	b := &SegmentBuilder{
		dir:        segDir,
		segID:      segID,
		baseNodeID: baseNodeID,
		dense:      dense,
		extPool:    extPool,
	}

	var err error
	if b.hotFile, err = os.Create(filepath.Join(segDir, "hot.col")); err != nil {
		b.Abort()
		return nil, fmt.Errorf("create hot.col: %w", err)
	}
	if b.namesFile, err = os.Create(filepath.Join(segDir, "names.col")); err != nil {
		b.Abort()
		return nil, fmt.Errorf("create names.col: %w", err)
	}
	if b.sizesFile, err = os.Create(filepath.Join(segDir, "sizes.col")); err != nil {
		b.Abort()
		return nil, fmt.Errorf("create sizes.col: %w", err)
	}
	if b.mtimesFile, err = os.Create(filepath.Join(segDir, "mtimes.col")); err != nil {
		b.Abort()
		return nil, fmt.Errorf("create mtimes.col: %w", err)
	}
	if !dense {
		if b.nodeIDsFile, err = os.Create(filepath.Join(segDir, "nodeids.col")); err != nil {
			b.Abort()
			return nil, fmt.Errorf("create nodeids.col: %w", err)
		}
		b.nodeIDsBuf = bufio.NewWriterSize(b.nodeIDsFile, 64*1024)
	}

	b.hotBuf = bufio.NewWriterSize(b.hotFile, 256*1024)
	b.namesBuf = bufio.NewWriterSize(b.namesFile, 256*1024)
	b.sizesBuf = bufio.NewWriterSize(b.sizesFile, 128*1024)
	b.mtimesBuf = bufio.NewWriterSize(b.mtimesFile, 128*1024)
	return b, nil
}

// Append writes a single node into hot.col, names.col, sizes.col, mtimes.col (and nodeids.col if delta).
func (b *SegmentBuilder) Append(
	nodeID uint32,
	volID uint16,
	nodeKey uint64,
	parentIdx uint32,
	name string,
	flags uint16,
	extID uint16,
	size int64,
	modTime int64,
) error {
	if b.closed {
		return ErrClosed
	}
	if b.dense {
		expected := b.baseNodeID + b.count
		if nodeID != expected {
			return fmt.Errorf("dense segment nodeID %d != expected %d", nodeID, expected)
		}
	} else if b.hasLastID && nodeID <= b.lastNodeID {
		return fmt.Errorf("delta segment nodeID %d must be strictly ascending (last %d)", nodeID, b.lastNodeID)
	}

	nameLen := len(name)
	if nameLen > math.MaxUint16 {
		return fmt.Errorf("filename length %d exceeds uint16 max", nameLen)
	}
	if b.nameOffset+uint64(nameLen) > math.MaxUint32 {
		return fmt.Errorf("names.col offset overflow (> 4 GiB)")
	}

	flags &^= FlagDeleted
	if HasNonASCII(name) {
		flags |= FlagNonASCII
	} else {
		flags &^= FlagNonASCII
	}
	if extID == 0 && b.extPool != nil && (flags&FlagDir) == 0 {
		extID = b.extPool.InternFromName(name, false)
	}

	binary.LittleEndian.PutUint32(b.hotScratch[0:4], uint32(b.nameOffset))
	binary.LittleEndian.PutUint32(b.hotScratch[4:8], parentIdx)
	binary.LittleEndian.PutUint16(b.hotScratch[8:10], uint16(nameLen))
	binary.LittleEndian.PutUint16(b.hotScratch[10:12], flags)
	binary.LittleEndian.PutUint16(b.hotScratch[12:14], extID)
	binary.LittleEndian.PutUint16(b.hotScratch[14:16], volID)

	if _, err := b.hotBuf.Write(b.hotScratch[:]); err != nil {
		return fmt.Errorf("write hot.col: %w", err)
	}
	if nameLen > 0 {
		if _, err := b.namesBuf.WriteString(name); err != nil {
			return fmt.Errorf("write names.col: %w", err)
		}
	}

	binary.LittleEndian.PutUint64(b.u64Scratch[:], uint64(size))
	if _, err := b.sizesBuf.Write(b.u64Scratch[:]); err != nil {
		return fmt.Errorf("write sizes.col: %w", err)
	}
	binary.LittleEndian.PutUint64(b.u64Scratch[:], uint64(modTime))
	if _, err := b.mtimesBuf.Write(b.u64Scratch[:]); err != nil {
		return fmt.Errorf("write mtimes.col: %w", err)
	}

	if !b.dense {
		binary.LittleEndian.PutUint32(b.u32Scratch[:], nodeID)
		if _, err := b.nodeIDsBuf.Write(b.u32Scratch[:]); err != nil {
			return fmt.Errorf("write nodeids.col: %w", err)
		}
	}

	entry := keyIndexEntry{
		NodeKey:  nodeKey,
		LocalIdx: b.count,
		VolID:    volID,
	}
	b.keyEntries = append(b.keyEntries, entry)
	if (flags & FlagDir) != 0 {
		b.dirEntries = append(b.dirEntries, entry)
	}

	b.nameOffset += uint64(nameLen)
	b.lastNodeID = nodeID
	b.hasLastID = true
	b.count++
	return nil
}

// AppendDense appends the next sequential node to a dense segment and returns its assigned NodeID.
func (b *SegmentBuilder) AppendDense(
	volID uint16,
	nodeKey uint64,
	parentIdx uint32,
	name string,
	flags uint16,
	extID uint16,
	size int64,
	modTime int64,
) (uint32, error) {
	nodeID := b.baseNodeID + b.count
	if err := b.Append(nodeID, volID, nodeKey, parentIdx, name, flags, extID, size, modTime); err != nil {
		return 0, err
	}
	return nodeID, nil
}

// Count returns the number of nodes written so far.
func (b *SegmentBuilder) Count() uint32 {
	return b.count
}

// Finish flushes all column files, writes keys.idx, dirs.col, del.bit, and meta.hdr, and returns SegmentMeta.
func (b *SegmentBuilder) Finish() (SegmentMeta, error) {
	if b.closed {
		return SegmentMeta{}, ErrClosed
	}
	b.closed = true

	flushClose := func(bw *bufio.Writer, f *os.File, name string) error {
		if f == nil {
			return nil
		}
		if bw != nil {
			if err := bw.Flush(); err != nil {
				_ = f.Close()
				return fmt.Errorf("flush %s: %w", name, err)
			}
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return fmt.Errorf("sync %s: %w", name, err)
		}
		if err := f.Close(); err != nil {
			return fmt.Errorf("close %s: %w", name, err)
		}
		return nil
	}

	if err := flushClose(b.hotBuf, b.hotFile, "hot.col"); err != nil {
		b.closeRemaining()
		return SegmentMeta{}, err
	}
	b.hotFile = nil
	if err := flushClose(b.namesBuf, b.namesFile, "names.col"); err != nil {
		b.closeRemaining()
		return SegmentMeta{}, err
	}
	b.namesFile = nil
	if err := flushClose(b.sizesBuf, b.sizesFile, "sizes.col"); err != nil {
		b.closeRemaining()
		return SegmentMeta{}, err
	}
	b.sizesFile = nil
	if err := flushClose(b.mtimesBuf, b.mtimesFile, "mtimes.col"); err != nil {
		b.closeRemaining()
		return SegmentMeta{}, err
	}
	b.mtimesFile = nil
	if b.nodeIDsFile != nil {
		if err := flushClose(b.nodeIDsBuf, b.nodeIDsFile, "nodeids.col"); err != nil {
			return SegmentMeta{}, err
		}
		b.nodeIDsFile = nil
	}

	slices.SortFunc(b.keyEntries, func(a, c keyIndexEntry) int {
		if a.VolID < c.VolID {
			return -1
		}
		if a.VolID > c.VolID {
			return 1
		}
		if a.NodeKey < c.NodeKey {
			return -1
		}
		if a.NodeKey > c.NodeKey {
			return 1
		}
		if a.LocalIdx < c.LocalIdx {
			return -1
		}
		if a.LocalIdx > c.LocalIdx {
			return 1
		}
		return 0
	})
	if err := writeKeysIndex(filepath.Join(b.dir, "keys.idx"), b.keyEntries); err != nil {
		return SegmentMeta{}, err
	}
	if err := writeKeysIndex(filepath.Join(b.dir, "dirs.col"), b.dirEntries); err != nil {
		return SegmentMeta{}, err
	}

	delBytes := make([]byte, (int(b.count)+7)/8)
	if err := atomicWriteFile(filepath.Join(b.dir, tombstoneFileName(0)), delBytes); err != nil {
		return SegmentMeta{}, fmt.Errorf("write del.bit: %w", err)
	}

	meta := SegmentMeta{
		ID:           b.segID,
		BaseNodeID:   b.baseNodeID,
		NodeCount:    b.count,
		NamesBytes:   b.nameOffset,
		Dense:        b.dense,
		TombstoneGen: 0,
	}
	if err := writeSegmentMeta(filepath.Join(b.dir, "meta.hdr"), meta); err != nil {
		return SegmentMeta{}, err
	}
	return meta, nil
}

// Abort closes any open files and deletes the unfinished segment directory.
func (b *SegmentBuilder) Abort() {
	wasClosed := b.closed
	b.closed = true
	b.closeRemaining()
	_ = os.RemoveAll(b.dir)
	if !wasClosed && b.onAbort != nil {
		b.onAbort()
	}
}

func (b *SegmentBuilder) closeRemaining() {
	for _, f := range []*os.File{b.hotFile, b.namesFile, b.sizesFile, b.mtimesFile, b.nodeIDsFile} {
		if f != nil {
			_ = f.Close()
		}
	}
	b.hotFile, b.namesFile, b.sizesFile, b.mtimesFile, b.nodeIDsFile = nil, nil, nil, nil, nil
}

func writeKeysIndex(path string, entries []keyIndexEntry) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", filepath.Base(path), err)
	}
	bw := bufio.NewWriterSize(f, 256*1024)
	var scratch [16]byte
	for i := range entries {
		e := entries[i]
		binary.LittleEndian.PutUint64(scratch[0:8], e.NodeKey)
		binary.LittleEndian.PutUint32(scratch[8:12], e.LocalIdx)
		binary.LittleEndian.PutUint16(scratch[12:14], e.VolID)
		binary.LittleEndian.PutUint16(scratch[14:16], 0)
		if _, err := bw.Write(scratch[:]); err != nil {
			_ = f.Close()
			return fmt.Errorf("write %s: %w", filepath.Base(path), err)
		}
	}
	if err := bw.Flush(); err != nil {
		_ = f.Close()
		return fmt.Errorf("flush %s: %w", filepath.Base(path), err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync %s: %w", filepath.Base(path), err)
	}
	return f.Close()
}

func writeSegmentMeta(path string, meta SegmentMeta) error {
	var buf [segMetaSize]byte
	copy(buf[0:8], segMetaMagic)
	binary.LittleEndian.PutUint32(buf[8:12], segMetaVersion)
	binary.LittleEndian.PutUint32(buf[12:16], meta.ID)
	var flags uint32
	if meta.Dense {
		flags |= segFlagDense
	}
	binary.LittleEndian.PutUint32(buf[16:20], flags)
	binary.LittleEndian.PutUint32(buf[20:24], meta.BaseNodeID)
	binary.LittleEndian.PutUint32(buf[24:28], meta.NodeCount)
	binary.LittleEndian.PutUint64(buf[28:36], meta.NamesBytes)
	binary.LittleEndian.PutUint32(buf[36:40], 0)
	binary.LittleEndian.PutUint32(buf[40:44], crc32c(buf[0:40]))
	binary.LittleEndian.PutUint32(buf[44:48], segMetaEndMagic)
	return atomicWriteFile(path, buf[:])
}

func readSegmentMeta(path string) (SegmentMeta, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return SegmentMeta{}, fmt.Errorf("%w: read meta.hdr: %v", ErrCorruptSegment, err)
	}
	if len(raw) != segMetaSize {
		return SegmentMeta{}, fmt.Errorf("%w: invalid meta.hdr size %d", ErrCorruptSegment, len(raw))
	}
	if string(raw[0:8]) != segMetaMagic {
		return SegmentMeta{}, fmt.Errorf("%w: invalid meta.hdr magic", ErrCorruptSegment)
	}
	if binary.LittleEndian.Uint32(raw[8:12]) != segMetaVersion {
		return SegmentMeta{}, fmt.Errorf("%w: unsupported segment version", ErrCorruptSegment)
	}
	if binary.LittleEndian.Uint32(raw[44:48]) != segMetaEndMagic {
		return SegmentMeta{}, fmt.Errorf("%w: invalid meta.hdr trailer", ErrCorruptSegment)
	}
	if crc32c(raw[0:40]) != binary.LittleEndian.Uint32(raw[40:44]) {
		return SegmentMeta{}, fmt.Errorf("%w: meta.hdr crc32c mismatch", ErrCorruptSegment)
	}
	flags := binary.LittleEndian.Uint32(raw[16:20])
	return SegmentMeta{
		ID:         binary.LittleEndian.Uint32(raw[12:16]),
		Dense:      (flags & segFlagDense) != 0,
		BaseNodeID: binary.LittleEndian.Uint32(raw[20:24]),
		NodeCount:  binary.LittleEndian.Uint32(raw[24:28]),
		NamesBytes: binary.LittleEndian.Uint64(raw[28:36]),
	}, nil
}

// Segment is an opened immutable columnar segment with reference-counted lifecycle and CoW MVCC tombstones.
// Hot columns are read-only mmap'd; cold columns (sizes.col, mtimes.col) are read strictly via ReadAt.
type Segment struct {
	dir  string
	meta SegmentMeta

	hotMap     *mmapRegion
	namesMap   *mmapRegion
	keysMap    *mmapRegion
	nodeIDsMap *mmapRegion

	nodes   []HotNode
	names   []byte
	keys    []keyIndexEntry
	nodeIDs []uint32

	sizesFile  *os.File
	mtimesFile *os.File

	// Copy-on-Write pointer-free tombstone bitmap (noscan).
	// Writers hold Engine.mu.Lock; AcquireSnapshot sets delShared under RLock for zero-copy MVCC sharing.
	delBits   []uint64
	deadCount uint32
	delDirty  bool
	delShared atomic.Bool

	refCount      atomic.Int32
	removeOnClose atomic.Bool
}

// OpenSegment mounts an immutable columnar segment from segDir with tombstone generation 0.
func OpenSegment(segDir string) (*Segment, error) {
	return OpenSegmentWithGen(segDir, 0)
}

// OpenSegmentWithGen mounts an immutable columnar segment from segDir using the specified tombstone generation.
func OpenSegmentWithGen(segDir string, tombGen uint32) (*Segment, error) {
	meta, err := readSegmentMeta(filepath.Join(segDir, "meta.hdr"))
	if err != nil {
		return nil, err
	}
	meta.TombstoneGen = tombGen

	s := &Segment{
		dir:  segDir,
		meta: meta,
	}
	s.refCount.Store(1)

	cleanup := true
	defer func() {
		if cleanup {
			s.closeHandles()
		}
	}()

	if s.hotMap, err = openReadOnlyMmap(filepath.Join(segDir, "hot.col"), mmapAdviceSequential); err != nil {
		return nil, fmt.Errorf("%w: mmap hot.col: %v", ErrCorruptSegment, err)
	}
	if uint64(len(s.hotMap.Bytes())) != uint64(meta.NodeCount)*HotNodeSize {
		return nil, fmt.Errorf("%w: hot.col size %d != %d", ErrCorruptSegment, len(s.hotMap.Bytes()), uint64(meta.NodeCount)*HotNodeSize)
	}
	if meta.NodeCount > 0 {
		s.nodes = unsafe.Slice((*HotNode)(unsafe.Pointer(&s.hotMap.Bytes()[0])), meta.NodeCount)
	}

	if s.namesMap, err = openReadOnlyMmap(filepath.Join(segDir, "names.col"), mmapAdviceSequential); err != nil {
		return nil, fmt.Errorf("%w: mmap names.col: %v", ErrCorruptSegment, err)
	}
	if uint64(len(s.namesMap.Bytes())) != meta.NamesBytes {
		return nil, fmt.Errorf("%w: names.col size %d != %d", ErrCorruptSegment, len(s.namesMap.Bytes()), meta.NamesBytes)
	}
	s.names = s.namesMap.Bytes()

	if s.keysMap, err = openReadOnlyMmap(filepath.Join(segDir, "keys.idx"), mmapAdviceRandom); err != nil {
		return nil, fmt.Errorf("%w: mmap keys.idx: %v", ErrCorruptSegment, err)
	}
	if uint64(len(s.keysMap.Bytes())) != uint64(meta.NodeCount)*16 {
		return nil, fmt.Errorf("%w: keys.idx size %d != %d", ErrCorruptSegment, len(s.keysMap.Bytes()), uint64(meta.NodeCount)*16)
	}
	if meta.NodeCount > 0 {
		s.keys = unsafe.Slice((*keyIndexEntry)(unsafe.Pointer(&s.keysMap.Bytes()[0])), meta.NodeCount)
	}

	if !meta.Dense {
		if s.nodeIDsMap, err = openReadOnlyMmap(filepath.Join(segDir, "nodeids.col"), mmapAdviceRandom); err != nil {
			return nil, fmt.Errorf("%w: mmap nodeids.col: %v", ErrCorruptSegment, err)
		}
		if uint64(len(s.nodeIDsMap.Bytes())) != uint64(meta.NodeCount)*4 {
			return nil, fmt.Errorf("%w: nodeids.col size %d != %d", ErrCorruptSegment, len(s.nodeIDsMap.Bytes()), uint64(meta.NodeCount)*4)
		}
		if meta.NodeCount > 0 {
			s.nodeIDs = unsafe.Slice((*uint32)(unsafe.Pointer(&s.nodeIDsMap.Bytes()[0])), meta.NodeCount)
		}
	}

	// Open cold columns strictly as regular read-only files (never mmapped).
	if s.sizesFile, err = os.Open(filepath.Join(segDir, "sizes.col")); err != nil {
		return nil, fmt.Errorf("%w: open sizes.col: %v", ErrCorruptSegment, err)
	}
	if fi, err := s.sizesFile.Stat(); err != nil || uint64(fi.Size()) != uint64(meta.NodeCount)*8 {
		return nil, fmt.Errorf("%w: invalid sizes.col size", ErrCorruptSegment)
	}
	if s.mtimesFile, err = os.Open(filepath.Join(segDir, "mtimes.col")); err != nil {
		return nil, fmt.Errorf("%w: open mtimes.col: %v", ErrCorruptSegment, err)
	}
	if fi, err := s.mtimesFile.Stat(); err != nil || uint64(fi.Size()) != uint64(meta.NodeCount)*8 {
		return nil, fmt.Errorf("%w: invalid mtimes.col size", ErrCorruptSegment)
	}

	// Load active tombstone generation into pointer-free []uint64.
	wordCount := (int(meta.NodeCount) + 63) / 64
	s.delBits = make([]uint64, wordCount)
	activeDelName := tombstoneFileName(tombGen)
	delRaw, err := os.ReadFile(filepath.Join(segDir, activeDelName))
	if err != nil {
		return nil, fmt.Errorf("%w: read %s: %v", ErrCorruptSegment, activeDelName, err)
	}
	expectedDelBytes := (int(meta.NodeCount) + 7) / 8
	if len(delRaw) != expectedDelBytes {
		return nil, fmt.Errorf("%w: %s size %d != %d", ErrCorruptSegment, activeDelName, len(delRaw), expectedDelBytes)
	}
	var dead uint32
	for wi := 0; wi < wordCount; wi++ {
		var w uint64
		base := wi * 8
		for b := 0; b < 8 && base+b < len(delRaw); b++ {
			w |= uint64(delRaw[base+b]) << (b * 8)
		}
		if wi == wordCount-1 {
			rem := meta.NodeCount & 63
			if rem != 0 {
				w &= (uint64(1) << rem) - 1
			}
		}
		s.delBits[wi] = w
		dead += uint32(bits.OnesCount64(w))
	}
	s.deadCount = dead

	// Clean up stale/orphan tombstone files or temp files left by interrupted flushes.
	if entries, err := os.ReadDir(segDir); err == nil {
		for _, ent := range entries {
			name := ent.Name()
			if strings.HasPrefix(name, ".xift-tmp-") ||
				((name == "del.bit" || (strings.HasPrefix(name, "del_") && strings.HasSuffix(name, ".bit"))) && name != activeDelName) {
				_ = os.Remove(filepath.Join(segDir, name))
			}
		}
	}

	cleanup = false
	return s, nil
}

// Retain increments the segment reference count for safe concurrent snapshot access.
func (s *Segment) Retain() {
	s.refCount.Add(1)
}

// Release decrements the reference count; when it reaches 0, unmaps and closes all handles
// and deletes the segment directory if marked for removal.
func (s *Segment) Release() {
	if s.refCount.Add(-1) == 0 {
		s.closeHandles()
		if s.removeOnClose.Load() {
			_ = os.RemoveAll(s.dir)
		}
	}
}

// MarkForRemoval marks the segment directory to be deleted once all readers release it,
// and drops the caller's own ownership reference.
func (s *Segment) MarkForRemoval() {
	s.removeOnClose.Store(true)
	s.Release()
}

func (s *Segment) closeHandles() {
	if s.hotMap != nil {
		_ = s.hotMap.Close()
		s.hotMap = nil
	}
	if s.namesMap != nil {
		_ = s.namesMap.Close()
		s.namesMap = nil
	}
	if s.keysMap != nil {
		_ = s.keysMap.Close()
		s.keysMap = nil
	}
	if s.nodeIDsMap != nil {
		_ = s.nodeIDsMap.Close()
		s.nodeIDsMap = nil
	}
	if s.sizesFile != nil {
		_ = s.sizesFile.Close()
		s.sizesFile = nil
	}
	if s.mtimesFile != nil {
		_ = s.mtimesFile.Close()
		s.mtimesFile = nil
	}
	s.nodes = nil
	s.names = nil
	s.keys = nil
	s.nodeIDs = nil
}

func (s *Segment) Meta() SegmentMeta { return s.meta }
func (s *Segment) Dir() string       { return s.dir }
func (s *Segment) Count() uint32     { return s.meta.NodeCount }
func (s *Segment) DeadCount() uint32 { return s.deadCount }
func (s *Segment) LiveCount() uint32 {
	if s.deadCount >= s.meta.NodeCount {
		return 0
	}
	return s.meta.NodeCount - s.deadCount
}

func (s *Segment) HotNodes() []HotNode { return s.nodes }
func (s *Segment) NamesBytes() []byte  { return s.names }
func (s *Segment) NodeAt(localIdx uint32) HotNode {
	return s.nodes[localIdx]
}

func (s *Segment) NodeIDAt(localIdx uint32) uint32 {
	if s.meta.Dense {
		return s.meta.BaseNodeID + localIdx
	}
	return s.nodeIDs[localIdx]
}

func (s *Segment) NameBytesAt(localIdx uint32) []byte {
	n := s.nodes[localIdx]
	start := uint64(n.NameOff)
	end := start + uint64(n.NameLen)
	if end > uint64(len(s.names)) {
		return nil
	}
	return s.names[start:end]
}

func isBitSet(delBits []uint64, nodeCount, localIdx uint32) bool {
	if localIdx >= nodeCount {
		return true
	}
	return (delBits[localIdx>>6] & (uint64(1) << (localIdx & 63))) != 0
}

// SnapshotDelBits returns a zero-copy immutable MVCC view of delBits (caller holds Engine.mu.RLock).
func (s *Segment) SnapshotDelBits() ([]uint64, uint32) {
	s.delShared.Store(true)
	return s.delBits, s.LiveCount()
}

// IsDeleted reports whether localIdx is marked logically deleted in the current bitmap.
func (s *Segment) IsDeleted(localIdx uint32) bool {
	return isBitSet(s.delBits, s.meta.NodeCount, localIdx)
}

// MarkDeleted sets the tombstone bit for localIdx using CoW if shared with snapshots (caller holds Engine.mu.Lock).
func (s *Segment) MarkDeleted(localIdx uint32) bool {
	if localIdx >= s.meta.NodeCount {
		return false
	}
	wordIdx := localIdx >> 6
	mask := uint64(1) << (localIdx & 63)
	if (s.delBits[wordIdx] & mask) != 0 {
		return false
	}
	if s.delShared.Swap(false) {
		s.delBits = slices.Clone(s.delBits)
	}
	s.delBits[wordIdx] |= mask
	s.deadCount++
	s.delDirty = true
	return true
}

// FlushTombstones atomically persists the current tombstone bitmap to its active file (used by direct Segment callers).
func (s *Segment) FlushTombstones() error {
	if !s.delDirty {
		return nil
	}
	buf := encodeTombstoneBytes(s.delBits, int(s.meta.NodeCount))
	if err := atomicWriteFile(filepath.Join(s.dir, tombstoneFileName(s.meta.TombstoneGen)), buf); err != nil {
		return fmt.Errorf("flush del.bit for seg %d: %w", s.meta.ID, err)
	}
	s.delDirty = false
	return nil
}

// StageTombstones writes a new generation tombstone file (current delBits | extraLocalIdxs) WITHOUT mutating
// s.delBits in memory or touching the currently committed tombstone file on disk.
func (s *Segment) StageTombstones(extraLocalIdxs []uint32, nextGen uint32) (string, error) {
	stagedWords := slices.Clone(s.delBits)
	for _, localIdx := range extraLocalIdxs {
		if localIdx < s.meta.NodeCount {
			stagedWords[localIdx>>6] |= uint64(1) << (localIdx & 63)
		}
	}
	buf := encodeTombstoneBytes(stagedWords, int(s.meta.NodeCount))
	stagedPath := filepath.Join(s.dir, tombstoneFileName(nextGen))
	if err := atomicWriteFile(stagedPath, buf); err != nil {
		return "", fmt.Errorf("stage tombstones seg %d gen %d: %w", s.meta.ID, nextGen, err)
	}
	return stagedPath, nil
}

// CommitStagedTombstones activates a staged tombstone generation in memory AFTER MANIFEST commits,
// and removes the superseded tombstone file from disk.
func (s *Segment) CommitStagedTombstones(extraLocalIdxs []uint32, newGen uint32) {
	for _, localIdx := range extraLocalIdxs {
		s.MarkDeleted(localIdx)
	}
	oldGen := s.meta.TombstoneGen
	s.meta.TombstoneGen = newGen
	s.delDirty = false
	if oldGen != newGen {
		_ = os.Remove(filepath.Join(s.dir, tombstoneFileName(oldGen)))
	}
}

func encodeTombstoneBytes(words []uint64, nodeCount int) []byte {
	nBytes := (nodeCount + 7) / 8
	buf := make([]byte, nBytes)
	for wi, w := range words {
		base := wi * 8
		for b := 0; b < 8 && base+b < nBytes; b++ {
			buf[base+b] = byte(w >> (b * 8))
		}
	}
	return buf
}

// LookupNodeID resolves global nodeID to a non-deleted local index.
func (s *Segment) LookupNodeID(nodeID uint32) (uint32, bool) {
	return s.LookupNodeIDWithDel(nodeID, s.delBits)
}

// LookupNodeIDWithDel resolves global nodeID against a snapshot tombstone bitmap.
func (s *Segment) LookupNodeIDWithDel(nodeID uint32, delBits []uint64) (uint32, bool) {
	localIdx, ok := s.lookupNodeIDRaw(nodeID)
	if !ok || isBitSet(delBits, s.meta.NodeCount, localIdx) {
		return 0, false
	}
	return localIdx, true
}

func (s *Segment) lookupNodeIDRaw(nodeID uint32) (uint32, bool) {
	if s.meta.NodeCount == 0 {
		return 0, false
	}
	if s.meta.Dense {
		if nodeID < s.meta.BaseNodeID {
			return 0, false
		}
		local := nodeID - s.meta.BaseNodeID
		if local >= s.meta.NodeCount {
			return 0, false
		}
		return local, true
	}
	lo, hi := 0, len(s.nodeIDs)-1
	for lo <= hi {
		mid := int(uint(lo+hi) >> 1)
		v := s.nodeIDs[mid]
		if v == nodeID {
			return uint32(mid), true
		}
		if v < nodeID {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return 0, false
}

// LookupByKey binary-searches keys.idx for a live entry matching (volID, nodeKey).
func (s *Segment) LookupByKey(volID uint16, nodeKey uint64) (nodeID uint32, localIdx uint32, ok bool) {
	n := len(s.keys)
	if n == 0 {
		return 0, 0, false
	}
	lo, hi := 0, n
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		e := s.keys[mid]
		if e.VolID < volID || (e.VolID == volID && e.NodeKey < nodeKey) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	for i := lo; i < n; i++ {
		e := s.keys[i]
		if e.VolID != volID || e.NodeKey != nodeKey {
			break
		}
		if !s.IsDeleted(e.LocalIdx) {
			return s.NodeIDAt(e.LocalIdx), e.LocalIdx, true
		}
	}
	return 0, 0, false
}

// ReadColdMeta reads Size and ModTime directly from sizes.col and mtimes.col via ReadAt (zero mmap).
func (s *Segment) ReadColdMeta(localIdx uint32) (ColdMeta, error) {
	if localIdx >= s.meta.NodeCount {
		return ColdMeta{}, ErrNotFound
	}
	off := int64(localIdx) * 8
	var buf [8]byte
	if _, err := s.sizesFile.ReadAt(buf[:], off); err != nil {
		return ColdMeta{}, fmt.Errorf("read sizes.col[%d]: %w", localIdx, err)
	}
	size := int64(binary.LittleEndian.Uint64(buf[:]))
	if _, err := s.mtimesFile.ReadAt(buf[:], off); err != nil {
		return ColdMeta{}, fmt.Errorf("read mtimes.col[%d]: %w", localIdx, err)
	}
	mtime := int64(binary.LittleEndian.Uint64(buf[:]))
	return ColdMeta{Size: size, ModTime: mtime}, nil
}

// coldStream performs buffered sequential streaming over sizes.col and mtimes.col during Compact().
type coldStream struct {
	sf  *os.File
	mf  *os.File
	sr  *bufio.Reader
	mr  *bufio.Reader
	buf [8]byte
}

func (s *Segment) openColdStream() (*coldStream, error) {
	sf, err := os.Open(filepath.Join(s.dir, "sizes.col"))
	if err != nil {
		return nil, err
	}
	mf, err := os.Open(filepath.Join(s.dir, "mtimes.col"))
	if err != nil {
		_ = sf.Close()
		return nil, err
	}
	return &coldStream{
		sf: sf,
		mf: mf,
		sr: bufio.NewReaderSize(sf, 128*1024),
		mr: bufio.NewReaderSize(mf, 128*1024),
	}, nil
}

func (c *coldStream) Next() (ColdMeta, error) {
	if _, err := io.ReadFull(c.sr, c.buf[:]); err != nil {
		return ColdMeta{}, err
	}
	size := int64(binary.LittleEndian.Uint64(c.buf[:]))
	if _, err := io.ReadFull(c.mr, c.buf[:]); err != nil {
		return ColdMeta{}, err
	}
	mtime := int64(binary.LittleEndian.Uint64(c.buf[:]))
	return ColdMeta{Size: size, ModTime: mtime}, nil
}

func (c *coldStream) Close() {
	if c.sf != nil {
		_ = c.sf.Close()
		c.sf = nil
	}
	if c.mf != nil {
		_ = c.mf.Close()
		c.mf = nil
	}
}

// PopulateLocalKeys fills dst[localIdx] = NodeKey from keys.idx in a single pass (used by Compact).
func (s *Segment) PopulateLocalKeys(dst []uint64) {
	for i := range s.keys {
		e := s.keys[i]
		if int(e.LocalIdx) < len(dst) {
			dst[e.LocalIdx] = e.NodeKey
		}
	}
}

// PopulateDirMap loads live directory mappings from dirs.col into dirMap without touching hot.col or keys.idx.
func (s *Segment) PopulateDirMap(dirMap *KeyMap) error {
	raw, err := os.ReadFile(filepath.Join(s.dir, "dirs.col"))
	if err == nil {
		if len(raw)%16 != 0 {
			return fmt.Errorf("%w: invalid dirs.col size %d", ErrCorruptSegment, len(raw))
		}
		for off := 0; off < len(raw); off += 16 {
			nodeKey := binary.LittleEndian.Uint64(raw[off : off+8])
			localIdx := binary.LittleEndian.Uint32(raw[off+8 : off+12])
			volID := binary.LittleEndian.Uint16(raw[off+12 : off+14])
			if localIdx < s.meta.NodeCount && !s.IsDeleted(localIdx) {
				dirMap.Put(volID, nodeKey, s.NodeIDAt(localIdx))
			}
		}
		return nil
	}
	for i := range s.keys {
		e := s.keys[i]
		if e.LocalIdx < s.meta.NodeCount && !s.IsDeleted(e.LocalIdx) && (s.nodes[e.LocalIdx].Flags&FlagDir) != 0 {
			dirMap.Put(e.VolID, e.NodeKey, s.NodeIDAt(e.LocalIdx))
		}
	}
	return nil
}

// ForEachLive iterates all non-deleted nodes in the segment using current tombstones.
func (s *Segment) ForEachLive(fn func(nodeID uint32, node HotNode, name []byte) bool) {
	s.ForEachLiveWithDel(s.delBits, fn)
}

// ForEachLiveWithDel iterates all non-deleted nodes in the segment against a point-in-time tombstone bitmap.
func (s *Segment) ForEachLiveWithDel(delBits []uint64, fn func(nodeID uint32, node HotNode, name []byte) bool) {
	for i := uint32(0); i < s.meta.NodeCount; i++ {
		if isBitSet(delBits, s.meta.NodeCount, i) {
			continue
		}
		n := s.nodes[i]
		name := s.NameBytesAt(i)
		if !fn(s.NodeIDAt(i), n, name) {
			return
		}
	}
}

// ForEachKey iterates all live (volID, nodeKey, nodeID) entries from keys.idx.
func (s *Segment) ForEachKey(fn func(volID uint16, nodeKey uint64, nodeID uint32) bool) {
	for i := range s.keys {
		e := s.keys[i]
		if e.LocalIdx >= s.meta.NodeCount || s.IsDeleted(e.LocalIdx) {
			continue
		}
		if !fn(e.VolID, e.NodeKey, s.NodeIDAt(e.LocalIdx)) {
			return
		}
	}
}
