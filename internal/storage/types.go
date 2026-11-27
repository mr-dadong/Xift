package storage

import (
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"strings"
	"unsafe"
)

const (
	// FlagDir marks a directory node.
	FlagDir uint16 = 1 << 0
	// FlagDeleted marks a logically deleted node.
	FlagDeleted uint16 = 1 << 1
	// FlagHidden marks a hidden file or directory.
	FlagHidden uint16 = 1 << 2
	// FlagNonASCII marks a filename containing non-ASCII bytes.
	FlagNonASCII uint16 = 1 << 3
	// FlagSymlink marks a symbolic link or reparse point.
	FlagSymlink uint16 = 1 << 4
	// FlagFolded marks a Level 1 folded dependency directory shell.
	FlagFolded uint16 = 1 << 5
	// FlagSystem marks a Level 2 soft-demoted system path node.
	FlagSystem uint16 = 1 << 6

	// HotNodeSize is the exact byte size of HotNode (16 bytes, zero Go pointers).
	HotNodeSize = 16

	// RootParentIdx denotes a volume root node with no parent.
	RootParentIdx uint32 = math.MaxUint32

	// OrphanParentIdx denotes a detached node whose parent directory was deleted.
	// ResolvePath returns ErrNotFound for detached nodes both before and after Compact.
	OrphanParentIdx uint32 = math.MaxUint32 - 1

	// DefaultTier2Threshold is the live node count above which Tier 2 LSM is recommended.
	DefaultTier2Threshold uint64 = 5_000_000

	// DefaultMemTableFlushThreshold is the default MemTable size before recommending Flush.
	DefaultMemTableFlushThreshold = 65536
)

var (
	// ErrNotFound is returned when a requested node or path does not exist.
	ErrNotFound = errors.New("storage: node not found")
	// ErrPathCycle is returned when parent references form a cycle.
	ErrPathCycle = errors.New("storage: parent cycle detected")
	// ErrCorruptManifest is returned when MANIFEST fails magic, format, or CRC validation.
	ErrCorruptManifest = errors.New("storage: corrupt manifest")
	// ErrCorruptWAL is returned when wal.log has invalid header or unrecoverable corruption.
	ErrCorruptWAL = errors.New("storage: corrupt wal")
	// ErrCorruptSegment is returned when a segment header or column file is invalid or truncated.
	ErrCorruptSegment = errors.New("storage: corrupt segment")
	// ErrCorruptExtPool is returned when ext.pool fails magic or CRC validation.
	ErrCorruptExtPool = errors.New("storage: corrupt ext pool")
	// ErrClosed is returned when operating on a closed Engine or Snapshot.
	ErrClosed = errors.New("storage: engine closed")

	crc32cTable = crc32.MakeTable(crc32.Castagnoli)
)

// HotNode is strictly 16 bytes with zero Go pointers (noscan) per Xift Architecture V2.1 Section 3.2.
type HotNode struct {
	NameOff   uint32
	ParentIdx uint32
	NameLen   uint16
	Flags     uint16
	ExtID     uint16
	VolID     uint16
}

// Compile-time assertion enforcing exact 16-byte alignment for HotNode.
var _ [HotNodeSize]byte = [unsafe.Sizeof(HotNode{})]byte{}

// ColdMeta holds cold-column metadata read on demand from sizes.col and mtimes.col.
type ColdMeta struct {
	Size    int64
	ModTime int64
}

// StorageTier identifies the active or recommended storage tier.
type StorageTier uint8

const (
	// Tier1Mmap uses columnar hot mmap and on-demand cold columns (< 5M live nodes).
	Tier1Mmap StorageTier = iota
	// Tier2LSM uses disk LSM + Trigram inverted index + bounded LRU block cache (>= 5M or low-memory mode).
	Tier2LSM
)

func (t StorageTier) String() string {
	switch t {
	case Tier1Mmap:
		return "tier1-mmap"
	case Tier2LSM:
		return "tier2-lsm"
	default:
		return fmt.Sprintf("unknown-tier(%d)", uint8(t))
	}
}

// VolumeCursor records the last persisted kernel journal cursor for a volume.
type VolumeCursor struct {
	VolID    uint16
	Cursor   uint64
	RootPath string
}

// MutationOp identifies an incremental real-time mutation type.
type MutationOp uint8

const (
	OpCreate MutationOp = iota + 1
	OpDelete
	OpModifyMeta
	OpRename
	OpMoveDir
	OpCursorUpdate
)

// Mutation represents a single incremental change applied via Engine.ApplyBatch.
type Mutation struct {
	Op           MutationOp
	VolID        uint16
	NodeKey      uint64
	ParentKey    uint64
	NewParentKey uint64
	Name         string
	Flags        uint16
	ReplaceFlags bool // When true on OpRename, replaces non-Dir flags even if Flags == 0.
	Size         int64
	ModTime      int64
	Cursor       uint64
	RootPath     string
}

type mmapAdvice uint8

const (
	mmapAdviceNormal mmapAdvice = iota
	mmapAdviceSequential
	mmapAdviceRandom
)

// HasNonASCII reports whether s contains any non-ASCII byte (>= 0x80).
func HasNonASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return true
		}
	}
	return false
}

// ExtractLowerExt returns the normalized lowercase file extension without leading dot.
// Leading-dot filenames without another dot (e.g. ".gitignore") return "".
func ExtractLowerExt(name string) string {
	dot := strings.LastIndexByte(name, '.')
	if dot <= 0 || dot >= len(name)-1 {
		return ""
	}
	return strings.ToLower(name[dot+1:])
}

func crc32c(data []byte) uint32 {
	return crc32.Checksum(data, crc32cTable)
}

func atomicWriteFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}
	tmp, err := os.CreateTemp(dir, ".xift-tmp-*")
	if err != nil {
		return fmt.Errorf("create temp file in %s: %w", dir, err)
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp file %s: %w", tmpName, err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync temp file %s: %w", tmpName, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp file %s: %w", tmpName, err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("rename %s -> %s: %w", tmpName, path, err)
	}
	cleanup = false
	syncDir(dir)
	return nil
}
