package provider

import (
	"context"
	"errors"
	"fmt"
)

// EventType identifies the normalized kind of real-time filesystem mutation.
type EventType uint8

const (
	// EventCreate represents a newly created file or directory.
	EventCreate EventType = iota + 1
	// EventDelete represents a deleted file or directory.
	EventDelete
	// EventModifyMeta represents a metadata update (size, mtime, or attributes).
	EventModifyMeta
	// EventRename represents a rename and/or cross-directory move.
	EventRename
	// EventRescanDir requests a bounded read-only subtree rescan after queue overflow
	// or when a directory moves out of a pruned subtree.
	EventRescanDir
)

// String returns a human-readable name for EventType.
func (t EventType) String() string {
	switch t {
	case EventCreate:
		return "create"
	case EventDelete:
		return "delete"
	case EventModifyMeta:
		return "modify_meta"
	case EventRename:
		return "rename"
	case EventRescanDir:
		return "rescan_dir"
	default:
		return fmt.Sprintf("unknown_event(%d)", uint8(t))
	}
}

var (
	// ErrCursorExpired indicates the kernel event journal wrapped or invalidated its cursor
	// (e.g., Windows ERROR_JOURNAL_ENTRY_DELETED or macOS kFSEventStreamEventFlagEventIdsWrapped),
	// signaling callers to perform a clean single-volume rebuild.
	ErrCursorExpired = errors.New("provider: journal cursor expired, volume rebuild required")

	// ErrPermissionDenied indicates the OS denied read-only access to the volume or protected directory
	// (e.g., missing Administrator privileges for NTFS MFT or missing macOS Full Disk Access).
	ErrPermissionDenied = errors.New("provider: read-only access permission denied")

	// ErrUnsupportedOS indicates no native FSProvider implementation exists for the current GOOS.
	ErrUnsupportedOS = errors.New("provider: unsupported operating system")

	// ErrInvalidVolume indicates a malformed or empty VolumeInfo was passed to a provider operation.
	ErrInvalidVolume = errors.New("provider: invalid volume specification")
)

// VolumeInfo describes a locally indexable filesystem volume or root directory.
type VolumeInfo struct {
	VolID           uint16
	RootPath        string
	FSType          string // e.g., "NTFS", "exFAT", "APFS", "ext4", "btrfs", "xfs"
	DeviceID        uint64 // OS volume serial / st_dev
	RootKey         uint64 // Root FRN (e.g., 5 on NTFS) or root inode
	ReadOnly        bool   // True when the underlying mount is mounted read-only
	CaseInsensitive bool   // Default case-folding behavior of the volume
	SupportsJournal bool   // True when kernel journal cursor catch-up is supported (USN / FSEvents)
}

// ScanRecord represents a single pruned node emitted during FullScan or ScanSubtree.
type ScanRecord struct {
	VolID     uint16
	NodeKey   uint64 // Windows: FRN | macOS/Linux: Inode
	ParentKey uint64 // RootKey or 0/self for volume root
	Name      string
	Flags     uint16 // storage.HotNode.Flags / filter.Flag* bitmask
	Size      int64
	ModTime   int64 // Unix nanoseconds
}

// SegmentWriter receives streaming ScanRecords during FullScan or ScanSubtree.
type SegmentWriter interface {
	WriteRecord(rec ScanRecord) error
}

// SegmentWriterFunc adapts a plain function to the SegmentWriter interface.
type SegmentWriterFunc func(rec ScanRecord) error

// WriteRecord implements SegmentWriter.
func (f SegmentWriterFunc) WriteRecord(rec ScanRecord) error {
	return f(rec)
}

// SizeUnknown indicates the event source (such as Windows NTFS USN Journal) does not carry file size,
// distinguishing "unknown size" from a legitimate truncation to 0 bytes.
const SizeUnknown int64 = -1

// FileEvent represents a single normalized real-time kernel filesystem event.
type FileEvent struct {
	VolID         uint16
	Type          EventType
	NodeKey       uint64 // Windows: FRN | macOS/Linux: Inode
	ParentKey     uint64
	NewParentKey  uint64 // For EventRename cross-directory move; 0 or == ParentKey means same-dir rename
	OldPath       string // Optional source path (or directory path for EventRescanDir)
	NewName       string // Basename after creation/rename
	RelPath       string // Optional volume-relative '/' path for path/glob/ignore evaluation
	Depth         int    // Optional directory depth (1 = direct child of volume root)
	Flags         uint16 // HotNode.Flags computed or enriched by Pruner
	Size          int64  // Byte size (>= 0), or SizeUnknown (-1) when not provided by the OS event
	ModTime       int64  // Unix nanoseconds
	Cursor        uint64
	IsDir         bool
	ReplaceFlags  bool // When true on EventRename, instructs storage to replace non-Dir flags cleanly
	PrunedSubtree bool // When true, any previously indexed descendants of this directory must be tombstoned
}

// Options configures platform FSProvider behavior.
type Options struct {
	// Workers controls concurrency for top-down directory scanning (Linux/fallback).
	// Zero uses a sensible default based on GOMAXPROCS.
	Workers int
	// IncludeRemovable includes removable drives during EnumerateVolumes when true.
	IncludeRemovable bool
	// IncludeRemoteMounts allows scanning remote/FUSE mounts on Linux (default false to prevent hangs).
	IncludeRemoteMounts bool
	// DisableLowPriority skips lowering OS thread/IO priority (primarily for testing).
	DisableLowPriority bool
}

// FSProvider is the unified cross-platform read-only filesystem collection contract (V2.1 Section 6.1).
// All implementations MUST remain strictly read-only with respect to user disks and files.
type FSProvider interface {
	// EnumerateVolumes discovers locally indexable volumes, skipping virtual/inaccessible filesystems.
	EnumerateVolumes(ctx context.Context) ([]VolumeInfo, error)

	// FullScan performs a read-only full scan of vol, applying noise pruning at the source
	// and streaming surviving records into writer. It returns the volume's latest journal cursor.
	FullScan(ctx context.Context, vol VolumeInfo, writer SegmentWriter) (cursor uint64, err error)

	// Watch streams real-time kernel filesystem changes starting from sinceCursor until ctx is cancelled
	// or an unrecoverable error (such as ErrCursorExpired) occurs.
	Watch(ctx context.Context, vol VolumeInfo, sinceCursor uint64, out chan<- FileEvent) error

	// ScanSubtree performs a bounded read-only subtree scan rooted at dirPath for EventRescanDir recovery.
	ScanSubtree(ctx context.Context, vol VolumeInfo, dirPath string, dirKey uint64, writer SegmentWriter) error

	// ScanSubtreeWithRoot scans dirPath like ScanSubtree but resolves the root's
	// key itself and emits the root directory as the first record
	// (ParentKey == NodeKey), letting callers compose paths from parent keys.
	ScanSubtreeWithRoot(ctx context.Context, vol VolumeInfo, dirPath string, writer SegmentWriter) error
}
