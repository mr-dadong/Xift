package windows

import (
	"encoding/binary"
	"fmt"
	"strings"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/mr-dadong/Xift/internal/provider"
	"github.com/mr-dadong/Xift/internal/storage"
)

// Windows file attribute and USN reason bitmasks (pure Go constants, cross-platform testable).
const (
	FileAttrHidden       uint32 = 0x00000002
	FileAttrDirectory    uint32 = 0x00000010
	FileAttrReparsePoint uint32 = 0x00000400

	USNReasonDataOverwrite       uint32 = 0x00000001
	USNReasonDataExtend          uint32 = 0x00000002
	USNReasonDataTruncation      uint32 = 0x00000004
	USNReasonNamedDataOverwrite  uint32 = 0x00000010
	USNReasonNamedDataExtend     uint32 = 0x00000020
	USNReasonNamedDataTruncation uint32 = 0x00000040
	USNReasonFileCreate          uint32 = 0x00000100
	USNReasonFileDelete          uint32 = 0x00000200
	USNReasonBasicInfoChange     uint32 = 0x00008000
	USNReasonRenameOldName       uint32 = 0x00001000
	USNReasonRenameNewName       uint32 = 0x00002000
	USNReasonClose               uint32 = 0x80000000

	usnV2MinHeaderLen = 60
	usnV3MinHeaderLen = 76

	// defaultNTFSRootFRN is the real 64-bit NTFS root FRN (sequence 5 << 48 | MFT index 5).
	defaultNTFSRootFRN uint64 = (5 << 48) | 5
	ntfsMFTIndexMask   uint64 = 0x0000FFFFFFFFFFFF

	// Windows FILETIME epoch (1601-01-01) to Unix epoch (1970-01-01) in 100ns ticks.
	filetimeEpochDiff = 116444736000000000
)

// USNRecord is a decoded NTFS USN_RECORD_V2 or USN_RECORD_V3 entry.
type USNRecord struct {
	FRN             uint64
	ParentFRN       uint64
	USN             int64
	ModTimeUnixNano int64
	Reason          uint32
	FileAttrs       uint32
	Name            string
	IsDir           bool
	Hidden          bool
	IsReparsePoint  bool
}

func filetimeToUnixNano(ft int64) int64 {
	if ft <= filetimeEpochDiff {
		return 0
	}
	return (ft - filetimeEpochDiff) * 100
}

// ParseUSNBuffer parses USN_RECORD_V2 and USN_RECORD_V3 frames returned by
// FSCTL_ENUM_USN_DATA or FSCTL_READ_USN_JOURNAL.
// When shouldDecode is non-nil and returns false, UTF-16LE filename decoding is skipped
// to avoid millions of temporary string allocations during Pass 1 / Pass 2 MFT scanning.
func ParseUSNBuffer(
	buf []byte,
	shouldDecode func(frn, parentFRN uint64, isDir bool) bool,
	emit func(USNRecord),
) (nextCursor uint64, totalSeen int, err error) {
	var wrapped func(frn, parentFRN uint64, isDir bool, nameLenBytes int) bool
	if shouldDecode != nil {
		wrapped = func(frn, parentFRN uint64, isDir bool, _ int) bool {
			return shouldDecode(frn, parentFRN, isDir)
		}
	}
	return ParseUSNBufferWithNameLen(buf, wrapped, emit)
}

// ParseUSNBufferWithNameLen is identical to ParseUSNBuffer, additionally passing UTF-16
// nameLenBytes to shouldDecode so Pass 1 can selectively decode potential project/ignore files.
func ParseUSNBufferWithNameLen(
	buf []byte,
	shouldDecode func(frn, parentFRN uint64, isDir bool, nameLenBytes int) bool,
	emit func(USNRecord),
) (nextCursor uint64, totalSeen int, err error) {
	if len(buf) < 8 {
		return 0, 0, fmt.Errorf("usn buffer too short: %d bytes (need >= 8)", len(buf))
	}

	nextCursor = binary.LittleEndian.Uint64(buf[0:8])
	u16Buf := make([]uint16, 0, 260)
	utf8Buf := make([]byte, 0, 520)

	offset := 8
	for offset < len(buf) {
		if len(buf)-offset < 8 {
			return nextCursor, totalSeen, fmt.Errorf("truncated USN record prefix at offset %d", offset)
		}

		recLen := int(binary.LittleEndian.Uint32(buf[offset : offset+4]))
		majorVer := binary.LittleEndian.Uint16(buf[offset+4 : offset+6])

		var minHeader int
		switch majorVer {
		case 2:
			minHeader = usnV2MinHeaderLen
		case 3:
			minHeader = usnV3MinHeaderLen
		default:
			if recLen < 8 || offset+recLen > len(buf) {
				return nextCursor, totalSeen, fmt.Errorf("invalid unknown USN v%d RecordLength %d at offset %d", majorVer, recLen, offset)
			}
			offset += recLen
			continue
		}

		if recLen < minHeader {
			return nextCursor, totalSeen, fmt.Errorf("invalid USN v%d RecordLength %d at offset %d (min %d)", majorVer, recLen, offset, minHeader)
		}
		if offset+recLen > len(buf) {
			return nextCursor, totalSeen, fmt.Errorf("USN v%d RecordLength %d exceeds buffer end at offset %d (len %d)", majorVer, recLen, offset, len(buf))
		}

		var (
			frn, parentFRN uint64
			usn            int64
			ft             int64
			reason, attrs  uint32
			nameLenBytes   int
			nameOffset     int
		)

		if majorVer == 2 {
			frn = binary.LittleEndian.Uint64(buf[offset+8 : offset+16])
			parentFRN = binary.LittleEndian.Uint64(buf[offset+16 : offset+24])
			usn = int64(binary.LittleEndian.Uint64(buf[offset+24 : offset+32]))
			ft = int64(binary.LittleEndian.Uint64(buf[offset+32 : offset+40]))
			reason = binary.LittleEndian.Uint32(buf[offset+40 : offset+44])
			attrs = binary.LittleEndian.Uint32(buf[offset+52 : offset+56])
			nameLenBytes = int(binary.LittleEndian.Uint16(buf[offset+56 : offset+58]))
			nameOffset = int(binary.LittleEndian.Uint16(buf[offset+58 : offset+60]))
		} else {
			// USN_RECORD_V3 uses 128-bit FileReferenceNumber; low 64 bits hold NTFS FRN.
			frn = binary.LittleEndian.Uint64(buf[offset+8 : offset+16])
			parentFRN = binary.LittleEndian.Uint64(buf[offset+24 : offset+32])
			usn = int64(binary.LittleEndian.Uint64(buf[offset+40 : offset+48]))
			ft = int64(binary.LittleEndian.Uint64(buf[offset+48 : offset+56]))
			reason = binary.LittleEndian.Uint32(buf[offset+56 : offset+60])
			attrs = binary.LittleEndian.Uint32(buf[offset+68 : offset+72])
			nameLenBytes = int(binary.LittleEndian.Uint16(buf[offset+72 : offset+74]))
			nameOffset = int(binary.LittleEndian.Uint16(buf[offset+74 : offset+76]))
		}

		isDir := (attrs & FileAttrDirectory) != 0
		totalSeen++

		if shouldDecode != nil && !shouldDecode(frn, parentFRN, isDir, nameLenBytes) {
			offset += recLen
			continue
		}

		if nameLenBytes%2 != 0 || nameOffset < minHeader || nameOffset+nameLenBytes > recLen {
			return nextCursor, totalSeen, fmt.Errorf("invalid USN v%d filename bounds (offset=%d len=%d recLen=%d)", majorVer, nameOffset, nameLenBytes, recLen)
		}

		nameStart := offset + nameOffset
		nameCount := nameLenBytes / 2
		u16Buf = u16Buf[:0]
		for i := 0; i < nameCount; i++ {
			pos := nameStart + i*2
			u16Buf = append(u16Buf, binary.LittleEndian.Uint16(buf[pos:pos+2]))
		}
		utf8Buf = utf8Buf[:0]
		for _, r := range utf16.Decode(u16Buf) {
			utf8Buf = utf8.AppendRune(utf8Buf, r)
		}

		emit(USNRecord{
			FRN:             frn,
			ParentFRN:       parentFRN,
			USN:             usn,
			ModTimeUnixNano: filetimeToUnixNano(ft),
			Reason:          reason,
			FileAttrs:       attrs,
			Name:            string(utf8Buf),
			IsDir:           isDir,
			Hidden:          (attrs & FileAttrHidden) != 0,
			IsReparsePoint:  (attrs & FileAttrReparsePoint) != 0,
		})

		offset += recLen
	}

	return nextCursor, totalSeen, nil
}

// isCandidateMarkerUTF16Len fast-filters non-directory filenames in MFT Pass 1 by UTF-16 byte length
// so only filenames matching known project markers or .gitignore/.xiftignore are decoded.
func isCandidateMarkerUTF16Len(nameLenBytes int) bool {
	switch nameLenBytes {
	case 6 * 2, // go.mod
		7 * 2,  // pom.xml, Gemfile, mix.exs
		8 * 2,  // Makefile
		10 * 2, // .gitignore, Cargo.toml
		11 * 2, // .xiftignore
		12 * 2, // package.json, build.gradle, pubspec.yaml
		13 * 2, // composer.json, Package.swift
		14 * 2, // pyproject.toml, CMakeLists.txt
		16 * 2: // build.gradle.kts
		return true
	default:
		return false
	}
}

func isProjectOrIgnoreFileName(name string) bool {
	switch strings.ToLower(name) {
	case ".gitignore", ".xiftignore",
		"package.json", "cargo.toml", "go.mod", "pyproject.toml",
		"pom.xml", "build.gradle", "build.gradle.kts", "cmakelists.txt",
		"composer.json", "gemfile", "pubspec.yaml", "mix.exs", "package.swift", "makefile":
		return true
	default:
		return false
	}
}

type pendingRenameOld struct {
	parentFRN uint64
	oldName   string
}

// USNEventNormalizer converts raw NTFS USN Journal records into normalized provider.FileEvents:
// - Captures RENAME_OLD_NAME until final USN_REASON_CLOSE arrives (preventing duplicate EventRename).
// - Filters all intermediate records without USN_REASON_CLOSE.
// - Prioritizes CREATE over RENAME_NEW_NAME when both occur in the same handle lifecycle.
// - Marks Size as provider.SizeUnknown because USN records do not carry byte sizes.
type USNEventNormalizer struct {
	volID      uint16
	pendingOld map[uint64]pendingRenameOld
}

// NewUSNEventNormalizer creates a USNEventNormalizer for volID.
func NewUSNEventNormalizer(volID uint16) *USNEventNormalizer {
	return &USNEventNormalizer{
		volID:      volID,
		pendingOld: make(map[uint64]pendingRenameOld, 64),
	}
}

// Consume processes one decoded USNRecord and emits zero or one normalized FileEvent.
func (n *USNEventNormalizer) Consume(rec USNRecord, emit func(provider.FileEvent)) {
	r := rec.Reason

	// Capture old location on RENAME_OLD_NAME (always emitted without CLOSE on NTFS) and wait for CLOSE.
	if r&USNReasonRenameOldName != 0 && r&USNReasonRenameNewName == 0 {
		if len(n.pendingOld) > 16384 {
			clear(n.pendingOld)
		}
		// Keep the earliest old location if renamed multiple times before CLOSE.
		if _, exists := n.pendingOld[rec.FRN]; !exists {
			n.pendingOld[rec.FRN] = pendingRenameOld{
				parentFRN: rec.ParentFRN,
				oldName:   rec.Name,
			}
		}
		return
	}

	// Gate all emissions on USN_REASON_CLOSE so intermediate writes and intermediate
	// RENAME_NEW_NAME (without CLOSE) never emit duplicate or premature events.
	if r&USNReasonClose == 0 {
		return
	}

	// File created and deleted within the same handle lifecycle -> cancels out at source.
	if r&USNReasonFileCreate != 0 && r&USNReasonFileDelete != 0 {
		delete(n.pendingOld, rec.FRN)
		return
	}

	var flags uint16
	if rec.IsDir {
		flags |= storage.FlagDir
	}
	if rec.Hidden {
		flags |= storage.FlagHidden
	}
	if rec.IsReparsePoint {
		flags |= storage.FlagSymlink
	}

	cursor := uint64(0)
	if rec.USN > 0 {
		cursor = uint64(rec.USN)
	}

	switch {
	case r&USNReasonFileDelete != 0:
		delete(n.pendingOld, rec.FRN)
		emit(provider.FileEvent{
			VolID:     n.volID,
			Type:      provider.EventDelete,
			NodeKey:   rec.FRN,
			ParentKey: rec.ParentFRN,
			NewName:   rec.Name,
			Flags:     flags,
			Size:      provider.SizeUnknown,
			ModTime:   rec.ModTimeUnixNano,
			Cursor:    cursor,
			IsDir:     rec.IsDir,
		})

	case r&USNReasonFileCreate != 0:
		// Check CREATE before RENAME_NEW_NAME: a file created and renamed in one handle
		// does not exist in storage yet and must be emitted as EventCreate at its final path.
		delete(n.pendingOld, rec.FRN)
		emit(provider.FileEvent{
			VolID:     n.volID,
			Type:      provider.EventCreate,
			NodeKey:   rec.FRN,
			ParentKey: rec.ParentFRN,
			NewName:   rec.Name,
			Flags:     flags,
			Size:      provider.SizeUnknown,
			ModTime:   rec.ModTimeUnixNano,
			Cursor:    cursor,
			IsDir:     rec.IsDir,
		})

	case r&USNReasonRenameNewName != 0:
		srcParent := rec.ParentFRN
		oldName := ""
		if old, ok := n.pendingOld[rec.FRN]; ok {
			srcParent = old.parentFRN
			oldName = old.oldName
			delete(n.pendingOld, rec.FRN)
		}
		emit(provider.FileEvent{
			VolID:        n.volID,
			Type:         provider.EventRename,
			NodeKey:      rec.FRN,
			ParentKey:    srcParent,
			NewParentKey: rec.ParentFRN,
			OldPath:      oldName,
			NewName:      rec.Name,
			Flags:        flags,
			Size:         provider.SizeUnknown,
			ModTime:      rec.ModTimeUnixNano,
			Cursor:       cursor,
			IsDir:        rec.IsDir,
		})

	case r&(USNReasonDataOverwrite|USNReasonDataExtend|USNReasonDataTruncation|USNReasonBasicInfoChange) != 0:
		emit(provider.FileEvent{
			VolID:     n.volID,
			Type:      provider.EventModifyMeta,
			NodeKey:   rec.FRN,
			ParentKey: rec.ParentFRN,
			NewName:   rec.Name,
			Flags:     flags,
			Size:      provider.SizeUnknown,
			ModTime:   rec.ModTimeUnixNano,
			Cursor:    cursor,
			IsDir:     rec.IsDir,
		})
	}
}

type mftDirInfo struct {
	frn       uint64
	parentFRN uint64
	name      string
	flags     uint16
}

// orderDirsTopologically orders surviving MFT directories in cycle-safe root-to-leaf BFS order
// so StorageSegmentWriter always sees parent directories before their children.
func orderDirsTopologically(rootFRN uint64, dirs []mftDirInfo) []mftDirInfo {
	n := len(dirs)
	if n <= 1 {
		return dirs
	}

	idxByFRN := make(map[uint64]int, n)
	children := make(map[uint64][]int, n/2+8)
	for i := range dirs {
		idxByFRN[dirs[i].frn] = i
	}

	roots := make([]int, 0, 8)
	for i := range dirs {
		d := &dirs[i]
		if d.frn == d.parentFRN || (rootFRN != 0 && d.frn == rootFRN) {
			roots = append(roots, i)
			continue
		}
		if _, hasParent := idxByFRN[d.parentFRN]; hasParent {
			children[d.parentFRN] = append(children[d.parentFRN], i)
		} else {
			roots = append(roots, i)
		}
	}

	visited := make([]bool, n)
	ordered := make([]mftDirInfo, 0, n)
	queue := make([]int, 0, n)

	enqueue := func(idx int) {
		if !visited[idx] {
			visited[idx] = true
			queue = append(queue, idx)
		}
	}

	for _, r := range roots {
		enqueue(r)
	}
	for head := 0; head < len(queue); head++ {
		idx := queue[head]
		ordered = append(ordered, dirs[idx])
		for _, c := range children[dirs[idx].frn] {
			enqueue(c)
		}
	}

	// Emit any remaining cyclic components safely so zero surviving directories are ever dropped.
	for i := 0; i < n; i++ {
		if visited[i] {
			continue
		}
		enqueue(i)
		for head := len(ordered); head < len(queue); head++ {
			idx := queue[head]
			ordered = append(ordered, dirs[idx])
			for _, c := range children[dirs[idx].frn] {
				enqueue(c)
			}
		}
	}
	return ordered
}
