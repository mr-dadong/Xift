package mftspike

import (
	"encoding/binary"
	"fmt"
	"unicode/utf16"
	"unicode/utf8"
)

const (
	// FileAttrHidden matches Windows FILE_ATTRIBUTE_HIDDEN (0x02).
	FileAttrHidden uint32 = 0x00000002
	// FileAttrDirectory matches Windows FILE_ATTRIBUTE_DIRECTORY (0x0010).
	FileAttrDirectory uint32 = 0x00000010

	usnV2MinHeaderLen = 60
)

// ParseUSNBuffer parses all USN_RECORD_V2 records from a Windows FSCTL_ENUM_USN_DATA output buffer.
func ParseUSNBuffer(buf []byte, emit func(RawRecord)) (nextFRN uint64, err error) {
	nextFRN, _, err = ParseUSNBufferFiltered(buf, nil, emit)
	return nextFRN, err
}

// ParseUSNBufferFiltered parses a Windows FSCTL_ENUM_USN_DATA output buffer and only decodes
// UTF-16LE filenames when shouldDecode returns true (avoiding millions of temporary string
// allocations for skipped files in Pass 1 and pruned-parent files in Pass 2).
func ParseUSNBufferFiltered(
	buf []byte,
	shouldDecode func(frn, parentFRN uint64, isDir bool) bool,
	emit func(RawRecord),
) (nextFRN uint64, totalSeen int, err error) {
	if len(buf) < 8 {
		return 0, 0, fmt.Errorf("usn buffer too short: %d bytes (need >= 8)", len(buf))
	}

	nextFRN = binary.LittleEndian.Uint64(buf[0:8])

	// Reusable scratch buffers to minimize per-record heap allocations during UTF-16LE decoding.
	u16Buf := make([]uint16, 0, 260)
	utf8Buf := make([]byte, 0, 520)

	offset := 8
	for offset < len(buf) {
		if len(buf)-offset < usnV2MinHeaderLen {
			return nextFRN, totalSeen, fmt.Errorf("truncated USN record header at offset %d (remaining %d)", offset, len(buf)-offset)
		}

		recLen := int(binary.LittleEndian.Uint32(buf[offset : offset+4]))
		if recLen < usnV2MinHeaderLen {
			return nextFRN, totalSeen, fmt.Errorf("invalid USN RecordLength %d at offset %d", recLen, offset)
		}
		if offset+recLen > len(buf) {
			return nextFRN, totalSeen, fmt.Errorf("USN RecordLength %d exceeds buffer end at offset %d (len %d)", recLen, offset, len(buf))
		}

		majorVer := binary.LittleEndian.Uint16(buf[offset+4 : offset+6])
		if majorVer != 2 {
			offset += recLen
			continue
		}

		frn := binary.LittleEndian.Uint64(buf[offset+8 : offset+16])
		parentFRN := binary.LittleEndian.Uint64(buf[offset+16 : offset+24])
		attrs := binary.LittleEndian.Uint32(buf[offset+52 : offset+56])
		isDir := (attrs & FileAttrDirectory) != 0
		totalSeen++

		if shouldDecode != nil && !shouldDecode(frn, parentFRN, isDir) {
			offset += recLen
			continue
		}

		nameLenBytes := int(binary.LittleEndian.Uint16(buf[offset+56 : offset+58]))
		nameOffset := int(binary.LittleEndian.Uint16(buf[offset+58 : offset+60]))

		if nameLenBytes%2 != 0 || nameOffset < usnV2MinHeaderLen || nameOffset+nameLenBytes > recLen {
			return nextFRN, totalSeen, fmt.Errorf("invalid USN filename bounds (offset=%d len=%d recLen=%d)", nameOffset, nameLenBytes, recLen)
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

		emit(RawRecord{
			FRN:       frn,
			ParentFRN: parentFRN,
			Name:      string(utf8Buf),
			IsDir:     isDir,
			Hidden:    (attrs & FileAttrHidden) != 0,
		})

		offset += recLen
	}

	return nextFRN, totalSeen, nil
}
