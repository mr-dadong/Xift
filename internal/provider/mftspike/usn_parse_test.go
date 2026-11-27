package mftspike

import (
	"encoding/binary"
	"testing"
	"unicode/utf16"
)

func buildTestUSNRecordV2(frn, parentFRN uint64, attrs uint32, name string) []byte {
	u16 := utf16.Encode([]rune(name))
	nameBytesLen := len(u16) * 2
	const headerLen = 60
	recLen := headerLen + nameBytesLen
	if rem := recLen % 8; rem != 0 {
		recLen += 8 - rem
	}

	buf := make([]byte, recLen)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(recLen))
	binary.LittleEndian.PutUint16(buf[4:6], 2) // MajorVersion = 2
	binary.LittleEndian.PutUint16(buf[6:8], 0) // MinorVersion = 0
	binary.LittleEndian.PutUint64(buf[8:16], frn)
	binary.LittleEndian.PutUint64(buf[16:24], parentFRN)
	binary.LittleEndian.PutUint32(buf[52:56], attrs)
	binary.LittleEndian.PutUint16(buf[56:58], uint16(nameBytesLen))
	binary.LittleEndian.PutUint16(buf[58:60], headerLen)
	for i, v := range u16 {
		binary.LittleEndian.PutUint16(buf[headerLen+i*2:headerLen+i*2+2], v)
	}
	return buf
}

func TestParseUSNBuffer_ValidMultipleRecordsWithCJK(t *testing.T) {
	var buf []byte
	nextBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(nextBytes, 0x0000_0000_0005_1234)
	buf = append(buf, nextBytes...)
	buf = append(buf, buildTestUSNRecordV2(1001, 5, FileAttrDirectory, "node_modules")...)
	buf = append(buf, buildTestUSNRecordV2(1002, 1001, FileAttrHidden, "系统设计文档.md")...)

	var got []RawRecord
	nextFRN, err := ParseUSNBuffer(buf, func(rec RawRecord) {
		got = append(got, rec)
	})
	if err != nil {
		t.Fatalf("ParseUSNBuffer unexpected error: %v", err)
	}
	if nextFRN != 0x0000_0000_0005_1234 {
		t.Fatalf("nextFRN = %x, want 51234", nextFRN)
	}
	if len(got) != 2 {
		t.Fatalf("record count = %d, want 2", len(got))
	}
	if got[0].FRN != 1001 || got[0].ParentFRN != 5 || !got[0].IsDir || got[0].Name != "node_modules" {
		t.Fatalf("record[0] = %+v", got[0])
	}
	if got[1].FRN != 1002 || got[1].ParentFRN != 1001 || got[1].IsDir || !got[1].Hidden || got[1].Name != "系统设计文档.md" {
		t.Fatalf("record[1] = %+v", got[1])
	}
}

func TestParseUSNBuffer_MalformedBoundsSafe(t *testing.T) {
	// Too short (< 8 bytes)
	if _, err := ParseUSNBuffer([]byte{1, 2, 3}, func(RawRecord) {}); err == nil {
		t.Fatalf("expected error for <8 byte buffer")
	}

	// Zero RecordLength (infinite-loop protection)
	zeroLen := make([]byte, 8+64)
	if _, err := ParseUSNBuffer(zeroLen, func(RawRecord) {}); err == nil {
		t.Fatalf("expected error for RecordLength == 0")
	}

	// RecordLength exceeds buffer
	overLen := make([]byte, 8+64)
	binary.LittleEndian.PutUint32(overLen[8:12], 512)
	binary.LittleEndian.PutUint16(overLen[12:14], 2)
	if _, err := ParseUSNBuffer(overLen, func(RawRecord) {}); err == nil {
		t.Fatalf("expected error for RecordLength > remaining buffer")
	}

	// FileNameOffset + FileNameLength exceeds RecordLength
	badName := buildTestUSNRecordV2(1, 5, 0, "abc")
	binary.LittleEndian.PutUint16(badName[56:58], 200)
	buf := append(make([]byte, 8), badName...)
	if _, err := ParseUSNBuffer(buf, func(RawRecord) {}); err == nil {
		t.Fatalf("expected error for out-of-bounds FileNameLength")
	}
}

func TestParseUSNBufferFiltered_SkipsWithoutDecoding(t *testing.T) {
	var buf []byte
	nextBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(nextBytes, 9999)
	buf = append(buf, nextBytes...)
	buf = append(buf, buildTestUSNRecordV2(100, 5, FileAttrDirectory, "node_modules")...)
	buf = append(buf, buildTestUSNRecordV2(101, 100, 0, "ignored_file.js")...)
	buf = append(buf, buildTestUSNRecordV2(102, 5, 0, "kept.go")...)

	// Pass 1 style: only decode directories
	var dirs []RawRecord
	nextFRN, totalSeen, err := ParseUSNBufferFiltered(buf,
		func(frn, parentFRN uint64, isDir bool) bool { return isDir },
		func(rec RawRecord) { dirs = append(dirs, rec) },
	)
	if err != nil {
		t.Fatalf("Pass 1 ParseUSNBufferFiltered error: %v", err)
	}
	if nextFRN != 9999 || totalSeen != 3 || len(dirs) != 1 || dirs[0].Name != "node_modules" {
		t.Fatalf("Pass 1 got next=%d total=%d dirs=%+v", nextFRN, totalSeen, dirs)
	}

	// Pass 2 style: skip children of pruned parentFRN=100 before decoding
	var kept []RawRecord
	_, totalSeen2, err := ParseUSNBufferFiltered(buf,
		func(frn, parentFRN uint64, isDir bool) bool { return parentFRN != 100 },
		func(rec RawRecord) { kept = append(kept, rec) },
	)
	if err != nil {
		t.Fatalf("Pass 2 ParseUSNBufferFiltered error: %v", err)
	}
	if totalSeen2 != 3 || len(kept) != 2 || kept[1].Name != "kept.go" {
		t.Fatalf("Pass 2 got total=%d kept=%+v", totalSeen2, kept)
	}
}
