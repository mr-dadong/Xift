package windows

import (
	"encoding/binary"
	"testing"
	"unicode/utf16"

	"github.com/mr-dadong/Xift/internal/provider"
)

func buildUSNV2Record(frn, parentFRN uint64, usn int64, filetime int64, reason, attrs uint32, name string) []byte {
	u16 := utf16.Encode([]rune(name))
	nameBytes := len(u16) * 2
	recLen := 60 + nameBytes
	// Align to 8 bytes like NTFS USN records.
	if rem := recLen % 8; rem != 0 {
		recLen += 8 - rem
	}
	buf := make([]byte, recLen)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(recLen))
	binary.LittleEndian.PutUint16(buf[4:6], 2) // MajorVersion = 2
	binary.LittleEndian.PutUint16(buf[6:8], 0) // MinorVersion = 0
	binary.LittleEndian.PutUint64(buf[8:16], frn)
	binary.LittleEndian.PutUint64(buf[16:24], parentFRN)
	binary.LittleEndian.PutUint64(buf[24:32], uint64(usn))
	binary.LittleEndian.PutUint64(buf[32:40], uint64(filetime))
	binary.LittleEndian.PutUint32(buf[40:44], reason)
	binary.LittleEndian.PutUint32(buf[52:56], attrs)
	binary.LittleEndian.PutUint16(buf[56:58], uint16(nameBytes))
	binary.LittleEndian.PutUint16(buf[58:60], 60)
	for i, c := range u16 {
		binary.LittleEndian.PutUint16(buf[60+i*2:62+i*2], c)
	}
	return buf
}

func buildUSNV3Record(frnLow, parentLow uint64, usn int64, filetime int64, reason, attrs uint32, name string) []byte {
	u16 := utf16.Encode([]rune(name))
	nameBytes := len(u16) * 2
	recLen := 76 + nameBytes
	if rem := recLen % 8; rem != 0 {
		recLen += 8 - rem
	}
	buf := make([]byte, recLen)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(recLen))
	binary.LittleEndian.PutUint16(buf[4:6], 3) // MajorVersion = 3
	binary.LittleEndian.PutUint16(buf[6:8], 0)
	// 128-bit FRN (low 8 bytes at [8:16]) and ParentFRN (low 8 bytes at [24:32]).
	binary.LittleEndian.PutUint64(buf[8:16], frnLow)
	binary.LittleEndian.PutUint64(buf[24:32], parentLow)
	binary.LittleEndian.PutUint64(buf[40:48], uint64(usn))
	binary.LittleEndian.PutUint64(buf[48:56], uint64(filetime))
	binary.LittleEndian.PutUint32(buf[56:60], reason)
	binary.LittleEndian.PutUint32(buf[68:72], attrs)
	binary.LittleEndian.PutUint16(buf[72:74], uint16(nameBytes))
	binary.LittleEndian.PutUint16(buf[74:76], 76)
	for i, c := range u16 {
		binary.LittleEndian.PutUint16(buf[76+i*2:78+i*2], c)
	}
	return buf
}

func TestParseUSNBufferV2AndV3WithPreFilter(t *testing.T) {
	// Windows FILETIME for 2026-01-01T00:00:00Z = (1767225600 + 11644473600) * 1e7
	const ft2026 = (1767225600 + 11644473600) * 10_000_000

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf[0:8], 999)
	buf = append(buf, buildUSNV2Record(10, 5, 100, ft2026, USNReasonFileCreate|USNReasonClose, FileAttrDirectory, "src")...)
	buf = append(buf, buildUSNV2Record(11, 99, 101, ft2026, USNReasonFileCreate|USNReasonClose, 0, "skipped.js")...)
	buf = append(buf, buildUSNV3Record(12, 10, 102, ft2026, USNReasonFileCreate|USNReasonClose, FileAttrReparsePoint|FileAttrDirectory, "junction_link")...)

	var recs []USNRecord
	nextCursor, seen, err := ParseUSNBuffer(buf, func(frn, parentFRN uint64, isDir bool) bool {
		return parentFRN != 99
	}, func(r USNRecord) {
		recs = append(recs, r)
	})
	if err != nil {
		t.Fatalf("ParseUSNBuffer: %v", err)
	}
	if nextCursor != 999 || seen != 3 {
		t.Fatalf("nextCursor=%d seen=%d, want 999, 3", nextCursor, seen)
	}
	if len(recs) != 2 {
		t.Fatalf("decoded recs = %d, want 2", len(recs))
	}
	if recs[0].FRN != 10 || recs[0].Name != "src" || !recs[0].IsDir || recs[0].IsReparsePoint {
		t.Fatalf("recs[0] = %+v", recs[0])
	}
	if recs[1].FRN != 12 || recs[1].Name != "junction_link" || !recs[1].IsDir || !recs[1].IsReparsePoint {
		t.Fatalf("recs[1] = %+v, want V3 reparse dir", recs[1])
	}
	if recs[0].ModTimeUnixNano != 1767225600*1_000_000_000 {
		t.Fatalf("ModTimeUnixNano = %d, want %d", recs[0].ModTimeUnixNano, int64(1767225600*1_000_000_000))
	}
}

func TestUSNEventNormalizerCloseFilterAndRenamePairing(t *testing.T) {
	norm := NewUSNEventNormalizer(1)
	var out []provider.FileEvent
	emit := func(ev provider.FileEvent) {
		out = append(out, ev)
	}

	// 1) Pure intermediate writes without USN_REASON_CLOSE MUST be filtered out.
	norm.Consume(USNRecord{FRN: 100, ParentFRN: 5, USN: 10, Reason: USNReasonDataExtend, Name: "a.bin"}, emit)
	norm.Consume(USNRecord{FRN: 100, ParentFRN: 5, USN: 11, Reason: USNReasonDataOverwrite, Name: "a.bin"}, emit)
	if len(out) != 0 {
		t.Fatalf("intermediate writes without CLOSE must be filtered, got %+v", out)
	}

	// 2) Final CLOSE emits ModifyMeta.
	norm.Consume(USNRecord{FRN: 100, ParentFRN: 5, USN: 12, Reason: USNReasonDataExtend | USNReasonClose, Name: "a.bin"}, emit)
	if len(out) != 1 || out[0].Type != provider.EventModifyMeta || out[0].Cursor != 12 {
		t.Fatalf("after CLOSE got %+v, want ModifyMeta cursor=12", out)
	}

	// 3) Realistic NTFS 3-record rename sequence: OLD_NAME -> NEW_NAME (no CLOSE) -> NEW_NAME|CLOSE.
	// MUST emit exactly ONE EventRename (not duplicate/corrupted second rename) with SizeUnknown.
	out = out[:0]
	norm.Consume(USNRecord{FRN: 200, ParentFRN: 10, USN: 20, Reason: USNReasonRenameOldName, Name: "old.go"}, emit)
	norm.Consume(USNRecord{FRN: 200, ParentFRN: 20, USN: 21, Reason: USNReasonRenameNewName, Name: "new.go"}, emit)
	if len(out) != 0 {
		t.Fatalf("intermediate RENAME_NEW_NAME without CLOSE must wait for CLOSE, got %+v", out)
	}
	norm.Consume(USNRecord{FRN: 200, ParentFRN: 20, USN: 22, Reason: USNReasonRenameNewName | USNReasonClose, Name: "new.go"}, emit)
	if len(out) != 1 {
		t.Fatalf("expected exactly 1 paired EventRename after 3-record sequence, got %+v", out)
	}
	ev := out[0]
	if ev.Type != provider.EventRename || ev.ParentKey != 10 || ev.NewParentKey != 20 || ev.OldPath != "old.go" || ev.NewName != "new.go" || ev.Size != provider.SizeUnknown {
		t.Fatalf("paired rename = %+v", ev)
	}

	// 4) Create + Rename in same handle lifecycle (FILE_CREATE|RENAME_NEW_NAME|CLOSE) MUST emit EventCreate.
	out = out[:0]
	norm.Consume(USNRecord{FRN: 250, ParentFRN: 10, USN: 25, Reason: USNReasonFileCreate | USNReasonRenameNewName | USNReasonClose, Name: "atomic_save.txt"}, emit)
	if len(out) != 1 || out[0].Type != provider.EventCreate || out[0].NewName != "atomic_save.txt" {
		t.Fatalf("CREATE|RENAME_NEW|CLOSE = %+v, want EventCreate", out)
	}

	// 5) File created and immediately deleted in same CLOSE record -> cancels out.
	out = out[:0]
	norm.Consume(USNRecord{FRN: 300, ParentFRN: 10, USN: 30, Reason: USNReasonFileCreate | USNReasonFileDelete | USNReasonClose, Name: "temp.tmp"}, emit)
	if len(out) != 0 {
		t.Fatalf("Create+Delete in single CLOSE record should cancel out, got %+v", out)
	}
}

func TestCandidateProjectMarkerUTF16Filter(t *testing.T) {
	for _, name := range []string{"go.mod", "package.json", ".gitignore", ".xiftignore", "Cargo.toml", "pyproject.toml"} {
		if !isCandidateMarkerUTF16Len(len(name)*2) || !isProjectOrIgnoreFileName(name) {
			t.Errorf("marker %q failed fast UTF-16 length or name check", name)
		}
	}
	if isCandidateMarkerUTF16Len(len("very_long_source_file_name.ts") * 2) {
		t.Errorf("long unrelated filename should be skipped by UTF-16 length pre-filter")
	}
}

func TestParseUSNBufferRejectsTruncatedAndInvalidBounds(t *testing.T) {
	if _, _, err := ParseUSNBuffer([]byte{1, 2, 3}, nil, func(USNRecord) {}); err == nil {
		t.Fatal("expected error for short <8B buffer")
	}

	// RecordLength larger than remaining bytes
	bad := make([]byte, 8+60)
	binary.LittleEndian.PutUint32(bad[8:12], 128)
	binary.LittleEndian.PutUint16(bad[12:14], 2)
	if _, _, err := ParseUSNBuffer(bad, nil, func(USNRecord) {}); err == nil {
		t.Fatal("expected error for out-of-bounds RecordLength")
	}

	// Odd UTF-16 byte length
	badName := buildUSNV2Record(1, 5, 1, 0, USNReasonFileCreate, 0, "abc")
	binary.LittleEndian.PutUint16(badName[56:58], 5)
	framed := append(make([]byte, 8), badName...)
	if _, _, err := ParseUSNBuffer(framed, nil, func(USNRecord) {}); err == nil {
		t.Fatal("expected error for odd UTF-16 filename length")
	}
}

func TestBuildTopologicalDirsCycleAndOrphanSafe(t *testing.T) {
	dirs := []mftDirInfo{
		{frn: 5, parentFRN: 5, name: "C:"},
		{frn: 30, parentFRN: 20, name: "deep"},
		{frn: 20, parentFRN: 10, name: "mid"},
		{frn: 10, parentFRN: 5, name: "top"},
		{frn: 90, parentFRN: 91, name: "cycA"},
		{frn: 91, parentFRN: 90, name: "cycB"},
		{frn: 77, parentFRN: 999, name: "orphan"},
	}

	ordered := orderDirsTopologically(5, dirs)
	if len(ordered) != len(dirs) {
		t.Fatalf("ordered len = %d, want %d", len(ordered), len(dirs))
	}

	pos := make(map[uint64]int, len(ordered))
	for i, d := range ordered {
		pos[d.frn] = i
	}
	if !(pos[5] < pos[10] && pos[10] < pos[20] && pos[20] < pos[30]) {
		t.Fatalf("expected root -> top -> mid -> deep order, got positions %+v", pos)
	}
}
