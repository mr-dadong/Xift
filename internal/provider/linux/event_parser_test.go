package linux

import (
	"encoding/binary"
	"testing"

	"github.com/mr-dadong/Xift/internal/provider"
)

func buildInotifyRawEvent(wd int32, mask, cookie uint32, name string) []byte {
	nameBytes := []byte(name)
	if len(nameBytes) > 0 {
		nameBytes = append(nameBytes, 0)
	}
	for len(nameBytes)%4 != 0 {
		nameBytes = append(nameBytes, 0)
	}
	buf := make([]byte, 16+len(nameBytes))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(wd))
	binary.LittleEndian.PutUint32(buf[4:8], mask)
	binary.LittleEndian.PutUint32(buf[8:12], cookie)
	binary.LittleEndian.PutUint32(buf[12:16], uint32(len(nameBytes)))
	copy(buf[16:], nameBytes)
	return buf
}

func TestParseInotifyBufferAndMoveCookiePairing(t *testing.T) {
	var raw []byte
	raw = append(raw, buildInotifyRawEvent(1, INCreate, 0, "hello.go")...)
	raw = append(raw, buildInotifyRawEvent(1, INMovedFrom, 42, "old.txt")...)
	raw = append(raw, buildInotifyRawEvent(2, INMovedTo, 42, "new.txt")...)
	raw = append(raw, buildInotifyRawEvent(-1, INQOverflow, 0, "")...)
	raw = append(raw, buildInotifyRawEvent(1, INDelete|INIsDir, 0, "removed_dir")...)

	parsed, err := ParseInotifyBuffer(raw)
	if err != nil {
		t.Fatalf("ParseInotifyBuffer: %v", err)
	}
	if len(parsed) != 5 {
		t.Fatalf("parsed = %d, want 5", len(parsed))
	}
	if parsed[0].Name != "hello.go" || parsed[1].Cookie != 42 || parsed[2].Name != "new.txt" {
		t.Fatalf("unexpected parsed events: %+v", parsed)
	}
	if parsed[3].Mask&INQOverflow == 0 {
		t.Fatalf("expected IN_Q_OVERFLOW on event 3")
	}

	// Verify MoveCookieTracker pairs IN_MOVED_FROM + IN_MOVED_TO into EventRename.
	tracker := NewMoveCookieTracker()
	var emitted []provider.FileEvent

	tracker.PushMovedFrom(42, PendingMove{WD: 1, ParentKey: 100, NodeKey: 500, OldPath: "/a/old.txt", Name: "old.txt"})
	if old, ok := tracker.TakeMovedFrom(42); !ok || old.ParentKey != 100 || old.OldPath != "/a/old.txt" {
		t.Fatalf("TakeMovedFrom(42) = %+v, ok=%v", old, ok)
	}
	// Orphan MOVED_FROM flushed on sweep -> becomes EventDelete.
	tracker.PushMovedFrom(99, PendingMove{WD: 1, ParentKey: 100, NodeKey: 501, OldPath: "/a/moved_out.txt", Name: "moved_out.txt"})
	tracker.FlushStale(1, func(ev provider.FileEvent) {
		emitted = append(emitted, ev)
	})
	if len(emitted) != 1 || emitted[0].Type != provider.EventDelete || emitted[0].NodeKey != 501 {
		t.Fatalf("stale MOVED_FROM flush = %+v, want Delete node 501", emitted)
	}
}

func TestParseFanotifyDFIDNameBuffer(t *testing.T) {
	// Build a valid fanotify_event_metadata (24B) + fanotify_event_info_fid with DFID_NAME.
	// Info header (4B): info_type=2 (DFID_NAME), pad=0, len=aligned
	// fsid (8B) + file_handle { handle_bytes=8 (4B), handle_type=1 (4B), f_handle (8B) } + "main.go\x00"
	name := append([]byte("main.go"), 0)
	infoLen := 4 + 8 + 8 + 8 + len(name)
	for infoLen%8 != 0 {
		infoLen++
	}
	totalLen := 24 + infoLen
	buf := make([]byte, totalLen)

	// fanotify_event_metadata
	binary.LittleEndian.PutUint32(buf[0:4], uint32(totalLen)) // event_len
	buf[4] = FanotifyMetadataVersion                          // vers = 3
	binary.LittleEndian.PutUint16(buf[6:8], 24)               // metadata_len = 24
	binary.LittleEndian.PutUint64(buf[8:16], FanCreate|FanOnDir)
	binary.LittleEndian.PutUint32(buf[16:20], 0xFFFFFFFF) // fd = FAN_NOFD (-1)
	binary.LittleEndian.PutUint32(buf[20:24], 1234)       // pid

	// fanotify_event_info_fid at offset 24
	info := buf[24:]
	info[0] = FanEventInfoTypeDFIDName
	binary.LittleEndian.PutUint16(info[2:4], uint16(infoLen))
	binary.LittleEndian.PutUint64(info[4:12], 0x1122334455667788) // fsid
	binary.LittleEndian.PutUint32(info[12:16], 8)                 // handle_bytes
	binary.LittleEndian.PutUint32(info[16:20], 1)                 // handle_type
	binary.LittleEndian.PutUint64(info[20:28], 0xAABBCCDDEEFF0011)
	copy(info[28:], name)

	events, err := ParseFanotifyBuffer(buf)
	if err != nil {
		t.Fatalf("ParseFanotifyBuffer: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("got %d fanotify events, want 1", len(events))
	}
	ev := events[0]
	if ev.Mask&(FanCreate|FanOnDir) != (FanCreate|FanOnDir) || ev.Name != "main.go" || ev.DirHandleHash == 0 {
		t.Fatalf("unexpected fanotify event: %+v", ev)
	}

	// C8-4 regression: trailing FAN_EVENT_INFO_TYPE_FID must NOT overwrite DFID_NAME dir handle.
	fidLen := 32
	multiBuf := make([]byte, len(buf)+fidLen)
	copy(multiBuf, buf)
	binary.LittleEndian.PutUint32(multiBuf[0:4], uint32(len(multiBuf)))
	fidInfo := multiBuf[len(buf):]
	fidInfo[0] = FanEventInfoTypeFID
	binary.LittleEndian.PutUint16(fidInfo[2:4], uint16(fidLen))
	binary.LittleEndian.PutUint64(fidInfo[4:12], 0x1122334455667788)
	binary.LittleEndian.PutUint32(fidInfo[12:16], 8)
	binary.LittleEndian.PutUint32(fidInfo[16:20], 1)
	binary.LittleEndian.PutUint64(fidInfo[20:28], 0x9988776655443322)

	multiEvs, err := ParseFanotifyBuffer(multiBuf)
	if err != nil || len(multiEvs) != 1 {
		t.Fatalf("ParseFanotifyBuffer(DFID+FID): err=%v len=%d", err, len(multiEvs))
	}
	if multiEvs[0].DirHandleHash != ev.DirHandleHash || multiEvs[0].FileHandleHash == 0 || multiEvs[0].FileHandleHash == multiEvs[0].DirHandleHash {
		t.Fatalf("FID overwrote DFID: got %+v", multiEvs[0])
	}
}
