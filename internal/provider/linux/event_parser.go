package linux

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"

	"github.com/mr-dadong/Xift/internal/provider"
)

// Linux inotify and fanotify binary protocol constants (cross-platform testable).
const (
	// inotify masks
	INModify     uint32 = 0x00000002
	INAttrib     uint32 = 0x00000004
	INCloseWrite uint32 = 0x00000008
	INMovedFrom  uint32 = 0x00000040
	INMovedTo    uint32 = 0x00000080
	INCreate     uint32 = 0x00000100
	INDelete     uint32 = 0x00000200
	INDeleteSelf uint32 = 0x00000400
	INMoveSelf   uint32 = 0x00000800
	INQOverflow  uint32 = 0x00004000
	INIgnored    uint32 = 0x00008000
	INIsDir      uint32 = 0x40000000

	// fanotify metadata & info constants
	FanotifyMetadataVersion uint8 = 3
	FanotifyMetadataLen     int   = 24

	FanEventInfoTypeFID      uint8 = 1
	FanEventInfoTypeDFIDName uint8 = 2
	FanEventInfoTypeDFID     uint8 = 3

	FanCloseWrite   uint64 = 0x00000008
	FanQOverflow    uint64 = 0x00004000
	FanCreate       uint64 = 0x00000100
	FanDelete       uint64 = 0x00000200
	FanDeleteSelf   uint64 = 0x00000400
	FanMovedFrom    uint64 = 0x00000040
	FanMovedTo      uint64 = 0x00000080
	FanMoveSelf     uint64 = 0x00000800
	FanAttrib       uint64 = 0x00000004
	FanOnDir        uint64 = 0x40000000
	FanEventOnChild uint64 = 0x08000000
)

// InotifyEvent is a decoded Linux struct inotify_event record.
type InotifyEvent struct {
	WD     int32
	Mask   uint32
	Cookie uint32
	Name   string
}

// ParseInotifyBuffer decodes all inotify_event records from raw kernel read bytes.
func ParseInotifyBuffer(buf []byte) ([]InotifyEvent, error) {
	const headerLen = 16
	var out []InotifyEvent
	offset := 0

	for offset < len(buf) {
		if len(buf)-offset < headerLen {
			return out, fmt.Errorf("truncated inotify header at offset %d", offset)
		}
		wd := int32(binary.LittleEndian.Uint32(buf[offset : offset+4]))
		mask := binary.LittleEndian.Uint32(buf[offset+4 : offset+8])
		cookie := binary.LittleEndian.Uint32(buf[offset+8 : offset+12])
		nameLen := int(binary.LittleEndian.Uint32(buf[offset+12 : offset+16]))
		offset += headerLen

		if nameLen < 0 || offset+nameLen > len(buf) {
			return out, fmt.Errorf("invalid inotify name len %d at offset %d", nameLen, offset)
		}
		var name string
		if nameLen > 0 {
			rawName := buf[offset : offset+nameLen]
			if nul := bytes.IndexByte(rawName, 0); nul >= 0 {
				rawName = rawName[:nul]
			}
			name = string(rawName)
		}
		offset += nameLen

		out = append(out, InotifyEvent{
			WD:     wd,
			Mask:   mask,
			Cookie: cookie,
			Name:   name,
		})
	}
	return out, nil
}

// PendingMove records the source side of an inotify IN_MOVED_FROM event awaiting its IN_MOVED_TO cookie pair.
type PendingMove struct {
	WD        int32
	ParentKey uint64
	NodeKey   uint64
	OldPath   string
	Name      string
	IsDir     bool
}

// MoveCookieTracker pairs IN_MOVED_FROM and IN_MOVED_TO events by kernel cookie,
// flushing unmatched MOVED_FROM entries (moved outside watched tree) as EventDelete.
type MoveCookieTracker struct {
	pending map[uint32]PendingMove
}

// NewMoveCookieTracker constructs a MoveCookieTracker.
func NewMoveCookieTracker() *MoveCookieTracker {
	return &MoveCookieTracker{
		pending: make(map[uint32]PendingMove, 32),
	}
}

// PushMovedFrom stores an IN_MOVED_FROM event keyed by cookie.
func (t *MoveCookieTracker) PushMovedFrom(cookie uint32, pm PendingMove) {
	if cookie == 0 {
		return
	}
	t.pending[cookie] = pm
}

// TakeMovedFrom retrieves and deletes the matching IN_MOVED_FROM entry for cookie.
func (t *MoveCookieTracker) TakeMovedFrom(cookie uint32) (PendingMove, bool) {
	if cookie == 0 {
		return PendingMove{}, false
	}
	pm, ok := t.pending[cookie]
	if ok {
		delete(t.pending, cookie)
	}
	return pm, ok
}

// FlushStale emits EventDelete for any unpaired IN_MOVED_FROM cookies (item moved out of watch scope).
func (t *MoveCookieTracker) FlushStale(volID uint16, emit func(provider.FileEvent)) {
	for cookie, pm := range t.pending {
		delete(t.pending, cookie)
		emit(provider.FileEvent{
			VolID:     volID,
			Type:      provider.EventDelete,
			NodeKey:   pm.NodeKey,
			ParentKey: pm.ParentKey,
			OldPath:   pm.OldPath,
			NewName:   pm.Name,
			IsDir:     pm.IsDir,
		})
	}
}

// FanotifyEvent is a decoded Linux fanotify_event_metadata frame with separate DFID and FID info.
type FanotifyEvent struct {
	Mask           uint64
	FD             int32
	PID            int32
	FSID           uint64
	DirHandleType  int32
	DirHandle      []byte
	DirHandleHash  uint64
	FileHandleType int32
	FileHandle     []byte
	FileHandleHash uint64
	Name           string
}

// ParseFanotifyBuffer decodes fanotify_event_metadata + FAN_EVENT_INFO_TYPE_DFID_NAME / FID frames
// from raw bytes read from a fanotify file descriptor without allowing FID headers to overwrite DFID.
func ParseFanotifyBuffer(buf []byte) ([]FanotifyEvent, error) {
	var out []FanotifyEvent
	offset := 0

	for offset < len(buf) {
		if len(buf)-offset < FanotifyMetadataLen {
			return out, fmt.Errorf("truncated fanotify metadata at offset %d", offset)
		}
		evLen := int(binary.LittleEndian.Uint32(buf[offset : offset+4]))
		vers := buf[offset+4]
		metaLen := int(binary.LittleEndian.Uint16(buf[offset+6 : offset+8]))
		if vers != FanotifyMetadataVersion {
			return out, fmt.Errorf("unsupported fanotify metadata version %d", vers)
		}
		if metaLen < FanotifyMetadataLen || evLen < metaLen || offset+evLen > len(buf) {
			return out, fmt.Errorf("invalid fanotify lengths event_len=%d metadata_len=%d at offset %d", evLen, metaLen, offset)
		}

		ev := FanotifyEvent{
			Mask: binary.LittleEndian.Uint64(buf[offset+8 : offset+16]),
			FD:   int32(binary.LittleEndian.Uint32(buf[offset+16 : offset+20])),
			PID:  int32(binary.LittleEndian.Uint32(buf[offset+20 : offset+24])),
		}

		// Parse trailing fanotify_event_info_header records without overwriting DFID with FID.
		infoOff := offset + metaLen
		evEnd := offset + evLen
		for infoOff+4 <= evEnd {
			infoType := buf[infoOff]
			infoLen := int(binary.LittleEndian.Uint16(buf[infoOff+2 : infoOff+4]))
			if infoLen < 4 || infoOff+infoLen > evEnd {
				break
			}
			if (infoType == FanEventInfoTypeDFIDName || infoType == FanEventInfoTypeDFID || infoType == FanEventInfoTypeFID) && infoLen >= 20 {
				fsid := binary.LittleEndian.Uint64(buf[infoOff+4 : infoOff+12])
				handleBytes := int(binary.LittleEndian.Uint32(buf[infoOff+12 : infoOff+16]))
				handleType := int32(binary.LittleEndian.Uint32(buf[infoOff+16 : infoOff+20]))
				handleEnd := infoOff + 20 + handleBytes
				if handleBytes >= 0 && handleEnd <= infoOff+infoLen {
					rawHandle := make([]byte, handleBytes)
					copy(rawHandle, buf[infoOff+20:handleEnd])
					hHash := hashHandleKey(fsid, handleType, rawHandle)
					ev.FSID = fsid

					if infoType == FanEventInfoTypeFID {
						ev.FileHandleType = handleType
						ev.FileHandle = rawHandle
						ev.FileHandleHash = hHash
					} else {
						ev.DirHandleType = handleType
						ev.DirHandle = rawHandle
						ev.DirHandleHash = hHash
						if infoType == FanEventInfoTypeDFIDName && handleEnd < infoOff+infoLen {
							nameSlice := buf[handleEnd : infoOff+infoLen]
							if nul := bytes.IndexByte(nameSlice, 0); nul >= 0 {
								nameSlice = nameSlice[:nul]
							}
							ev.Name = string(nameSlice)
						}
					}
				}
			}
			infoOff += infoLen
		}

		out = append(out, ev)
		offset = evEnd
	}
	return out, nil
}

func hashHandleKey(fsid uint64, handleType int32, handle []byte) uint64 {
	h := fnv.New64a()
	var hdr [12]byte
	binary.LittleEndian.PutUint64(hdr[0:8], fsid)
	binary.LittleEndian.PutUint32(hdr[8:12], uint32(handleType))
	_, _ = h.Write(hdr[:])
	_, _ = h.Write(handle)
	v := h.Sum64()
	if v == 0 {
		v = 1
	}
	return v
}
