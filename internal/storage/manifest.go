package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"slices"
)

const (
	manifestMagic   = "XIFTMNF1"
	manifestVersion = uint32(1)
	manifestHdrSize = 20 // magic(8) + version(4) + payloadLen(4) + crc32c(4)
	manifestSegSize = 25 // ID(4) + BaseNodeID(4) + NodeCount(4) + NamesBytes(8) + Flags(1) + TombstoneGen(4)
)

// Manifest holds the atomic cluster state for <DataDir>/index/MANIFEST.
type Manifest struct {
	Version       uint32
	NextSegmentID uint32
	NextNodeID    uint32
	WALGeneration uint64
	Segments      []SegmentMeta
	Volumes       []VolumeCursor
}

// NewManifest returns an initialized empty Manifest.
func NewManifest() *Manifest {
	return &Manifest{
		Version:       manifestVersion,
		NextSegmentID: 1,
		NextNodeID:    0,
		WALGeneration: 1,
	}
}

// Clone returns a deep copy of Manifest.
func (m *Manifest) Clone() *Manifest {
	cp := *m
	cp.Segments = slices.Clone(m.Segments)
	cp.Volumes = slices.Clone(m.Volumes)
	return &cp
}

// SetVolumeCursor updates or inserts a VolumeCursor, keeping Volumes sorted by VolID.
func (m *Manifest) SetVolumeCursor(vc VolumeCursor) {
	for i := range m.Volumes {
		if m.Volumes[i].VolID == vc.VolID {
			if vc.RootPath == "" {
				vc.RootPath = m.Volumes[i].RootPath
			}
			m.Volumes[i] = vc
			return
		}
	}
	m.Volumes = append(m.Volumes, vc)
	slices.SortFunc(m.Volumes, func(a, b VolumeCursor) int {
		if a.VolID < b.VolID {
			return -1
		}
		if a.VolID > b.VolID {
			return 1
		}
		return 0
	})
}

// GetVolumeCursor looks up a volume's persisted cursor.
func (m *Manifest) GetVolumeCursor(volID uint16) (VolumeCursor, bool) {
	for i := range m.Volumes {
		if m.Volumes[i].VolID == volID {
			return m.Volumes[i], true
		}
	}
	return VolumeCursor{}, false
}

// LoadManifest reads and verifies MANIFEST from path.
// If the file does not exist, it returns (NewManifest(), false, nil).
func LoadManifest(path string) (*Manifest, bool, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return NewManifest(), false, nil
		}
		return nil, true, fmt.Errorf("read manifest: %w", err)
	}
	if len(raw) < manifestHdrSize {
		return nil, true, fmt.Errorf("%w: truncated header (%d bytes)", ErrCorruptManifest, len(raw))
	}
	if string(raw[0:8]) != manifestMagic {
		return nil, true, fmt.Errorf("%w: invalid magic", ErrCorruptManifest)
	}
	ver := binary.LittleEndian.Uint32(raw[8:12])
	if ver != manifestVersion {
		return nil, true, fmt.Errorf("%w: unsupported version %d", ErrCorruptManifest, ver)
	}
	payloadLen := binary.LittleEndian.Uint32(raw[12:16])
	expectedCRC := binary.LittleEndian.Uint32(raw[16:20])
	if uint64(manifestHdrSize)+uint64(payloadLen) != uint64(len(raw)) {
		return nil, true, fmt.Errorf("%w: payload length mismatch", ErrCorruptManifest)
	}
	payload := raw[manifestHdrSize:]
	if crc32c(payload) != expectedCRC {
		return nil, true, fmt.Errorf("%w: crc32c mismatch", ErrCorruptManifest)
	}

	off := 0
	if len(payload) < 20 {
		return nil, true, fmt.Errorf("%w: truncated body", ErrCorruptManifest)
	}
	m := &Manifest{
		Version:       ver,
		NextSegmentID: binary.LittleEndian.Uint32(payload[off : off+4]),
		NextNodeID:    binary.LittleEndian.Uint32(payload[off+4 : off+8]),
		WALGeneration: binary.LittleEndian.Uint64(payload[off+8 : off+16]),
	}
	segCount := binary.LittleEndian.Uint32(payload[off+16 : off+20])
	off += 20

	if uint64(segCount)*manifestSegSize > uint64(len(payload)-off) {
		return nil, true, fmt.Errorf("%w: segment count %d exceeds payload", ErrCorruptManifest, segCount)
	}
	m.Segments = make([]SegmentMeta, 0, segCount)
	for i := uint32(0); i < segCount; i++ {
		sm := SegmentMeta{
			ID:           binary.LittleEndian.Uint32(payload[off : off+4]),
			BaseNodeID:   binary.LittleEndian.Uint32(payload[off+4 : off+8]),
			NodeCount:    binary.LittleEndian.Uint32(payload[off+8 : off+12]),
			NamesBytes:   binary.LittleEndian.Uint64(payload[off+12 : off+20]),
			Dense:        (payload[off+20] & 1) != 0,
			TombstoneGen: binary.LittleEndian.Uint32(payload[off+21 : off+25]),
		}
		off += manifestSegSize
		m.Segments = append(m.Segments, sm)
	}

	if off+4 > len(payload) {
		return nil, true, fmt.Errorf("%w: truncated volume count", ErrCorruptManifest)
	}
	volCount := binary.LittleEndian.Uint32(payload[off : off+4])
	off += 4

	if uint64(volCount)*12 > uint64(len(payload)-off) {
		return nil, true, fmt.Errorf("%w: volume count %d exceeds payload", ErrCorruptManifest, volCount)
	}
	m.Volumes = make([]VolumeCursor, 0, volCount)
	for i := uint32(0); i < volCount; i++ {
		if off+12 > len(payload) {
			return nil, true, fmt.Errorf("%w: truncated volume[%d]", ErrCorruptManifest, i)
		}
		volID := binary.LittleEndian.Uint16(payload[off : off+2])
		cursor := binary.LittleEndian.Uint64(payload[off+2 : off+10])
		rpLen := int(binary.LittleEndian.Uint16(payload[off+10 : off+12]))
		off += 12
		if off+rpLen > len(payload) {
			return nil, true, fmt.Errorf("%w: truncated volume root path[%d]", ErrCorruptManifest, i)
		}
		rp := string(payload[off : off+rpLen])
		off += rpLen
		m.Volumes = append(m.Volumes, VolumeCursor{
			VolID:    volID,
			Cursor:   cursor,
			RootPath: rp,
		})
	}
	if off != len(payload) {
		return nil, true, fmt.Errorf("%w: trailing bytes (%d)", ErrCorruptManifest, len(payload)-off)
	}
	if m.NextSegmentID == 0 {
		m.NextSegmentID = 1
	}
	if m.WALGeneration == 0 {
		m.WALGeneration = 1
	}
	return m, true, nil
}

// Save atomically persists MANIFEST with CRC32C protection.
func (m *Manifest) Save(path string) error {
	payloadSize := 20 + len(m.Segments)*manifestSegSize + 4
	for i := range m.Volumes {
		if len(m.Volumes[i].RootPath) > math.MaxUint16 {
			return fmt.Errorf("volume root path exceeds uint16 max")
		}
		payloadSize += 12 + len(m.Volumes[i].RootPath)
	}

	buf := make([]byte, manifestHdrSize+payloadSize)
	copy(buf[0:8], manifestMagic)
	binary.LittleEndian.PutUint32(buf[8:12], manifestVersion)
	binary.LittleEndian.PutUint32(buf[12:16], uint32(payloadSize))

	off := manifestHdrSize
	binary.LittleEndian.PutUint32(buf[off:off+4], m.NextSegmentID)
	binary.LittleEndian.PutUint32(buf[off+4:off+8], m.NextNodeID)
	binary.LittleEndian.PutUint64(buf[off+8:off+16], m.WALGeneration)
	binary.LittleEndian.PutUint32(buf[off+16:off+20], uint32(len(m.Segments)))
	off += 20

	for i := range m.Segments {
		sm := m.Segments[i]
		binary.LittleEndian.PutUint32(buf[off:off+4], sm.ID)
		binary.LittleEndian.PutUint32(buf[off+4:off+8], sm.BaseNodeID)
		binary.LittleEndian.PutUint32(buf[off+8:off+12], sm.NodeCount)
		binary.LittleEndian.PutUint64(buf[off+12:off+20], sm.NamesBytes)
		if sm.Dense {
			buf[off+20] = 1
		}
		binary.LittleEndian.PutUint32(buf[off+21:off+25], sm.TombstoneGen)
		off += manifestSegSize
	}

	binary.LittleEndian.PutUint32(buf[off:off+4], uint32(len(m.Volumes)))
	off += 4

	for i := range m.Volumes {
		vc := m.Volumes[i]
		binary.LittleEndian.PutUint16(buf[off:off+2], vc.VolID)
		binary.LittleEndian.PutUint64(buf[off+2:off+10], vc.Cursor)
		binary.LittleEndian.PutUint16(buf[off+10:off+12], uint16(len(vc.RootPath)))
		off += 12
		copy(buf[off:off+len(vc.RootPath)], vc.RootPath)
		off += len(vc.RootPath)
	}

	binary.LittleEndian.PutUint32(buf[16:20], crc32c(buf[manifestHdrSize:]))
	return atomicWriteFile(path, buf)
}
