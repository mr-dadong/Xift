package mftspike

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"unsafe"
)

const (
	FlagDir      uint16 = 1 << 0
	FlagDeleted  uint16 = 1 << 1
	FlagHidden   uint16 = 1 << 2
	FlagNonASCII uint16 = 1 << 3
	FlagSymlink  uint16 = 1 << 4
	FlagFolded   uint16 = 1 << 5
	FlagSystem   uint16 = 1 << 6

	HotNodeSize = 16
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

// ColumnWriteStats summarizes the flushed hot.col and names.col files.
type ColumnWriteStats struct {
	NodeCount     int
	HotColBytes   int64
	NamesColBytes int64
	ExtCount      int
}

// HotColumnWriter streams 16B HotNodes to hot.col and UTF-8 names to names.col.
type HotColumnWriter struct {
	hotFile   *os.File
	namesFile *os.File
	hotBuf    *bufio.Writer
	namesBuf  *bufio.Writer

	nameOffset  uint64
	nodeCount   int
	extPool     map[string]uint16
	nextExtID   uint16
	nodeScratch [HotNodeSize]byte
}

// NewHotColumnWriter creates hot.col and names.col inside dir for streaming writes.
func NewHotColumnWriter(dir string) (*HotColumnWriter, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create hot column dir: %w", err)
	}
	hotFile, err := os.Create(filepath.Join(dir, "hot.col"))
	if err != nil {
		return nil, fmt.Errorf("create hot.col: %w", err)
	}
	namesFile, err := os.Create(filepath.Join(dir, "names.col"))
	if err != nil {
		_ = hotFile.Close()
		return nil, fmt.Errorf("create names.col: %w", err)
	}

	return &HotColumnWriter{
		hotFile:   hotFile,
		namesFile: namesFile,
		hotBuf:    bufio.NewWriterSize(hotFile, 256*1024),
		namesBuf:  bufio.NewWriterSize(namesFile, 256*1024),
		extPool:   make(map[string]uint16, 1024),
		nextExtID: 1,
	}, nil
}

// Append writes a single HotNode and its UTF-8 filename into the hot columns.
func (w *HotColumnWriter) Append(name string, parentIdx uint32, flags uint16, volID uint16) error {
	nameLen := len(name)
	if nameLen > math.MaxUint16 {
		return fmt.Errorf("filename length %d exceeds uint16 max", nameLen)
	}
	if w.nameOffset+uint64(nameLen) > math.MaxUint32 {
		return fmt.Errorf("names.col offset overflow (> 4 GiB)")
	}

	var extID uint16
	if (flags & FlagDir) == 0 {
		if dot := strings.LastIndexByte(name, '.'); dot >= 0 && dot < nameLen-1 {
			ext := strings.ToLower(name[dot+1:])
			if id, ok := w.extPool[ext]; ok {
				extID = id
			} else if w.nextExtID < math.MaxUint16 {
				extID = w.nextExtID
				w.extPool[ext] = extID
				w.nextExtID++
			}
		}
	}

	binary.LittleEndian.PutUint32(w.nodeScratch[0:4], uint32(w.nameOffset))
	binary.LittleEndian.PutUint32(w.nodeScratch[4:8], parentIdx)
	binary.LittleEndian.PutUint16(w.nodeScratch[8:10], uint16(nameLen))
	binary.LittleEndian.PutUint16(w.nodeScratch[10:12], flags)
	binary.LittleEndian.PutUint16(w.nodeScratch[12:14], extID)
	binary.LittleEndian.PutUint16(w.nodeScratch[14:16], volID)

	if _, err := w.hotBuf.Write(w.nodeScratch[:]); err != nil {
		return fmt.Errorf("write hot.col: %w", err)
	}
	if nameLen > 0 {
		if _, err := w.namesBuf.WriteString(name); err != nil {
			return fmt.Errorf("write names.col: %w", err)
		}
	}

	w.nameOffset += uint64(nameLen)
	w.nodeCount++
	return nil
}

// Close flushes and closes hot.col and names.col, returning final byte statistics.
func (w *HotColumnWriter) Close() (ColumnWriteStats, error) {
	var firstErr error
	if err := w.hotBuf.Flush(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := w.namesBuf.Flush(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := w.hotFile.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := w.namesFile.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return ColumnWriteStats{
		NodeCount:     w.nodeCount,
		HotColBytes:   int64(w.nodeCount) * HotNodeSize,
		NamesColBytes: int64(w.nameOffset),
		ExtCount:      len(w.extPool),
	}, firstErr
}

// MmapHotColumns provides zero-copy read-only mmap access to hot.col and names.col.
type MmapHotColumns struct {
	hotMap   *mmapRegion
	namesMap *mmapRegion
	nodes    []HotNode
	names    []byte
}

// OpenMmapHotColumns maps hot.col and names.col read-only into memory.
func OpenMmapHotColumns(dir string) (*MmapHotColumns, error) {
	hotMap, err := openReadOnlyMmap(filepath.Join(dir, "hot.col"))
	if err != nil {
		return nil, fmt.Errorf("mmap hot.col: %w", err)
	}
	namesMap, err := openReadOnlyMmap(filepath.Join(dir, "names.col"))
	if err != nil {
		_ = hotMap.Close()
		return nil, fmt.Errorf("mmap names.col: %w", err)
	}

	if len(hotMap.data)%HotNodeSize != 0 {
		_ = hotMap.Close()
		_ = namesMap.Close()
		return nil, fmt.Errorf("corrupt hot.col size %d (not multiple of %d)", len(hotMap.data), HotNodeSize)
	}

	var nodes []HotNode
	if len(hotMap.data) > 0 {
		count := len(hotMap.data) / HotNodeSize
		nodes = unsafe.Slice((*HotNode)(unsafe.Pointer(&hotMap.data[0])), count)
	}

	return &MmapHotColumns{
		hotMap:   hotMap,
		namesMap: namesMap,
		nodes:    nodes,
		names:    namesMap.data,
	}, nil
}

// Count returns the number of mapped HotNodes.
func (m *MmapHotColumns) Count() int {
	return len(m.nodes)
}

// Node returns the HotNode at index i.
func (m *MmapHotColumns) Node(i int) HotNode {
	return m.nodes[i]
}

// NameBytes returns a zero-copy byte slice view into names.col for node i.
func (m *MmapHotColumns) NameBytes(i int) []byte {
	n := m.nodes[i]
	start := uint64(n.NameOff)
	end := start + uint64(n.NameLen)
	if end > uint64(len(m.names)) {
		return nil
	}
	return m.names[start:end]
}

// Name returns the UTF-8 filename string for node i.
func (m *MmapHotColumns) Name(i int) string {
	return string(m.NameBytes(i))
}

// SearchSubstring scans the mapped hot columns and returns up to limit matching node indices.
// Pure ASCII queries use zero-allocation case-insensitive byte matching.
func (m *MmapHotColumns) SearchSubstring(query string, limit int) []int {
	if query == "" || limit <= 0 {
		return nil
	}
	asciiOnly := isASCII(query)
	var needle []byte
	if asciiOnly {
		needle = []byte(strings.ToLower(query))
	} else {
		needle = []byte(strings.ToLower(query))
	}

	hits := make([]int, 0, min(limit, 64))
	for i := range m.nodes {
		nb := m.NameBytes(i)
		if len(nb) < len(needle) {
			continue
		}
		if asciiOnly {
			if containsFoldASCII(nb, needle) {
				hits = append(hits, i)
				if len(hits) >= limit {
					break
				}
			}
		} else {
			if bytes.Contains(bytes.ToLower(nb), needle) {
				hits = append(hits, i)
				if len(hits) >= limit {
					break
				}
			}
		}
	}
	return hits
}

// Close unmaps both regions and closes underlying file handles.
func (m *MmapHotColumns) Close() error {
	var firstErr error
	if m.hotMap != nil {
		if err := m.hotMap.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		m.hotMap = nil
	}
	if m.namesMap != nil {
		if err := m.namesMap.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		m.namesMap = nil
	}
	m.nodes = nil
	m.names = nil
	return firstErr
}

// HasNonASCII reports whether s contains any non-ASCII byte (>= 0x80).
func HasNonASCII(s string) bool {
	return !isASCII(s)
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return false
		}
	}
	return true
}

func containsFoldASCII(haystack, lowerNeedle []byte) bool {
	nLen := len(lowerNeedle)
	if nLen == 0 {
		return true
	}
	maxStart := len(haystack) - nLen
	first := lowerNeedle[0]
	for i := 0; i <= maxStart; i++ {
		c := haystack[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != first {
			continue
		}
		match := true
		for j := 1; j < nLen; j++ {
			hc := haystack[i+j]
			if hc >= 'A' && hc <= 'Z' {
				hc += 'a' - 'A'
			}
			if hc != lowerNeedle[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}
