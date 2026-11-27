package storage

import "math"

// MemTable is a zero-GC (noscan) incremental L0 layer holding recent real-time mutations.
// All backing slices contain pointer-free scalar/struct types only.
type MemTable struct {
	nodes         []HotNode
	nodeIDs       []uint32
	keys          []uint64
	sizes         []int64
	mtimes        []int64
	nameArena     []byte
	byKey         *KeyMap
	byNodeID      *u32Map
	coldOverrides *coldOverrideMap
	liveCount     int
}

// NewMemTable creates a zero-GC MemTable pre-sized for initialCap entries.
func NewMemTable(initialCap int) *MemTable {
	if initialCap < 16 {
		initialCap = 16
	}
	return &MemTable{
		nodes:         make([]HotNode, 0, initialCap),
		nodeIDs:       make([]uint32, 0, initialCap),
		keys:          make([]uint64, 0, initialCap),
		sizes:         make([]int64, 0, initialCap),
		mtimes:        make([]int64, 0, initialCap),
		nameArena:     make([]byte, 0, initialCap*24),
		byKey:         NewKeyMap(initialCap),
		byNodeID:      newU32Map(initialCap),
		coldOverrides: newColdOverrideMap(16),
	}
}

// Upsert inserts or updates a node keyed by nodeID inside MemTable.
// Returns the MemTable slot index and whether nodeID previously existed in MemTable.
func (m *MemTable) Upsert(
	nodeID uint32,
	volID uint16,
	nodeKey uint64,
	parentIdx uint32,
	name string,
	flags uint16,
	extID uint16,
	size int64,
	modTime int64,
) (uint32, bool) {
	flags &^= FlagDeleted
	if HasNonASCII(name) {
		flags |= FlagNonASCII
	} else {
		flags &^= FlagNonASCII
	}
	nameLen := len(name)
	if nameLen > math.MaxUint16 {
		nameLen = math.MaxUint16
		name = name[:nameLen]
	}

	m.coldOverrides.Delete(nodeID)

	if slotIdx, ok := m.byNodeID.Get(nodeID); ok {
		old := &m.nodes[slotIdx]
		wasDeleted := (old.Flags & FlagDeleted) != 0
		oldVol := old.VolID
		oldKey := m.keys[slotIdx]

		if oldVol != volID || oldKey != nodeKey {
			if !wasDeleted {
				m.byKey.Delete(oldVol, oldKey)
			}
			m.keys[slotIdx] = nodeKey
		}

		// Reuse existing nameArena span if new name fits; otherwise append.
		var nameOff uint32
		if nameLen <= int(old.NameLen) {
			nameOff = old.NameOff
			copy(m.nameArena[nameOff:nameOff+uint32(nameLen)], name)
		} else {
			nameOff = uint32(len(m.nameArena))
			m.nameArena = append(m.nameArena, name...)
		}

		*old = HotNode{
			NameOff:   nameOff,
			ParentIdx: parentIdx,
			NameLen:   uint16(nameLen),
			Flags:     flags,
			ExtID:     extID,
			VolID:     volID,
		}
		m.sizes[slotIdx] = size
		m.mtimes[slotIdx] = modTime
		m.byKey.Put(volID, nodeKey, slotIdx)
		if wasDeleted {
			m.liveCount++
		}
		return slotIdx, true
	}

	slotIdx := uint32(len(m.nodes))
	nameOff := uint32(len(m.nameArena))
	m.nameArena = append(m.nameArena, name...)

	m.nodes = append(m.nodes, HotNode{
		NameOff:   nameOff,
		ParentIdx: parentIdx,
		NameLen:   uint16(nameLen),
		Flags:     flags,
		ExtID:     extID,
		VolID:     volID,
	})
	m.nodeIDs = append(m.nodeIDs, nodeID)
	m.keys = append(m.keys, nodeKey)
	m.sizes = append(m.sizes, size)
	m.mtimes = append(m.mtimes, modTime)

	m.byKey.Put(volID, nodeKey, slotIdx)
	m.byNodeID.Put(nodeID, slotIdx)
	m.liveCount++
	return slotIdx, false
}

// GetByKey resolves (volID, nodeKey) to a live MemTable slot index.
func (m *MemTable) GetByKey(volID uint16, nodeKey uint64) (uint32, bool) {
	slotIdx, ok := m.byKey.Get(volID, nodeKey)
	if !ok {
		return 0, false
	}
	if (m.nodes[slotIdx].Flags & FlagDeleted) != 0 {
		return 0, false
	}
	return slotIdx, true
}

// GetByNodeID resolves nodeID to a live MemTable slot index.
func (m *MemTable) GetByNodeID(nodeID uint32) (uint32, bool) {
	slotIdx, ok := m.byNodeID.Get(nodeID)
	if !ok {
		return 0, false
	}
	if (m.nodes[slotIdx].Flags & FlagDeleted) != 0 {
		return 0, false
	}
	return slotIdx, true
}

// MarkDeletedByNodeID logically deletes a node in MemTable and clears any cold override.
func (m *MemTable) MarkDeletedByNodeID(nodeID uint32) bool {
	m.coldOverrides.Delete(nodeID)
	slotIdx, ok := m.byNodeID.Get(nodeID)
	if !ok {
		return false
	}
	n := &m.nodes[slotIdx]
	if (n.Flags & FlagDeleted) != 0 {
		return false
	}
	n.Flags |= FlagDeleted
	m.byKey.Delete(n.VolID, m.keys[slotIdx])
	m.liveCount--
	return true
}

// SetColdOverride updates cold metadata for nodeID. If nodeID is live in MemTable,
// it updates the slot in place; otherwise it records a lightweight cold override.
func (m *MemTable) SetColdOverride(nodeID uint32, meta ColdMeta) {
	m.SetColdOverrideWithKey(nodeID, 0, meta)
}

// SetColdOverrideWithKey records a cold metadata override along with nodeKey for O(1) Flush materialization.
func (m *MemTable) SetColdOverrideWithKey(nodeID uint32, nodeKey uint64, meta ColdMeta) {
	if slotIdx, ok := m.GetByNodeID(nodeID); ok {
		m.sizes[slotIdx] = meta.Size
		m.mtimes[slotIdx] = meta.ModTime
		return
	}
	m.coldOverrides.Put(nodeID, nodeKey, meta)
}

// GetColdOverride returns the latest cold metadata for nodeID if present in MemTable.
func (m *MemTable) GetColdOverride(nodeID uint32) (ColdMeta, bool) {
	if slotIdx, ok := m.GetByNodeID(nodeID); ok {
		return ColdMeta{Size: m.sizes[slotIdx], ModTime: m.mtimes[slotIdx]}, true
	}
	return m.coldOverrides.Get(nodeID)
}

// DeleteColdOverride removes a standalone cold override entry.
func (m *MemTable) DeleteColdOverride(nodeID uint32) {
	m.coldOverrides.Delete(nodeID)
}

func (m *MemTable) NodeAt(slotIdx uint32) HotNode  { return m.nodes[slotIdx] }
func (m *MemTable) NodeIDAt(slotIdx uint32) uint32 { return m.nodeIDs[slotIdx] }
func (m *MemTable) KeyAt(slotIdx uint32) uint64    { return m.keys[slotIdx] }
func (m *MemTable) ColdMetaAt(slotIdx uint32) ColdMeta {
	return ColdMeta{Size: m.sizes[slotIdx], ModTime: m.mtimes[slotIdx]}
}
func (m *MemTable) NameBytesAt(slotIdx uint32) []byte {
	n := m.nodes[slotIdx]
	start := uint64(n.NameOff)
	end := start + uint64(n.NameLen)
	if end > uint64(len(m.nameArena)) {
		return nil
	}
	return m.nameArena[start:end]
}

// Len returns total allocated slots (including logically deleted slots).
func (m *MemTable) Len() int { return len(m.nodes) }

// LiveCount returns the number of active non-deleted nodes in MemTable.
func (m *MemTable) LiveCount() int { return m.liveCount }

// ColdOverrideCount returns the number of standalone cold metadata overrides.
func (m *MemTable) ColdOverrideCount() int { return m.coldOverrides.Len() }

// EntryCount returns total pending entries (active slots + cold overrides).
func (m *MemTable) EntryCount() int { return len(m.nodes) + m.coldOverrides.Len() }

// ForEachLive iterates all active (non-deleted) nodes in MemTable.
func (m *MemTable) ForEachLive(fn func(nodeID uint32, node HotNode, name []byte) bool) {
	for i := range m.nodes {
		n := m.nodes[i]
		if (n.Flags & FlagDeleted) != 0 {
			continue
		}
		name := m.NameBytesAt(uint32(i))
		if !fn(m.nodeIDs[i], n, name) {
			return
		}
	}
}

// ForEachColdOverride iterates standalone cold metadata overrides.
func (m *MemTable) ForEachColdOverride(fn func(nodeID uint32, nodeKey uint64, meta ColdMeta) bool) {
	m.coldOverrides.ForEach(fn)
}

// CloneSnapshot produces an immutable pointer-free copy of the MemTable for lock-free concurrent reads.
func (m *MemTable) CloneSnapshot() *MemTable {
	if m == nil || (m.liveCount == 0 && m.coldOverrides.Len() == 0) {
		return NewMemTable(16)
	}
	cp := &MemTable{
		nodes:         append([]HotNode(nil), m.nodes...),
		nodeIDs:       append([]uint32(nil), m.nodeIDs...),
		keys:          append([]uint64(nil), m.keys...),
		sizes:         append([]int64(nil), m.sizes...),
		mtimes:        append([]int64(nil), m.mtimes...),
		nameArena:     append([]byte(nil), m.nameArena...),
		byKey:         &KeyMap{slots: append([]keySlot(nil), m.byKey.slots...), mask: m.byKey.mask, count: m.byKey.count},
		byNodeID:      &u32Map{slots: append([]u32Slot(nil), m.byNodeID.slots...), mask: m.byNodeID.mask, count: m.byNodeID.count},
		coldOverrides: &coldOverrideMap{slots: append([]coldOverrideSlot(nil), m.coldOverrides.slots...), mask: m.coldOverrides.mask, count: m.coldOverrides.count},
		liveCount:     m.liveCount,
	}
	return cp
}

// Reset clears all entries in MemTable.
func (m *MemTable) Reset() {
	m.nodes = m.nodes[:0]
	m.nodeIDs = m.nodeIDs[:0]
	m.keys = m.keys[:0]
	m.sizes = m.sizes[:0]
	m.mtimes = m.mtimes[:0]
	m.nameArena = m.nameArena[:0]
	m.byKey.Clear()
	m.byNodeID.Clear()
	m.coldOverrides.Clear()
	m.liveCount = 0
}
