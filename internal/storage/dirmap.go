package storage

import "unsafe"

type keySlot struct {
	nodeKey uint64
	val     uint32
	volID   uint16
	state   uint16 // 0 = empty, 1 = occupied
}

var _ [16]byte = [unsafe.Sizeof(keySlot{})]byte{}

// KeyMap is a zero-GC (noscan) open-addressing hash map from (VolID, NodeKey) to uint32.
// It uses linear probing with backward-shift deletion to avoid tombstone degradation.
type KeyMap struct {
	slots []keySlot
	mask  uint64
	count int
}

// NewKeyMap allocates a KeyMap sized for at least initialCap entries.
func NewKeyMap(initialCap int) *KeyMap {
	size := nextPow2(max(initialCap*10/7+1, 16))
	return &KeyMap{
		slots: make([]keySlot, size),
		mask:  uint64(size - 1),
	}
}

func nextPow2(n int) int {
	p := 16
	for p < n {
		p <<= 1
	}
	return p
}

func hashVolKey(volID uint16, nodeKey uint64) uint64 {
	// Mix VolID and NodeKey with SplitMix64 finalizer.
	x := nodeKey ^ (uint64(volID) * 0x9e3779b97f4a7c15)
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// Len returns the number of entries in the map.
func (m *KeyMap) Len() int {
	return m.count
}

// Get looks up (volID, nodeKey).
func (m *KeyMap) Get(volID uint16, nodeKey uint64) (uint32, bool) {
	if m.count == 0 {
		return 0, false
	}
	idx := hashVolKey(volID, nodeKey) & m.mask
	for {
		s := m.slots[idx]
		if s.state == 0 {
			return 0, false
		}
		if s.nodeKey == nodeKey && s.volID == volID {
			return s.val, true
		}
		idx = (idx + 1) & m.mask
	}
}

// Put inserts or updates (volID, nodeKey) -> val.
func (m *KeyMap) Put(volID uint16, nodeKey uint64, val uint32) {
	if (m.count+1)*10 >= len(m.slots)*7 {
		m.grow()
	}
	idx := hashVolKey(volID, nodeKey) & m.mask
	for {
		s := &m.slots[idx]
		if s.state == 0 {
			*s = keySlot{
				nodeKey: nodeKey,
				val:     val,
				volID:   volID,
				state:   1,
			}
			m.count++
			return
		}
		if s.nodeKey == nodeKey && s.volID == volID {
			s.val = val
			return
		}
		idx = (idx + 1) & m.mask
	}
}

// Delete removes (volID, nodeKey) using backward-shift deletion. Returns true if removed.
func (m *KeyMap) Delete(volID uint16, nodeKey uint64) bool {
	if m.count == 0 {
		return false
	}
	idx := hashVolKey(volID, nodeKey) & m.mask
	for {
		s := m.slots[idx]
		if s.state == 0 {
			return false
		}
		if s.nodeKey == nodeKey && s.volID == volID {
			hole := idx
			next := (hole + 1) & m.mask
			for m.slots[next].state != 0 {
				ideal := hashVolKey(m.slots[next].volID, m.slots[next].nodeKey) & m.mask
				if ((hole - ideal) & m.mask) < ((next - ideal) & m.mask) {
					m.slots[hole] = m.slots[next]
					hole = next
				}
				next = (next + 1) & m.mask
			}
			m.slots[hole] = keySlot{}
			m.count--
			return true
		}
		idx = (idx + 1) & m.mask
	}
}

// Clear removes all entries without releasing the underlying backing array.
func (m *KeyMap) Clear() {
	clear(m.slots)
	m.count = 0
}

// ForEach iterates all occupied entries until fn returns false.
func (m *KeyMap) ForEach(fn func(volID uint16, nodeKey uint64, val uint32) bool) {
	for i := range m.slots {
		s := m.slots[i]
		if s.state != 0 {
			if !fn(s.volID, s.nodeKey, s.val) {
				return
			}
		}
	}
}

func (m *KeyMap) grow() {
	oldSlots := m.slots
	newSize := len(oldSlots) * 2
	m.slots = make([]keySlot, newSize)
	m.mask = uint64(newSize - 1)
	m.count = 0
	for i := range oldSlots {
		s := oldSlots[i]
		if s.state != 0 {
			m.Put(s.volID, s.nodeKey, s.val)
		}
	}
}

// u32Slot is a pointer-free 12-byte open-addressing slot for uint32 -> uint32 maps.
type u32Slot struct {
	key   uint32
	val   uint32
	state uint32 // 0 = empty, 1 = occupied
}

type u32Map struct {
	slots []u32Slot
	mask  uint64
	count int
}

func newU32Map(initialCap int) *u32Map {
	size := nextPow2(max(initialCap*10/7+1, 16))
	return &u32Map{
		slots: make([]u32Slot, size),
		mask:  uint64(size - 1),
	}
}

func hashU32(k uint32) uint64 {
	x := uint64(k) * 0x9e3779b185ebca87
	x ^= x >> 33
	x *= 0xc2b2ae3d27d4eb4f
	x ^= x >> 29
	return x
}

func (m *u32Map) Len() int { return m.count }

func (m *u32Map) Get(key uint32) (uint32, bool) {
	if m.count == 0 {
		return 0, false
	}
	idx := hashU32(key) & m.mask
	for {
		s := m.slots[idx]
		if s.state == 0 {
			return 0, false
		}
		if s.key == key {
			return s.val, true
		}
		idx = (idx + 1) & m.mask
	}
}

func (m *u32Map) Put(key, val uint32) {
	if (m.count+1)*10 >= len(m.slots)*7 {
		m.grow()
	}
	idx := hashU32(key) & m.mask
	for {
		s := &m.slots[idx]
		if s.state == 0 {
			*s = u32Slot{key: key, val: val, state: 1}
			m.count++
			return
		}
		if s.key == key {
			s.val = val
			return
		}
		idx = (idx + 1) & m.mask
	}
}

func (m *u32Map) Delete(key uint32) bool {
	if m.count == 0 {
		return false
	}
	idx := hashU32(key) & m.mask
	for {
		s := m.slots[idx]
		if s.state == 0 {
			return false
		}
		if s.key == key {
			hole := idx
			next := (hole + 1) & m.mask
			for m.slots[next].state != 0 {
				ideal := hashU32(m.slots[next].key) & m.mask
				if ((hole - ideal) & m.mask) < ((next - ideal) & m.mask) {
					m.slots[hole] = m.slots[next]
					hole = next
				}
				next = (next + 1) & m.mask
			}
			m.slots[hole] = u32Slot{}
			m.count--
			return true
		}
		idx = (idx + 1) & m.mask
	}
}

func (m *u32Map) Clear() {
	clear(m.slots)
	m.count = 0
}

func (m *u32Map) grow() {
	oldSlots := m.slots
	newSize := len(oldSlots) * 2
	m.slots = make([]u32Slot, newSize)
	m.mask = uint64(newSize - 1)
	m.count = 0
	for i := range oldSlots {
		s := oldSlots[i]
		if s.state != 0 {
			m.Put(s.key, s.val)
		}
	}
}

// coldOverrideSlot is a pointer-free 32-byte slot for NodeID -> (NodeKey, ColdMeta) overrides.
type coldOverrideSlot struct {
	nodeID  uint32
	state   uint32 // 0 = empty, 1 = occupied
	nodeKey uint64
	size    int64
	modTime int64
}

type coldOverrideMap struct {
	slots []coldOverrideSlot
	mask  uint64
	count int
}

func newColdOverrideMap(initialCap int) *coldOverrideMap {
	size := nextPow2(max(initialCap*10/7+1, 16))
	return &coldOverrideMap{
		slots: make([]coldOverrideSlot, size),
		mask:  uint64(size - 1),
	}
}

func (m *coldOverrideMap) Len() int { return m.count }

func (m *coldOverrideMap) Get(nodeID uint32) (ColdMeta, bool) {
	if m.count == 0 {
		return ColdMeta{}, false
	}
	idx := hashU32(nodeID) & m.mask
	for {
		s := m.slots[idx]
		if s.state == 0 {
			return ColdMeta{}, false
		}
		if s.nodeID == nodeID {
			return ColdMeta{Size: s.size, ModTime: s.modTime}, true
		}
		idx = (idx + 1) & m.mask
	}
}

func (m *coldOverrideMap) Put(nodeID uint32, nodeKey uint64, meta ColdMeta) {
	if (m.count+1)*10 >= len(m.slots)*7 {
		m.grow()
	}
	idx := hashU32(nodeID) & m.mask
	for {
		s := &m.slots[idx]
		if s.state == 0 {
			*s = coldOverrideSlot{
				nodeID:  nodeID,
				state:   1,
				nodeKey: nodeKey,
				size:    meta.Size,
				modTime: meta.ModTime,
			}
			m.count++
			return
		}
		if s.nodeID == nodeID {
			if nodeKey != 0 {
				s.nodeKey = nodeKey
			}
			s.size = meta.Size
			s.modTime = meta.ModTime
			return
		}
		idx = (idx + 1) & m.mask
	}
}

func (m *coldOverrideMap) Delete(nodeID uint32) bool {
	if m.count == 0 {
		return false
	}
	idx := hashU32(nodeID) & m.mask
	for {
		s := m.slots[idx]
		if s.state == 0 {
			return false
		}
		if s.nodeID == nodeID {
			hole := idx
			next := (hole + 1) & m.mask
			for m.slots[next].state != 0 {
				ideal := hashU32(m.slots[next].nodeID) & m.mask
				if ((hole - ideal) & m.mask) < ((next - ideal) & m.mask) {
					m.slots[hole] = m.slots[next]
					hole = next
				}
				next = (next + 1) & m.mask
			}
			m.slots[hole] = coldOverrideSlot{}
			m.count--
			return true
		}
		idx = (idx + 1) & m.mask
	}
}

func (m *coldOverrideMap) Clear() {
	clear(m.slots)
	m.count = 0
}

func (m *coldOverrideMap) ForEach(fn func(nodeID uint32, nodeKey uint64, meta ColdMeta) bool) {
	for i := range m.slots {
		s := m.slots[i]
		if s.state != 0 {
			if !fn(s.nodeID, s.nodeKey, ColdMeta{Size: s.size, ModTime: s.modTime}) {
				return
			}
		}
	}
}

func (m *coldOverrideMap) grow() {
	oldSlots := m.slots
	newSize := len(oldSlots) * 2
	m.slots = make([]coldOverrideSlot, newSize)
	m.mask = uint64(newSize - 1)
	m.count = 0
	for i := range oldSlots {
		s := oldSlots[i]
		if s.state != 0 {
			m.Put(s.nodeID, s.nodeKey, ColdMeta{Size: s.size, ModTime: s.modTime})
		}
	}
}
