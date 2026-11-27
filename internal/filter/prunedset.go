package filter

import (
	"sync"
	"unsafe"
)

// DirFlags is a compact bitmask recording pruning and system-demotion state for a directory key.
type DirFlags uint8

const (
	// DirFlagHardExcluded marks a directory excluded at Level 0 (itself and all descendants dropped).
	DirFlagHardExcluded DirFlags = 1 << 0
	// DirFlagFoldedShell marks a Level 1 folded directory shell (shell kept with FlagFolded, descendants dropped).
	DirFlagFoldedShell DirFlags = 1 << 1
	// DirFlagPrunedDescendant marks a directory nested under a hard-excluded or folded ancestor.
	DirFlagPrunedDescendant DirFlags = 1 << 2
	// DirFlagSystem marks a Level 2 soft-demoted system directory or descendant.
	DirFlagSystem DirFlags = 1 << 3

	// DirFlagsPrunedMask matches any directory whose children must be intercepted at the source.
	DirFlagsPrunedMask = DirFlagHardExcluded | DirFlagFoldedShell | DirFlagPrunedDescendant
)

// IsPruned reports whether descendants of this directory must be pruned at the source.
func (f DirFlags) IsPruned() bool {
	return f&DirFlagsPrunedMask != 0
}

// IsSystem reports whether this directory carries Level 2 soft-demotion.
func (f DirFlags) IsSystem() bool {
	return f&DirFlagSystem != 0
}

// prunedSlot is a strictly 16-byte, pointer-free (noscan) hash table slot.
type prunedSlot struct {
	dirKey uint64
	volID  uint16
	flags  DirFlags
	state  uint8 // 0 = empty, 1 = occupied
	_      uint32
}

var _ [16]byte = [unsafe.Sizeof(prunedSlot{})]byte{}

const (
	prunedLoadNum = 78
	prunedLoadDen = 100
)

// PrunedDirSet is a concurrent-safe, zero-GC (noscan) open-addressing hash map from (VolID, DirKey) to DirFlags.
// It uses linear probing with backward-shift deletion to avoid tombstone degradation.
type PrunedDirSet struct {
	mu    sync.RWMutex
	slots []prunedSlot
	mask  uint64
	count int
}

// NewPrunedDirSet allocates a PrunedDirSet sized for at least initialCap entries.
func NewPrunedDirSet(initialCap int) *PrunedDirSet {
	if initialCap < 0 {
		initialCap = 0
	}
	minSlots := initialCap*prunedLoadDen/prunedLoadNum + 1
	size := nextPow2(max(minSlots, 16))
	return &PrunedDirSet{
		slots: make([]prunedSlot, size),
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

func hashVolDirKey(volID uint16, dirKey uint64) uint64 {
	x := dirKey ^ (uint64(volID) * 0x9e3779b97f4a7c15)
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// Len returns the total number of tracked directories across all volumes.
func (s *PrunedDirSet) Len() int {
	s.mu.RLock()
	n := s.count
	s.mu.RUnlock()
	return n
}

// MemoryBytes returns the size in bytes of the underlying pointer-free slot array.
func (s *PrunedDirSet) MemoryBytes() int {
	s.mu.RLock()
	b := len(s.slots) * int(unsafe.Sizeof(prunedSlot{}))
	s.mu.RUnlock()
	return b
}

// GetFlags looks up (volID, dirKey) in O(1) time with zero heap allocations.
func (s *PrunedDirSet) GetFlags(volID uint16, dirKey uint64) (DirFlags, bool) {
	s.mu.RLock()
	fl, ok := s.getFlagsLocked(volID, dirKey)
	s.mu.RUnlock()
	return fl, ok
}

func (s *PrunedDirSet) getFlagsLocked(volID uint16, dirKey uint64) (DirFlags, bool) {
	if s.count == 0 {
		return 0, false
	}
	idx := hashVolDirKey(volID, dirKey) & s.mask
	for {
		slot := s.slots[idx]
		if slot.state == 0 {
			return 0, false
		}
		if slot.dirKey == dirKey && slot.volID == volID {
			return slot.flags, true
		}
		idx = (idx + 1) & s.mask
	}
}

// IsPruned reports whether (volID, dirKey) is in the pruned set (HardExcluded, FoldedShell, or PrunedDescendant).
func (s *PrunedDirSet) IsPruned(volID uint16, dirKey uint64) bool {
	fl, ok := s.GetFlags(volID, dirKey)
	return ok && fl.IsPruned()
}

// IsSystem reports whether (volID, dirKey) carries the Level 2 soft-demoted system flag.
func (s *PrunedDirSet) IsSystem(volID uint16, dirKey uint64) bool {
	fl, ok := s.GetFlags(volID, dirKey)
	return ok && fl.IsSystem()
}

// Set assigns flags to (volID, dirKey). Setting flags == 0 removes the entry.
func (s *PrunedDirSet) Set(volID uint16, dirKey uint64, flags DirFlags) {
	if flags == 0 {
		s.Remove(volID, dirKey)
		return
	}
	s.mu.Lock()
	s.setLocked(volID, dirKey, flags, false)
	s.mu.Unlock()
}

// AddFlags bitwise-ORs flags into (volID, dirKey), inserting the entry if absent.
func (s *PrunedDirSet) AddFlags(volID uint16, dirKey uint64, flags DirFlags) {
	if flags == 0 {
		return
	}
	s.mu.Lock()
	s.setLocked(volID, dirKey, flags, true)
	s.mu.Unlock()
}

// ClearFlags removes specific flag bits from (volID, dirKey), deleting the entry if no bits remain.
func (s *PrunedDirSet) ClearFlags(volID uint16, dirKey uint64, mask DirFlags) {
	if mask == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.count == 0 {
		return
	}
	idx := hashVolDirKey(volID, dirKey) & s.mask
	for {
		slot := &s.slots[idx]
		if slot.state == 0 {
			return
		}
		if slot.dirKey == dirKey && slot.volID == volID {
			remaining := slot.flags &^ mask
			if remaining == 0 {
				s.deleteAtLocked(idx)
			} else {
				slot.flags = remaining
			}
			return
		}
		idx = (idx + 1) & s.mask
	}
}

func (s *PrunedDirSet) setLocked(volID uint16, dirKey uint64, flags DirFlags, merge bool) {
	// First check whether (volID, dirKey) already exists to avoid needlessly doubling
	// memory when updating an existing key at the 78% load threshold.
	idx := hashVolDirKey(volID, dirKey) & s.mask
	for {
		slot := &s.slots[idx]
		if slot.state == 0 {
			break
		}
		if slot.dirKey == dirKey && slot.volID == volID {
			if merge {
				slot.flags |= flags
			} else {
				slot.flags = flags
			}
			return
		}
		idx = (idx + 1) & s.mask
	}

	if (s.count+1)*prunedLoadDen >= len(s.slots)*prunedLoadNum {
		s.growLocked()
		idx = hashVolDirKey(volID, dirKey) & s.mask
		for s.slots[idx].state != 0 {
			idx = (idx + 1) & s.mask
		}
	}
	s.slots[idx] = prunedSlot{
		dirKey: dirKey,
		volID:  volID,
		flags:  flags,
		state:  1,
	}
	s.count++
}

// Remove deletes (volID, dirKey) using backward-shift deletion. Returns true if removed.
func (s *PrunedDirSet) Remove(volID uint16, dirKey uint64) bool {
	s.mu.Lock()
	ok := s.removeLocked(volID, dirKey)
	s.mu.Unlock()
	return ok
}

func (s *PrunedDirSet) removeLocked(volID uint16, dirKey uint64) bool {
	if s.count == 0 {
		return false
	}
	idx := hashVolDirKey(volID, dirKey) & s.mask
	for {
		slot := s.slots[idx]
		if slot.state == 0 {
			return false
		}
		if slot.dirKey == dirKey && slot.volID == volID {
			s.deleteAtLocked(idx)
			return true
		}
		idx = (idx + 1) & s.mask
	}
}

func (s *PrunedDirSet) deleteAtLocked(idx uint64) {
	hole := idx
	next := (hole + 1) & s.mask
	for s.slots[next].state != 0 {
		ideal := hashVolDirKey(s.slots[next].volID, s.slots[next].dirKey) & s.mask
		if ((hole - ideal) & s.mask) < ((next - ideal) & s.mask) {
			s.slots[hole] = s.slots[next]
			hole = next
		}
		next = (next + 1) & s.mask
	}
	s.slots[hole] = prunedSlot{}
	s.count--
}

// ClearVolume removes all entries belonging to volID without affecting other volumes.
// Returns the number of entries removed.
func (s *PrunedDirSet) ClearVolume(volID uint16) int {
	s.mu.Lock()
	removed := s.clearVolumeLocked(volID)
	s.mu.Unlock()
	return removed
}

func (s *PrunedDirSet) clearVolumeLocked(volID uint16) int {
	if s.count == 0 {
		return 0
	}
	removed := 0
	for {
		passRemoved := 0
		for i := uint64(0); i < uint64(len(s.slots)); {
			slot := s.slots[i]
			if slot.state != 0 && slot.volID == volID {
				s.deleteAtLocked(i)
				passRemoved++
				continue
			}
			i++
		}
		removed += passRemoved
		if passRemoved == 0 {
			break
		}
	}
	return removed
}

// ReplaceVolume atomically replaces all entries for volID with entries belonging to volID from src.
// Other volumes in s remain untouched.
func (s *PrunedDirSet) ReplaceVolume(volID uint16, src *PrunedDirSet) {
	if src == s {
		return
	}
	var incoming []prunedSlot
	if src != nil {
		src.mu.RLock()
		for i := range src.slots {
			slot := src.slots[i]
			if slot.state != 0 && slot.volID == volID {
				incoming = append(incoming, slot)
			}
		}
		src.mu.RUnlock()
	}

	s.mu.Lock()
	s.clearVolumeLocked(volID)
	for i := range incoming {
		s.setLocked(volID, incoming[i].dirKey, incoming[i].flags, false)
	}
	s.mu.Unlock()
}

// ForEach iterates all occupied entries under a read lock until fn returns false.
func (s *PrunedDirSet) ForEach(fn func(volID uint16, dirKey uint64, flags DirFlags) bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for i := range s.slots {
		slot := s.slots[i]
		if slot.state != 0 {
			if !fn(slot.volID, slot.dirKey, slot.flags) {
				return
			}
		}
	}
}

func (s *PrunedDirSet) growLocked() {
	oldSlots := s.slots
	newSize := len(oldSlots) * 2
	s.slots = make([]prunedSlot, newSize)
	s.mask = uint64(newSize - 1)
	s.count = 0
	for i := range oldSlots {
		slot := oldSlots[i]
		if slot.state != 0 {
			s.setLocked(slot.volID, slot.dirKey, slot.flags, false)
		}
	}
}
