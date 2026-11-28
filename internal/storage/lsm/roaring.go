// Package lsm implements the Tier 2 disk-LSM storage primitives referenced by
// Xift Architecture V2.1 Section 3.3: Roaring-compatible posting bitmaps,
// Front-Coded sorted name blocks, a fixed-budget sharded LRU block cache, and
// trigram/bigram inverted index construction and lookup.
//
// All bulk data structures are flat, pointer-free byte/word slices so the Go
// runtime keeps high-cardinality payloads in noscan heap spans (architecture
// rule 5).
package lsm

import (
	"encoding/binary"
	"errors"
	"math/bits"
	"slices"
)

const (
	// roaringArrayMaxCard is the Roaring portable-format threshold: containers
	// with cardinality <= 4096 use array encoding, denser containers use a
	// 1024-word bitmap.
	roaringArrayMaxCard = 4096
	// roaringBitmapWords is the number of uint64 words in one bitmap container.
	roaringBitmapWords = 1024
	// roaringBitmapBytes is the serialized size of one bitmap container payload.
	roaringBitmapBytes = roaringBitmapWords * 8
)

// ErrRoaringFormat reports a structurally invalid Roaring byte stream.
var ErrRoaringFormat = errors.New("lsm: corrupt roaring bitmap")

// RoaringBuilder incrementally serializes one posting list in the official
// Roaring portable no-cookie format:
//
//	(key uint16, card-1 uint16, payload) triples in ascending key order where
//	payload is card uint16 LE values (array container, card <= 4096) or 1024
//	uint64 LE words (bitmap container).
//
// Values MUST be added in strictly ascending order, which lets the builder
// keep only one open container in memory regardless of posting-list size.
type RoaringBuilder struct {
	buf     []byte
	curKey  uint16
	card    int
	arrVals []uint16
	words   []uint64 // reused 1024-word scratch once array->bitmap conversion happened
	inBmp   bool
	total   uint64
	last    uint32
	hasLast bool
	fin     bool
}

// Add appends value v. It returns an error when v is not strictly greater
// than the previously added value.
func (b *RoaringBuilder) Add(v uint32) error {
	if b.fin {
		return errors.New("lsm: roaring builder already finished")
	}
	if b.hasLast && v <= b.last {
		return errors.New("lsm: roaring add out of order or duplicate")
	}
	key := uint16(v >> 16)
	low := uint16(v)

	if !b.hasLast || key != b.curKey {
		b.closeContainer()
		b.curKey = key
		b.card = 0
		b.inBmp = false
		b.arrVals = b.arrVals[:0]
	}
	if b.inBmp {
		b.words[low>>6] |= 1 << (low & 63)
	} else if b.card < roaringArrayMaxCard {
		b.arrVals = append(b.arrVals, low)
	} else {
		// Dense threshold crossed: convert the open array container to a bitmap.
		if cap(b.words) >= roaringBitmapWords {
			b.words = b.words[:roaringBitmapWords]
			clear(b.words)
		} else {
			b.words = make([]uint64, roaringBitmapWords)
		}
		for _, lo := range b.arrVals {
			b.words[lo>>6] |= 1 << (lo & 63)
		}
		b.words[low>>6] |= 1 << (low & 63)
		b.arrVals = b.arrVals[:0]
		b.inBmp = true
	}
	b.card++
	b.total++
	b.last = v
	b.hasLast = true
	return nil
}

// Len returns the number of values added so far.
func (b *RoaringBuilder) Len() int { return int(b.total) }

func (b *RoaringBuilder) closeContainer() {
	if b.card == 0 {
		return
	}
	b.buf = binary.LittleEndian.AppendUint16(b.buf, b.curKey)
	b.buf = binary.LittleEndian.AppendUint16(b.buf, uint16(b.card-1))
	if b.inBmp {
		for i := 0; i < roaringBitmapWords; i++ {
			b.buf = binary.LittleEndian.AppendUint64(b.buf, b.words[i])
		}
	} else {
		for _, lo := range b.arrVals {
			b.buf = binary.LittleEndian.AppendUint16(b.buf, lo)
		}
	}
}

// Spill returns the containers completed so far and releases that buffer,
// keeping the open container and ordering state intact. Because container keys
// only move forward, concatenating spills in order reproduces the exact stream
// Finish would have produced.
func (b *RoaringBuilder) Spill() []byte {
	if b.fin {
		return nil
	}
	out := b.buf
	b.buf = nil
	return out
}

// Finish closes the final container and returns the serialized Roaring stream
// plus the total cardinality. The builder must not be used afterwards.
func (b *RoaringBuilder) Finish() ([]byte, uint64) {
	if !b.fin {
		b.closeContainer()
		b.fin = true
	}
	return b.buf, b.total
}

// RoaringView is a parsed, read-only view over a serialized Roaring stream.
// Parsing validates structure and records per-container offsets; payloads are
// decoded on demand directly from the backing bytes (zero-copy reads).
type RoaringView struct {
	data  []byte
	keys  []uint16
	card1 []uint16 // cardinality - 1 per container
	offs  []uint32 // payload offset per container
}

// ParseRoaring validates a Roaring portable no-cookie stream and returns a view.
func ParseRoaring(raw []byte) (*RoaringView, error) {
	v := &RoaringView{data: raw}
	pos := 0
	prevKey := -1
	for pos < len(raw) {
		if len(raw)-pos < 4 {
			return nil, ErrRoaringFormat
		}
		key := int(binary.LittleEndian.Uint16(raw[pos:]))
		c1 := int(binary.LittleEndian.Uint16(raw[pos+2:]))
		card := c1 + 1
		payload := 2 * card
		if card > roaringArrayMaxCard {
			payload = roaringBitmapBytes
		}
		if pos+4+payload > len(raw) {
			return nil, ErrRoaringFormat
		}
		if key <= prevKey {
			return nil, ErrRoaringFormat
		}
		v.keys = append(v.keys, uint16(key))
		v.card1 = append(v.card1, uint16(c1))
		v.offs = append(v.offs, uint32(pos+4))
		prevKey = key
		pos += 4 + payload
	}
	return v, nil
}

// Cardinality returns the number of values in the bitmap.
func (v *RoaringView) Cardinality() uint64 {
	var total uint64
	for _, c1 := range v.card1 {
		total += uint64(c1) + 1
	}
	return total
}

// ContainerCount returns the number of 16-bit containers.
func (v *RoaringView) ContainerCount() int { return len(v.keys) }

// containerCard returns the cardinality of container i.
func (v *RoaringView) containerCard(i int) int { return int(v.card1[i]) + 1 }

// containerIsArray reports whether container i uses array encoding.
func (v *RoaringView) containerIsArray(i int) bool {
	return v.containerCard(i) <= roaringArrayMaxCard
}

// findContainer returns the index of the container with the given key, or -1.
func (v *RoaringView) findContainer(key uint16) int {
	i, ok := slices.BinarySearch(v.keys, key)
	if !ok {
		return -1
	}
	return i
}

// arrayContains reports whether the array container i contains low.
func (v *RoaringView) arrayContains(i int, low uint16) bool {
	off := int(v.offs[i])
	card := v.containerCard(i)
	lo, hi := 0, card
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		val := binary.LittleEndian.Uint16(v.data[off+2*mid:])
		if val < low {
			lo = mid + 1
		} else if val > low {
			hi = mid
		} else {
			return true
		}
	}
	return false
}

// bitmapBit reports whether bit low is set in bitmap container i.
func (v *RoaringView) bitmapBit(i int, low uint16) bool {
	off := int(v.offs[i])
	word := binary.LittleEndian.Uint64(v.data[off+8*int(low>>6):])
	return word&(1<<(low&63)) != 0
}

// Contains reports whether value x is present.
func (v *RoaringView) Contains(x uint32) bool {
	i := v.findContainer(uint16(x >> 16))
	if i < 0 {
		return false
	}
	low := uint16(x)
	if v.containerIsArray(i) {
		return v.arrayContains(i, low)
	}
	return v.bitmapBit(i, low)
}

// ForEach invokes fn for every value in ascending order until fn returns false.
// It reports whether iteration stopped early.
func (v *RoaringView) ForEach(fn func(uint32) bool) bool {
	for i, key := range v.keys {
		base := uint32(key) << 16
		off := int(v.offs[i])
		if v.containerIsArray(i) {
			card := v.containerCard(i)
			for j := 0; j < card; j++ {
				low := binary.LittleEndian.Uint16(v.data[off+2*j:])
				if !fn(base | uint32(low)) {
					return true
				}
			}
		} else {
			for w := 0; w < roaringBitmapWords; w++ {
				word := binary.LittleEndian.Uint64(v.data[off+8*w:])
				for word != 0 {
					bit := bits.TrailingZeros64(word)
					word &= word - 1
					if !fn(base | uint32(w*64+bit)) {
						return true
					}
				}
			}
		}
	}
	return false
}

// intersect2 streams the intersection of two bitmaps into fn in
// ascending order. It reports whether iteration stopped early (fn returned
// false). Containers absent from either side are skipped wholesale.
func intersect2(a, b *RoaringView, fn func(uint32) bool) bool {
	views := [2]*RoaringView{a, b}
	return IntersectAll(views[:], fn)
}

// IntersectAll streams the intersection of all views into fn in
// ascending order, iterating from the smallest-cardinality view. It reports
// whether iteration stopped early.
func IntersectAll(views []*RoaringView, fn func(uint32) bool) bool {
	if len(views) == 0 {
		return false
	}
	base := views[0]
	for _, v := range views[1:] {
		if v.Cardinality() < base.Cardinality() {
			base = v
		}
	}

	// Stack-resident container index scratch keeps the hot path allocation-free.
	var idxsBuf [32]int
	idxs := idxsBuf[:len(views)]
	if len(views) > len(idxsBuf) {
		idxs = make([]int, len(views))
	}

	var scratch [roaringBitmapWords]uint64
	for _, key := range base.keys {
		// Bucket-level pruning: every view must have a container for this key.
		ok := true
		for vi, v := range views {
			i := v.findContainer(key)
			if i < 0 {
				ok = false
				break
			}
			idxs[vi] = i
		}
		if !ok {
			continue
		}

		baseHi := uint32(key) << 16

		// Partition containers into bitmap and array encodings; track the
		// smallest array container as candidate iteration source.
		bmpViews := 0
		minArrView, minArrIdx := -1, -1
		var minArrCard int
		for vi, v := range views {
			if v.containerIsArray(idxs[vi]) {
				card := v.containerCard(idxs[vi])
				if minArrView == -1 || card < minArrCard {
					minArrView, minArrIdx, minArrCard = vi, idxs[vi], card
				}
			} else {
				bmpViews++
			}
		}

		if bmpViews == 0 {
			// All containers are arrays: iterate the smallest, binary-search the rest.
			src := views[minArrView]
			off := int(src.offs[minArrIdx])
			card := src.containerCard(minArrIdx)
			for j := 0; j < card; j++ {
				low := binary.LittleEndian.Uint16(src.data[off+2*j:])
				if inAllArrays(views, idxs, minArrView, low) {
					if !fn(baseHi | uint32(low)) {
						return true
					}
				}
			}
			continue
		}

		// Fold all bitmap containers word-wise into scratch (first assigns,
		// the rest AND in place).
		clear(scratch[:])
		first := true
		for vi, v := range views {
			if v.containerIsArray(idxs[vi]) {
				continue
			}
			off := int(v.offs[idxs[vi]])
			if first {
				for w := 0; w < roaringBitmapWords; w++ {
					scratch[w] = binary.LittleEndian.Uint64(v.data[off+8*w:])
				}
				first = false
			} else {
				for w := 0; w < roaringBitmapWords; w++ {
					scratch[w] &= binary.LittleEndian.Uint64(v.data[off+8*w:])
				}
			}
		}

		if minArrView >= 0 {
			// Iterate the smallest array container, testing the folded bitmap
			// words and every other array container.
			src := views[minArrView]
			off := int(src.offs[minArrIdx])
			card := src.containerCard(minArrIdx)
			for j := 0; j < card; j++ {
				low := binary.LittleEndian.Uint16(src.data[off+2*j:])
				if scratch[low>>6]&(1<<(low&63)) == 0 {
					continue
				}
				if inAllArrays(views, idxs, minArrView, low) {
					if !fn(baseHi | uint32(low)) {
						return true
					}
				}
			}
		} else {
			// No array containers: emit bits straight from the folded words.
			for w := 0; w < roaringBitmapWords; w++ {
				word := scratch[w]
				for word != 0 {
					bit := bits.TrailingZeros64(word)
					word &= word - 1
					low := uint16(w*64 + bit)
					if !fn(baseHi | uint32(low)) {
						return true
					}
				}
			}
		}
	}
	return false
}

// inAllArrays reports whether low is present in every array container except
// skipView's (bitmap containers are covered by the folded scratch words).
func inAllArrays(views []*RoaringView, idxs []int, skipView int, low uint16) bool {
	for vi, v := range views {
		if vi == skipView || !v.containerIsArray(idxs[vi]) {
			continue
		}
		if !v.arrayContains(idxs[vi], low) {
			return false
		}
	}
	return true
}
