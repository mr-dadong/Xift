package storage

import (
	"math/rand/v2"
	"reflect"
	"testing"
	"unsafe"
)

func TestKeyMapZeroGCAndRandomizedEquivalence(t *testing.T) {
	if got := unsafe.Sizeof(keySlot{}); got != 16 {
		t.Fatalf("unsafe.Sizeof(keySlot) = %d, want 16", got)
	}
	assertNoPointers(t, reflect.TypeOf(keySlot{}))
	assertNoPointers(t, reflect.TypeOf(u32Slot{}))
	assertNoPointers(t, reflect.TypeOf(coldOverrideSlot{}))

	km := NewKeyMap(16)
	type volKey struct {
		vol uint16
		key uint64
	}
	ref := make(map[volKey]uint32, 20000)

	// Volume isolation: same nodeKey on different VolIDs must not collide.
	km.Put(1, 42, 100)
	km.Put(2, 42, 200)
	if v, ok := km.Get(1, 42); !ok || v != 100 {
		t.Fatalf("Get(1,42) = (%d,%v), want (100,true)", v, ok)
	}
	if v, ok := km.Get(2, 42); !ok || v != 200 {
		t.Fatalf("Get(2,42) = (%d,%v), want (200,true)", v, ok)
	}
	km.Clear()

	rng := rand.New(rand.NewPCG(2026, 1127))
	const ops = 100_000
	const keySpace = 20_000
	for i := 0; i < ops; i++ {
		vol := uint16(rng.IntN(4) + 1)
		key := uint64(rng.IntN(keySpace))
		k := volKey{vol: vol, key: key}

		switch rng.IntN(10) {
		case 0, 1, 2, 3, 4: // Put/update
			val := uint32(i)
			km.Put(vol, key, val)
			ref[k] = val
		case 5, 6: // Delete (backward-shift)
			gotDel := km.Delete(vol, key)
			_, existed := ref[k]
			delete(ref, k)
			if gotDel != existed {
				t.Fatalf("op %d Delete(%d,%d) = %v, want %v", i, vol, key, gotDel, existed)
			}
		default: // Get
			gotVal, gotOK := km.Get(vol, key)
			wantVal, wantOK := ref[k]
			if gotOK != wantOK || (wantOK && gotVal != wantVal) {
				t.Fatalf("op %d Get(%d,%d) = (%d,%v), want (%d,%v)", i, vol, key, gotVal, gotOK, wantVal, wantOK)
			}
		}
	}

	if km.Len() != len(ref) {
		t.Fatalf("Len() = %d, want %d", km.Len(), len(ref))
	}
	for k, want := range ref {
		got, ok := km.Get(k.vol, k.key)
		if !ok || got != want {
			t.Fatalf("final check Get(%d,%d) = (%d,%v), want (%d,true)", k.vol, k.key, got, ok, want)
		}
	}

	seen := 0
	km.ForEach(func(volID uint16, nodeKey uint64, val uint32) bool {
		seen++
		if want, ok := ref[volKey{vol: volID, key: nodeKey}]; !ok || want != val {
			t.Fatalf("ForEach unexpected (%d,%d)=%d", volID, nodeKey, val)
		}
		return true
	})
	if seen != len(ref) {
		t.Fatalf("ForEach visited %d, want %d", seen, len(ref))
	}
}
