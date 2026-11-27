package filter

import (
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"unsafe"
)

func assertTypePointerFree(t *testing.T, typ reflect.Type) {
	t.Helper()
	switch typ.Kind() {
	case reflect.Pointer, reflect.UnsafePointer, reflect.String, reflect.Slice,
		reflect.Map, reflect.Chan, reflect.Func, reflect.Interface:
		t.Fatalf("type %v contains Go pointer kind %v (violates noscan zero-GC invariant)", typ, typ.Kind())
	case reflect.Struct:
		for i := 0; i < typ.NumField(); i++ {
			assertTypePointerFree(t, typ.Field(i).Type)
		}
	case reflect.Array:
		assertTypePointerFree(t, typ.Elem())
	}
}

func TestPrunedDirSetSlotAlignmentAndZeroPointers(t *testing.T) {
	if got := unsafe.Sizeof(prunedSlot{}); got != 16 {
		t.Fatalf("unsafe.Sizeof(prunedSlot{}) = %d, want 16", got)
	}
	assertTypePointerFree(t, reflect.TypeOf(prunedSlot{}))
}

func TestPrunedDirSetRandomizedEquivalence(t *testing.T) {
	set := NewPrunedDirSet(16)
	type key struct {
		vol uint16
		dir uint64
	}
	ref := make(map[key]DirFlags)
	rng := rand.New(rand.NewSource(42))

	const ops = 50_000
	for i := 0; i < ops; i++ {
		vol := uint16(rng.Intn(4))
		dir := uint64(rng.Intn(2000) + 1)
		k := key{vol: vol, dir: dir}

		switch rng.Intn(4) {
		case 0: // Set
			fl := DirFlags(1 << uint(rng.Intn(4)))
			set.Set(vol, dir, fl)
			ref[k] = fl
		case 1: // AddFlags
			fl := DirFlags(1 << uint(rng.Intn(4)))
			set.AddFlags(vol, dir, fl)
			ref[k] |= fl
		case 2: // Remove
			got := set.Remove(vol, dir)
			_, existed := ref[k]
			delete(ref, k)
			if got != existed {
				t.Fatalf("step %d Remove(%d,%d) = %v, want %v", i, vol, dir, got, existed)
			}
		case 3: // GetFlags
			gotFl, gotOK := set.GetFlags(vol, dir)
			wantFl, wantOK := ref[k]
			if gotOK != wantOK || gotFl != wantFl {
				t.Fatalf("step %d GetFlags(%d,%d) = (%v,%v), want (%v,%v)", i, vol, dir, gotFl, gotOK, wantFl, wantOK)
			}
		}
	}

	if set.Len() != len(ref) {
		t.Fatalf("final Len() = %d, want %d", set.Len(), len(ref))
	}
	for k, wantFl := range ref {
		gotFl, ok := set.GetFlags(k.vol, k.dir)
		if !ok || gotFl != wantFl {
			t.Fatalf("final verify (%d,%d) = (%v,%v), want (%v,true)", k.vol, k.dir, gotFl, ok, wantFl)
		}
	}
}

func TestPrunedDirSetMemoryBudget50K(t *testing.T) {
	const count = 50_000
	set := NewPrunedDirSet(count)
	for i := uint64(1); i <= count; i++ {
		set.Set(1, i*17, DirFlagPrunedDescendant)
	}
	if set.Len() != count {
		t.Fatalf("Len() = %d, want %d", set.Len(), count)
	}
	mem := set.MemoryBytes()
	const budget = 2 * 1024 * 1024 // < 2 MB
	if mem >= budget {
		t.Fatalf("50K entries MemoryBytes() = %d (%.2f MB), want < 2 MB", mem, float64(mem)/(1024*1024))
	}
	t.Logf("PrunedDirSet 50,000 entries backing array: %d bytes (%.2f MB)", mem, float64(mem)/(1024*1024))
}

func TestPrunedDirSetVolumeIsolationAndReplace(t *testing.T) {
	set := NewPrunedDirSet(32)
	set.Set(1, 100, DirFlagFoldedShell)
	set.Set(1, 101, DirFlagPrunedDescendant)
	set.Set(2, 100, DirFlagHardExcluded)
	set.Set(2, 200, DirFlagSystem)

	removed := set.ClearVolume(1)
	if removed != 2 {
		t.Fatalf("ClearVolume(1) removed %d, want 2", removed)
	}
	if set.IsPruned(1, 100) || set.IsPruned(1, 101) {
		t.Errorf("vol 1 entries should be cleared")
	}
	if !set.IsPruned(2, 100) || !set.IsSystem(2, 200) {
		t.Errorf("vol 2 entries should remain intact after ClearVolume(1)")
	}

	replacement := NewPrunedDirSet(16)
	replacement.Set(2, 300, DirFlagFoldedShell|DirFlagSystem)
	replacement.Set(9, 999, DirFlagHardExcluded) // different vol, must not leak into ReplaceVolume(2)

	set.ReplaceVolume(2, replacement)
	if set.IsPruned(2, 100) || set.IsSystem(2, 200) {
		t.Errorf("old vol 2 entries should be replaced")
	}
	fl, ok := set.GetFlags(2, 300)
	if !ok || !fl.IsPruned() || !fl.IsSystem() {
		t.Errorf("new vol 2 entry = (%v,%v), want FoldedShell|System", fl, ok)
	}
	if set.IsPruned(9, 999) {
		t.Errorf("ReplaceVolume(2) unexpectedly imported vol 9 entry")
	}
}

func TestPrunedDirSetConcurrentReadWrite(t *testing.T) {
	set := NewPrunedDirSet(1024)
	for i := uint64(1); i <= 500; i++ {
		set.Set(1, i, DirFlagPrunedDescendant)
	}

	var readerWG sync.WaitGroup
	var writerWG sync.WaitGroup
	stop := make(chan struct{})

	for r := 0; r < 4; r++ {
		readerWG.Add(1)
		go func() {
			defer readerWG.Done()
			for i := uint64(1); ; i++ {
				select {
				case <-stop:
					return
				default:
					k := (i % 1000) + 1
					_ = set.IsPruned(1, k)
					_ = set.IsSystem(2, k)
					_, _ = set.GetFlags(1, k)
				}
			}
		}()
	}

	for w := 0; w < 2; w++ {
		writerWG.Add(1)
		go func(vol uint16) {
			defer writerWG.Done()
			for i := uint64(1); i <= 4000; i++ {
				set.Set(vol, i%1000+1, DirFlagPrunedDescendant)
				if i%3 == 0 {
					set.Remove(vol, (i/3)%1000+1)
				}
				if i%500 == 0 && vol == 2 {
					set.ClearVolume(2)
				}
			}
		}(uint16(w + 1))
	}

	writerWG.Wait()
	close(stop)
	readerWG.Wait()
}
