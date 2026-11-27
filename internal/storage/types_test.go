package storage

import (
	"reflect"
	"testing"
	"unsafe"
)

func assertNoPointers(t *testing.T, typ reflect.Type) {
	t.Helper()
	switch typ.Kind() {
	case reflect.Pointer, reflect.UnsafePointer, reflect.Slice, reflect.Map, reflect.Chan, reflect.Func, reflect.Interface, reflect.String:
		t.Fatalf("type %s contains Go pointer kind %s (violates noscan zero-GC invariant)", typ, typ.Kind())
	case reflect.Struct:
		for i := 0; i < typ.NumField(); i++ {
			assertNoPointers(t, typ.Field(i).Type)
		}
	case reflect.Array:
		assertNoPointers(t, typ.Elem())
	}
}

func TestHotNodeLayoutAndZeroGC(t *testing.T) {
	if got := unsafe.Sizeof(HotNode{}); got != HotNodeSize {
		t.Fatalf("unsafe.Sizeof(HotNode) = %d, want %d", got, HotNodeSize)
	}
	assertNoPointers(t, reflect.TypeOf(HotNode{}))
	assertNoPointers(t, reflect.TypeOf(ColdMeta{}))

	if FlagDir != 1<<0 || FlagDeleted != 1<<1 || FlagHidden != 1<<2 ||
		FlagNonASCII != 1<<3 || FlagSymlink != 1<<4 || FlagFolded != 1<<5 || FlagSystem != 1<<6 {
		t.Fatalf("unexpected flag bit layout")
	}

	if Tier1Mmap.String() != "tier1-mmap" || Tier2LSM.String() != "tier2-lsm" {
		t.Fatalf("unexpected StorageTier strings: %q, %q", Tier1Mmap.String(), Tier2LSM.String())
	}

	if !HasNonASCII("设计文档.md") || HasNonASCII("report_2026.pdf") {
		t.Fatalf("HasNonASCII mismatch")
	}
}
