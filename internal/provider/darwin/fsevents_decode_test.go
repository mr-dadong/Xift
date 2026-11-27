package darwin

import (
	"errors"
	"testing"

	"github.com/mr-dadong/Xift/internal/provider"
)

func TestNormalizeFSEventsBatch(t *testing.T) {
	deduper := NewFirmlinkDeduper(ParseFirmlinks("/Users\tUsers\n"))

	raw := []RawFSEvent{
		{
			EventID: 100,
			Path:    "/System/Volumes/Data/Users/alice/a.go",
			Flags:   FSEventFlagItemCreated | FSEventFlagItemIsFile,
		},
		{
			EventID: 101,
			Path:    "/Users/alice/b.go",
			Flags:   FSEventFlagItemModified | FSEventFlagItemIsFile,
		},
		{
			EventID: 102,
			Path:    "/Users/alice/old.go",
			Flags:   FSEventFlagItemRenamed | FSEventFlagItemIsFile,
		},
		{
			EventID: 103,
			Path:    "/Users/alice/new.go",
			Flags:   FSEventFlagItemRenamed | FSEventFlagItemIsFile,
		},
		{
			EventID: 104,
			Path:    "/Users/alice/gone.go",
			Flags:   FSEventFlagItemRemoved | FSEventFlagItemIsFile,
		},
		{
			EventID: 105,
			Path:    "/Users/alice/overflow_dir",
			Flags:   FSEventFlagMustScanSubDirs,
		},
		{
			EventID: 106,
			Path:    "/Volumes/.timemachine/snap/x",
			Flags:   FSEventFlagItemCreated | FSEventFlagItemIsFile,
		},
	}

	// Simulate disk existence check: only new.go exists after rename pair.
	exists := map[string]bool{
		"/Users/alice/a.go":   true,
		"/Users/alice/b.go":   true,
		"/Users/alice/new.go": true,
	}

	events, err := NormalizeFSEvents(1, "/", raw, deduper, func(p string) (nodeKey, parentKey uint64, size, mtime int64, isDir, ok bool) {
		if !exists[p] {
			return 0, 0, 0, 0, false, false
		}
		return hashPathKey(p), hashPathKey("/Users/alice"), 123, 456, false, true
	})
	if err != nil {
		t.Fatalf("NormalizeFSEvents: %v", err)
	}

	// Expect: Create(a.go canonicalized), Modify(b.go), Rename(old.go -> new.go), Delete(gone.go), RescanDir(overflow_dir).
	// Time Machine event must be skipped.
	if len(events) != 5 {
		t.Fatalf("got %d events, want 5: %+v", len(events), events)
	}
	if events[0].Type != provider.EventCreate || events[0].NewName != "a.go" || events[0].RelPath != "Users/alice/a.go" {
		t.Fatalf("events[0] = %+v, want canonicalized Create a.go", events[0])
	}
	if events[1].Type != provider.EventModifyMeta || events[1].NewName != "b.go" {
		t.Fatalf("events[1] = %+v, want ModifyMeta b.go", events[1])
	}
	if events[2].Type != provider.EventRename || events[2].OldPath != "/Users/alice/old.go" || events[2].NewName != "new.go" {
		t.Fatalf("events[2] = %+v, want paired Rename old.go -> new.go", events[2])
	}
	if events[3].Type != provider.EventDelete || events[3].NewName != "gone.go" {
		t.Fatalf("events[3] = %+v, want Delete gone.go", events[3])
	}
	if events[4].Type != provider.EventRescanDir || events[4].OldPath != "/Users/alice/overflow_dir" {
		t.Fatalf("events[4] = %+v, want RescanDir overflow_dir", events[4])
	}
}

func TestNormalizeFSEventsWrappedReturnsErrCursorExpired(t *testing.T) {
	_, err := NormalizeFSEvents(1, "/", []RawFSEvent{
		{EventID: 999, Path: "/", Flags: FSEventFlagEventIdsWrapped},
	}, nil, nil)
	if !errors.Is(err, provider.ErrCursorExpired) {
		t.Fatalf("err = %v, want provider.ErrCursorExpired", err)
	}

	// M2 regression: EventIdsWrapped on the second item of a paired rename MUST NOT be skipped.
	_, err = NormalizeFSEvents(1, "/", []RawFSEvent{
		{EventID: 1000, Path: "/Users/alice/old.go", Flags: FSEventFlagItemRenamed | FSEventFlagItemIsFile},
		{EventID: 1001, Path: "/Users/alice/new.go", Flags: FSEventFlagItemRenamed | FSEventFlagEventIdsWrapped},
	}, nil, nil)
	if !errors.Is(err, provider.ErrCursorExpired) {
		t.Fatalf("wrapped on 2nd rename item err = %v, want ErrCursorExpired", err)
	}
}

func TestRegressionC6RealAPFSInodesPreservedAcrossDeleteAndRename(t *testing.T) {
	cache := NewPathKeyCache()
	// Pre-populate realistic integer APFS inodes as FullScan would.
	cache.Put("/Users/alice", CachedNode{NodeKey: 500, ParentKey: 10, IsDir: true})
	cache.Put("/Users/alice/old.go", CachedNode{NodeKey: 1001, ParentKey: 500, IsDir: false})
	cache.Put("/Users/alice/gone.go", CachedNode{NodeKey: 1002, ParentKey: 500, IsDir: false})

	statFn := func(p string) (nodeKey, parentKey uint64, size, mtime int64, isDir, ok bool) {
		switch p {
		case "/Users/alice":
			return 500, 10, 0, 0, true, true
		case "/Users/alice/new.go":
			return 1001, 500, 42, 999, false, true
		default:
			// Simulate realistic lstat on deleted path: ok=false, parentKey=500 still resolved!
			return 0, 500, 0, 0, false, false
		}
	}

	events, err := NormalizeFSEventsWithCache(1, "/", []RawFSEvent{
		{EventID: 10, Path: "/Users/alice/old.go", Flags: FSEventFlagItemRenamed | FSEventFlagItemIsFile},
		{EventID: 11, Path: "/Users/alice/new.go", Flags: FSEventFlagItemRenamed | FSEventFlagItemIsFile},
		{EventID: 12, Path: "/Users/alice/gone.go", Flags: FSEventFlagItemRemoved | FSEventFlagItemIsFile},
	}, nil, cache, statFn)
	if err != nil {
		t.Fatalf("NormalizeFSEventsWithCache: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2: %+v", len(events), events)
	}
	// Same-dir rename MUST have identical real integer inodes for ParentKey and NewParentKey!
	if events[0].Type != provider.EventRename || events[0].NodeKey != 1001 || events[0].ParentKey != 500 || events[0].NewParentKey != 500 {
		t.Fatalf("rename event = %+v, want NodeKey=1001 ParentKey=500 NewParentKey=500", events[0])
	}
	// Delete MUST resolve real cached inode 1002 and real parent inode 500 (not FNV hashes!).
	if events[1].Type != provider.EventDelete || events[1].NodeKey != 1002 || events[1].ParentKey != 500 {
		t.Fatalf("delete event = %+v, want NodeKey=1002 ParentKey=500", events[1])
	}
}
