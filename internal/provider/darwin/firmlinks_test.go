package darwin

import "testing"

func TestParseFirmlinksAndDeduper(t *testing.T) {
	sample := `
# macOS APFS firmlinks
/AppleInternal	AppleInternal
/Applications	Applications
/Library	Library
/Library/Application Support	Library/Application Support
/System/Library/Caches	System/Library/Caches
/Users	Users
/usr/local	usr/local
/opt	opt
`
	links := ParseFirmlinks(sample)
	if len(links) != 8 {
		t.Fatalf("parsed %d firmlinks, want 8: %+v", len(links), links)
	}

	d := NewFirmlinkDeduper(links)

	// Duplicate data-volume paths (including spaces!) corresponding to a firmlink MUST be skipped.
	for _, dup := range []string{
		"/System/Volumes/Data/Users",
		"/System/Volumes/Data/Users/alice/docs",
		"/System/Volumes/Data/Applications",
		"/System/Volumes/Data/Library/Application Support/Xift",
		"/System/Volumes/Data/usr/local/bin",
	} {
		if !d.IsDuplicateDataPath(dup) {
			t.Errorf("IsDuplicateDataPath(%q) = false, want true", dup)
		}
	}

	// Canonical paths MUST NOT be treated as duplicates.
	for _, keep := range []string{
		"/Users",
		"/Users/alice/docs",
		"/Applications/Xcode.app",
		"/System/Volumes/Data/private/var/log",
		"/System/Volumes/Data/UsersExtra",
	} {
		if d.IsDuplicateDataPath(keep) {
			t.Errorf("IsDuplicateDataPath(%q) = true, want false", keep)
		}
	}

	// Canonicalize maps /System/Volumes/Data/<target>/... -> /<source>/...
	if got := d.Canonicalize("/System/Volumes/Data/Users/alice/a.go"); got != "/Users/alice/a.go" {
		t.Fatalf("Canonicalize = %q, want %q", got, "/Users/alice/a.go")
	}
	if got := d.Canonicalize("/System/Volumes/Data/Users"); got != "/Users" {
		t.Fatalf("Canonicalize = %q, want %q", got, "/Users")
	}
}

func TestIsTimeMachineSnapshotPath(t *testing.T) {
	shouldSkip := []string{
		"/.MobileBackups",
		"/.MobileBackups/Computer/2026-11-27",
		"/Volumes/.timemachine",
		"/Volumes/.timemachine/ABC-123/2026-11-27.backup",
		"/Volumes/com.apple.TimeMachine.localsnapshots/Backups.backupdb",
	}
	for _, p := range shouldSkip {
		if !IsTimeMachineSnapshotPath(p) {
			t.Errorf("IsTimeMachineSnapshotPath(%q) = false, want true", p)
		}
	}

	shouldKeep := []string{
		"/Users/alice/Documents/timemachine_notes.md",
		"/Volumes/ExternalSSD/Projects",
	}
	for _, p := range shouldKeep {
		if IsTimeMachineSnapshotPath(p) {
			t.Errorf("IsTimeMachineSnapshotPath(%q) = true, want false", p)
		}
	}
}
