package filter

import (
	"testing"
)

func boolPtr(v bool) *bool { return &v }

func TestPresetThreeLevelActionsAndRootAnchoring(t *testing.T) {
	eng := NewRuleEngine(RuleEngineConfig{
		CaseInsensitive: boolPtr(true),
	})

	// Level 0: HardExclude dirs and files
	for _, dir := range []string{"GPUCache", "Cache_Data", "$Recycle.Bin", "__pycache__", "overlay2", "Media Cache Files"} {
		if got := eng.ClassifyDir(dir, "Users/dev/"+dir, 3); got != ActionHardExclude {
			t.Errorf("ClassifyDir(%q) = %v, want ActionHardExclude", dir, got)
		}
	}
	for _, file := range []string{"draft.tmp", "backup.BAK", "vim.swp", "$MFT", "crash.dmp"} {
		if got := eng.ClassifyFile(file, "Users/dev/"+file); got != ActionHardExclude {
			t.Errorf("ClassifyFile(%q) = %v, want ActionHardExclude", file, got)
		}
	}

	// Level 1: Fold dirs
	for _, dir := range []string{"node_modules", ".git", "target", ".venv", "vendor", "Pods", ".next"} {
		if got := eng.ClassifyDir(dir, "work/app/"+dir, 3); got != ActionFold {
			t.Errorf("ClassifyDir(%q) = %v, want ActionFold", dir, got)
		}
	}

	// Level 2: Root-anchored OS system dirs (depth == 1)
	for _, rootDir := range []string{"Windows", "Program Files", "System", "Library", "usr", "var", "lib"} {
		if got := eng.ClassifyDir(rootDir, rootDir, 1); got != ActionSoftDemote {
			t.Errorf("root ClassifyDir(%q, depth=1) = %v, want ActionSoftDemote", rootDir, got)
		}
	}

	// Deep user project dirs with same names as root OS dirs MUST NOT be soft-demoted
	for _, tc := range []struct {
		name string
		path string
	}{
		{"lib", "projects/myapp/lib"},
		{"Library", "projects/myapp/Library"},
		{"var", "projects/myapp/var"},
		{"etc", "projects/myapp/etc"},
		{"System", "projects/game/System"},
		{"Windows", "projects/ui/Windows"},
	} {
		if got := eng.ClassifyDir(tc.name, tc.path, 3); got != ActionKeep {
			t.Errorf("deep ClassifyDir(%q, %q, depth=3) = %v, want ActionKeep", tc.name, tc.path, got)
		}
	}

	// Unanchored AppData should still be soft-demoted at depth 3
	if got := eng.ClassifyDir("AppData", "Users/dev/AppData", 3); got != ActionSoftDemote {
		t.Errorf("ClassifyDir(AppData, depth=3) = %v, want ActionSoftDemote", got)
	}
}

func TestPresetProfileToggleIndependence(t *testing.T) {
	eng := NewRuleEngine(RuleEngineConfig{
		CaseInsensitive: boolPtr(true),
	})

	if !eng.ProfileEnabled(ProfileWebNode) {
		t.Fatalf("expected ProfileWebNode enabled by default")
	}
	if got := eng.ClassifyDir("node_modules", "proj/node_modules", 2); got != ActionFold {
		t.Fatalf("before toggle: node_modules = %v, want ActionFold", got)
	}

	// Disable Web/Node profile while keeping System/Browser Cache enabled
	eng.SetProfileEnabled(ProfileWebNode, false)
	if eng.ProfileEnabled(ProfileWebNode) {
		t.Fatalf("expected ProfileWebNode disabled")
	}
	if got := eng.ClassifyDir("node_modules", "proj/node_modules", 2); got != ActionKeep {
		t.Errorf("after disabling ProfileWebNode: node_modules = %v, want ActionKeep", got)
	}
	if got := eng.ClassifyDir("GPUCache", "Users/dev/GPUCache", 3); got != ActionHardExclude {
		t.Errorf("GPUCache = %v, want ActionHardExclude while system-browser-cache remains enabled", got)
	}

	// Re-enable Web/Node
	eng.SetProfileEnabled(ProfileWebNode, true)
	if got := eng.ClassifyDir("node_modules", "proj/node_modules", 2); got != ActionFold {
		t.Errorf("after re-enabling ProfileWebNode: node_modules = %v, want ActionFold", got)
	}
}

func TestPresetCaseSensitivityModes(t *testing.T) {
	// Linux case-sensitive mode
	linuxEng := NewRuleEngine(RuleEngineConfig{
		CaseInsensitive: boolPtr(false),
	})
	if got := linuxEng.ClassifyDir("node_modules", "app/node_modules", 2); got != ActionFold {
		t.Errorf("case-sensitive exact node_modules = %v, want ActionFold", got)
	}
	if got := linuxEng.ClassifyDir("NODE_MODULES", "app/NODE_MODULES", 2); got != ActionKeep {
		t.Errorf("case-sensitive NODE_MODULES = %v, want ActionKeep", got)
	}
	if got := linuxEng.ClassifyDir("usr", "usr", 1); got != ActionSoftDemote {
		t.Errorf("case-sensitive root usr = %v, want ActionSoftDemote", got)
	}
	if got := linuxEng.ClassifyDir("USR", "USR", 1); got != ActionKeep {
		t.Errorf("case-sensitive root USR = %v, want ActionKeep", got)
	}

	// Windows/macOS case-insensitive mode
	winEng := NewRuleEngine(RuleEngineConfig{
		CaseInsensitive: boolPtr(true),
	})
	if got := winEng.ClassifyDir("NODE_MODULES", "app/NODE_MODULES", 2); got != ActionFold {
		t.Errorf("case-insensitive NODE_MODULES = %v, want ActionFold", got)
	}
	if got := winEng.ClassifyDir("gpucache", "app/gpucache", 2); got != ActionHardExclude {
		t.Errorf("case-insensitive gpucache = %v, want ActionHardExclude", got)
	}
}

func TestAllBuiltInProfilesExist(t *testing.T) {
	profiles := BuiltInProfiles()
	want := []ProfileID{
		ProfileSystemBrowserCache,
		ProfileWebNode,
		ProfileRustGoJavaCpp,
		ProfilePythonAI,
		ProfileMediaRenderCache,
		ProfileOSSystem,
	}
	if len(profiles) != len(want) {
		t.Fatalf("len(BuiltInProfiles()) = %d, want %d", len(profiles), len(want))
	}
	seen := make(map[ProfileID]bool, len(profiles))
	for _, p := range profiles {
		seen[p.ID] = true
		if p.Name == "" {
			t.Errorf("profile %q has empty Name", p.ID)
		}
	}
	for _, id := range want {
		if !seen[id] {
			t.Errorf("missing built-in profile %q", id)
		}
	}
}
