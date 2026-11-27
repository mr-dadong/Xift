package filter

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestRuleEngineUserOverridesAndPriority(t *testing.T) {
	eng := NewRuleEngine(RuleEngineConfig{
		CaseInsensitive: boolPtr(true),
	})

	// Built-in folds "dist"
	if got := eng.ClassifyDir("dist", "work/frontend/dist", 3); got != ActionFold {
		t.Fatalf("default dist = %v, want ActionFold", got)
	}

	// User whitelist (ActionKeep) overrides built-in fold for specific path prefix
	if err := eng.AddUserRule(UserRule{
		ID:      "keep-frontend-dist",
		Kind:    RuleKindPathPrefix,
		Pattern: "work/frontend/dist",
		Action:  ActionKeep,
		Enabled: true,
	}); err != nil {
		t.Fatalf("AddUserRule failed: %v", err)
	}

	if got := eng.ClassifyDir("dist", "work/frontend/dist", 3); got != ActionKeep {
		t.Errorf("whitelisted work/frontend/dist = %v, want ActionKeep", got)
	}
	// Backslashes in input path must normalize cleanly
	if got := eng.ClassifyDir("dist", `work\frontend\dist`, 3); got != ActionKeep {
		t.Errorf("whitelisted backslash path = %v, want ActionKeep", got)
	}
	// Other dist dirs remain folded
	if got := eng.ClassifyDir("dist", "work/other/dist", 3); got != ActionFold {
		t.Errorf("other dist = %v, want ActionFold", got)
	}

	// User DirName rule overrides built-in Keep
	if err := eng.AddUserRule(UserRule{
		ID:      "fold-custom-assets",
		Kind:    RuleKindDirName,
		Pattern: "generated_assets",
		Action:  ActionFold,
		Enabled: true,
	}); err != nil {
		t.Fatalf("AddUserRule dir_name failed: %v", err)
	}
	if got := eng.ClassifyDir("generated_assets", "work/app/generated_assets", 3); got != ActionFold {
		t.Errorf("generated_assets = %v, want ActionFold", got)
	}

	// PathPrefix rule has higher specificity than Glob and DirName
	if err := eng.AddUserRule(UserRule{
		ID:      "glob-exclude-assets",
		Kind:    RuleKindGlob,
		Pattern: "work/**/generated_assets",
		Action:  ActionHardExclude,
		Enabled: true,
	}); err != nil {
		t.Fatalf("AddUserRule glob failed: %v", err)
	}
	if got := eng.ClassifyDir("generated_assets", "work/app/generated_assets", 3); got != ActionHardExclude {
		t.Errorf("glob override = %v, want ActionHardExclude", got)
	}

	// User extension whitelist overrides built-in .tmp hard exclude
	if err := eng.AddUserRule(UserRule{
		ID:      "keep-tmp",
		Kind:    RuleKindExt,
		Pattern: ".tmp",
		Action:  ActionKeep,
		Enabled: true,
	}); err != nil {
		t.Fatalf("AddUserRule ext failed: %v", err)
	}
	if got := eng.ClassifyFile("notes.tmp", "work/notes.tmp"); got != ActionKeep {
		t.Errorf("whitelisted .tmp = %v, want ActionKeep", got)
	}

	// Remove rule restores built-in behavior
	if !eng.RemoveUserRule("keep-tmp") {
		t.Fatalf("RemoveUserRule(keep-tmp) returned false")
	}
	if got := eng.ClassifyFile("notes.tmp", "work/notes.tmp"); got != ActionHardExclude {
		t.Errorf("after removing keep-tmp: notes.tmp = %v, want ActionHardExclude", got)
	}
}

func TestRuleEnginePersistenceAtomicAndCorruptHandling(t *testing.T) {
	dir := t.TempDir()
	rulesPath := filepath.Join(dir, "filter_rules.json")

	eng1 := NewRuleEngine(RuleEngineConfig{CaseInsensitive: boolPtr(true)})
	rules := []UserRule{
		{ID: "r1", Kind: RuleKindPathPrefix, Pattern: "work/keep/dist", Action: ActionKeep, Enabled: true},
		{ID: "r2", Kind: RuleKindDirName, Pattern: "big_datasets", Action: ActionFold, Enabled: true},
		{ID: "r3", Kind: RuleKindExt, Pattern: "log", Action: ActionHardExclude, Enabled: true},
		{ID: "r4", Kind: RuleKindGlob, Pattern: "builds/*/artifacts", Action: ActionHardExclude, Enabled: true},
	}
	for _, r := range rules {
		if err := eng1.AddUserRule(r); err != nil {
			t.Fatalf("AddUserRule(%s): %v", r.ID, err)
		}
	}

	if err := eng1.SaveUserRules(rulesPath); err != nil {
		t.Fatalf("SaveUserRules: %v", err)
	}

	eng2 := NewRuleEngine(RuleEngineConfig{CaseInsensitive: boolPtr(true)})
	if err := eng2.LoadUserRules(rulesPath); err != nil {
		t.Fatalf("LoadUserRules: %v", err)
	}

	if got := len(eng2.UserRules()); got != len(rules) {
		t.Fatalf("loaded %d rules, want %d", got, len(rules))
	}
	if got := eng2.ClassifyDir("dist", "work/keep/dist", 3); got != ActionKeep {
		t.Errorf("loaded r1: got %v, want ActionKeep", got)
	}
	if got := eng2.ClassifyDir("big_datasets", "data/big_datasets", 2); got != ActionFold {
		t.Errorf("loaded r2: got %v, want ActionFold", got)
	}
	if got := eng2.ClassifyFile("server.log", "logs/server.log"); got != ActionHardExclude {
		t.Errorf("loaded r3: got %v, want ActionHardExclude", got)
	}
	if got := eng2.ClassifyDir("artifacts", "builds/v1/artifacts", 3); got != ActionHardExclude {
		t.Errorf("loaded r4: got %v, want ActionHardExclude", got)
	}

	// Corrupt JSON must return error AND preserve existing in-memory rules
	corruptPath := filepath.Join(dir, "corrupt.json")
	if err := os.WriteFile(corruptPath, []byte(`{"version":1, "rules": [BROKEN`), 0o644); err != nil {
		t.Fatalf("write corrupt file: %v", err)
	}
	if err := eng2.LoadUserRules(corruptPath); err == nil {
		t.Fatalf("expected error loading corrupt JSON, got nil")
	}
	if got := eng2.ClassifyDir("big_datasets", "data/big_datasets", 2); got != ActionFold {
		t.Errorf("existing rules were clobbered after failed load: got %v, want ActionFold", got)
	}
}

func TestRuleEngineConcurrentReadsAndUpdates(t *testing.T) {
	eng := NewRuleEngine(RuleEngineConfig{CaseInsensitive: boolPtr(true)})

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// 4 concurrent readers
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = eng.ClassifyDir("node_modules", "a/node_modules", 2)
					_ = eng.ClassifyDir("custom_dir", "a/custom_dir", 2)
					_ = eng.ClassifyFile("test.tmp", "a/test.tmp")
				}
			}
		}()
	}

	// Concurrent writer toggling profiles and adding/removing user rules
	for i := 0; i < 100; i++ {
		eng.SetProfileEnabled(ProfileWebNode, i%2 == 0)
		_ = eng.AddUserRule(UserRule{
			ID:      "dyn",
			Kind:    RuleKindDirName,
			Pattern: "custom_dir",
			Action:  ActionFold,
			Enabled: i%2 == 0,
		})
		if i%5 == 0 {
			eng.RemoveUserRule("dyn")
		}
	}

	close(stop)
	wg.Wait()
}

func TestRuleEngineClassifyDirZeroAllocHotPath(t *testing.T) {
	eng := NewRuleEngine(RuleEngineConfig{CaseInsensitive: boolPtr(true)})

	allocs := testing.AllocsPerRun(200, func() {
		if eng.ClassifyDir("Node_Modules", "", 2) != ActionFold {
			t.Fatal("unexpected action")
		}
		if eng.ClassifyDir("src", "", 2) != ActionKeep {
			t.Fatal("unexpected action")
		}
		if eng.ClassifyFile("main.go", "") != ActionKeep {
			t.Fatal("unexpected action")
		}
		if eng.ClassifyFile("TEMP.TMP", "") != ActionHardExclude {
			t.Fatal("unexpected action")
		}
	})
	if allocs > 0 {
		t.Errorf("ClassifyDir/ClassifyFile hot path allocated %.2f allocs/run, want 0", allocs)
	}
}
