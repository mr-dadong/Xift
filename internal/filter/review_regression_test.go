package filter

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// Regression tests locking in every Critical & Important finding from adversarial code review.

func TestRegressionC1RescanClearsStalePrunedDescendants(t *testing.T) {
	rules := NewRuleEngine(RuleEngineConfig{CaseInsensitive: boolPtr(true)})
	pruner := NewPruner(rules, nil)
	const (
		volID   uint16 = 1
		projKey uint64 = 10
		srcKey  uint64 = 11
		nmKey   uint64 = 20
		pkgKey  uint64 = 21
		subKey  uint64 = 22
	)

	// Populate node_modules -> pkg -> sub in PrunedDirSet
	_ = pruner.InterceptEvent(EventInput{VolID: volID, Op: EventCreate, NodeKey: nmKey, ParentKey: projKey, Name: "node_modules", IsDir: true}, nil)
	_ = pruner.InterceptEvent(EventInput{VolID: volID, Op: EventCreate, NodeKey: pkgKey, ParentKey: nmKey, Name: "pkg", IsDir: true}, nil)
	_ = pruner.InterceptEvent(EventInput{VolID: volID, Op: EventCreate, NodeKey: subKey, ParentKey: pkgKey, Name: "sub", IsDir: true}, nil)

	if !pruner.PrunedDirSet().IsPruned(volID, subKey) {
		t.Fatalf("subKey should initially be pruned")
	}

	// Move pkg out into visible src/ -> triggers DecisionRescanDir
	decOut := pruner.InterceptEvent(EventInput{
		VolID: volID, Op: EventRename, NodeKey: pkgKey, ParentKey: nmKey, NewParentKey: srcKey, Name: "pkg", IsDir: true,
	}, nil)
	if decOut.Action != DecisionRescanDir {
		t.Fatalf("move out = %+v, want DecisionRescanDir", decOut)
	}

	// Provider rescans src/pkg/sub via EvalTopDownDir -> MUST clear stale subKey from PrunedDirSet
	decSub := pruner.EvalTopDownDir(volID, subKey, "sub", "src/pkg/sub", 3, false, nil)
	if !decSub.Keep || !decSub.Descend {
		t.Fatalf("rescan EvalTopDownDir(sub) = %+v, want Keep+Descend", decSub)
	}
	if pruner.PrunedDirSet().IsPruned(volID, subKey) {
		t.Fatalf("stale subKey remained pruned after rescan")
	}

	// Subsequent live file event under src/pkg/sub MUST pass
	decLive := pruner.InterceptEvent(EventInput{
		VolID: volID, Op: EventCreate, NodeKey: 99, ParentKey: subKey, Name: "app.go", IsDir: false,
	}, nil)
	if decLive.Action != DecisionPass {
		t.Fatalf("live file under rescanned subdir = %+v, want DecisionPass", decLive)
	}
}

func TestRegressionC2StrayGitignoreOutsideProjectDoesNotFold(t *testing.T) {
	rules := NewRuleEngine(RuleEngineConfig{CaseInsensitive: boolPtr(false)})
	pruner := NewPruner(rules, nil)

	homeDir := t.TempDir()
	// Stray .gitignore in home dir WITHOUT any project marker
	if err := os.WriteFile(filepath.Join(homeDir, ".gitignore"), []byte("documents/\nnotes/\n"), 0o644); err != nil {
		t.Fatalf("write stray .gitignore: %v", err)
	}

	stack := NewIgnoreStack(false, false)
	scope := pruner.EnterDirIgnoreScope(stack, homeDir, "home", []string{".gitignore", "documents", "notes"})
	defer stack.RestoreScope(scope)

	if stack.InProject() {
		t.Fatalf("stack.InProject() = true without project markers")
	}
	decDocs := pruner.EvalTopDownDir(1, 100, "documents", "home/documents", 2, false, stack)
	if !decDocs.Keep || !decDocs.Descend || decDocs.Flags != FlagDir {
		t.Fatalf("home/documents folded by stray .gitignore: %+v", decDocs)
	}

	// Now enter a real project subdir with go.mod + .gitignore -> MUST fold ignored dir
	projDir := filepath.Join(homeDir, "myproj")
	if err := os.MkdirAll(projDir, 0o755); err != nil {
		t.Fatalf("mkdir myproj: %v", err)
	}
	if err := os.WriteFile(filepath.Join(projDir, ".gitignore"), []byte("generated_out/\n"), 0o644); err != nil {
		t.Fatalf("write project .gitignore: %v", err)
	}
	pScope := pruner.EnterDirIgnoreScope(stack, projDir, "home/myproj", []string{"go.mod", ".gitignore", "generated_out"})
	decGen := pruner.EvalTopDownDir(1, 101, "generated_out", "home/myproj/generated_out", 3, false, stack)
	stack.RestoreScope(pScope)

	if !decGen.Keep || decGen.Descend || decGen.Flags != (FlagDir|FlagFolded) {
		t.Fatalf("project generated_out = %+v, want Fold", decGen)
	}
	if stack.InProject() {
		t.Fatalf("stack.InProject() should be false after RestoreScope")
	}
}

func TestRegressionC3AutoIDNoCollisionAfterLoadUserRules(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rules.json")

	eng1 := NewRuleEngine(RuleEngineConfig{CaseInsensitive: boolPtr(true)})
	if err := eng1.AddUserRule(UserRule{Kind: RuleKindDirName, Pattern: "alpha", Action: ActionFold, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := eng1.AddUserRule(UserRule{Kind: RuleKindDirName, Pattern: "beta", Action: ActionFold, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := eng1.SaveUserRules(path); err != nil {
		t.Fatal(err)
	}

	// Fresh process loads persisted auto-ID rules, then appends a new auto-ID rule
	eng2 := NewRuleEngine(RuleEngineConfig{CaseInsensitive: boolPtr(true)})
	if err := eng2.LoadUserRules(path); err != nil {
		t.Fatal(err)
	}
	if err := eng2.AddUserRule(UserRule{Kind: RuleKindDirName, Pattern: "gamma", Action: ActionFold, Enabled: true}); err != nil {
		t.Fatal(err)
	}

	gotRules := eng2.UserRules()
	if len(gotRules) != 3 {
		t.Fatalf("len(UserRules()) = %d, want 3 (old auto-ID rule was overwritten!)", len(gotRules))
	}
	for _, name := range []string{"alpha", "beta", "gamma"} {
		if got := eng2.ClassifyDir(name, "w/"+name, 2); got != ActionFold {
			t.Errorf("ClassifyDir(%q) = %v, want ActionFold", name, got)
		}
	}
}

func TestRegressionC4AndI8SkeletonSkipsPrunedProjectsAndAppliesFileIgnores(t *testing.T) {
	rules := NewRuleEngine(RuleEngineConfig{CaseInsensitive: boolPtr(true)})
	const volID uint16 = 1
	const rootFRN uint64 = 5

	sb := NewSkeletonBuilder(volID, rootFRN, rules)
	var loaderCalls atomic.Int32
	sb.SetIgnoreLoader(func(dirKey uint64, relPath string) (*IgnoreMatcher, *IgnoreMatcher) {
		loaderCalls.Add(1)
		if dirKey == 100 { // work/app
			xiftM, _ := ParseIgnore(strings.NewReader("!dist/\n*.gen.tmp\n"), relPath, IgnoreKindXift, true)
			gitM, _ := ParseIgnore(strings.NewReader("coverage/\n"), relPath, IgnoreKindGit, true)
			return xiftM, gitM
		}
		if dirKey == 103 {
			t.Errorf("ignoreLoader MUST NOT be called for package inside node_modules (key 103)")
		}
		return nil, nil
	})

	sb.AddDir(rootFRN, rootFRN, "")
	sb.AddDir(99, rootFRN, "work")
	sb.AddDir(100, 99, "app")
	sb.AddDir(102, 100, "node_modules")
	sb.AddDir(103, 102, "lodash") // has package.json inside node_modules!
	sb.AddDir(104, 100, "dist")   // preset folds dist, but .xiftignore negates !dist/
	sb.AddDir(105, 104, "assets") // child of negated dist/
	sb.AddDir(106, 100, "coverage")

	sb.MarkProjectDir(100)
	sb.MarkProjectDir(103) // Must be skipped because 102 (node_modules) is already pruned!

	eval := sb.Build()

	if got := loaderCalls.Load(); got != 1 {
		t.Fatalf("ignoreLoader called %d times, want 1 (must skip node_modules/lodash)", got)
	}
	// I8: File-level .xiftignore (*.gen.tmp) enforced in Pass 2!
	if dec := eval.Evaluate(Pass2Record{NodeKey: 500, ParentKey: 100, Name: "cache.gen.tmp", IsDir: false}); dec.Keep {
		t.Errorf("Pass2 cache.gen.tmp should be hard-excluded by .xiftignore, got %+v", dec)
	}
	if dec := eval.Evaluate(Pass2Record{NodeKey: 501, ParentKey: 100, Name: "index.ts", IsDir: false}); !dec.Keep {
		t.Errorf("Pass2 index.ts should be kept, got %+v", dec)
	}
	// .xiftignore !dist/ overrides preset Fold and un-prunes child assets/
	if dec := eval.Evaluate(Pass2Record{NodeKey: 104, ParentKey: 100, Name: "dist", IsDir: true}); !dec.Keep || dec.Flags != FlagDir {
		t.Errorf("Pass2 !dist/ = %+v, want Keep FlagDir", dec)
	}
	if dec := eval.Evaluate(Pass2Record{NodeKey: 105, ParentKey: 104, Name: "assets", IsDir: true}); !dec.Keep || dec.Flags != FlagDir {
		t.Errorf("Pass2 !dist/assets = %+v, want Keep FlagDir", dec)
	}
	if dec := eval.Evaluate(Pass2Record{NodeKey: 502, ParentKey: 105, Name: "logo.png", IsDir: false}); !dec.Keep {
		t.Errorf("Pass2 !dist/assets/logo.png should be kept, got %+v", dec)
	}
	// .gitignore coverage/ still folded
	if dec := eval.Evaluate(Pass2Record{NodeKey: 106, ParentKey: 100, Name: "coverage", IsDir: true}); !dec.Keep || dec.Flags != (FlagDir|FlagFolded) {
		t.Errorf("Pass2 coverage = %+v, want Keep FlagDir|FlagFolded", dec)
	}
}

func TestRegressionI1I2I3I9GlobAndGitignoreEdgeCases(t *testing.T) {
	// I1: trailing /** matches inside dir, NEVER dir itself
	m1, err := ParseIgnore(strings.NewReader("build/**\n"), "proj", IgnoreKindGit, false)
	if err != nil {
		t.Fatal(err)
	}
	if got := m1.Match("proj/build", true); got != IgnoreUnmatched {
		t.Errorf("build/** matched dir proj/build itself: %v", got)
	}
	if got := m1.Match("proj/build", false); got != IgnoreUnmatched {
		t.Errorf("build/** matched file proj/build itself: %v", got)
	}
	if got := m1.Match("proj/build/out.js", false); got != IgnoreMatched {
		t.Errorf("build/** failed to match proj/build/out.js: %v", got)
	}
	if got := m1.Match("proj/build/sub/out.js", false); got != IgnoreMatched {
		t.Errorf("build/** failed to match nested proj/build/sub/out.js: %v", got)
	}

	// I2: UTF-8 BOM on first line
	bomContent := "\xef\xbb\xbf# BOM comment\n\xef\xbb\xbf" // test BOM before comment and before rule separately
	mBOM1, err := ParseIgnore(strings.NewReader("\xef\xbb\xbf# comment\ncache_dir/\n"), "proj", IgnoreKindGit, false)
	if err != nil {
		t.Fatal(err)
	}
	if mBOM1.RuleCount() != 1 || mBOM1.Match("proj/cache_dir", true) != IgnoreMatched {
		t.Errorf("BOM before comment failed: rules=%d", mBOM1.RuleCount())
	}
	_ = bomContent
	mBOM2, err := ParseIgnore(strings.NewReader("\xef\xbb\xbfnode_out/\n"), "proj", IgnoreKindGit, false)
	if err != nil {
		t.Fatal(err)
	}
	if mBOM2.Match("proj/node_out", true) != IgnoreMatched {
		t.Errorf("BOM before first rule broke matching")
	}

	// I3: UTF-8 runes in ? and [...]
	if !matchSegmentGlob("??.txt", "测试.txt", false) {
		t.Errorf("??.txt failed to match 2-rune CJK filename 测试.txt")
	}
	if matchSegmentGlob("??????.txt", "测试.txt", false) {
		t.Errorf("??????.txt erroneously matched 2-rune CJK filename 测试.txt (byte-based bug)")
	}
	if !matchSegmentGlob("[!a]", "中", false) {
		t.Errorf("[!a] failed to match single CJK rune 中")
	}
	if matchSegmentGlob("[中]", "国", false) {
		t.Errorf("[中] erroneously matched 国")
	}
	if !matchSegmentGlob("[中]", "中", false) {
		t.Errorf("[中] failed to match 中")
	}

	// I9: escaped backslash + space ("\\ ") stays literal backslash + space (not collapsed to "\ ")
	if got := unescapeEscapedSpaces(`foo\\ bar`); got != `foo\\ bar` {
		t.Errorf(`unescapeEscapedSpaces("foo\\\\ bar") = %q, want "foo\\\\ bar"`, got)
	}
	if matchSegmentGlob(`foo\\ bar`, `foo bar`, false) {
		t.Errorf(`matchSegmentGlob("foo\\\\ bar", "foo bar") = true, want false`)
	}
	if !matchSegmentGlob(`foo\\ bar`, `foo\ bar`, false) {
		t.Errorf(`matchSegmentGlob("foo\\\\ bar", "foo\\ bar") = false, want true`)
	}
	mBS, err := ParseIgnore(strings.NewReader(`foo\\ bar`+"\n"), "proj", IgnoreKindGit, false)
	if err != nil {
		t.Fatal(err)
	}
	if mBS.Match(`proj/foo bar`, false) == IgnoreMatched {
		t.Errorf(`foo\\ bar erroneously matched "foo bar"`)
	}
}

func TestRegressionI4I5I6I7PrunerEdgeCases(t *testing.T) {
	rules := NewRuleEngine(RuleEngineConfig{CaseInsensitive: boolPtr(true)})
	if err := rules.AddUserRule(UserRule{
		ID:      "demote-logs",
		Kind:    RuleKindExt,
		Pattern: ".log",
		Action:  ActionSoftDemote,
		Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}

	pruner := NewPruner(rules, nil)

	// I4: ActionSoftDemote on regular files across TopDown, Live Intercept, and Pass2
	if dec := pruner.EvalTopDownFile("app.log", "work/app.log", false, nil); !dec.Keep || dec.Flags != FlagSystem {
		t.Errorf("EvalTopDownFile(.log) = %+v, want Keep FlagSystem", dec)
	}
	if dec := pruner.InterceptEvent(EventInput{VolID: 1, Op: EventCreate, NodeKey: 500, ParentKey: 10, Name: "app.log", IsDir: false}, nil); dec.Action != DecisionPass || dec.Flags != FlagSystem {
		t.Errorf("InterceptEvent(.log) = %+v, want DecisionPass FlagSystem", dec)
	}
	sb := NewSkeletonBuilder(1, 5, rules)
	sb.AddDir(5, 5, "")
	sb.AddDir(10, 5, "work")
	eval := sb.Build()
	if dec := eval.Evaluate(Pass2Record{NodeKey: 500, ParentKey: 10, Name: "app.log", IsDir: false}); !dec.Keep || dec.Flags != FlagSystem {
		t.Errorf("Pass2(.log) = %+v, want Keep FlagSystem", dec)
	}

	// I5: Delayed delete/modify of DirFlagPrunedDescendant after parent removal MUST drop
	_ = pruner.InterceptEvent(EventInput{VolID: 1, Op: EventCreate, NodeKey: 20, ParentKey: 10, Name: "node_modules", IsDir: true}, nil)
	_ = pruner.InterceptEvent(EventInput{VolID: 1, Op: EventCreate, NodeKey: 21, ParentKey: 20, Name: "lodash", IsDir: true}, nil)
	// Parent deleted first
	_ = pruner.InterceptEvent(EventInput{VolID: 1, Op: EventDelete, NodeKey: 20, ParentKey: 10, Name: "node_modules", IsDir: true}, nil)
	// Delayed ModifyMeta and Delete of child 21 (still marked DirFlagPrunedDescendant)
	if dec := pruner.InterceptEvent(EventInput{VolID: 1, Op: EventModifyMeta, NodeKey: 21, ParentKey: 20, Name: "lodash", IsDir: true}, nil); dec.Action != DecisionDrop {
		t.Errorf("delayed ModifyMeta on pruned descendant = %+v, want DecisionDrop", dec)
	}
	if dec := pruner.InterceptEvent(EventInput{VolID: 1, Op: EventDelete, NodeKey: 21, ParentKey: 20, Name: "lodash", IsDir: true}, nil); dec.Action != DecisionDrop {
		t.Errorf("delayed Delete on pruned descendant = %+v, want DecisionDrop", dec)
	}
	if pruner.PrunedDirSet().IsPruned(1, 21) {
		t.Errorf("delayed Delete should still clean up key 21 from PrunedDirSet")
	}

	// I6: NodeKey == 0 must NEVER poison sentinel key 0 in PrunedDirSet
	_ = pruner.InterceptEvent(EventInput{VolID: 1, Op: EventCreate, NodeKey: 0, ParentKey: 10, Name: "node_modules", IsDir: true}, nil)
	if pruner.PrunedDirSet().IsPruned(1, 0) {
		t.Fatalf("sentinel dirKey 0 was poisoned in PrunedDirSet")
	}

	// I7: ApplyDynamicDirRule changing Fold -> SoftDemote or Enabled: false clears pruned flags
	const dynKey uint64 = 77
	if err := pruner.ApplyDynamicDirRule(1, dynKey, UserRule{
		ID: "dyn-1", Kind: RuleKindPathPrefix, Pattern: "work/data", Action: ActionFold, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	if !pruner.PrunedDirSet().IsPruned(1, dynKey) {
		t.Fatalf("dynKey should be pruned")
	}
	if err := pruner.ApplyDynamicDirRule(1, dynKey, UserRule{
		ID: "dyn-1", Kind: RuleKindPathPrefix, Pattern: "work/data", Action: ActionSoftDemote, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	if pruner.PrunedDirSet().IsPruned(1, dynKey) || !pruner.PrunedDirSet().IsSystem(1, dynKey) {
		t.Errorf("switching Fold -> SoftDemote failed: pruned=%v sys=%v", pruner.PrunedDirSet().IsPruned(1, dynKey), pruner.PrunedDirSet().IsSystem(1, dynKey))
	}

	if err := pruner.ApplyDynamicDirRule(1, dynKey, UserRule{
		ID: "dyn-1", Kind: RuleKindPathPrefix, Pattern: "work/data", Action: ActionFold, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := pruner.ApplyDynamicDirRule(1, dynKey, UserRule{
		ID: "dyn-1", Kind: RuleKindPathPrefix, Pattern: "work/data", Action: ActionFold, Enabled: false,
	}); err != nil {
		t.Fatal(err)
	}
	if pruner.PrunedDirSet().IsPruned(1, dynKey) {
		t.Errorf("disabling dynamic rule failed to clear pruned flags")
	}
}

func TestRegressionI10AndMinorsValidationAndConcurrentSave(t *testing.T) {
	eng := NewRuleEngine(RuleEngineConfig{CaseInsensitive: boolPtr(true)})

	// M1: Degenerate rules must be rejected
	for _, bad := range []UserRule{
		{Kind: RuleKindPathPrefix, Pattern: "///", Action: ActionFold, Enabled: true},
		{Kind: RuleKindGlob, Pattern: " / ", Action: ActionFold, Enabled: true},
		{Kind: RuleKindExt, Pattern: ".", Action: ActionHardExclude, Enabled: true},
		{Kind: RuleKindExt, Pattern: "..", Action: ActionHardExclude, Enabled: true},
	} {
		if err := eng.AddUserRule(bad); err == nil {
			t.Errorf("expected ErrInvalidRule for degenerate rule %+v", bad)
		}
	}

	// M2: collapseDoubleStarSegments must not corrupt non-segment "**/**"
	if got := collapseDoubleStarSegments("foo**/**bar"); got != "foo**/**bar" {
		t.Errorf("collapseDoubleStarSegments corrupted non-segment pattern: got %q", got)
	}
	if got := collapseDoubleStarSegments("a/**/**/b"); got != "a/**/b" {
		t.Errorf("collapseDoubleStarSegments failed on segment boundary: got %q", got)
	}

	// M7: Updating existing keys at 78% load threshold must NOT double memory
	set := NewPrunedDirSet(50_000)
	for i := uint64(1); i <= 50_000; i++ {
		set.Set(1, i, DirFlagPrunedDescendant)
	}
	beforeMem := set.MemoryBytes()
	for i := uint64(1); i <= 50_000; i++ {
		set.Set(1, i, DirFlagFoldedShell)
	}
	if afterMem := set.MemoryBytes(); afterMem != beforeMem {
		t.Errorf("updating existing keys at high load grew memory from %d to %d", beforeMem, afterMem)
	}

	// I10: Concurrent SaveUserRules + LoadUserRules under -race
	rulesPath := filepath.Join(t.TempDir(), "concurrent_rules.json")
	if err := eng.AddUserRule(UserRule{ID: "base", Kind: RuleKindDirName, Pattern: "tmp_build", Action: ActionFold, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := eng.SaveUserRules(rulesPath); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_ = eng.SaveUserRules(rulesPath)
				if j%4 == 0 {
					_ = eng.LoadUserRules(rulesPath)
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestRegressionNamelessDeleteAndModifyMetaUnderVisibleParent(t *testing.T) {
	pruner := NewPruner(DefaultRuleEngine(), nil)
	pruner.PrunedDirSet().Set(1, 99, DirFlagFoldedShell)

	// Nameless Delete under visible parent MUST pass so Batcher can cancel Create+Delete or storage can delete by NodeKey.
	if dec := pruner.InterceptEvent(EventInput{VolID: 1, Op: EventDelete, NodeKey: 500, ParentKey: 10}, nil); dec.Action != DecisionPass {
		t.Fatalf("nameless Delete under visible parent = %v, want DecisionPass", dec.Action)
	}
	// Nameless ModifyMeta under visible parent MUST pass so cold metadata updates by NodeKey are not dropped.
	if dec := pruner.InterceptEvent(EventInput{VolID: 1, Op: EventModifyMeta, NodeKey: 500, ParentKey: 10}, nil); dec.Action != DecisionPass {
		t.Fatalf("nameless ModifyMeta under visible parent = %v, want DecisionPass", dec.Action)
	}
	// Nameless Delete under pruned parent MUST still be dropped.
	if dec := pruner.InterceptEvent(EventInput{VolID: 1, Op: EventDelete, NodeKey: 501, ParentKey: 99}, nil); dec.Action != DecisionDrop {
		t.Fatalf("nameless Delete under pruned parent = %v, want DecisionDrop", dec.Action)
	}
	// Nameless Create under visible parent MUST still be dropped.
	if dec := pruner.InterceptEvent(EventInput{VolID: 1, Op: EventCreate, NodeKey: 502, ParentKey: 10}, nil); dec.Action != DecisionDrop {
		t.Fatalf("nameless Create under visible parent = %v, want DecisionDrop", dec.Action)
	}
}
