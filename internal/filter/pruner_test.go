package filter

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPrunerTopDownEarlyStopAndIgnoreIntegration(t *testing.T) {
	rules := NewRuleEngine(RuleEngineConfig{CaseInsensitive: boolPtr(false)})
	pruner := NewPruner(rules, nil)

	// Build a temporary project tree on disk to verify EnterDirIgnoreScope + EvalTopDownDir/File
	root := t.TempDir()
	projDir := filepath.Join(root, "proj")
	if err := os.MkdirAll(projDir, 0o755); err != nil {
		t.Fatalf("mkdir proj: %v", err)
	}
	if err := os.WriteFile(filepath.Join(projDir, "package.json"), []byte(`{"name":"demo"}`), 0o644); err != nil {
		t.Fatalf("write package.json: %v", err)
	}
	if err := os.WriteFile(filepath.Join(projDir, ".gitignore"), []byte("coverage/\ngenerated/\n.env\n"), 0o644); err != nil {
		t.Fatalf("write .gitignore: %v", err)
	}
	if err := os.WriteFile(filepath.Join(projDir, ".xiftignore"), []byte("!generated/\n*.gen.tmp\n"), 0o644); err != nil {
		t.Fatalf("write .xiftignore: %v", err)
	}

	const volID uint16 = 1
	stack := NewIgnoreStack(false, rules.GitignoreIgnoreFiles())

	// Root-level OS system dir "/usr" -> Keep + Descend + FlagSystem
	decUsr := pruner.EvalTopDownDir(volID, 10, "usr", "usr", 1, false, stack)
	if !decUsr.Keep || !decUsr.Descend || decUsr.Flags != (FlagDir|FlagSystem) {
		t.Fatalf("EvalTopDownDir(usr) = %+v, want Keep=true Descend=true FlagDir|FlagSystem", decUsr)
	}
	// Child under /usr inherits FlagSystem
	decUsrBin := pruner.EvalTopDownDir(volID, 11, "bin", "usr/bin", 2, true, stack)
	if !decUsrBin.Keep || !decUsrBin.Descend || decUsrBin.Flags != (FlagDir|FlagSystem) {
		t.Fatalf("EvalTopDownDir(usr/bin) = %+v, want inherited FlagSystem", decUsrBin)
	}

	// Enter project directory and push ignore scope
	scope := pruner.EnterDirIgnoreScope(stack, projDir, "proj", []string{"package.json", ".gitignore", ".xiftignore", "src"})
	defer stack.RestoreScope(scope)

	// HardExclude dir -> Keep=false, Descend=false, recorded in PrunedDirSet
	decCache := pruner.EvalTopDownDir(volID, 20, "Cache", "proj/Cache", 2, false, stack)
	if decCache.Keep || decCache.Descend {
		t.Fatalf("proj/Cache = %+v, want Keep=false Descend=false", decCache)
	}
	if !pruner.PrunedDirSet().IsPruned(volID, 20) {
		t.Errorf("proj/Cache should be recorded in PrunedDirSet")
	}

	// Fold dir (node_modules) -> Keep=true, Descend=false, FlagDir|FlagFolded, recorded in PrunedDirSet
	decNM := pruner.EvalTopDownDir(volID, 21, "node_modules", "proj/node_modules", 2, false, stack)
	if !decNM.Keep || decNM.Descend || decNM.Flags != (FlagDir|FlagFolded) {
		t.Fatalf("proj/node_modules = %+v, want Keep=true Descend=false FlagDir|FlagFolded", decNM)
	}
	if !pruner.PrunedDirSet().IsPruned(volID, 21) {
		t.Errorf("proj/node_modules should be recorded in PrunedDirSet")
	}

	// .gitignore ignored dir (coverage) -> Fold (Keep=true, Descend=false, FlagFolded)
	decCov := pruner.EvalTopDownDir(volID, 22, "coverage", "proj/coverage", 2, false, stack)
	if !decCov.Keep || decCov.Descend || decCov.Flags != (FlagDir|FlagFolded) {
		t.Fatalf("proj/coverage = %+v, want Fold via .gitignore", decCov)
	}

	// .xiftignore negated dir (!generated/) -> overrides .gitignore, Keep=true, Descend=true
	decGen := pruner.EvalTopDownDir(volID, 23, "generated", "proj/generated", 2, false, stack)
	if !decGen.Keep || !decGen.Descend || decGen.Flags != FlagDir {
		t.Fatalf("proj/generated = %+v, want Keep+Descend via .xiftignore negation", decGen)
	}

	// .gitignore regular file (.env) -> kept by default!
	decEnv := pruner.EvalTopDownFile(".env", "proj/.env", false, stack)
	if !decEnv.Keep {
		t.Fatalf("proj/.env = %+v, want Keep=true by default", decEnv)
	}

	// .xiftignore regular file (*.gen.tmp) -> HardExcluded
	decGenTmp := pruner.EvalTopDownFile("foo.gen.tmp", "proj/foo.gen.tmp", false, stack)
	if decGenTmp.Keep {
		t.Fatalf("proj/foo.gen.tmp = %+v, want Keep=false via .xiftignore", decGenTmp)
	}

	// Regular source file
	decMain := pruner.EvalTopDownFile("main.go", "proj/src/main.go", false, stack)
	if !decMain.Keep || decMain.Flags != 0 {
		t.Fatalf("proj/src/main.go = %+v, want Keep=true Flags=0", decMain)
	}
}

func TestPrunerUnorderedMFTSkeletonTwoPassAndHandoff(t *testing.T) {
	rules := NewRuleEngine(RuleEngineConfig{CaseInsensitive: boolPtr(true)})
	const volID uint16 = 1
	const rootFRN uint64 = 5

	sb := NewSkeletonBuilder(volID, rootFRN, rules)
	sb.SetIgnoreLoader(func(dirKey uint64, relPath string) (*IgnoreMatcher, *IgnoreMatcher) {
		if dirKey == 100 { // work/app
			gitM, _ := ParseIgnore(strings.NewReader("coverage/\n"), relPath, IgnoreKindGit, true)
			return nil, gitM
		}
		return nil, nil
	})

	// Deliberately unordered: children BEFORE parents + cycle + self-parent root
	sb.AddDir(103, 102, "lodash")    // child of node_modules (before node_modules!)
	sb.AddDir(104, 100, "coverage")  // ignored by project .gitignore
	sb.AddDir(202, 201, "System32")  // under root Windows
	sb.AddDir(302, 301, "GPUCache")  // hard exclude
	sb.AddDir(303, 302, "index-dir") // child of GPUCache
	sb.AddDir(402, 401, "lib")       // deep user project "lib" (depth 3, must NOT be soft-demoted!)
	sb.AddDir(900, 901, "cycleA")    // parent cycle A <-> B
	sb.AddDir(901, 900, "cycleB")    // parent cycle B <-> A
	sb.AddDir(102, 100, "node_modules")
	sb.AddDir(100, 99, "app")
	sb.AddDir(99, rootFRN, "work")
	sb.AddDir(401, 99, "myapp")
	sb.AddDir(201, rootFRN, "Windows")
	sb.AddDir(301, rootFRN, "Users")
	sb.AddDir(rootFRN, rootFRN, "")
	sb.MarkProjectDir(100)

	eval := sb.Build()

	// Pass 2 checks:
	// 1. node_modules shell kept with FlagDir|FlagFolded
	if dec := eval.Evaluate(Pass2Record{NodeKey: 102, ParentKey: 100, Name: "node_modules", IsDir: true}); !dec.Keep || dec.Flags != (FlagDir|FlagFolded) {
		t.Errorf("node_modules = %+v, want Keep FlagDir|FlagFolded", dec)
	}
	// 2. node_modules/lodash and its file dropped
	if dec := eval.Evaluate(Pass2Record{NodeKey: 103, ParentKey: 102, Name: "lodash", IsDir: true}); dec.Keep {
		t.Errorf("node_modules/lodash should be dropped, got %+v", dec)
	}
	if dec := eval.Evaluate(Pass2Record{NodeKey: 1030, ParentKey: 103, Name: "index.js", IsDir: false}); dec.Keep {
		t.Errorf("node_modules/lodash/index.js should be dropped, got %+v", dec)
	}
	// 3. Project .gitignore coverage/ folded + child dropped
	if dec := eval.Evaluate(Pass2Record{NodeKey: 104, ParentKey: 100, Name: "coverage", IsDir: true}); !dec.Keep || dec.Flags != (FlagDir|FlagFolded) {
		t.Errorf("work/app/coverage = %+v, want Keep FlagDir|FlagFolded", dec)
	}
	if dec := eval.Evaluate(Pass2Record{NodeKey: 1040, ParentKey: 104, Name: "lcov.info", IsDir: false}); dec.Keep {
		t.Errorf("coverage/lcov.info should be dropped, got %+v", dec)
	}
	// 4. GPUCache + child hard excluded
	if dec := eval.Evaluate(Pass2Record{NodeKey: 302, ParentKey: 301, Name: "GPUCache", IsDir: true}); dec.Keep {
		t.Errorf("GPUCache should be dropped")
	}
	if dec := eval.Evaluate(Pass2Record{NodeKey: 303, ParentKey: 302, Name: "index-dir", IsDir: true}); dec.Keep {
		t.Errorf("GPUCache/index-dir should be dropped")
	}
	// 5. Root Windows + System32 + kernel32.dll soft-demoted
	if dec := eval.Evaluate(Pass2Record{NodeKey: 201, ParentKey: rootFRN, Name: "Windows", IsDir: true}); !dec.Keep || dec.Flags != (FlagDir|FlagSystem) {
		t.Errorf("Windows = %+v, want FlagDir|FlagSystem", dec)
	}
	if dec := eval.Evaluate(Pass2Record{NodeKey: 2020, ParentKey: 202, Name: "kernel32.dll", IsDir: false}); !dec.Keep || dec.Flags != FlagSystem {
		t.Errorf("kernel32.dll = %+v, want Keep FlagSystem", dec)
	}
	// 6. Deep user project work/myapp/lib NOT soft-demoted
	if dec := eval.Evaluate(Pass2Record{NodeKey: 402, ParentKey: 401, Name: "lib", IsDir: true}); !dec.Keep || dec.Flags != FlagDir {
		t.Errorf("work/myapp/lib = %+v, want Keep FlagDir (no FlagSystem)", dec)
	}

	// Seamless handoff of Pass 1 PrunedDirSet into live Pruner
	livePruner := NewPruner(rules, nil)
	livePruner.InstallVolumeSet(volID, eval.PrunedDirSet())

	// Subsequent live file create under node_modules/lodash (103) is immediately O(1) dropped
	decLive := livePruner.InterceptEvent(EventInput{
		VolID:     volID,
		Op:        EventCreate,
		NodeKey:   99999,
		ParentKey: 103,
		Name:      "new.js",
		IsDir:     false,
	}, nil)
	if decLive.Action != DecisionDrop {
		t.Fatalf("live event under handed-off pruned dir = %+v, want DecisionDrop", decLive)
	}
}

func TestPrunerInterceptNpmInstallStorm80K(t *testing.T) {
	rules := NewRuleEngine(RuleEngineConfig{CaseInsensitive: boolPtr(true)})
	pruner := NewPruner(rules, nil)

	const (
		volID      uint16 = 1
		projectKey uint64 = 100
		nodeModKey uint64 = 101
		numSubDirs        = 1000
		totalFiles        = 79_999
	)

	passed := 0
	dropped := 0

	// 1. Top-level node_modules creation MUST pass exactly once with FlagDir|FlagFolded
	topDec := pruner.InterceptEvent(EventInput{
		VolID:     volID,
		Op:        EventCreate,
		NodeKey:   nodeModKey,
		ParentKey: projectKey,
		Name:      "node_modules",
		IsDir:     true,
	}, nil)
	if topDec.Action != DecisionPass || topDec.Flags != (FlagDir|FlagFolded) {
		t.Fatalf("top node_modules decision = %+v, want DecisionPass FlagDir|FlagFolded", topDec)
	}
	passed++

	// 2. 1,000 nested package subdirs created inside node_modules
	for d := uint64(0); d < numSubDirs; d++ {
		dirKey := 10_000 + d
		parent := nodeModKey
		if d >= 100 {
			parent = 10_000 + (d % 100) // multi-level nested subdirs
		}
		dec := pruner.InterceptEvent(EventInput{
			VolID:     volID,
			Op:        EventCreate,
			NodeKey:   dirKey,
			ParentKey: parent,
			Name:      "pkg",
			IsDir:     true,
		}, nil)
		if dec.Action != DecisionDrop {
			t.Fatalf("nested subdir %d leaked with %+v", dirKey, dec)
		}
		dropped++
	}

	// 3. 79,999 file creates/modifies scattered across all nested subdirs -> 0 leaks!
	for f := uint64(0); f < totalFiles; f++ {
		parent := 10_000 + (f % numSubDirs)
		dec := pruner.InterceptEvent(EventInput{
			VolID:     volID,
			Op:        EventCreate,
			NodeKey:   1_000_000 + f,
			ParentKey: parent,
			Name:      "index.js",
			IsDir:     false,
		}, nil)
		if dec.Action != DecisionDrop {
			t.Fatalf("storm file %d leaked with %+v", f, dec)
		}
		dropped++
	}

	if passed != 1 || dropped != numSubDirs+totalFiles {
		t.Fatalf("storm summary: passed=%d dropped=%d, want passed=1 dropped=%d", passed, dropped, numSubDirs+totalFiles)
	}

	// Hot path check: dropping files inside a pruned dir MUST allocate 0 bytes
	hotEv := EventInput{
		VolID:     volID,
		Op:        EventCreate,
		NodeKey:   9_999_999,
		ParentKey: 10_050,
		Name:      "bundle.js",
		IsDir:     false,
	}
	allocs := testing.AllocsPerRun(1000, func() {
		if pruner.InterceptEvent(hotEv, nil).Action != DecisionDrop {
			t.Fatal("expected drop")
		}
	})
	if allocs > 0 {
		t.Errorf("storm hot path allocated %.2f allocs/op, want 0", allocs)
	}
}

func TestPrunerCrossBoundaryMovesDeletesAndDynamicRules(t *testing.T) {
	rules := NewRuleEngine(RuleEngineConfig{CaseInsensitive: boolPtr(true)})
	pruner := NewPruner(rules, nil)

	const (
		volID      uint16 = 1
		projKey    uint64 = 10
		srcKey     uint64 = 11
		nodeModKey uint64 = 20
		pkgDirKey  uint64 = 21
		pkgFileKey uint64 = 22
		extDirKey  uint64 = 30
	)

	// Setup: create node_modules and nested pkg dir
	_ = pruner.InterceptEvent(EventInput{VolID: volID, Op: EventCreate, NodeKey: nodeModKey, ParentKey: projKey, Name: "node_modules", IsDir: true}, nil)
	_ = pruner.InterceptEvent(EventInput{VolID: volID, Op: EventCreate, NodeKey: pkgDirKey, ParentKey: nodeModKey, Name: "pkg", IsDir: true}, nil)

	// (1a) File moved OUT of pruned node_modules/pkg into visible src/ -> MUST pass
	decFileOut := pruner.InterceptEvent(EventInput{
		VolID:        volID,
		Op:           EventRename,
		NodeKey:      pkgFileKey,
		ParentKey:    pkgDirKey,
		NewParentKey: srcKey,
		Name:         "extracted.js",
		IsDir:        false,
	}, nil)
	if decFileOut.Action != DecisionPass {
		t.Errorf("file moved out of pruned dir = %+v, want DecisionPass", decFileOut)
	}

	// (1b) Subdir moved OUT of pruned node_modules into visible src/ -> MUST trigger DecisionRescanDir and leave PrunedDirSet
	decDirOut := pruner.InterceptEvent(EventInput{
		VolID:        volID,
		Op:           EventRename,
		NodeKey:      pkgDirKey,
		ParentKey:    nodeModKey,
		NewParentKey: srcKey,
		Name:         "pkg",
		IsDir:        true,
	}, nil)
	if decDirOut.Action != DecisionRescanDir || decDirOut.Flags != FlagDir {
		t.Errorf("dir moved out of pruned dir = %+v, want DecisionRescanDir FlagDir", decDirOut)
	}
	if pruner.PrunedDirSet().IsPruned(volID, pkgDirKey) {
		t.Errorf("pkgDirKey should have been removed from PrunedDirSet after moving out")
	}

	// (2) External visible dir moved INTO pruned node_modules -> DecisionDeleteFromIndex + added to PrunedDirSet
	decDirIn := pruner.InterceptEvent(EventInput{
		VolID:        volID,
		Op:           EventRename,
		NodeKey:      extDirKey,
		ParentKey:    srcKey,
		NewParentKey: nodeModKey,
		Name:         "tmp_dir",
		IsDir:        true,
	}, nil)
	if decDirIn.Action != DecisionDeleteFromIndex || !decDirIn.PrunedSubtree {
		t.Errorf("dir moved into pruned dir = %+v, want DecisionDeleteFromIndex PrunedSubtree=true", decDirIn)
	}
	if !pruner.PrunedDirSet().IsPruned(volID, extDirKey) {
		t.Errorf("extDirKey should be added to PrunedDirSet after moving into node_modules")
	}

	// (3) Folded node_modules renamed to normal name "my_libs" -> DecisionRescanDir & un-folded
	decUnfold := pruner.InterceptEvent(EventInput{
		VolID:        volID,
		Op:           EventRename,
		NodeKey:      nodeModKey,
		ParentKey:    projKey,
		NewParentKey: projKey,
		Name:         "my_libs",
		IsDir:        true,
	}, nil)
	if decUnfold.Action != DecisionRescanDir || decUnfold.Flags != FlagDir {
		t.Errorf("unfolding rename node_modules->my_libs = %+v, want DecisionRescanDir FlagDir", decUnfold)
	}
	if pruner.PrunedDirSet().IsPruned(volID, nodeModKey) {
		t.Errorf("nodeModKey should no longer be pruned after rename to my_libs")
	}

	// (4) Delete a folded dir -> passes delete to index AND removes key from PrunedDirSet (no leak)
	const nm2 uint64 = 50
	_ = pruner.InterceptEvent(EventInput{VolID: volID, Op: EventCreate, NodeKey: nm2, ParentKey: projKey, Name: "node_modules", IsDir: true}, nil)
	if !pruner.PrunedDirSet().IsPruned(volID, nm2) {
		t.Fatalf("nm2 should be pruned")
	}
	decDelFolded := pruner.InterceptEvent(EventInput{VolID: volID, Op: EventDelete, NodeKey: nm2, ParentKey: projKey, Name: "node_modules", IsDir: true}, nil)
	if decDelFolded.Action != DecisionPass {
		t.Errorf("delete folded shell = %+v, want DecisionPass so index deletes shell", decDelFolded)
	}
	if pruner.PrunedDirSet().IsPruned(volID, nm2) {
		t.Errorf("deleted folded dir key leaked in PrunedDirSet")
	}

	// (5) Dynamic user rule (UI right-click "Fold this directory") takes immediate effect in PrunedDirSet
	const dynDir uint64 = 60
	if err := pruner.ApplyDynamicDirRule(volID, dynDir, UserRule{
		ID:      "fold-datasets",
		Kind:    RuleKindPathPrefix,
		Pattern: "work/datasets",
		Action:  ActionFold,
		Enabled: true,
	}); err != nil {
		t.Fatalf("ApplyDynamicDirRule: %v", err)
	}
	if !pruner.PrunedDirSet().IsPruned(volID, dynDir) {
		t.Errorf("dynDir should immediately be pruned after ApplyDynamicDirRule")
	}
	decChild := pruner.InterceptEvent(EventInput{VolID: volID, Op: EventCreate, NodeKey: 61, ParentKey: dynDir, Name: "sample.csv", IsDir: false}, nil)
	if decChild.Action != DecisionDrop {
		t.Errorf("child under dynamically folded dir = %+v, want DecisionDrop", decChild)
	}
}

func BenchmarkPrunerInterceptStorm80K(b *testing.B) {
	rules := NewRuleEngine(RuleEngineConfig{CaseInsensitive: boolPtr(true)})
	pruner := NewPruner(rules, nil)
	const volID uint16 = 1
	_ = pruner.InterceptEvent(EventInput{VolID: volID, Op: EventCreate, NodeKey: 101, ParentKey: 100, Name: "node_modules", IsDir: true}, nil)
	_ = pruner.InterceptEvent(EventInput{VolID: volID, Op: EventCreate, NodeKey: 102, ParentKey: 101, Name: "lodash", IsDir: true}, nil)

	ev := EventInput{
		VolID:     volID,
		Op:        EventCreate,
		NodeKey:   5000,
		ParentKey: 102,
		Name:      "index.js",
		IsDir:     false,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if pruner.InterceptEvent(ev, nil).Action != DecisionDrop {
			b.Fatal("expected DecisionDrop")
		}
	}
}
