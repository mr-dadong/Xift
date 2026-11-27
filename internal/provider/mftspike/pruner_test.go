package mftspike

import "testing"

func TestTwoPassPruner_OutOfOrderFoldNodeModules(t *testing.T) {
	// Out-of-order MFT stream: child file and nested dir appear BEFORE node_modules.
	records := []RawRecord{
		{FRN: 300, ParentFRN: 200, Name: "index.js", IsDir: false},
		{FRN: 200, ParentFRN: 100, Name: "lodash", IsDir: true},
		{FRN: 400, ParentFRN: 10, Name: "app.go", IsDir: false},
		{FRN: 100, ParentFRN: 10, Name: "node_modules", IsDir: true},
		{FRN: 10, ParentFRN: 5, Name: "myproj", IsDir: true},
		{FRN: 5, ParentFRN: 5, Name: "", IsDir: true}, // NTFS root self-parent
	}

	rules := DefaultRules()
	sb := NewSkeletonBuilder(rules)
	for _, r := range records {
		if r.IsDir {
			sb.AddDir(r.FRN, r.ParentFRN, r.Name)
		}
	}
	pruner := sb.Build()

	decisions := make(map[uint64]Pass2Decision, len(records))
	for _, r := range records {
		decisions[r.FRN] = pruner.Evaluate(r)
	}

	// node_modules itself is kept as a single folded shell
	if d := decisions[100]; !d.Keep || !d.Folded {
		t.Fatalf("node_modules decision = %+v, want Keep=true Folded=true", d)
	}
	// descendants under node_modules are dropped
	if d := decisions[200]; d.Keep {
		t.Fatalf("node_modules/lodash decision = %+v, want Keep=false", d)
	}
	if d := decisions[300]; d.Keep {
		t.Fatalf("node_modules/lodash/index.js decision = %+v, want Keep=false", d)
	}
	// normal user file is kept
	if d := decisions[400]; !d.Keep || d.Folded || d.System {
		t.Fatalf("app.go decision = %+v, want Keep=true Folded=false System=false", d)
	}
}

func TestTwoPassPruner_HardExcludeAndSoftDemotePropagation(t *testing.T) {
	records := []RawRecord{
		{FRN: 11, ParentFRN: 10, Name: "deleted.txt", IsDir: false},
		{FRN: 10, ParentFRN: 5, Name: "$Recycle.Bin", IsDir: true},
		{FRN: 21, ParentFRN: 20, Name: "data_0", IsDir: false},
		{FRN: 20, ParentFRN: 5, Name: "GPUCache", IsDir: true},
		{FRN: 30, ParentFRN: 5, Name: "scratch.tmp", IsDir: false},
		{FRN: 31, ParentFRN: 5, Name: "$MFT", IsDir: false},
		{FRN: 42, ParentFRN: 41, Name: "kernel32.dll", IsDir: false},
		{FRN: 41, ParentFRN: 40, Name: "System32", IsDir: true},
		{FRN: 40, ParentFRN: 5, Name: "Windows", IsDir: true},
	}

	sb := NewSkeletonBuilder(DefaultRules())
	for _, r := range records {
		if r.IsDir {
			sb.AddDir(r.FRN, r.ParentFRN, r.Name)
		}
	}
	pruner := sb.Build()

	for _, frn := range []uint64{10, 11, 20, 21, 30, 31} {
		var rec RawRecord
		for _, r := range records {
			if r.FRN == frn {
				rec = r
				break
			}
		}
		if d := pruner.Evaluate(rec); d.Keep {
			t.Fatalf("hard-excluded FRN %d (%s) decision = %+v, want Keep=false", frn, rec.Name, d)
		}
	}

	for _, frn := range []uint64{40, 41, 42} {
		var rec RawRecord
		for _, r := range records {
			if r.FRN == frn {
				rec = r
				break
			}
		}
		if d := pruner.Evaluate(rec); !d.Keep || !d.System {
			t.Fatalf("soft-demoted FRN %d (%s) decision = %+v, want Keep=true System=true", frn, rec.Name, d)
		}
	}
}

func TestTwoPassPruner_CycleAndSelfReferenceSafe(t *testing.T) {
	sb := NewSkeletonBuilder(DefaultRules())
	// Self-referencing directory + mutual 2-cycle under folded dir
	sb.AddDir(5, 5, "")
	sb.AddDir(100, 101, "node_modules")
	sb.AddDir(101, 100, "loop_child")
	sb.AddDir(200, 201, "Windows")
	sb.AddDir(201, 200, "SysCycle")

	pruner := sb.Build()
	if !pruner.IsPrunedDir(100) || !pruner.IsPrunedDir(101) {
		t.Fatalf("expected cyclic folded dirs 100 and 101 to be pruned")
	}
	if !pruner.IsSystemDir(200) || !pruner.IsSystemDir(201) {
		t.Fatalf("expected cyclic system dirs 200 and 201 to be marked system")
	}
}
