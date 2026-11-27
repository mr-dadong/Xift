package storage

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"
)

func TestEngineBaseCommitMutationsAndWALRecovery(t *testing.T) {
	dataDir := t.TempDir()

	eng, err := Open(dataDir, Options{})
	if err != nil {
		t.Fatalf("Open initial: %v", err)
	}

	// Build a Dense base segment with root, src/ (with 2000 children), archive/, and files.
	builder, err := eng.BeginDenseSegment()
	if err != nil {
		t.Fatalf("BeginDenseSegment: %v", err)
	}
	rootID, err := builder.AppendDense(1, 10, RootParentIdx, "C:", FlagDir, 0, 0, 1700000000)
	if err != nil {
		t.Fatalf("append root: %v", err)
	}
	srcID, err := builder.AppendDense(1, 20, rootID, "src", FlagDir, 0, 0, 1700000001)
	if err != nil {
		t.Fatalf("append src: %v", err)
	}
	archiveID, err := builder.AppendDense(1, 30, rootID, "archive", FlagDir, 0, 0, 1700000002)
	if err != nil {
		t.Fatalf("append archive: %v", err)
	}
	alphaID, err := builder.AppendDense(1, 40, rootID, "alpha.txt", 0, 0, 100, 1700000003)
	if err != nil {
		t.Fatalf("append alpha: %v", err)
	}
	betaID, err := builder.AppendDense(1, 50, rootID, "beta.txt", 0, 0, 200, 1700000004)
	if err != nil {
		t.Fatalf("append beta: %v", err)
	}
	reportID, err := builder.AppendDense(1, 60, rootID, "report.pdf", 0, 0, 1024, 1700000005)
	if err != nil {
		t.Fatalf("append report: %v", err)
	}
	var lastChildID uint32
	for i := 0; i < 2000; i++ {
		id, err := builder.AppendDense(1, uint64(1000+i), srcID, fmt.Sprintf("file_%04d.go", i), 0, 0, int64(i*10), 1700001000+int64(i))
		if err != nil {
			t.Fatalf("append child %d: %v", i, err)
		}
		lastChildID = id
	}
	segMeta, err := builder.Finish()
	if err != nil {
		t.Fatalf("Finish base segment: %v", err)
	}
	if err := eng.CommitSegment(segMeta, &VolumeCursor{VolID: 1, Cursor: 500, RootPath: `C:\`}); err != nil {
		t.Fatalf("CommitSegment: %v", err)
	}

	// Verify initial ResolvePath and ReadColdMetaBatch.
	if p, err := eng.ResolvePath(lastChildID); err != nil || p != "C:/src/file_1999.go" {
		t.Fatalf("ResolvePath(lastChild) = (%q, %v)", p, err)
	}
	batchMeta := make([]ColdMeta, 2)
	if err := eng.ReadColdMetaBatch([]uint32{reportID, lastChildID}, batchMeta); err != nil {
		t.Fatalf("ReadColdMetaBatch: %v", err)
	}
	if batchMeta[0].Size != 1024 || batchMeta[1].Size != 19990 {
		t.Fatalf("unexpected batch cold meta: %+v", batchMeta)
	}

	// Apply real-time mutations: MoveDir src -> archive, Delete alpha, Rename beta -> beta_v2, ModifyMeta report, Create new file, Update cursor.
	muts := []Mutation{
		{Op: OpMoveDir, VolID: 1, NodeKey: 20, NewParentKey: 30},
		{Op: OpDelete, VolID: 1, NodeKey: 40},
		{Op: OpRename, VolID: 1, NodeKey: 50, Name: "beta_v2.txt"},
		{Op: OpModifyMeta, VolID: 1, NodeKey: 60, Size: 4096, ModTime: 1700099999},
		{Op: OpCreate, VolID: 1, NodeKey: 9999, ParentKey: 20, Name: "新增文档.md", Size: 777, ModTime: 1700088888},
		{Op: OpCursorUpdate, VolID: 1, Cursor: 999, RootPath: `C:\`},
	}
	if err := eng.ApplyBatch(muts); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}

	// Verify O(1) MoveDir immediately updates all 2000 children's paths without rewriting children.
	if p, err := eng.ResolvePath(lastChildID); err != nil || p != "C:/archive/src/file_1999.go" {
		t.Fatalf("after MoveDir ResolvePath(lastChild) = (%q, %v), want C:/archive/src/file_1999.go", p, err)
	}
	if _, err := eng.ResolvePath(alphaID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted alpha ResolvePath = %v, want ErrNotFound", err)
	}
	if p, err := eng.ResolvePath(betaID); err != nil || p != "C:/beta_v2.txt" {
		t.Fatalf("renamed beta ResolvePath = (%q, %v), want C:/beta_v2.txt", p, err)
	}
	if cm, err := eng.ReadColdMeta(reportID); err != nil || cm.Size != 4096 || cm.ModTime != 1700099999 {
		t.Fatalf("modified report cold meta = (%+v, %v)", cm, err)
	}
	// Verify archiveID still exists and live count is exact (2006 - 1 deleted + 1 created = 2006).
	if got := eng.LiveCount(); got != 2006 {
		t.Fatalf("LiveCount = %d, want 2006", got)
	}
	if _ = archiveID; eng.MemTableEntryCount() == 0 {
		t.Fatalf("expected MemTable entries before Flush")
	}

	// Plant an orphan segment dir to verify Open cleans it up.
	orphanDir := filepath.Join(dataDir, "index", "segments", "seg_000999")
	if err := os.MkdirAll(orphanDir, 0o755); err != nil {
		t.Fatalf("mkdir orphan: %v", err)
	}

	// Close WITHOUT Flush, reopen, and verify WAL replay restores everything identically!
	if err := eng.Close(); err != nil {
		t.Fatalf("Close eng: %v", err)
	}

	eng2, err := Open(dataDir, Options{})
	if err != nil {
		t.Fatalf("Reopen eng2: %v", err)
	}
	defer eng2.Close()

	if _, err := os.Stat(orphanDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphan segment dir was not cleaned on Open: %v", err)
	}
	if eng2.OpenStats().ReplayedWALCount != len(muts) {
		t.Fatalf("ReplayedWALCount = %d, want %d", eng2.OpenStats().ReplayedWALCount, len(muts))
	}
	if p, err := eng2.ResolvePath(lastChildID); err != nil || p != "C:/archive/src/file_1999.go" {
		t.Fatalf("reopened ResolvePath(lastChild) = (%q, %v)", p, err)
	}
	if p, err := eng2.ResolvePath(betaID); err != nil || p != "C:/beta_v2.txt" {
		t.Fatalf("reopened ResolvePath(beta) = (%q, %v)", p, err)
	}
	if cm, err := eng2.ReadColdMeta(reportID); err != nil || cm.Size != 4096 || cm.ModTime != 1700099999 {
		t.Fatalf("reopened cold meta = (%+v, %v)", cm, err)
	}
	if vc, ok := eng2.VolumeCursor(1); !ok || vc.Cursor != 999 {
		t.Fatalf("reopened cursor = (%+v, %v), want 999", vc, ok)
	}
	if got := eng2.LiveCount(); got != 2006 {
		t.Fatalf("reopened LiveCount = %d, want 2006", got)
	}
	// Verify no duplicates on ForEachLive.
	snap := eng2.AcquireSnapshot()
	seenIDs := make(map[uint32]int, 2048)
	foundNew := false
	snap.ForEachLive(func(nodeID uint32, node HotNode, name []byte) bool {
		seenIDs[nodeID]++
		if string(name) == "新增文档.md" {
			foundNew = true
		}
		return true
	})
	snap.Release()
	if !foundNew || len(seenIDs) != 2006 {
		t.Fatalf("ForEachLive foundNew=%v unique=%d, want true/2006", foundNew, len(seenIDs))
	}
	for id, c := range seenIDs {
		if c != 1 {
			t.Fatalf("duplicate nodeID %d visited %d times", id, c)
		}
	}
}

func TestEngineResolvePathCycleProtection(t *testing.T) {
	dataDir := t.TempDir()
	eng, err := Open(dataDir, Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer eng.Close()

	// Build a segment containing a malicious cycle A(0) -> B(1) -> A(0).
	b, err := eng.BeginDenseSegment()
	if err != nil {
		t.Fatalf("BeginDenseSegment: %v", err)
	}
	if err := b.Append(0, 1, 100, 1, "A", FlagDir, 0, 0, 1); err != nil {
		t.Fatalf("append A: %v", err)
	}
	if err := b.Append(1, 1, 200, 0, "B", FlagDir, 0, 0, 1); err != nil {
		t.Fatalf("append B: %v", err)
	}
	meta, err := b.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if err := eng.CommitSegment(meta, nil); err != nil {
		t.Fatalf("CommitSegment: %v", err)
	}
	if _, err := eng.ResolvePath(0); !errors.Is(err, ErrPathCycle) {
		t.Fatalf("expected ErrPathCycle, got %v", err)
	}
}

func TestEngineFlushCompactTierAndStateMachine(t *testing.T) {
	dataDir := t.TempDir()
	eng, err := Open(dataDir, Options{Tier2Threshold: 500})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Tier recommendation checks.
	if eng.RecommendedTier() != Tier1Mmap {
		t.Fatalf("empty engine should recommend Tier1Mmap")
	}

	// Reference model for randomized state-machine equivalence testing.
	type refNode struct {
		volID     uint16
		key       uint64
		parentKey uint64 // 0 means root
		name      string
		isDir     bool
		size      int64
		modTime   int64
	}
	ref := make(map[uint64]*refNode)

	// Seed root and 2 initial dirs.
	seed := []Mutation{
		{Op: OpCreate, VolID: 1, NodeKey: 1, ParentKey: 0, Name: "root", Flags: FlagDir, Size: 0, ModTime: 1},
		{Op: OpCreate, VolID: 1, NodeKey: 2, ParentKey: 1, Name: "dirA", Flags: FlagDir, Size: 0, ModTime: 2},
		{Op: OpCreate, VolID: 1, NodeKey: 3, ParentKey: 1, Name: "dirB", Flags: FlagDir, Size: 0, ModTime: 3},
	}
	if err := eng.ApplyBatch(seed); err != nil {
		t.Fatalf("seed ApplyBatch: %v", err)
	}
	ref[1] = &refNode{volID: 1, key: 1, parentKey: 0, name: "root", isDir: true, modTime: 1}
	ref[2] = &refNode{volID: 1, key: 2, parentKey: 1, name: "dirA", isDir: true, modTime: 2}
	ref[3] = &refNode{volID: 1, key: 3, parentKey: 1, name: "dirB", isDir: true, modTime: 3}

	// Flush to create first delta segment.
	if err := eng.Flush(); err != nil {
		t.Fatalf("initial Flush: %v", err)
	}

	rng := rand.New(rand.NewPCG(42, 99))
	nextKey := uint64(10)
	fileKeys := make([]uint64, 0, 600)

	for step := 0; step < 600; step++ {
		action := rng.IntN(10)
		switch {
		case action < 5 || len(fileKeys) == 0: // Create file
			k := nextKey
			nextKey++
			parent := uint64(2)
			if rng.IntN(2) == 1 {
				parent = 3
			}
			name := fmt.Sprintf("item_%d.go", k)
			sz := int64(rng.IntN(10000))
			mt := int64(1700000000 + step)
			if err := eng.ApplyBatch([]Mutation{{
				Op: OpCreate, VolID: 1, NodeKey: k, ParentKey: parent, Name: name, Size: sz, ModTime: mt,
			}}); err != nil {
				t.Fatalf("step %d create: %v", step, err)
			}
			ref[k] = &refNode{volID: 1, key: k, parentKey: parent, name: name, size: sz, modTime: mt}
			fileKeys = append(fileKeys, k)

		case action == 5: // ModifyMeta
			k := fileKeys[rng.IntN(len(fileKeys))]
			if rn, ok := ref[k]; ok {
				sz := int64(rng.IntN(50000) + 1)
				mt := int64(1800000000 + step)
				if err := eng.ApplyBatch([]Mutation{{
					Op: OpModifyMeta, VolID: 1, NodeKey: k, Size: sz, ModTime: mt,
				}}); err != nil {
					t.Fatalf("step %d modify: %v", step, err)
				}
				rn.size = sz
				rn.modTime = mt
			}

		case action == 6: // Rename file
			k := fileKeys[rng.IntN(len(fileKeys))]
			if rn, ok := ref[k]; ok {
				newName := fmt.Sprintf("renamed_%d_%d.md", k, step)
				if err := eng.ApplyBatch([]Mutation{{
					Op: OpRename, VolID: 1, NodeKey: k, Name: newName,
				}}); err != nil {
					t.Fatalf("step %d rename: %v", step, err)
				}
				rn.name = newName
			}

		case action == 7: // Delete file
			idx := rng.IntN(len(fileKeys))
			k := fileKeys[idx]
			if _, ok := ref[k]; ok {
				if err := eng.ApplyBatch([]Mutation{{
					Op: OpDelete, VolID: 1, NodeKey: k,
				}}); err != nil {
					t.Fatalf("step %d delete: %v", step, err)
				}
				delete(ref, k)
			}

		case action == 8: // Move dirB under dirA or back to root
			newParent := uint64(2)
			if ref[3].parentKey == 2 {
				newParent = 1
			}
			if err := eng.ApplyBatch([]Mutation{{
				Op: OpMoveDir, VolID: 1, NodeKey: 3, NewParentKey: newParent,
			}}); err != nil {
				t.Fatalf("step %d movedir: %v", step, err)
			}
			ref[3].parentKey = newParent

		case action == 9: // Periodic Flush
			if err := eng.Flush(); err != nil {
				t.Fatalf("step %d flush: %v", step, err)
			}
		}
	}

	resolveRefPath := func(k uint64) string {
		parts := []string{}
		cur := k
		for cur != 0 {
			rn := ref[cur]
			parts = append(parts, rn.name)
			cur = rn.parentKey
		}
		for i, j := 0, len(parts)-1; i < j; i, j = i+1, j-1 {
			parts[i], parts[j] = parts[j], parts[i]
		}
		out := ""
		for i, p := range parts {
			if i > 0 {
				out += "/"
			}
			out += p
		}
		return out
	}

	verifyMatchesRef := func(label string, e *Engine) {
		t.Helper()
		if got := e.LiveCount(); got != uint64(len(ref)) {
			t.Fatalf("%s: LiveCount = %d, want %d", label, got, len(ref))
		}
		snap := e.AcquireSnapshot()
		defer snap.Release()

		gotByPath := make(map[string]ColdMeta, len(ref))
		snap.ForEachLive(func(nodeID uint32, node HotNode, name []byte) bool {
			p, err := snap.ResolvePath(nodeID)
			if err != nil {
				t.Fatalf("%s: ResolvePath(%d) failed: %v", label, nodeID, err)
			}
			cm, err := snap.ReadColdMeta(nodeID)
			if err != nil {
				t.Fatalf("%s: ReadColdMeta(%d) failed: %v", label, nodeID, err)
			}
			if _, dup := gotByPath[p]; dup {
				t.Fatalf("%s: duplicate path %q", label, p)
			}
			gotByPath[p] = cm
			return true
		})

		if len(gotByPath) != len(ref) {
			t.Fatalf("%s: visited %d paths, want %d", label, len(gotByPath), len(ref))
		}
		for k, rn := range ref {
			wantPath := resolveRefPath(k)
			gotCM, ok := gotByPath[wantPath]
			if !ok {
				t.Fatalf("%s: missing expected path %q", label, wantPath)
			}
			if gotCM.Size != rn.size || gotCM.ModTime != rn.modTime {
				t.Fatalf("%s: path %q cold meta = %+v, want {Size:%d ModTime:%d}", label, wantPath, gotCM, rn.size, rn.modTime)
			}
		}
	}

	verifyMatchesRef("pre-compact", eng)

	// Compact must merge all delta segments into 1 zero-tombstone Dense segment and clean old dirs.
	if err := eng.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	verifyMatchesRef("post-compact", eng)

	segEntries, err := os.ReadDir(filepath.Join(dataDir, "index", "segments"))
	if err != nil {
		t.Fatalf("ReadDir segments: %v", err)
	}
	if len(segEntries) != 1 {
		t.Fatalf("after Compact expected exactly 1 segment dir on disk, got %d", len(segEntries))
	}

	if err := eng.Close(); err != nil {
		t.Fatalf("Close after compact: %v", err)
	}

	// Reopen with LowMemoryMode=true -> RecommendedTier must be Tier2LSM.
	engLowMem, err := Open(dataDir, Options{LowMemoryMode: true})
	if err != nil {
		t.Fatalf("Reopen low mem: %v", err)
	}
	defer engLowMem.Close()
	if engLowMem.RecommendedTier() != Tier2LSM {
		t.Fatalf("LowMemoryMode should recommend Tier2LSM")
	}
	verifyMatchesRef("reopen-post-compact", engLowMem)

	// Check threshold trigger with small Tier2Threshold.
	engSmallThresh, err := Open(dataDir, Options{Tier2Threshold: 10})
	if err != nil {
		t.Fatalf("Reopen small threshold: %v", err)
	}
	defer engSmallThresh.Close()
	if engSmallThresh.RecommendedTier() != Tier2LSM {
		t.Fatalf("LiveCount >= Tier2Threshold should recommend Tier2LSM")
	}
}

func TestEngineMVCCSnapshotIsolationAndCrashFencing(t *testing.T) {
	dataDir := t.TempDir()
	eng, err := Open(dataDir, Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	b, err := eng.BeginDenseSegment()
	if err != nil {
		t.Fatalf("BeginDenseSegment: %v", err)
	}
	rootID, _ := b.AppendDense(1, 1, RootParentIdx, "C:", FlagDir, 0, 0, 10)
	dirID, _ := b.AppendDense(1, 2, rootID, "docs", FlagDir, 0, 0, 11)
	otherID, _ := b.AppendDense(1, 3, rootID, "archive", FlagDir, 0, 0, 12)
	fileID, _ := b.AppendDense(1, 4, dirID, "spec.md", FlagHidden, 0, 1000, 13)
	_ = otherID
	meta, err := b.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if err := eng.CommitSegment(meta, &VolumeCursor{VolID: 1, Cursor: 100, RootPath: `C:\`}); err != nil {
		t.Fatalf("CommitSegment: %v", err)
	}

	// Capture MVCC Snapshot BEFORE concurrent mutations + Flush.
	snapBefore := eng.AcquireSnapshot()

	// Perform MoveDir, Rename (clearing FlagHidden via ReplaceFlags), ModifyMeta, Create, then Flush.
	if err := eng.ApplyBatch([]Mutation{
		{Op: OpMoveDir, VolID: 1, NodeKey: 2, NewParentKey: 3},
		{Op: OpRename, VolID: 1, NodeKey: 4, Name: "spec_v2.md", Flags: 0, ReplaceFlags: true},
		{Op: OpModifyMeta, VolID: 1, NodeKey: 4, Size: 2000, ModTime: 99},
		{Op: OpCreate, VolID: 1, NodeKey: 5, ParentKey: 2, Name: "extra.go", Size: 50, ModTime: 100},
	}); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
	if err := eng.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// snapBefore MUST still observe the exact pre-mutation point-in-time state!
	if p, err := snapBefore.ResolvePath(fileID); err != nil || p != "C:/docs/spec.md" {
		t.Fatalf("snapBefore ResolvePath(file) = (%q, %v), want C:/docs/spec.md", p, err)
	}
	if cm, err := snapBefore.ReadColdMeta(fileID); err != nil || cm.Size != 1000 {
		t.Fatalf("snapBefore ReadColdMeta(file) = (%+v, %v), want Size=1000", cm, err)
	}
	if snapBefore.LiveCount() != 4 {
		t.Fatalf("snapBefore LiveCount = %d, want 4", snapBefore.LiveCount())
	}
	snapBefore.Release()

	// Current engine state MUST reflect all mutations and cleared FlagHidden.
	if p, err := eng.ResolvePath(fileID); err != nil || p != "C:/archive/docs/spec_v2.md" {
		t.Fatalf("post-flush ResolvePath(file) = (%q, %v), want C:/archive/docs/spec_v2.md", p, err)
	}
	snapAfter := eng.AcquireSnapshot()
	if n, _, ok := snapAfter.LookupNode(fileID); !ok || (n.Flags&FlagHidden) != 0 {
		t.Fatalf("expected FlagHidden cleared via ReplaceFlags, got ok=%v flags=0x%x", ok, n.Flags)
	}
	snapAfter.Release()

	// Verify detached orphan child does NOT get promoted to root across Compact().
	if err := eng.ApplyBatch([]Mutation{{Op: OpDelete, VolID: 1, NodeKey: 2}}); err != nil {
		t.Fatalf("delete dir docs: %v", err)
	}
	if _, err := eng.ResolvePath(fileID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("pre-compact orphan ResolvePath = %v, want ErrNotFound", err)
	}
	if err := eng.Compact(); err != nil {
		t.Fatalf("Compact with orphan: %v", err)
	}
	// After Compact, find spec_v2.md nodeID and confirm ResolvePath still returns ErrNotFound (not promoted to root!).
	snapCompact := eng.AcquireSnapshot()
	var orphanID uint32
	foundOrphan := false
	snapCompact.ForEachLive(func(nodeID uint32, node HotNode, name []byte) bool {
		if string(name) == "spec_v2.md" {
			orphanID = nodeID
			foundOrphan = true
			return false
		}
		return true
	})
	snapCompact.Release()
	if !foundOrphan {
		t.Fatalf("expected spec_v2.md present after Compact")
	}
	if p, err := eng.ResolvePath(orphanID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("post-compact orphan promoted unexpectedly: path=%q err=%v", p, err)
	}

	// Verify Closed engine returns ErrClosed consistently.
	if err := eng.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := eng.ReadColdMeta(0); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed ReadColdMeta = %v, want ErrClosed", err)
	}
	if _, err := eng.ResolvePath(0); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed ResolvePath = %v, want ErrClosed", err)
	}
}
