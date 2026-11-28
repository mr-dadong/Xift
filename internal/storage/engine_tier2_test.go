package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mr-dadong/Xift/internal/storage/lsm"
	"github.com/mr-dadong/Xift/internal/textkey"
)

// tierSwitchFixture seeds one Tier 1 segment (below threshold), then crosses
// the threshold and flushes again so the newest segment is Tier 2. Returns the
// data dir, the live engine, and the node keys in creation order.
type tierSwitchNode struct {
	nodeKey uint64
	name    string
	size    int64
	mtime   int64
	isDir   bool
}

func tierSwitchCorpus() []tierSwitchNode {
	return []tierSwitchNode{
		{nodeKey: 1, name: "root", isDir: true, mtime: 1},
		{nodeKey: 2, name: "dirA", isDir: true, mtime: 2},
		{nodeKey: 3, name: "alpha.txt", size: 11, mtime: 3},
		{nodeKey: 4, name: "beta.txt", size: 22, mtime: 4},
		{nodeKey: 5, name: "设计文档.md", size: 33, mtime: 5},
		{nodeKey: 6, name: "gamma.go", size: 44, mtime: 6},
	}
}

func tierSwitchParentKey(nodeKey uint64) uint64 {
	switch nodeKey {
	case 1:
		return 0
	case 2:
		return 1
	default:
		return 2
	}
}

func applyTierSwitchNodes(t *testing.T, eng *Engine, nodes []tierSwitchNode) {
	t.Helper()
	for _, n := range nodes {
		var flags uint16
		if n.isDir {
			flags |= FlagDir
		}
		if err := eng.ApplyBatch([]Mutation{{
			Op:        OpCreate,
			VolID:     1,
			NodeKey:   n.nodeKey,
			ParentKey: tierSwitchParentKey(n.nodeKey),
			Name:      n.name,
			Flags:     flags,
			Size:      n.size,
			ModTime:   n.mtime,
		}}); err != nil {
			t.Fatalf("create %q: %v", n.name, err)
		}
	}
}

// verifyHybridLive checks that every node is visible with identical semantics
// regardless of which segment tier owns it.
func verifyHybridLive(t *testing.T, eng *Engine, corpus []tierSwitchNode) {
	t.Helper()
	snap := eng.AcquireSnapshot()
	defer snap.Release()

	if got := snap.LiveCount(); got != uint64(len(corpus)) {
		t.Fatalf("snapshot LiveCount = %d, want %d", got, len(corpus))
	}
	if got := eng.LiveCount(); got != uint64(len(corpus)) {
		t.Fatalf("engine LiveCount = %d, want %d", got, len(corpus))
	}

	seen := make([]string, 0, len(corpus))
	snap.ForEachLive(func(nodeID uint32, node HotNode, name []byte) bool {
		seen = append(seen, string(name))
		return true
	})
	want := make([]string, 0, len(corpus))
	for _, n := range corpus {
		want = append(want, n.name)
	}
	slices.Sort(seen)
	slices.Sort(want)
	if !slices.Equal(seen, want) {
		t.Fatalf("ForEachLive = %v, want %v", seen, want)
	}

	for id, n := range corpus {
		node, name, ok := snap.LookupNode(uint32(id))
		if !ok {
			t.Fatalf("LookupNode(%d) miss", id)
		}
		if string(name) != n.name {
			t.Fatalf("LookupNode(%d) name = %q, want %q", id, name, n.name)
		}
		if isDir := (node.Flags & FlagDir) != 0; isDir != n.isDir {
			t.Fatalf("LookupNode(%d) dir = %v, want %v", id, isDir, n.isDir)
		}
		cm, err := snap.ReadColdMeta(uint32(id))
		if err != nil {
			t.Fatalf("ReadColdMeta(%d): %v", id, err)
		}
		if cm.Size != n.size || cm.ModTime != n.mtime {
			t.Fatalf("ReadColdMeta(%d) = %+v, want size=%d mtime=%d", id, cm, n.size, n.mtime)
		}
		path, err := snap.ResolvePath(uint32(id))
		if err != nil {
			t.Fatalf("ResolvePath(%d): %v", id, err)
		}
		if n.nodeKey == 1 {
			if path != "root" {
				t.Fatalf("ResolvePath(root) = %q", path)
			}
		} else if n.nodeKey == 2 {
			if path != "root/dirA" {
				t.Fatalf("ResolvePath(dirA) = %q", path)
			}
		} else if want := "root/dirA/" + n.name; path != want {
			t.Fatalf("ResolvePath(%q) = %q, want %q", n.name, path, want)
		}
	}
}

func TestTierSwitchHybrid(t *testing.T) {
	dataDir := t.TempDir()
	const cacheBytes int64 = 1 << 20
	eng, err := Open(dataDir, Options{
		Tier2Threshold:               4,
		DisableWALSync:               true,
		BlockCacheBytes:              cacheBytes,
		DisableBackgroundMaintenance: true, // keep the hybrid window open for assertions
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	corpus := tierSwitchCorpus()

	// Phase 1: 3 live nodes < threshold 4 -> Tier 1 segment.
	applyTierSwitchNodes(t, eng, corpus[:3])
	if eng.RecommendedTier() != Tier1Mmap {
		t.Fatalf("below threshold recommended tier = %s", eng.RecommendedTier())
	}
	if err := eng.Flush(); err != nil {
		t.Fatalf("first Flush: %v", err)
	}
	if len(eng.manifest.Segments) != 1 {
		t.Fatalf("segments after first flush = %d", len(eng.manifest.Segments))
	}
	if eng.manifest.Segments[0].Tier2 {
		t.Fatal("first segment must stay Tier 1 below the threshold")
	}

	// Phase 2: 6 live nodes >= threshold 4 -> new segment is Tier 2.
	applyTierSwitchNodes(t, eng, corpus[3:])
	if err := eng.Flush(); err != nil {
		t.Fatalf("second Flush: %v", err)
	}
	if got := len(eng.manifest.Segments); got != 2 {
		t.Fatalf("segments after second flush = %d", got)
	}
	if eng.manifest.Segments[0].Tier2 {
		t.Fatal("existing Tier 1 segment must not be rewritten in place")
	}
	if !eng.manifest.Segments[1].Tier2 {
		t.Fatal("post-switch segment must be Tier 2")
	}
	if !eng.segments[1].IsTier2() || eng.segments[0].IsTier2() {
		t.Fatalf("hybrid segment tiers = (%v, %v)", eng.segments[0].IsTier2(), eng.segments[1].IsTier2())
	}
	if eng.RecommendedTier() != Tier2LSM {
		t.Fatalf("post-switch recommended tier = %s", eng.RecommendedTier())
	}
	if got := eng.ActiveTier(); got != Tier2LSM {
		t.Fatalf("post-switch active tier = %s", got)
	}
	if used, limit := eng.BlockCacheStats(); limit != cacheBytes || used > limit {
		t.Fatalf("block cache stats = (%d, %d), limit want %d", used, limit, cacheBytes)
	}

	// Hybrid coverage: Tier 1 and Tier 2 segments are both fully readable.
	verifyHybridLive(t, eng, corpus)

	// The Tier 2 segment's inverted index covers its own names.
	tri, bi := eng.segments[1].Tier2Inverted()
	if tri == nil || bi == nil {
		t.Fatal("Tier 2 segment must expose inverted indexes")
	}
	gram := textkey.AppendTrigramKeys([]byte("beta.txt"), nil)[0]
	raw, ok, err := tri.LookupPosting(eng.manifest.Segments[1].ID, 0, gram)
	if err != nil || !ok {
		t.Fatalf("trigram posting for beta.txt: ok=%v err=%v", ok, err)
	}
	posting, err := lsm.ParseRoaring(raw)
	if err != nil {
		t.Fatal(err)
	}
	// beta.txt is the first row (localIdx 0) of the second segment.
	if !posting.Contains(0) {
		t.Fatalf("trigram posting for beta.txt = %v, want contains localIdx 0", posting)
	}

	// Cached reads must stay inside the configured budget.
	if cacheBytes != eng.BlockCache().Limit() {
		t.Fatalf("block cache limit = %d, want %d", eng.BlockCache().Limit(), cacheBytes)
	}
	if eng.BlockCache().Bytes() == 0 {
		t.Fatal("Tier 2 reads must populate the block cache")
	}
	if eng.BlockCache().Bytes() > eng.BlockCache().Limit() {
		t.Fatalf("block cache %d exceeds limit %d", eng.BlockCache().Bytes(), eng.BlockCache().Limit())
	}

	if err := eng.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen: the switch is durable and stays latched for new segments.
	reopened, err := Open(dataDir, Options{
		DisableWALSync:               true,
		BlockCacheBytes:              cacheBytes,
		DisableBackgroundMaintenance: true,
	})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	if got := len(reopened.manifest.Segments); got != 2 {
		t.Fatalf("reopened segments = %d", got)
	}
	if reopened.manifest.Segments[0].Tier2 || !reopened.manifest.Segments[1].Tier2 {
		t.Fatal("Tier2 flags must roundtrip through MANIFEST")
	}
	if !reopened.tier2Sticky {
		t.Fatal("reopening a Tier 2 index must latch Tier 2 mode")
	}
	verifyHybridLive(t, reopened, corpus)

	// Sticky: a fresh segment after reopen is Tier 2 even though the default
	// threshold (5M) is far above the live count.
	applyTierSwitchNodes(t, reopened, []tierSwitchNode{
		{nodeKey: 7, name: "epsilon.txt", size: 55, mtime: 7},
	})
	if err := reopened.Flush(); err != nil {
		t.Fatalf("post-reopen Flush: %v", err)
	}
	segs := reopened.manifest.Segments
	if !segs[len(segs)-1].Tier2 {
		t.Fatal("latched Tier 2 mode must build new segments as Tier 2")
	}
	corpus = append(corpus, tierSwitchNode{nodeKey: 7, name: "epsilon.txt", size: 55, mtime: 7})
	verifyHybridLive(t, reopened, corpus)

	// Segment directories must all exist for the manifest entries.
	for _, sm := range segs {
		dir := filepath.Join(dataDir, "index", "segments", SegmentDirName(sm.ID))
		if _, err := filepath.Glob(filepath.Join(dir, "meta.hdr")); err != nil {
			t.Fatalf("segment %d: %v", sm.ID, err)
		}
	}
}

// engineSegmentMetas returns a copy of the committed segment metadata.
func engineSegmentMetas(e *Engine) []SegmentMeta {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]SegmentMeta, len(e.manifest.Segments))
	copy(out, e.manifest.Segments)
	return out
}

func snapshotNames(snap *Snapshot) []string {
	var names []string
	snap.ForEachLive(func(_ uint32, _ HotNode, name []byte) bool {
		names = append(names, string(name))
		return true
	})
	slices.Sort(names)
	return names
}

func TestTier2Rebuild(t *testing.T) {
	dataDir := t.TempDir()
	eng, err := Open(dataDir, Options{
		Tier2Threshold:  8,
		DisableWALSync:  true,
		BlockCacheBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Phase 1: 4 live nodes below the threshold -> Tier 1 segment.
	first := tierSwitchCorpus()[:4] // root, dirA, alpha.txt, beta.txt
	applyTierSwitchNodes(t, eng, first)
	if err := eng.Flush(); err != nil {
		t.Fatalf("first Flush: %v", err)
	}
	segs := engineSegmentMetas(eng)
	if len(segs) != 1 || segs[0].Tier2 {
		t.Fatalf("pre-switch segments = %+v", segs)
	}

	// Snapshot taken before the switch must keep its point-in-time view
	// through the background rebuild.
	pre := eng.AcquireSnapshot()
	preNames := snapshotNames(pre)

	// Phase 2: cross the threshold -> Tier 2 segment + background rebuild.
	second := []tierSwitchNode{
		{nodeKey: 5, name: "设计文档.md", size: 33, mtime: 5},
		{nodeKey: 6, name: "gamma.go", size: 44, mtime: 6},
		{nodeKey: 7, name: "delta.txt", size: 55, mtime: 7},
		{nodeKey: 8, name: "epsilon.txt", size: 66, mtime: 8},
		{nodeKey: 9, name: "zeta.log", size: 77, mtime: 9},
		{nodeKey: 10, name: "eta.bin", size: 88, mtime: 10},
	}
	applyTierSwitchNodes(t, eng, second)
	if err := eng.Flush(); err != nil {
		t.Fatalf("second Flush: %v", err)
	}

	// Hammer the engine with live mutations and queries while the background
	// rebuild runs; neither may fail or observe an intermediate state.
	var (
		writeErr   atomic.Value
		writeCount atomic.Int64
		stop       = make(chan struct{})
		wg         sync.WaitGroup
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		key := uint64(100)
		for {
			select {
			case <-stop:
				return
			default:
			}
			err := eng.ApplyBatch([]Mutation{{
				Op:        OpCreate,
				VolID:     1,
				NodeKey:   key,
				ParentKey: 2,
				Name:      fmt.Sprintf("concurrent_%d.txt", key),
				Size:      1,
				ModTime:   int64(key),
			}})
			if err != nil {
				writeErr.Store(err)
				return
			}
			writeCount.Add(1)
			key++
			snap := eng.AcquireSnapshot()
			snap.ForEachLive(func(uint32, HotNode, []byte) bool { return true })
			snap.Release()
			time.Sleep(time.Millisecond)
		}
	}()

	deadline := time.Now().Add(10 * time.Second)
	for {
		segs = engineSegmentMetas(eng)
		settled := len(segs) == 1 && segs[0].Tier2
		if settled {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("background rebuild did not settle: %+v", segs)
		}
		time.Sleep(2 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	if v := writeErr.Load(); v != nil {
		t.Fatalf("concurrent write failed: %v", v)
	}
	if writeCount.Load() == 0 {
		t.Fatal("concurrent writer made no progress")
	}

	// MVCC isolation: the pre-switch snapshot still sees exactly the original
	// four names.
	wantPre := make([]string, 0, len(first))
	for _, n := range first {
		wantPre = append(wantPre, n.name)
	}
	slices.Sort(wantPre)
	if !slices.Equal(preNames, wantPre) {
		t.Fatalf("pre-switch snapshot = %v, want %v", preNames, wantPre)
	}
	pre.Release()

	// Post-rebuild view: all pre-switch names survived the rebuild and every
	// concurrent live mutation is visible.
	snap := eng.AcquireSnapshot()
	names := snapshotNames(snap)
	wantLive := uint64(len(first)+len(second)) + uint64(writeCount.Load())
	if got := snap.LiveCount(); got != wantLive {
		t.Fatalf("LiveCount after rebuild = %d, want %d", got, wantLive)
	}
	for _, n := range append(append([]tierSwitchNode(nil), first...), second...) {
		if !slices.Contains(names, n.name) {
			t.Fatalf("rebuild dropped %q from live set", n.name)
		}
	}
	concurrentSeen := 0
	for _, n := range names {
		if len(n) > len("concurrent_") && n[:len("concurrent_")] == "concurrent_" {
			concurrentSeen++
		}
	}
	if int64(concurrentSeen) != writeCount.Load() {
		t.Fatalf("concurrent names = %d, want %d", concurrentSeen, writeCount.Load())
	}

	// The rebuilt dense Tier 2 segment must carry inverted postings for names
	// that predate the switch (i.e. the rebuild reindexed Tier 1 data).
	segs = engineSegmentMetas(eng)
	tri, bi := eng.segments[0].Tier2Inverted()
	if tri == nil || bi == nil {
		t.Fatal("rebuilt segment must expose inverted indexes")
	}
	raw, ok, err := tri.LookupPosting(segs[0].ID, 0, textkey.AppendTrigramKeys([]byte("alpha.txt"), nil)[0])
	if err != nil || !ok {
		t.Fatalf("rebuilt trigram posting for alpha.txt: ok=%v err=%v", ok, err)
	}
	v, err := lsm.ParseRoaring(raw)
	if err != nil {
		t.Fatal(err)
	}
	if v.Cardinality() == 0 {
		t.Fatal("rebuilt trigram posting for alpha.txt must not be empty")
	}
	snap.Release()

	// Real-time visibility after the rebuild: a brand-new node resolves
	// immediately, without any flush.
	if err := eng.ApplyBatch([]Mutation{{
		Op:        OpCreate,
		VolID:     1,
		NodeKey:   999,
		ParentKey: 2,
		Name:      "realtime_after_rebuild.txt",
		Size:      42,
		ModTime:   42,
	}}); err != nil {
		t.Fatalf("post-rebuild ApplyBatch: %v", err)
	}
	var found bool
	post := eng.AcquireSnapshot()
	post.ForEachLive(func(_ uint32, _ HotNode, name []byte) bool {
		if string(name) == "realtime_after_rebuild.txt" {
			found = true
			return false
		}
		return true
	})
	post.Release()
	if !found {
		t.Fatal("real-time mutation not visible after rebuild")
	}

	if err := eng.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Restart must recover the rebuilt, fully Tier 2 layout.
	reopened, err := Open(dataDir, Options{DisableWALSync: true, BlockCacheBytes: 1 << 20})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	segs = engineSegmentMetas(reopened)
	if len(segs) != 1 || !segs[0].Tier2 {
		t.Fatalf("reopened segments = %+v, want a single Tier 2 segment", segs)
	}
	if got, want := reopened.LiveCount(), wantLive+1; got != want {
		t.Fatalf("reopened LiveCount = %d, want %d", got, want)
	}
}

// liveView reconstructs path -> cold metadata for every live node.
func liveView(t *testing.T, e *Engine) map[string]ColdMeta {
	t.Helper()
	snap := e.AcquireSnapshot()
	defer snap.Release()
	out := make(map[string]ColdMeta)
	snap.ForEachLive(func(nodeID uint32, _ HotNode, _ []byte) bool {
		p, err := snap.ResolvePath(nodeID)
		if err != nil {
			t.Fatalf("ResolvePath(%d): %v", nodeID, err)
		}
		cm, err := snap.ReadColdMeta(nodeID)
		if err != nil {
			t.Fatalf("ReadColdMeta(%d): %v", nodeID, err)
		}
		if _, dup := out[p]; dup {
			t.Fatalf("duplicate live path %q", p)
		}
		out[p] = cm
		return true
	})
	return out
}

func TestTierUpgradeCompat(t *testing.T) {
	dataDir := t.TempDir()
	manifestPath := filepath.Join(dataDir, "index", "MANIFEST")
	backupPath := manifestPath + manifestBackupSuffix

	// Build a plain Tier 1 index (default 5M threshold never latches).
	eng, err := Open(dataDir, Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	applyTierSwitchNodes(t, eng, tierSwitchCorpus())
	if err := eng.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	before := liveView(t, eng)
	if len(before) != len(tierSwitchCorpus()) {
		t.Fatalf("pre-upgrade live nodes = %d", len(before))
	}
	for _, sm := range engineSegmentMetas(eng) {
		if sm.Tier2 {
			t.Fatal("pre-upgrade index must be Tier 1 only")
		}
	}
	if _, err := os.Stat(backupPath); !os.IsNotExist(err) {
		t.Fatalf("no MANIFEST backup expected before upgrade, stat err = %v", err)
	}
	preUpgradeManifest, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read MANIFEST: %v", err)
	}
	if err := eng.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen in low-memory (Tier 2) mode and trigger the first Tier 2 build.
	// The background rebuild is disabled so the hybrid upgrade state is
	// observable; Compact is then exercised explicitly.
	upgraded, err := Open(dataDir, Options{
		DisableWALSync:               true,
		LowMemoryMode:                true,
		DisableBackgroundMaintenance: true,
	})
	if err != nil {
		t.Fatalf("reopen for upgrade: %v", err)
	}
	if upgraded.RecommendedTier() != Tier2LSM {
		t.Fatalf("low-memory reopen recommended tier = %s", upgraded.RecommendedTier())
	}
	applyTierSwitchNodes(t, upgraded, []tierSwitchNode{
		{nodeKey: 7, name: "omega.txt", size: 99, mtime: 7},
	})
	if err := upgraded.Flush(); err != nil {
		t.Fatalf("upgrade Flush: %v", err)
	}

	// The pre-upgrade MANIFEST bytes must be preserved verbatim.
	backup, err := os.ReadFile(backupPath)
	if err != nil {
		t.Fatalf("MANIFEST backup missing after upgrade: %v", err)
	}
	if !bytes.Equal(backup, preUpgradeManifest) {
		t.Fatal("MANIFEST backup must be the exact pre-upgrade bytes")
	}
	backupManifest, existed, err := LoadManifest(backupPath)
	if err != nil || !existed {
		t.Fatalf("LoadManifest(backup): existed=%v err=%v", existed, err)
	}
	if backupManifest.HasTier2() {
		t.Fatal("backup MANIFEST must be the pre-upgrade (Tier 1) state")
	}

	// Hybrid window: every pre-upgrade node keeps its path and cold metadata.
	hybrid := liveView(t, upgraded)
	if len(hybrid) != len(before)+1 {
		t.Fatalf("hybrid live nodes = %d, want %d", len(hybrid), len(before)+1)
	}
	for p, cm := range before {
		got, ok := hybrid[p]
		if !ok {
			t.Fatalf("upgrade dropped live path %q", p)
		}
		if got != cm {
			t.Fatalf("upgrade changed cold meta for %q: %+v -> %+v", p, cm, got)
		}
	}
	if _, ok := hybrid["root/dirA/omega.txt"]; !ok {
		t.Fatal("new post-upgrade node missing")
	}

	// Explicit Compact closes the hybrid window and reindexes everything.
	if err := upgraded.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	compacted := liveView(t, upgraded)
	if len(compacted) != len(hybrid) {
		t.Fatalf("post-compact live nodes = %d, want %d", len(compacted), len(hybrid))
	}
	for p, cm := range hybrid {
		if got := compacted[p]; got != cm {
			t.Fatalf("post-compact cold meta for %q = %+v, want %+v", p, got, cm)
		}
	}
	for _, sm := range engineSegmentMetas(upgraded) {
		if !sm.Tier2 {
			t.Fatalf("post-compact segment %d must be Tier 2", sm.ID)
		}
	}

	// Compaction must never overwrite the rollback snapshot.
	backupAfterCompact, err := os.ReadFile(backupPath)
	if err != nil {
		t.Fatalf("read backup after compact: %v", err)
	}
	if !bytes.Equal(backupAfterCompact, preUpgradeManifest) {
		t.Fatal("compaction overwrote the pre-upgrade MANIFEST backup")
	}
	if err := upgraded.Close(); err != nil {
		t.Fatalf("Close upgraded: %v", err)
	}

	// A normal reopen reads the upgraded index with identical results.
	reopened, err := Open(dataDir, Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("reopen upgraded: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	final := liveView(t, reopened)
	if len(final) != len(compacted) {
		t.Fatalf("reopened live nodes = %d, want %d", len(final), len(compacted))
	}
	for p, cm := range compacted {
		if got := final[p]; got != cm {
			t.Fatalf("reopened cold meta for %q = %+v, want %+v", p, got, cm)
		}
	}
}

// tieredCompactionNodes builds 9 flush batches: the first carries the root and
// one directory, each of the rest carries a single file.
func tieredCompactionNodes() [][]tierSwitchNode {
	batches := make([][]tierSwitchNode, 0, 9)
	batches = append(batches, []tierSwitchNode{
		{nodeKey: 1, name: "root", isDir: true, mtime: 1},
		{nodeKey: 2, name: "dirA", isDir: true, mtime: 2},
	})
	key := uint64(10)
	for i := 0; i < 8; i++ {
		batches = append(batches, []tierSwitchNode{
			{nodeKey: key, name: fmt.Sprintf("seg_%02d_file.txt", i), size: int64(key), mtime: int64(key)},
		})
		key++
	}
	return batches
}

// flushTieredSegments creates one segment per batch.
func flushTieredSegments(t *testing.T, eng *Engine, batches [][]tierSwitchNode) {
	t.Helper()
	for _, batch := range batches {
		for _, n := range batch {
			var flags uint16
			if n.isDir {
				flags |= FlagDir
			}
			parent := uint64(2)
			if n.nodeKey == 1 {
				parent = 0
			}
			if err := eng.ApplyBatch([]Mutation{{
				Op:        OpCreate,
				VolID:     1,
				NodeKey:   n.nodeKey,
				ParentKey: parent,
				Name:      n.name,
				Flags:     flags,
				Size:      n.size,
				ModTime:   n.mtime,
			}}); err != nil {
				t.Fatalf("create %q: %v", n.name, err)
			}
		}
		if err := eng.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}
	}
}

func TestTieredCompaction(t *testing.T) {
	dataDir := t.TempDir()
	eng, err := Open(dataDir, Options{
		DisableWALSync:               true,
		LowMemoryMode:                true,
		BlockCacheBytes:              1 << 20,
		DisableBackgroundMaintenance: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	batches := tieredCompactionNodes()
	flushTieredSegments(t, eng, batches)
	if got := len(engineSegmentMetas(eng)); got != 9 {
		t.Fatalf("segments = %d, want 9", got)
	}
	for _, sm := range engineSegmentMetas(eng) {
		if !sm.Tier2 {
			t.Fatalf("low-memory engine must emit Tier 2 segments, got %+v", sm)
		}
	}

	beforeNames := liveView(t, eng)
	oldSegs := make([]*Segment, 8)
	oldDirs := make([]string, 8)
	for i := 0; i < 8; i++ {
		oldSegs[i] = eng.segments[i]
		oldDirs[i] = eng.segments[i].Dir()
	}
	untouched := eng.segments[8]

	// An open snapshot must keep the merged segments readable and their files
	// on disk until it is released.
	snap := eng.AcquireSnapshot()

	if err := eng.CompactOldest(8); err != nil {
		t.Fatalf("CompactOldest: %v", err)
	}

	metas := engineSegmentMetas(eng)
	if len(metas) != 2 {
		t.Fatalf("post-merge segments = %d, want 2", len(metas))
	}
	if !metas[0].Tier2 || !metas[1].Tier2 {
		t.Fatalf("merged layout must stay Tier 2: %+v", metas)
	}
	if eng.segments[1] != untouched {
		t.Fatal("the newest segment must not be rewritten by a tiered merge")
	}
	if eng.segments[0] == oldSegs[0] {
		t.Fatal("oldest segments must be replaced by the merged segment")
	}
	if eng.segments[0].Count() != 9 {
		t.Fatalf("merged segment node count = %d, want 9 live rows", eng.segments[0].Count())
	}

	// All live data survives with identical paths/metadata.
	afterNames := liveView(t, eng)
	if len(afterNames) != len(beforeNames) {
		t.Fatalf("live nodes = %d, want %d", len(afterNames), len(beforeNames))
	}
	for p, cm := range beforeNames {
		if got := afterNames[p]; got != cm {
			t.Fatalf("post-merge meta for %q = %+v, want %+v", p, got, cm)
		}
	}

	// The merged segment's inverted index must cover names from the oldest
	// segment (root/dirA/first file), proving the rebuild reindexed everything.
	tri, bi := eng.segments[0].Tier2Inverted()
	if tri == nil || bi == nil {
		t.Fatal("merged Tier 2 segment must expose inverted indexes")
	}
	raw, ok, err := tri.LookupPosting(metas[0].ID, 0, textkey.AppendTrigramKeys([]byte("seg_00_file.txt"), nil)[0])
	if err != nil || !ok {
		t.Fatalf("merged trigram posting: ok=%v err=%v", ok, err)
	}
	posting, err := lsm.ParseRoaring(raw)
	if err != nil {
		t.Fatal(err)
	}
	if posting.Cardinality() == 0 {
		t.Fatal("merged trigram posting must not be empty")
	}

	// Reference-counted deferred deletion: the pre-merge snapshot still reads
	// old segments and their files remain until it is released.
	if _, err := os.Stat(oldDirs[0]); err != nil {
		t.Fatalf("old segment dir removed while a snapshot is open: %v", err)
	}
	oldView := snapshotNames(snap)
	if len(oldView) != len(beforeNames) {
		t.Fatalf("open snapshot live view = %d names, want %d", len(oldView), len(beforeNames))
	}
	snap.Release()
	for _, dir := range oldDirs {
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			t.Fatalf("old segment dir %s not removed after snapshot release (err=%v)", dir, err)
		}
	}

	// The merge is durable.
	if err := eng.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened, err := Open(dataDir, Options{DisableWALSync: true, DisableBackgroundMaintenance: true})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	if got := len(engineSegmentMetas(reopened)); got != 2 {
		t.Fatalf("reopened segments = %d, want 2", got)
	}
	finalNames := liveView(t, reopened)
	if len(finalNames) != len(beforeNames) {
		t.Fatalf("reopened live view = %d names, want %d", len(finalNames), len(beforeNames))
	}
}

func TestTieredCompactionAutoTrigger(t *testing.T) {
	dataDir := t.TempDir()
	eng, err := Open(dataDir, Options{
		DisableWALSync:  true,
		LowMemoryMode:   true,
		BlockCacheBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = eng.Close() }()

	// One segment per batch: 1 dir batch + 32 single-file batches crosses the
	// 32-segment threshold, so the worker merges the oldest 8 into one.
	const files = 32
	batches := make([][]tierSwitchNode, 0, files+1)
	batches = append(batches, []tierSwitchNode{
		{nodeKey: 1, name: "root", isDir: true, mtime: 1},
		{nodeKey: 2, name: "dirA", isDir: true, mtime: 2},
	})
	for i := 0; i < files; i++ {
		key := uint64(10 + i)
		batches = append(batches, []tierSwitchNode{
			{nodeKey: key, name: fmt.Sprintf("seg_%02d_file.txt", i), size: int64(key), mtime: int64(key)},
		})
	}
	flushTieredSegments(t, eng, batches)

	wantSegs := files + 1 - tier2CompactionBatch + 1
	wantLive := 2 + files
	deadline := time.Now().Add(30 * time.Second)
	for {
		segs := engineSegmentMetas(eng)
		if len(segs) == wantSegs {
			for _, sm := range segs {
				if !sm.Tier2 {
					t.Fatalf("auto-merged layout must stay Tier 2: %+v", segs)
				}
			}
			// The merge must preserve every live node.
			live := liveView(t, eng)
			if len(live) != wantLive {
				t.Fatalf("post-merge live nodes = %d, want %d", len(live), wantLive)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("background tiered compaction did not trigger: %d segments", len(segs))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestCompactionCrashRecovery simulates a process crash between the merged
// segment being written and the MANIFEST switch, then verifies recovery.
func TestCompactionCrashRecovery(t *testing.T) {
	if os.Getenv("XIFT_TEST_COMPACTION_CRASH") == "1" {
		dataDir := os.Getenv("XIFT_TEST_COMPACTION_CRASH_DIR")
		eng, err := Open(dataDir, Options{
			DisableWALSync:               true,
			LowMemoryMode:                true,
			DisableBackgroundMaintenance: true,
		})
		if err != nil {
			os.Exit(3)
		}
		compactionTestHook = func(point string) {
			if point == "segment-written" {
				os.Exit(2) // die without committing MANIFEST or cleaning up
			}
		}
		_ = eng.CompactOldest(8)
		os.Exit(4) // the hook must have fired
	}

	dataDir := t.TempDir()
	eng, err := Open(dataDir, Options{
		DisableWALSync:               true,
		LowMemoryMode:                true,
		DisableBackgroundMaintenance: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	flushTieredSegments(t, eng, tieredCompactionNodes())
	before := liveView(t, eng)
	if got := len(engineSegmentMetas(eng)); got != 9 {
		t.Fatalf("segments = %d, want 9", got)
	}
	if err := eng.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	segDir := filepath.Join(dataDir, "index", "segments")
	dirsBefore, err := os.ReadDir(segDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestCompactionCrashRecovery$")
	cmd.Env = append(os.Environ(),
		"XIFT_TEST_COMPACTION_CRASH=1",
		"XIFT_TEST_COMPACTION_CRASH_DIR="+dataDir,
	)
	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 2 {
		t.Fatalf("helper did not crash at the commit boundary: err=%v output=%s", err, out)
	}

	// Crash state: the uncommitted merged segment exists on disk, but the
	// MANIFEST still lists the original nine segments.
	dirsAfterCrash, err := os.ReadDir(segDir)
	if err != nil {
		t.Fatalf("ReadDir after crash: %v", err)
	}
	if len(dirsAfterCrash) != len(dirsBefore)+1 {
		t.Fatalf("segment dirs after crash = %d, want %d", len(dirsAfterCrash), len(dirsBefore)+1)
	}
	crashedManifest, existed, err := LoadManifest(filepath.Join(dataDir, "index", "MANIFEST"))
	if err != nil || !existed {
		t.Fatalf("LoadManifest after crash: existed=%v err=%v", existed, err)
	}
	if len(crashedManifest.Segments) != 9 {
		t.Fatalf("crashed MANIFEST segments = %d, want 9", len(crashedManifest.Segments))
	}

	// Recovery: orphan cleanup removes the aborted merge output and the index
	// remains fully readable.
	recovered, err := Open(dataDir, Options{
		DisableWALSync:               true,
		LowMemoryMode:                true,
		DisableBackgroundMaintenance: true,
	})
	if err != nil {
		t.Fatalf("recovery Open: %v", err)
	}
	if got := recovered.OpenStats().OrphanSegmentsRemoved; got < 1 {
		t.Fatalf("orphan segments removed = %d, want >= 1", got)
	}
	if got := len(engineSegmentMetas(recovered)); got != 9 {
		t.Fatalf("recovered segments = %d, want 9", got)
	}
	recoveredNames := liveView(t, recovered)
	if len(recoveredNames) != len(before) {
		t.Fatalf("recovered live nodes = %d, want %d", len(recoveredNames), len(before))
	}
	for p, cm := range before {
		if got := recoveredNames[p]; got != cm {
			t.Fatalf("recovered meta for %q = %+v, want %+v", p, got, cm)
		}
	}
	cleaned, err := os.ReadDir(segDir)
	if err != nil {
		t.Fatalf("ReadDir after recovery: %v", err)
	}
	if len(cleaned) != 9 {
		t.Fatalf("segment dirs after recovery = %d, want 9", len(cleaned))
	}

	// Compaction must be retryable after recovery.
	if err := recovered.CompactOldest(8); err != nil {
		t.Fatalf("CompactOldest after recovery: %v", err)
	}
	if got := len(engineSegmentMetas(recovered)); got != 2 {
		t.Fatalf("post-retry segments = %d, want 2", got)
	}
	if err := recovered.Close(); err != nil {
		t.Fatalf("Close recovered: %v", err)
	}
}

// TestCompactionDoesNotBlockQueries pauses the merge after the segment is
// written (before the commit lock) and proves queries and real-time mutations
// keep working with no intermediate state.
func TestCompactionDoesNotBlockQueries(t *testing.T) {
	dataDir := t.TempDir()
	eng, err := Open(dataDir, Options{
		DisableWALSync:               true,
		LowMemoryMode:                true,
		DisableBackgroundMaintenance: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = eng.Close() }()
	flushTieredSegments(t, eng, tieredCompactionNodes())
	before := liveView(t, eng)

	built := make(chan struct{})
	allow := make(chan struct{})
	compactionTestHook = func(point string) {
		if point == "segment-written" {
			close(built)
			<-allow
		}
	}
	defer func() { compactionTestHook = nil }()

	done := make(chan error, 1)
	go func() { done <- eng.CompactOldest(8) }()

	select {
	case <-built:
	case <-time.After(10 * time.Second):
		t.Fatal("compaction never reached the commit boundary")
	}

	// The merge holds no engine lock here: queries must complete promptly and
	// must observe either the pre-merge or post-merge view, never a partial one.
	queryDone := make(chan error, 1)
	go func() {
		for i := 0; i < 20; i++ {
			snap := eng.AcquireSnapshot()
			if got := snap.LiveCount(); got != uint64(len(before)) {
				snap.Release()
				queryDone <- fmt.Errorf("query observed intermediate LiveCount %d, want %d", got, len(before))
				return
			}
			if names := snapshotNames(snap); len(names) != len(before) {
				snap.Release()
				queryDone <- fmt.Errorf("query observed %d names, want %d", len(names), len(before))
				return
			}
			snap.Release()
		}
		queryDone <- nil
	}()
	select {
	case err := <-queryDone:
		if err != nil {
			close(allow)
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		close(allow)
		t.Fatal("queries blocked while compaction was pending")
	}

	// Real-time mutations during the merge must be visible immediately.
	if err := eng.ApplyBatch([]Mutation{{
		Op:        OpCreate,
		VolID:     1,
		NodeKey:   777,
		ParentKey: 2,
		Name:      "during_merge.txt",
		Size:      7,
		ModTime:   7,
	}}); err != nil {
		close(allow)
		t.Fatalf("mutation during merge: %v", err)
	}
	snap := eng.AcquireSnapshot()
	var seen bool
	snap.ForEachLive(func(_ uint32, _ HotNode, name []byte) bool {
		if string(name) == "during_merge.txt" {
			seen = true
			return false
		}
		return true
	})
	if got := snap.LiveCount(); got != uint64(len(before)+1) {
		snap.Release()
		close(allow)
		t.Fatalf("snapshot LiveCount during merge = %d, want %d", got, len(before)+1)
	}
	snap.Release()
	if !seen {
		close(allow)
		t.Fatal("mutation during merge not visible")
	}

	close(allow)
	if err := <-done; err != nil {
		t.Fatalf("CompactOldest: %v", err)
	}
	if got := len(engineSegmentMetas(eng)); got != 2 {
		t.Fatalf("post-commit segments = %d, want 2", got)
	}
	after := liveView(t, eng)
	if len(after) != len(before)+1 {
		t.Fatalf("post-commit live nodes = %d, want %d", len(after), len(before)+1)
	}
}

// TestTieredCompactionOutOfOrderNodeIDs covers a rename/modify flush that lands
// a smaller NodeID in a newer segment: the merge must k-way merge by NodeID,
// not concatenate segments in order.
func TestTieredCompactionOutOfOrderNodeIDs(t *testing.T) {
	dataDir := t.TempDir()
	eng, err := Open(dataDir, Options{
		DisableWALSync:               true,
		LowMemoryMode:                true,
		DisableBackgroundMaintenance: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = eng.Close() }()

	apply := func(muts ...Mutation) {
		t.Helper()
		if err := eng.ApplyBatch(muts); err != nil {
			t.Fatalf("ApplyBatch: %v", err)
		}
	}
	apply(
		Mutation{Op: OpCreate, VolID: 1, NodeKey: 1, ParentKey: 0, Name: "root", Flags: FlagDir},
		Mutation{Op: OpCreate, VolID: 1, NodeKey: 2, ParentKey: 1, Name: "alpha.txt"},
		Mutation{Op: OpCreate, VolID: 1, NodeKey: 3, ParentKey: 1, Name: "beta.txt"},
	)
	if err := eng.Flush(); err != nil {
		t.Fatalf("first Flush: %v", err)
	}
	// Rename the middle node: it is tombstoned in seg0 and rewritten as a
	// smaller NodeID in the next delta segment.
	apply(Mutation{Op: OpRename, VolID: 1, NodeKey: 2, Name: "alpha_renamed.txt"})
	if err := eng.Flush(); err != nil {
		t.Fatalf("rename Flush: %v", err)
	}
	if got := len(engineSegmentMetas(eng)); got != 2 {
		t.Fatalf("segments = %d, want 2", got)
	}

	if err := eng.CompactOldest(2); err != nil {
		t.Fatalf("CompactOldest: %v", err)
	}
	metas := engineSegmentMetas(eng)
	if len(metas) != 1 {
		t.Fatalf("post-merge segments = %d, want 1", len(metas))
	}
	names := snapshotNames(func() *Snapshot { s := eng.AcquireSnapshot(); t.Cleanup(s.Release); return s }())
	want := []string{"alpha_renamed.txt", "beta.txt", "root"}
	if !slices.Equal(names, want) {
		t.Fatalf("merged live names = %v, want %v", names, want)
	}
	if _, err := eng.ResolvePath(0); err != nil {
		t.Fatalf("ResolvePath(root) after merge: %v", err)
	}
}

// TestTieredCompactionAbortsOnConcurrentTombstone proves a merge whose frozen
// tombstone view becomes stale is discarded instead of resurrecting rows.
func TestTieredCompactionAbortsOnConcurrentTombstone(t *testing.T) {
	dataDir := t.TempDir()
	eng, err := Open(dataDir, Options{
		DisableWALSync:               true,
		LowMemoryMode:                true,
		DisableBackgroundMaintenance: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = eng.Close() }()

	flushTieredSegments(t, eng, tieredCompactionNodes())
	if got := len(engineSegmentMetas(eng)); got != 9 {
		t.Fatalf("segments = %d, want 9", got)
	}

	built := make(chan struct{})
	allow := make(chan struct{})
	compactionTestHook = func(point string) {
		if point == "segment-written" {
			close(built)
			<-allow
		}
	}
	defer func() { compactionTestHook = nil }()

	done := make(chan error, 1)
	go func() { done <- eng.CompactOldest(8) }()
	select {
	case <-built:
	case <-time.After(10 * time.Second):
		t.Fatal("merge never reached the commit boundary")
	}

	// Delete a node that lives in the oldest segment and commit its tombstone.
	if err := eng.ApplyBatch([]Mutation{{Op: OpDelete, VolID: 1, NodeKey: 10}}); err != nil {
		close(allow)
		t.Fatalf("delete during merge: %v", err)
	}
	if err := eng.Flush(); err != nil {
		close(allow)
		t.Fatalf("flush during merge: %v", err)
	}
	close(allow)
	if err := <-done; err != nil {
		t.Fatalf("CompactOldest: %v", err)
	}
	compactionTestHook = nil

	// The merge must have been discarded, not committed with a stale view.
	metas := engineSegmentMetas(eng)
	if len(metas) != 9 {
		t.Fatalf("stale merge committed: segments = %d, want 9", len(metas))
	}
	if names := snapshotNames(func() *Snapshot { s := eng.AcquireSnapshot(); t.Cleanup(s.Release); return s }()); slices.Contains(names, "seg_00_file.txt") {
		t.Fatalf("deleted node resurrected after aborted merge: %v", names)
	}

	// A retry after the tombstone is committed must succeed and keep the delete.
	if err := eng.CompactOldest(8); err != nil {
		t.Fatalf("retry CompactOldest: %v", err)
	}
	snap := eng.AcquireSnapshot()
	names := snapshotNames(snap)
	snap.Release()
	if slices.Contains(names, "seg_00_file.txt") {
		t.Fatalf("deleted node present after retry merge: %v", names)
	}
	if len(names) != 9 {
		t.Fatalf("live names after retry = %d, want 9", len(names))
	}
}

func TestEngineConcurrentClose(t *testing.T) {
	dataDir := t.TempDir()
	eng, err := Open(dataDir, Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := eng.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		}()
	}
	wg.Wait()

	reopened, err := Open(dataDir, Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("reopen after concurrent Close: %v", err)
	}
	_ = reopened.Close()
}
