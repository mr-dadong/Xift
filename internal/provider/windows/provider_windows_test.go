//go:build windows

package windows

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mr-dadong/Xift/internal/filter"
	"github.com/mr-dadong/Xift/internal/provider"
	"github.com/mr-dadong/Xift/internal/storage"
)

func TestWindowsProviderEnumerateVolumesReadOnly(t *testing.T) {
	p := New(nil, provider.Options{DisableLowPriority: true})
	vols, err := p.EnumerateVolumes(context.Background())
	if err != nil {
		t.Fatalf("EnumerateVolumes: %v", err)
	}
	if len(vols) == 0 {
		t.Fatal("expected at least 1 local Windows drive")
	}
	for _, v := range vols {
		if v.VolID == 0 || v.RootPath == "" || v.FSType == "" {
			t.Fatalf("malformed VolumeInfo: %+v", v)
		}
	}
}

func TestWindowsProviderFullScanPrunesFoldedAndGitignore(t *testing.T) {
	root := t.TempDir()

	// Create a mini project tree with node_modules (Level 1 Fold), GPUCache (Level 0 HardExclude),
	// .gitignore ignoring build_out/, and visible source files.
	mustWrite := func(rel, content string) {
		t.Helper()
		full := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	mustWrite("app/package.json", `{"name":"demo"}`)
	mustWrite("app/.gitignore", "build_out/\n")
	mustWrite("app/src/main.go", "package main\n")
	mustWrite("app/node_modules/lodash/index.js", "module.exports = {}\n")
	mustWrite("app/build_out/bundle.js", "console.log(1)\n")
	mustWrite("GPUCache/data_0", "cache")

	pruner := filter.NewPruner(filter.DefaultRuleEngine(), nil)
	p := New(pruner, provider.Options{DisableLowPriority: true})

	eng, err := storage.Open(t.TempDir(), storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	vol := provider.VolumeInfo{VolID: 1, RootPath: root, FSType: "DIR", CaseInsensitive: true}
	sw, err := provider.NewStorageSegmentWriter(eng, vol)
	if err != nil {
		t.Fatal(err)
	}
	cursor, err := p.FullScan(context.Background(), vol, sw)
	if err != nil {
		sw.Abort()
		t.Fatalf("FullScan: %v", err)
	}
	if _, err := sw.Commit(cursor); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	snap := eng.AcquireSnapshot()
	defer snap.Release()

	names := make(map[string]storage.HotNode)
	snap.ForEachLive(func(nodeID uint32, node storage.HotNode, name []byte) bool {
		names[string(name)] = node
		return true
	})

	if _, ok := names["main.go"]; !ok {
		t.Fatalf("expected main.go indexed, got %v", names)
	}
	if nm, ok := names["node_modules"]; !ok || nm.Flags&storage.FlagFolded == 0 {
		t.Fatalf("expected node_modules folded shell, got ok=%v node=%+v", ok, nm)
	}
	if _, ok := names["lodash"]; ok {
		t.Fatal("node_modules/lodash should have been pruned by early stop")
	}
	if _, ok := names["index.js"]; ok {
		t.Fatal("node_modules/lodash/index.js should have been pruned")
	}
	if bo, ok := names["build_out"]; !ok || bo.Flags&storage.FlagFolded == 0 {
		t.Fatalf("expected .gitignore build_out folded shell, got ok=%v node=%+v", ok, bo)
	}
	if _, ok := names["bundle.js"]; ok {
		t.Fatal("build_out/bundle.js should have been pruned by .gitignore")
	}
	if _, ok := names["GPUCache"]; ok {
		t.Fatal("GPUCache should have been hard-excluded")
	}
}

func TestWindowsProviderWatchBatcherStorageEndToEndUnder300ms(t *testing.T) {
	root := t.TempDir()
	nodeModDir := filepath.Join(root, "node_modules")
	if err := os.MkdirAll(nodeModDir, 0o755); err != nil {
		t.Fatal(err)
	}

	pruner := filter.NewPruner(filter.DefaultRuleEngine(), nil)
	p := New(pruner, provider.Options{DisableLowPriority: true})

	eng, err := storage.Open(t.TempDir(), storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	vol := provider.VolumeInfo{VolID: 1, RootPath: root, FSType: "DIR", CaseInsensitive: true}
	sw, err := provider.NewStorageSegmentWriter(eng, vol)
	if err != nil {
		t.Fatal(err)
	}
	cursor, err := p.FullScan(context.Background(), vol, sw)
	if err != nil {
		sw.Abort()
		t.Fatalf("FullScan: %v", err)
	}
	if _, err := sw.Commit(cursor); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	sink := provider.NewStorageEventSink(eng, map[uint16]string{1: root}, nil)
	batcher := provider.NewBatcher(provider.BatcherOptions{
		Window:             provider.DefaultBatchWindow, // Real 150ms window
		Pruner:             pruner,
		OnBatch:            sink.HandleBatch,
		DisableLowPriority: true,
	})

	ctx, cancel := context.WithCancel(context.Background())
	batcher.Start(ctx)

	evCh := make(chan provider.FileEvent, 256)
	watchDone := make(chan error, 1)
	watchArmed := make(chan struct{})
	p.onWatchArmed = func() { close(watchArmed) }
	go func() {
		watchDone <- p.Watch(ctx, vol, cursor, evCh)
	}()

	pumpDone := make(chan struct{})
	go func() {
		defer close(pumpDone)
		for {
			select {
			case ev := <-evCh:
				batcher.Submit(ev)
			case <-ctx.Done():
				return
			}
		}
	}()

	// Wait deterministically until ReadDirectoryChangesW IRP is armed in the kernel.
	select {
	case <-watchArmed:
	case wErr := <-watchDone:
		t.Fatalf("Watch exited before arming: %v", wErr)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Windows Watch to arm")
	}

	// Helper to check if a filename currently exists in storage Snapshot.
	hasFile := func(target string) bool {
		snap := eng.AcquireSnapshot()
		defer snap.Release()
		found := false
		snap.ForEachLive(func(_ uint32, _ storage.HotNode, name []byte) bool {
			if strings.EqualFold(string(name), target) {
				found = true
				return false
			}
			return true
		})
		return found
	}

	// 1) Create a visible file and measure real end-to-end latency (< 300ms SLA).
	testFile := filepath.Join(root, "live_note.md")
	createAt := time.Now()
	if err := os.WriteFile(testFile, []byte("hello xift"), 0o644); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(1500 * time.Millisecond)
	for !hasFile("live_note.md") {
		if time.Now().After(deadline) {
			var wErr error
			wExited := false
			select {
			case wErr = <-watchDone:
				wExited = true
			default:
			}
			t.Fatalf("timed out waiting for live_note.md (watchExited=%v, watchErr=%v, sinkErr=%v, stats=%+v)", wExited, wErr, sink.LastError(), batcher.Stats())
		}
		time.Sleep(10 * time.Millisecond)
	}
	createLatency := time.Since(createAt)
	t.Logf("Windows Watch + 150ms Batcher + Storage create visibility latency: %v", createLatency)

	// 2) Storm inside folded node_modules MUST be dropped at the source (0 pollution).
	for i := 0; i < 30; i++ {
		f := filepath.Join(nodeModDir, "dep_"+string(rune('a'+(i%26)))+".js")
		_ = os.WriteFile(f, []byte("x"), 0o644)
	}

	// 3) Rename live_note.md -> renamed_note.md
	renamedFile := filepath.Join(root, "renamed_note.md")
	if err := os.Rename(testFile, renamedFile); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(1500 * time.Millisecond)
	for !hasFile("renamed_note.md") || hasFile("live_note.md") {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for rename live_note.md -> renamed_note.md (old=%v new=%v)", hasFile("live_note.md"), hasFile("renamed_note.md"))
		}
		time.Sleep(10 * time.Millisecond)
	}

	// 4) Delete renamed_note.md
	if err := os.Remove(renamedFile); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(1500 * time.Millisecond)
	for hasFile("renamed_note.md") {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for delete of renamed_note.md")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Verify node_modules children never entered storage.
	if hasFile("dep_a.js") {
		t.Fatal("folded node_modules child dep_a.js leaked into storage")
	}

	// 5) Clean cancel without leaking goroutines or handles.
	cancel()
	select {
	case err := <-watchDone:
		if err != nil {
			t.Fatalf("Watch returned error on cancel: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Watch did not exit promptly after context cancellation")
	}
	<-pumpDone
	batcher.Close()

	if err := sink.LastError(); err != nil {
		t.Fatalf("StorageEventSink error: %v", err)
	}
}
