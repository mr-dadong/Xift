//go:build linux

package linux

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mr-dadong/Xift/internal/filter"
	"github.com/mr-dadong/Xift/internal/provider"
	"github.com/mr-dadong/Xift/internal/storage"
)

func TestLinuxProviderEnumerateVolumesReadOnly(t *testing.T) {
	p := New(nil, provider.Options{DisableLowPriority: true})
	vols, err := p.EnumerateVolumes(context.Background())
	if err != nil {
		t.Fatalf("EnumerateVolumes: %v", err)
	}
	if len(vols) == 0 {
		t.Fatal("expected at least 1 indexable Linux mount")
	}
	for _, v := range vols {
		if IsVirtualFSType(v.FSType) {
			t.Fatalf("virtual FS leaked into EnumerateVolumes: %+v", v)
		}
	}
}

func TestLinuxProviderFullScanAndInotifyFallbackEndToEnd(t *testing.T) {
	root := t.TempDir()
	nodeModDir := filepath.Join(root, "node_modules")
	if err := os.MkdirAll(filepath.Join(nodeModDir, "pkg"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(nodeModDir, "pkg", "index.js"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "seed.go"), []byte("package main\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	pruner := filter.NewPruner(filter.DefaultRuleEngine(), nil)
	p := New(pruner, provider.Options{DisableLowPriority: true, Workers: 2})

	eng, err := storage.Open(t.TempDir(), storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	vol := provider.VolumeInfo{VolID: 1, RootPath: root, FSType: "ext4"}
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

	hasFile := func(target string) bool {
		snap := eng.AcquireSnapshot()
		defer snap.Release()
		found := false
		snap.ForEachLive(func(_ uint32, _ storage.HotNode, name []byte) bool {
			if string(name) == target {
				found = true
				return false
			}
			return true
		})
		return found
	}

	if !hasFile("seed.go") || !hasFile("node_modules") {
		t.Fatal("expected seed.go and folded node_modules shell after FullScan")
	}
	if hasFile("index.js") {
		t.Fatal("node_modules/pkg/index.js should have been pruned during FullScan")
	}

	// Start real-time Watch (falls back smoothly to inotify without CAP_SYS_ADMIN).
	sink := provider.NewStorageEventSink(eng, map[uint16]string{1: root}, nil)
	batcher := provider.NewBatcher(provider.BatcherOptions{
		Window:             provider.DefaultBatchWindow,
		Pruner:             pruner,
		OnBatch:            sink.HandleBatch,
		DisableLowPriority: true,
	})

	ctx, cancel := context.WithCancel(context.Background())
	batcher.Start(ctx)

	evCh := make(chan provider.FileEvent, 128)
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

	select {
	case <-watchArmed:
	case wErr := <-watchDone:
		t.Fatalf("Watch exited before arming: %v", wErr)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Linux Watch to arm")
	}

	liveFile := filepath.Join(root, "live_linux.go")
	start := time.Now()
	if err := os.WriteFile(liveFile, []byte("package live\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(1500 * time.Millisecond)
	for !hasFile("live_linux.go") {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for live_linux.go visibility")
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Logf("Linux Watch + 150ms Batcher + Storage create visibility latency: %v", time.Since(start))

	renamedFile := filepath.Join(root, "renamed_linux.go")
	if err := os.Rename(liveFile, renamedFile); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(1500 * time.Millisecond)
	for !hasFile("renamed_linux.go") || hasFile("live_linux.go") {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for rename live_linux.go -> renamed_linux.go")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := os.Remove(renamedFile); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(1500 * time.Millisecond)
	for hasFile("renamed_linux.go") {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for delete of renamed_linux.go")
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-watchDone:
		if err != nil {
			t.Fatalf("Watch error on cancel: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Watch did not exit promptly after cancel")
	}
	<-pumpDone
	batcher.Close()
}
