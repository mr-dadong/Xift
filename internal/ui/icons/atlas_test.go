package icons

import (
	"fmt"
	"image"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

type mockExtractor struct {
	extCalls  atomic.Int64
	fileCalls atomic.Int64
}

func (m *mockExtractor) ExtractExt(ext string, isDir bool) (image.Image, error) {
	m.extCalls.Add(1)
	return RenderBadge(ext, isDir), nil
}

func (m *mockExtractor) ExtractFile(absPath string) (image.Image, error) {
	m.fileCalls.Add(1)
	return RenderBadge(".exe", false), nil
}

func TestAtlasSharedExtIconAndIdleDebounceAndClear(t *testing.T) {
	mock := &mockExtractor{}
	readyCh := make(chan struct{}, 8)
	atlas := NewAtlas(Config{
		Extractor:      mock,
		MaxExtEntries:  32,
		MaxFileEntries: 16,
		IdleDelay:      80 * time.Millisecond,
		Workers:        2,
		OnReady: func() {
			select {
			case readyCh <- struct{}{}:
			default:
			}
		},
	})
	defer atlas.Close()

	// 1. 60 .go files share 1 single ExtractExt call and 0 ExtractFile calls.
	for i := 0; i < 60; i++ {
		bmp := atlas.IconFor(fmt.Sprintf("file_%02d.go", i), fmt.Sprintf("/src/file_%02d.go", i), false)
		if bmp == nil {
			t.Fatalf("expected non-nil bitmap for row %d", i)
		}
	}
	if got := mock.extCalls.Load(); got != 1 {
		t.Fatalf("ExtractExt calls = %d, want 1", got)
	}
	if got := mock.fileCalls.Load(); got != 0 {
		t.Fatalf("ExtractFile calls = %d, want 0", got)
	}

	// 2. Rapid scrolling resets idle timer: no per-file extraction while scrolling is active.
	for i := 0; i < 4; i++ {
		atlas.NotifyVisible([]string{"/bin/tool.exe"})
		time.Sleep(25 * time.Millisecond)
	}
	if got := mock.fileCalls.Load(); got != 0 {
		t.Fatalf("ExtractFile fired during active scrolling: %d", got)
	}

	// 3. After scroll stops for >= 80ms, async per-file extraction completes and notifies OnReady.
	select {
	case <-readyCh:
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for idle per-file icon extraction")
	}
	if got := mock.fileCalls.Load(); got != 1 {
		t.Fatalf("ExtractFile calls after idle = %d, want 1", got)
	}

	extCount, fileCount := atlas.Stats()
	if extCount != 1 || fileCount != 1 {
		t.Fatalf("Stats = (%d, %d), want (1, 1)", extCount, fileCount)
	}

	// 4. Clear releases all cached GPU icon bitmaps (30s reclaim).
	atlas.Clear()
	extCount, fileCount = atlas.Stats()
	if extCount != 0 || fileCount != 0 {
		t.Fatalf("Stats after Clear = (%d, %d), want (0, 0)", extCount, fileCount)
	}
}

func TestPlatformExtractorRepeatedNoLeakSmoke(t *testing.T) {
	ext := newPlatformExtractor()
	iters := 20
	if runtime.GOOS == "windows" {
		iters = 200
	}
	for i := 0; i < iters; i++ {
		img, err := ext.ExtractExt(".go", false)
		if err != nil || img == nil {
			t.Fatalf("iter %d ExtractExt(.go) failed: img=%v err=%v", i, img, err)
		}
		dirImg, err := ext.ExtractExt("", true)
		if err != nil || dirImg == nil {
			t.Fatalf("iter %d ExtractExt(dir) failed: img=%v err=%v", i, dirImg, err)
		}
	}
}

func TestAtlasCloseConcurrentWithIdleTimerNoPanic(t *testing.T) {
	for i := 0; i < 30; i++ {
		mock := &mockExtractor{}
		atlas := NewAtlas(Config{
			Extractor: mock,
			IdleDelay: 1 * time.Millisecond,
			Workers:   2,
		})
		atlas.NotifyVisible([]string{"/bin/a.exe", "/bin/b.exe"})
		time.Sleep(1 * time.Millisecond)
		atlas.Close()
	}
}
