package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/mr-dadong/Xift/internal/filter"
	"github.com/mr-dadong/Xift/internal/provider"
)

type fakeVolProvider struct {
	provider.FSProvider
	vols []provider.VolumeInfo
}

func (f fakeVolProvider) EnumerateVolumes(context.Context) ([]provider.VolumeInfo, error) {
	return f.vols, nil
}

// TestDaemonConfigWithoutFeatures verifies the disabled path never opens a
// provider.
func TestDaemonConfigWithoutFeatures(t *testing.T) {
	old := newProvider
	t.Cleanup(func() { newProvider = old })
	called := false
	newProvider = func(*filter.Pruner) (provider.FSProvider, error) {
		called = true
		return nil, errors.New("must not be called")
	}
	cfg, err := daemonConfig(context.Background(), t.TempDir(), "endpoint", false, false)
	if err != nil {
		t.Fatalf("daemonConfig: %v", err)
	}
	if called {
		t.Fatal("provider was opened with both features disabled")
	}
	if cfg.L2 != nil || cfg.Watch != nil {
		t.Fatalf("config = %+v, want no L2 and no watch", cfg)
	}
	if cfg.Endpoint != "endpoint" {
		t.Fatalf("Endpoint = %q, want endpoint", cfg.Endpoint)
	}
}

// TestDaemonConfigL2Unsupported verifies an unsupported platform surfaces as a
// startup error when L2 was explicitly requested.
func TestDaemonConfigL2Unsupported(t *testing.T) {
	old := newProvider
	t.Cleanup(func() { newProvider = old })
	newProvider = func(*filter.Pruner) (provider.FSProvider, error) {
		return nil, provider.ErrUnsupportedOS
	}
	if _, err := daemonConfig(context.Background(), t.TempDir(), "endpoint", true, false); !errors.Is(err, provider.ErrUnsupportedOS) {
		t.Fatalf("daemonConfig err = %v, want ErrUnsupportedOS", err)
	}
}

// TestDaemonWatchFlag verifies --watch defaults to on, shares one provider
// with L2, and degrades to static serving when the platform cannot watch.
func TestDaemonWatchFlag(t *testing.T) {
	old := newProvider
	t.Cleanup(func() { newProvider = old })

	var calls atomic.Int32
	newProvider = func(*filter.Pruner) (provider.FSProvider, error) {
		calls.Add(1)
		return fakeVolProvider{vols: []provider.VolumeInfo{{VolID: 1, RootPath: t.TempDir()}}}, nil
	}
	cfg, err := daemonConfig(context.Background(), t.TempDir(), "endpoint", false, true)
	if err != nil {
		t.Fatalf("watch-only daemonConfig: %v", err)
	}
	if cfg.Watch == nil || !cfg.Watch.Enabled || len(cfg.Watch.Volumes) != 1 {
		t.Fatalf("watch config = %+v, want enabled with one volume", cfg.Watch)
	}
	if cfg.L2 != nil {
		t.Fatal("watch-only config must not enable L2")
	}

	cfg, err = daemonConfig(context.Background(), t.TempDir(), "endpoint", true, true)
	if err != nil {
		t.Fatalf("l2+watch daemonConfig: %v", err)
	}
	if cfg.L2 == nil || cfg.Watch == nil || !cfg.Watch.Enabled {
		t.Fatalf("config = %+v, want both L2 and watch", cfg)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("provider calls = %d, want one per daemonConfig (2 total)", got)
	}

	newProvider = func(*filter.Pruner) (provider.FSProvider, error) {
		return nil, provider.ErrUnsupportedOS
	}
	cfg, err = daemonConfig(context.Background(), t.TempDir(), "endpoint", false, true)
	if err != nil {
		t.Fatalf("unsupported watch-only daemonConfig: %v", err)
	}
	if cfg.Watch == nil || cfg.Watch.Enabled {
		t.Fatalf("watch config = %+v, want explicitly disabled", cfg.Watch)
	}
}

// TestDaemonWatchUnsupportedPlatform verifies the default watch flag degrades
// to static serving on platforms without a native provider, while an explicit
// L2 request still fails fast.
func TestDaemonWatchUnsupportedPlatform(t *testing.T) {
	old := newProvider
	t.Cleanup(func() { newProvider = old })
	newProvider = func(*filter.Pruner) (provider.FSProvider, error) {
		return nil, provider.ErrUnsupportedOS
	}
	cfg, err := daemonConfig(context.Background(), t.TempDir(), "endpoint", false, true)
	if err != nil {
		t.Fatalf("watch-only on unsupported platform: %v", err)
	}
	if cfg.Watch == nil || cfg.Watch.Enabled {
		t.Fatalf("watch config = %+v, want explicitly disabled", cfg.Watch)
	}
	if _, err := daemonConfig(context.Background(), t.TempDir(), "endpoint", true, true); !errors.Is(err, provider.ErrUnsupportedOS) {
		t.Fatalf("l2 on unsupported platform err = %v, want ErrUnsupportedOS", err)
	}
}
