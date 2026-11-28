package main

import (
	"context"
	"errors"
	"testing"

	"github.com/mr-dadong/Xift/internal/filter"
	"github.com/mr-dadong/Xift/internal/provider"
)

// TestDaemonConfigWithoutL2 verifies the default path never opens a provider,
// so daemons that do not opt in cannot read whitelisted directories.
func TestDaemonConfigWithoutL2(t *testing.T) {
	old := newProvider
	t.Cleanup(func() { newProvider = old })
	called := false
	newProvider = func(*filter.Pruner) (provider.FSProvider, error) {
		called = true
		return nil, errors.New("must not be called")
	}
	cfg, err := daemonConfig(context.Background(), t.TempDir(), "endpoint", false)
	if err != nil {
		t.Fatalf("daemonConfig: %v", err)
	}
	if called {
		t.Fatal("provider was opened without --l2")
	}
	if cfg.L2 != nil {
		t.Fatal("L2 config must stay nil without --l2")
	}
	if cfg.Endpoint != "endpoint" {
		t.Fatalf("Endpoint = %q, want endpoint", cfg.Endpoint)
	}
}

// TestDaemonConfigL2Unsupported verifies an unsupported platform surfaces as a
// startup error instead of silently serving without content search.
func TestDaemonConfigL2Unsupported(t *testing.T) {
	old := newProvider
	t.Cleanup(func() { newProvider = old })
	newProvider = func(*filter.Pruner) (provider.FSProvider, error) {
		return nil, provider.ErrUnsupportedOS
	}
	if _, err := daemonConfig(context.Background(), t.TempDir(), "endpoint", true); !errors.Is(err, provider.ErrUnsupportedOS) {
		t.Fatalf("daemonConfig err = %v, want ErrUnsupportedOS", err)
	}
}
