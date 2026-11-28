package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/mr-dadong/Xift/internal/daemon"
	"github.com/mr-dadong/Xift/internal/filter"
	"github.com/mr-dadong/Xift/internal/provider"
	"github.com/mr-dadong/Xift/internal/provider/platform"
	"github.com/mr-dadong/Xift/internal/storage"
)

// newProvider opens the platform read-only scan provider. Tests replace it to
// exercise the unsupported-platform path.
var newProvider = func(pruner *filter.Pruner) (provider.FSProvider, error) {
	return platform.NewDefaultProvider(pruner, provider.Options{})
}

func main() {
	var (
		dataDir  string
		endpoint string
		enableL2 bool
	)
	flag.StringVar(&dataDir, "data-dir", storage.DefaultDataDir(), "index data directory")
	flag.StringVar(&endpoint, "endpoint", "", "IPC endpoint override (default: per-user local endpoint)")
	flag.BoolVar(&enableL2, "l2", false, "enable the whitelisted L2 content index")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := daemonConfig(ctx, dataDir, endpoint, enableL2)
	if err != nil {
		fmt.Fprintf(os.Stderr, "xift-daemon: %v\n", err)
		os.Exit(1)
	}
	if err := daemon.Run(ctx, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "xift-daemon: %v\n", err)
		os.Exit(1)
	}
}

// daemonConfig assembles the daemon configuration. When L2 is enabled it loads
// the same pruning rules as `xift index`, enumerates volumes, and wires the
// content-index scanner; otherwise no provider is created and no user file is
// touched.
func daemonConfig(ctx context.Context, dataDir, endpoint string, enableL2 bool) (daemon.Config, error) {
	cfg := daemon.Config{DataDir: dataDir, Endpoint: endpoint}
	if !enableL2 {
		return cfg, nil
	}
	if dataDir == "" {
		dataDir = storage.DefaultDataDir()
		cfg.DataDir = dataDir
	}
	rules := filter.DefaultRuleEngine()
	rulesPath := filepath.Join(dataDir, "index", "rules.json")
	if err := rules.LoadUserRules(rulesPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return daemon.Config{}, fmt.Errorf("load pruning rules: %w", err)
	}
	prov, err := newProvider(filter.NewPruner(rules, nil))
	if err != nil {
		return daemon.Config{}, fmt.Errorf("initialize platform provider: %w", err)
	}
	l2cfg, err := daemon.BuildL2Config(ctx, prov)
	if err != nil {
		return daemon.Config{}, err
	}
	cfg.L2 = l2cfg
	return cfg, nil
}
