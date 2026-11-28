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
		dataDir     string
		endpoint    string
		enableL2    bool
		enableWatch bool
	)
	flag.StringVar(&dataDir, "data-dir", storage.DefaultDataDir(), "index data directory")
	flag.StringVar(&endpoint, "endpoint", "", "IPC endpoint override (default: per-user local endpoint)")
	flag.BoolVar(&enableL2, "l2", false, "enable the whitelisted L2 content index")
	flag.BoolVar(&enableWatch, "watch", true, "deliver live kernel file events to the index")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := daemonConfig(ctx, dataDir, endpoint, enableL2, enableWatch)
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
func daemonConfig(ctx context.Context, dataDir, endpoint string, enableL2, enableWatch bool) (daemon.Config, error) {
	cfg := daemon.Config{DataDir: dataDir, Endpoint: endpoint}
	if !enableL2 && !enableWatch {
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
		if enableL2 {
			return daemon.Config{}, fmt.Errorf("initialize platform provider: %w", err)
		}
		// Watch-only installs keep serving the static index when the platform
		// has no native provider.
		cfg.Watch = &daemon.WatchConfig{Enabled: false}
		return cfg, nil
	}
	vols, err := prov.EnumerateVolumes(ctx)
	if err != nil {
		if enableL2 {
			return daemon.Config{}, fmt.Errorf("enumerate volumes: %w", err)
		}
		cfg.Watch = &daemon.WatchConfig{Enabled: false}
		return cfg, nil
	}
	if enableL2 {
		cfg.L2 = daemon.BuildL2ConfigFromVolumes(prov, vols)
	}
	if enableWatch {
		cfg.Watch = &daemon.WatchConfig{Provider: prov, Volumes: vols, Enabled: true}
	}
	return cfg, nil
}
