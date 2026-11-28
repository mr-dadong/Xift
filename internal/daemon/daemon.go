package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/mr-dadong/Xift/internal/filter"
	"github.com/mr-dadong/Xift/internal/ipc"
	"github.com/mr-dadong/Xift/internal/query"
	"github.com/mr-dadong/Xift/internal/storage"
)

// Config configures the Core Daemon query service.
type Config struct {
	DataDir  string
	Endpoint string
	// BlockCacheBytes caps the shared Tier 2 block cache; zero selects the
	// 128 MB default.
	BlockCacheBytes int64
	ServerOptions   ipc.ServerOptions
	// OnReady is optionally invoked once the IPC listener is bound and accepting connections.
	OnReady func(endpoint string)
}

// Run opens the index directory read-only for user files, binds the per-user IPC endpoint,
// and serves viewport RPC queries until ctx is canceled. On cancellation it gracefully
// closes all client connections, releases all sessions, and closes the index.
func Run(ctx context.Context, cfg Config) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = storage.DefaultDataDir()
	}

	// Acquire the single-instance IPC endpoint first so a duplicate Daemon fails fast
	// without opening the active Daemon's index directory.
	ln, err := ipc.Listen(cfg.Endpoint)
	if err != nil {
		return err
	}

	store, err := storage.Open(dataDir, storage.Options{BlockCacheBytes: cfg.BlockCacheBytes})
	if err != nil {
		_ = ln.Close()
		return fmt.Errorf("daemon: open index %q: %w", dataDir, err)
	}
	defer store.Close()

	indexDir := filepath.Join(dataDir, "index")
	frecencyStore, err := query.OpenFrecencyStore(filepath.Join(indexDir, "frecency.db"), query.FrecencyOptions{})
	if err != nil {
		_ = ln.Close()
		return fmt.Errorf("daemon: open frecency store: %w", err)
	}
	defer frecencyStore.Close()

	rulesPath := filepath.Join(indexDir, "rules.json")
	ruleEng := filter.DefaultRuleEngine()
	if err := ruleEng.LoadUserRules(rulesPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		// Log-free graceful degradation: keep defaults if rules file cannot be loaded.
		ruleEng = filter.DefaultRuleEngine()
	}
	pruner := filter.NewPruner(ruleEng, nil)

	qe := query.NewEngine(store, query.WithFrecency(frecencyStore))
	backend := ipc.NewQueryBackend(
		store,
		qe,
		ipc.WithFrecencyStore(frecencyStore),
		ipc.WithPruner(pruner, rulesPath),
	)
	srv := ipc.NewServer(backend, cfg.ServerOptions)

	serveErrCh := make(chan error, 1)
	go func() {
		serveErrCh <- srv.Serve(ln)
	}()

	if cfg.OnReady != nil {
		cfg.OnReady(ln.Addr())
	}

	select {
	case err := <-serveErrCh:
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		<-serveErrCh
		return nil
	}
}
