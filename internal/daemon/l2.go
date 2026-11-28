package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mr-dadong/Xift/internal/l2search"
	"github.com/mr-dadong/Xift/internal/provider"
	"github.com/mr-dadong/Xift/internal/storage/lsm"
)

// L2Config configures the whitelisted content index served by the daemon.
type L2Config struct {
	// VolRoots maps a volume ID to its filesystem root so that volume-relative
	// event paths can be turned into absolute paths.
	VolRoots map[uint16]string
	// Roots are whitelisted directories indexed at startup. Roots persisted by
	// earlier `l2 add` calls are restored as well.
	Roots []string
	// Resolver maps a file path to its L1 node identity. It is required: without
	// it the L2 index cannot name the L1 rows that content hits refer to.
	Resolver l2search.Resolver
	// Excluder mirrors L1 pruning (HardExclude / Fold) for whitelisted trees.
	Excluder     l2search.Excluder
	MaxFileBytes int64
	DiskCapBytes int64
	CacheBytes   int64
	Debounce     time.Duration
	BatchDocs    int
}

// L2Pipeline routes kernel events into the L2 content index. HandleBatch only
// records paths under the whitelisted roots; all file reads and index writes
// happen on a single low-priority background worker, so L1 query latency is
// unaffected.
type L2Pipeline struct {
	store    *l2search.Store
	cache    *lsm.BlockCache
	ws       *l2search.Workspace
	volRoots map[uint16]string

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu            sync.Mutex
	indexing      map[string]bool
	skippedEvents atomic.Uint64
	indexErrors   atomic.Uint64
}

// OpenL2Pipeline opens the content index under dataDir/l2, restores the
// persisted whitelist and starts the background worker. Whitelisted roots begin
// indexing in the background; OpenL2Pipeline itself does not read user files.
func OpenL2Pipeline(parent context.Context, dataDir string, cfg L2Config) (*L2Pipeline, error) {
	if cfg.Resolver == nil {
		return nil, errors.New("daemon: L2 content index requires an L1 path resolver")
	}
	cacheBytes := cfg.CacheBytes
	if cacheBytes <= 0 {
		cacheBytes = l2search.DefaultCacheBytes
	}
	cache := lsm.NewBlockCache(cacheBytes)
	st, err := l2search.OpenStore(filepath.Join(dataDir, "l2"), cache)
	if err != nil {
		cache.Close()
		return nil, fmt.Errorf("daemon: open L2 store: %w", err)
	}
	if cfg.DiskCapBytes > 0 {
		st.SetDiskCap(cfg.DiskCapBytes)
	}
	ws := l2search.NewWorkspace(st, l2search.WSOptions{
		MaxFileBytes: cfg.MaxFileBytes,
		DiskCapBytes: cfg.DiskCapBytes,
		Debounce:     cfg.Debounce,
		BatchDocs:    cfg.BatchDocs,
		Resolver:     cfg.Resolver,
		Excluder:     cfg.Excluder,
	})
	ctx, cancel := context.WithCancel(parent)
	p := &L2Pipeline{
		store:    st,
		cache:    cache,
		ws:       ws,
		volRoots: cfg.VolRoots,
		ctx:      ctx,
		cancel:   cancel,
		indexing: make(map[string]bool),
	}
	roots, err := l2search.LoadRoots(filepath.Join(dataDir, "l2"))
	if err != nil {
		cancel()
		_ = st.Close()
		cache.Close()
		return nil, err
	}
	for _, root := range cfg.Roots {
		abs, err := filepath.Abs(root)
		if err != nil {
			continue
		}
		roots = appendRoot(roots, abs)
	}
	for _, root := range roots {
		ws.AddRoot(root)
	}
	ws.Start(ctx)
	for _, root := range roots {
		p.scheduleIndex(root)
	}
	return p, nil
}

// HandleBatch is a provider.Batcher sink: it filters events to whitelisted
// roots and appends them to the debounce queue. It never touches the disk.
func (p *L2Pipeline) HandleBatch(batch provider.EventBatch) {
	for i := range batch.Events {
		p.handleEvent(batch.Events[i])
	}
}

// AddRoot whitelists dir, persists it, and starts a background index pass.
func (p *L2Pipeline) AddRoot(dir string) ([]string, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("daemon: resolve L2 root: %w", err)
	}
	info, err := os.Stat(abs)
	if err != nil {
		return nil, fmt.Errorf("daemon: L2 root %q: %w", abs, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("daemon: L2 root %q is not a directory", abs)
	}
	roots := p.ws.AddRoot(abs)
	if err := l2search.SaveRoots(p.store.Dir(), roots); err != nil {
		return roots, err
	}
	p.scheduleIndex(abs)
	return roots, nil
}

// RemoveRoot drops dir from the whitelist, purges its indexed documents and
// persists the new whitelist.
func (p *L2Pipeline) RemoveRoot(dir string) ([]string, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		abs = dir
	}
	roots := p.ws.RemoveRoot(abs)
	return roots, l2search.SaveRoots(p.store.Dir(), roots)
}

// CacheBytes reports the L2 block-cache budget, which is independent of L1.
func (p *L2Pipeline) CacheBytes() int64 { return p.cache.Limit() }

// Roots returns the current whitelist.
func (p *L2Pipeline) Roots() []string { return p.ws.Roots() }

// Status reports whitelist, indexing counters and disk usage.
func (p *L2Pipeline) Status() l2search.Stats {
	st := p.ws.Stats()
	st.Skipped[l2search.SkipNoPath] += int(p.skippedEvents.Load())
	st.Skipped[l2search.SkipReadError] += int(p.indexErrors.Load())
	return st
}

// Search returns the L1 node IDs whose content contains term. The second
// result reports whether the result set was truncated by budget.
func (p *L2Pipeline) Search(term []byte, budget int) ([]uint32, bool, error) {
	return p.ws.Search(term, budget)
}

// Close stops the worker and closes the content index.
func (p *L2Pipeline) Close() error {
	p.cancel()
	p.ws.Stop()
	p.wg.Wait()
	err := p.store.Close()
	p.cache.Close()
	return err
}

func (p *L2Pipeline) handleEvent(ev provider.FileEvent) {
	if ev.IsDir {
		return
	}
	switch ev.Type {
	case provider.EventRescanDir:
		return
	case provider.EventDelete:
		if path, ok := p.deletedPath(ev); ok {
			p.enqueue(false, path)
		} else {
			p.skippedEvents.Add(1)
		}
	case provider.EventRename:
		if path, ok := p.deletedPath(ev); ok {
			p.enqueue(false, path)
		}
		if path, ok := p.currentPath(ev); ok {
			p.enqueue(true, path)
		} else {
			p.skippedEvents.Add(1)
		}
	default: // create, modify
		if path, ok := p.currentPath(ev); ok {
			p.enqueue(true, path)
		} else {
			p.skippedEvents.Add(1)
		}
	}
}

func (p *L2Pipeline) enqueue(index bool, path string) {
	if !p.ws.InRoot(path) {
		return
	}
	if index {
		p.ws.Enqueue(path)
		return
	}
	p.ws.EnqueueDelete(path)
}

// currentPath resolves the post-event path of ev.
func (p *L2Pipeline) currentPath(ev provider.FileEvent) (string, bool) {
	if ev.RelPath != "" {
		if root := p.volRoots[ev.VolID]; root != "" {
			return filepath.Clean(filepath.Join(root, filepath.FromSlash(ev.RelPath))), true
		}
	}
	if ev.OldPath != "" && filepath.IsAbs(ev.OldPath) {
		return filepath.Clean(ev.OldPath), true
	}
	return "", false
}

// deletedPath resolves the path that a delete or rename removed. Renames only
// qualify when the provider supplied an absolute source path.
func (p *L2Pipeline) deletedPath(ev provider.FileEvent) (string, bool) {
	if ev.OldPath != "" && filepath.IsAbs(ev.OldPath) {
		return filepath.Clean(ev.OldPath), true
	}
	if ev.Type == provider.EventDelete {
		return p.currentPath(ev)
	}
	return "", false
}

// scheduleIndex starts at most one background index pass per root. The pass is
// a plain goroutine (no query-path work) that yields before opening files.
func (p *L2Pipeline) scheduleIndex(root string) {
	p.mu.Lock()
	if p.indexing[root] {
		p.mu.Unlock()
		return
	}
	p.indexing[root] = true
	p.mu.Unlock()

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer func() {
			p.mu.Lock()
			delete(p.indexing, root)
			p.mu.Unlock()
		}()
		runtime.Gosched()
		if err := p.ws.IndexRoot(p.ctx, root); err != nil && !errors.Is(err, context.Canceled) {
			p.indexErrors.Add(1)
		}
	}()
}

func appendRoot(roots []string, root string) []string {
	for _, r := range roots {
		if r == root {
			return roots
		}
	}
	return append(roots, root)
}
