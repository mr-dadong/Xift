package l2search

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"
)

const (
	// DefaultDebounce coalesces bursts of changes before one indexing pass.
	DefaultDebounce = 3 * time.Second
	// DefaultBatchDocs caps how many documents one flush writes into a segment.
	DefaultBatchDocs = 2000
)

// Excluder mirrors the L1 pruning decision for a path (Fold / HardExclude).
type Excluder interface {
	Excluded(path string, isDir bool) bool
}

// Resolver maps a filesystem path to its L1 node identity.
type Resolver interface {
	Resolve(path string) (nodeID uint32, mtime int64, ok bool)
}

// WSOptions configures the L2 workspace.
type WSOptions struct {
	MaxFileBytes int64
	DiskCapBytes int64
	Debounce     time.Duration
	BatchDocs    int
	Resolver     Resolver
	Excluder     Excluder
}

// Stats reports what the L2 indexer has done so far.
type Stats struct {
	Indexed  int
	Deleted  int
	Skipped  map[SkipReason]int
	Batches  int
	Roots    []string
	Paused   bool
	DiskUsed int64
	DiskCap  int64
}

type docRef struct {
	segID uint32
	local uint32
}

// Workspace owns the whitelist, the debounce queue and the write path into the
// L2 store. It never writes user files: only reads them and appends to the
// content index.
type Workspace struct {
	store  *Store
	opts   WSOptions
	now    func() time.Time
	onTick func() // test hook, called after each flush evaluation

	persistMu sync.Mutex
	// mergeMu serializes segment writes, tombstones and compaction so the
	// refs map always points at a live document after a merge.
	mergeMu   sync.Mutex
	mu        sync.Mutex
	roots     []string
	refs      map[string]docRef
	pending   map[string]bool // true = index, false = delete
	lastEvent time.Time
	stats     Stats
	started   bool
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

// NewWorkspace creates a workspace over store.
func NewWorkspace(store *Store, opts WSOptions) *Workspace {
	if opts.MaxFileBytes <= 0 {
		opts.MaxFileBytes = DefaultMaxFileBytes
	}
	if opts.Debounce <= 0 {
		opts.Debounce = DefaultDebounce
	}
	if opts.BatchDocs <= 0 {
		opts.BatchDocs = DefaultBatchDocs
	}
	return &Workspace{
		store:   store,
		opts:    opts,
		now:     time.Now,
		refs:    make(map[string]docRef),
		pending: make(map[string]bool),
		stats:   Stats{Skipped: make(map[SkipReason]int)},
	}
}

// AddRoot adds a whitelisted directory and returns the current root list.
func (w *Workspace) AddRoot(root string) []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	abs, err := filepath.Abs(root)
	if err != nil {
		abs = root
	}
	if !slices.Contains(w.roots, abs) {
		w.roots = append(w.roots, abs)
	}
	return slices.Clone(w.roots)
}

// RemoveRoot drops a whitelisted directory and immediately invalidates every
// document indexed below it, so removal is visible without rescanning disk.
func (w *Workspace) RemoveRoot(root string) []string {
	abs, err := filepath.Abs(root)
	if err != nil {
		abs = root
	}
	w.mu.Lock()
	w.roots = slices.DeleteFunc(w.roots, func(p string) bool { return p == abs })
	var dead []docRef
	for p, ref := range w.refs {
		if pathWithin(p, abs) {
			delete(w.refs, p)
			dead = append(dead, ref)
		}
	}
	for p := range w.pending {
		if pathWithin(p, abs) {
			delete(w.pending, p)
		}
	}
	roots := slices.Clone(w.roots)
	w.mu.Unlock()

	for _, ref := range dead {
		_ = w.store.MarkDead(ref.segID, []uint32{ref.local})
	}
	w.persistStats()
	return roots
}

// Roots returns the current whitelist.
func (w *Workspace) Roots() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.roots)
}

// InRoot reports whether path is inside a whitelisted root.
func (w *Workspace) InRoot(path string) bool {
	abs, err := filepath.Abs(path)
	if err != nil {
		abs = path
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, root := range w.roots {
		if pathWithin(abs, root) {
			return true
		}
	}
	return false
}

// pathWithin reports whether path is root itself or lies below it.
func pathWithin(path, root string) bool {
	if path == root {
		return true
	}
	if len(path) <= len(root) || path[:len(root)] != root {
		return false
	}
	c := path[len(root)]
	return c == '/' || c == '\\'
}

// Enqueue schedules path for reindexing after the debounce window.
func (w *Workspace) Enqueue(path string) {
	w.mu.Lock()
	w.pending[path] = true
	w.lastEvent = w.now()
	w.mu.Unlock()
}

// EnqueueDelete schedules path for removal after the debounce window.
func (w *Workspace) EnqueueDelete(path string) {
	w.mu.Lock()
	w.pending[path] = false
	w.lastEvent = w.now()
	w.mu.Unlock()
}

// Start launches the debounce loop; Stop cancels it.
func (w *Workspace) Start(ctx context.Context) {
	w.mu.Lock()
	if w.started {
		w.mu.Unlock()
		return
	}
	w.started = true
	ctx, cancel := context.WithCancel(ctx)
	w.cancel = cancel
	w.wg.Add(1)
	w.mu.Unlock()

	go func() {
		defer w.wg.Done()
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				w.flushIfDue(ctx)
			}
		}
	}()
}

// Stop waits for the debounce loop to exit.
func (w *Workspace) Stop() {
	w.mu.Lock()
	cancel := w.cancel
	w.cancel = nil
	w.started = false
	w.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	w.wg.Wait()
}

func (w *Workspace) flushIfDue(ctx context.Context) {
	w.mu.Lock()
	if len(w.pending) == 0 || w.now().Sub(w.lastEvent) < w.opts.Debounce {
		w.mu.Unlock()
		return
	}
	pending := w.pending
	w.pending = make(map[string]bool)
	w.mu.Unlock()

	paths := make([]string, 0, len(pending))
	for p := range pending {
		paths = append(paths, p)
	}
	slices.Sort(paths)
	w.mergeMu.Lock()
	w.applyBatch(ctx, paths, pending)
	w.mergeMu.Unlock()
	w.persistStats()
	w.maybeMerge()
	if w.onTick != nil {
		w.onTick()
	}
}

// applyBatch indexes/deletes the given paths, then flushes new documents as one
// or more segments.
func (w *Workspace) applyBatch(ctx context.Context, paths []string, ops map[string]bool) {
	var docs []Doc
	var docPaths []string
	flush := func() {
		if len(docs) == 0 {
			return
		}
		segID, err := w.store.AddSegmentID(docs)
		if err != nil {
			w.mu.Lock()
			w.stats.Skipped[SkipReadError] += len(docs)
			w.mu.Unlock()
			docs = docs[:0]
			docPaths = docPaths[:0]
			return
		}
		w.mu.Lock()
		for i, p := range docPaths {
			w.refs[p] = docRef{segID: segID, local: uint32(i)}
		}
		w.stats.Batches++
		w.mu.Unlock()
		docs = docs[:0]
		docPaths = docPaths[:0]
	}

	for _, path := range paths {
		if err := ctx.Err(); err != nil {
			break
		}
		if !w.InRoot(path) || w.excluded(path, false) {
			continue
		}
		if !ops[path] {
			w.markDeleted(path)
			continue
		}
		text, reason, err := ReadTextFile(path, w.opts.MaxFileBytes)
		if err != nil || reason != SkipNone {
			w.mu.Lock()
			w.stats.Skipped[reason]++
			w.mu.Unlock()
			continue
		}
		nodeID, mtime, ok := uint32(0), int64(0), true
		if w.opts.Resolver != nil {
			nodeID, mtime, ok = w.opts.Resolver.Resolve(path)
		}
		if !ok {
			w.mu.Lock()
			w.stats.Skipped[SkipReadError]++
			w.mu.Unlock()
			continue
		}
		w.markDeleted(path)
		docs = append(docs, Doc{NodeID: nodeID, MTime: mtime, Text: text})
		docPaths = append(docPaths, path)
		w.mu.Lock()
		w.stats.Indexed++
		w.mu.Unlock()
		if len(docs) >= w.opts.BatchDocs {
			flush()
		}
	}
	flush()
}

func (w *Workspace) excluded(path string, isDir bool) bool {
	return w.opts.Excluder != nil && w.opts.Excluder.Excluded(path, isDir)
}

func (w *Workspace) markDeleted(path string) {
	w.mu.Lock()
	ref, ok := w.refs[path]
	if ok {
		delete(w.refs, path)
		w.stats.Deleted++
	}
	w.mu.Unlock()
	if ok {
		_ = w.store.MarkDead(ref.segID, []uint32{ref.local})
	}
}

// IndexRoot walks root and indexes every eligible file under the context.
// Index writes and the trailing compaction are serialized by mergeMu.
func (w *Workspace) IndexRoot(ctx context.Context, root string) error {
	w.mergeMu.Lock()
	err := w.indexRootLocked(ctx, root)
	w.mergeMu.Unlock()
	w.maybeMerge()
	return err
}

func (w *Workspace) indexRootLocked(ctx context.Context, root string) error {
	defer w.persistStats()
	abs, err := filepath.Abs(root)
	if err != nil {
		return fmt.Errorf("l2search: resolve root: %w", err)
	}
	if info, err := os.Stat(abs); err != nil || !info.IsDir() {
		return fmt.Errorf("l2search: %s is not a directory", abs)
	}
	var docs []Doc
	var paths []string
	flush := func() error {
		if len(docs) == 0 {
			return nil
		}
		if !w.InRoot(abs) {
			// The root was removed while this pass was walking; drop the batch
			// instead of re-adding documents the removal just purged.
			docs = docs[:0]
			paths = paths[:0]
			return nil
		}
		segID, err := w.store.AddSegmentID(docs)
		if err != nil {
			return err
		}
		w.mu.Lock()
		for i, p := range paths {
			w.refs[p] = docRef{segID: segID, local: uint32(i)}
		}
		w.stats.Batches++
		w.mu.Unlock()
		docs = docs[:0]
		paths = paths[:0]
		return nil
	}

	walkErr := filepath.WalkDir(abs, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil // unreadable entries are skipped, not fatal
		}
		if serr := ctx.Err(); serr != nil {
			return serr
		}
		if d.IsDir() {
			if path != abs && w.excluded(path, true) {
				return fs.SkipDir
			}
			return nil
		}
		if w.excluded(path, false) {
			return nil
		}
		text, reason, rerr := ReadTextFile(path, w.opts.MaxFileBytes)
		if rerr != nil || reason != SkipNone {
			w.mu.Lock()
			w.stats.Skipped[reason]++
			w.mu.Unlock()
			return nil
		}
		nodeID, mtime, ok := uint32(0), int64(0), true
		if w.opts.Resolver != nil {
			nodeID, mtime, ok = w.opts.Resolver.Resolve(path)
		}
		if !ok {
			w.mu.Lock()
			w.stats.Skipped[SkipReadError]++
			w.mu.Unlock()
			return nil
		}
		docs = append(docs, Doc{NodeID: nodeID, MTime: mtime, Text: text})
		paths = append(paths, path)
		w.mu.Lock()
		w.stats.Indexed++
		w.mu.Unlock()
		if len(docs) >= w.opts.BatchDocs {
			return flush()
		}
		return nil
	})
	if walkErr != nil {
		_ = flush() // keep documents indexed before cancellation
		return walkErr
	}
	return flush()
}

// Stats returns a snapshot of the indexer counters and store limits.
func (w *Workspace) Stats() Stats {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := w.stats
	out.Skipped = make(map[SkipReason]int, len(w.stats.Skipped))
	for k, v := range w.stats.Skipped {
		out.Skipped[k] = v
	}
	out.Roots = slices.Clone(w.roots)
	out.Paused = w.store.Paused()
	out.DiskUsed = w.store.DiskBytes()
	out.DiskCap = w.store.diskCap
	return out
}

// Search exposes the store's content lookup to the query layer.
func (w *Workspace) Search(term []byte, budget int) ([]uint32, bool, error) {
	return w.store.Search(term, budget)
}

// maybeMerge compacts segments once the count or dead ratio crosses the
// thresholds, then rewrites per-path references to the merged segment.
func (w *Workspace) maybeMerge() {
	w.mergeMu.Lock()
	defer w.mergeMu.Unlock()
	if !w.store.NeedsMerge() {
		return
	}
	w.mu.Lock()
	paths := make([]string, 0, len(w.refs))
	refs := make([]docRef, 0, len(w.refs))
	for p, ref := range w.refs {
		paths = append(paths, p)
		refs = append(refs, ref)
	}
	w.mu.Unlock()

	nodeIDs := make([]uint32, len(refs))
	for i, ref := range refs {
		if id, ok := w.store.DocNodeID(ref.segID, ref.local); ok {
			nodeIDs[i] = id
		}
	}
	locations, err := w.store.MergeWithLocations()
	if err != nil {
		return
	}
	w.mu.Lock()
	for i, p := range paths {
		if nodeIDs[i] == 0 {
			continue
		}
		if loc, ok := locations[nodeIDs[i]]; ok {
			w.refs[p] = docRef{segID: loc.SegID, local: loc.Local}
		} else {
			delete(w.refs, p)
		}
	}
	w.mu.Unlock()
}

// persistStats writes the counters that `l2 status` reports offline. The write
// is best-effort: a failed status snapshot never fails indexing.
func (w *Workspace) persistStats() {
	w.persistMu.Lock()
	defer w.persistMu.Unlock()
	_ = SaveStats(w.store.Dir(), statsToDisk(w.Stats()))
}
