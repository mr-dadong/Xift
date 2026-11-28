package l2search

import (
	"context"
	"errors"
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

// ScanRecord is one node emitted by the L1 subtree scan. Directory records are
// guaranteed to precede their children.
type ScanRecord struct {
	VolID     uint16
	NodeKey   uint64
	ParentKey uint64
	Name      string
	IsDir     bool
	ModTime   int64
}

// SubtreeScanner streams a read-only subtree scan rooted at root. The first
// record MUST be the root directory itself (ParentKey == NodeKey).
type SubtreeScanner interface {
	ScanSubtreeWithRoot(ctx context.Context, root string, fn func(ScanRecord) error) error
}

// WSOptions configures the L2 workspace.
type WSOptions struct {
	MaxFileBytes int64
	DiskCapBytes int64
	Debounce     time.Duration
	BatchDocs    int
	Scanner      SubtreeScanner
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

type pendingOp struct {
	path  string
	mtime int64
	index bool
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
	refs      map[DocKey]docRef
	paths     map[DocKey]string
	pending   map[DocKey]pendingOp
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
		refs:    make(map[DocKey]docRef),
		paths:   make(map[DocKey]string),
		pending: make(map[DocKey]pendingOp),
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
	for key, path := range w.paths {
		if pathWithin(path, abs) {
			if ref, ok := w.refs[key]; ok {
				dead = append(dead, ref)
			}
			delete(w.refs, key)
			delete(w.paths, key)
			delete(w.pending, key)
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

// EnqueueEvent schedules key at path for reindexing after the debounce window.
// index=false records a deletion of that key.
func (w *Workspace) EnqueueEvent(key DocKey, path string, mtime int64, index bool) {
	w.mu.Lock()
	w.pending[key] = pendingOp{path: path, mtime: mtime, index: index}
	if index {
		w.paths[key] = path
	}
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
	w.pending = make(map[DocKey]pendingOp)
	w.mu.Unlock()

	w.mergeMu.Lock()
	w.applyBatch(ctx, pending)
	w.mergeMu.Unlock()
	w.persistStats()
	w.maybeMerge()
	if w.onTick != nil {
		w.onTick()
	}
}

// applyBatch indexes/deletes the queued keys, flushing new documents as one or
// more segments. Callers hold mergeMu.
func (w *Workspace) applyBatch(ctx context.Context, ops map[DocKey]pendingOp) {
	keys := make([]DocKey, 0, len(ops))
	for key := range ops {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(a, b DocKey) int {
		if a.VolID != b.VolID {
			if a.VolID < b.VolID {
				return -1
			}
			return 1
		}
		switch {
		case a.NodeKey < b.NodeKey:
			return -1
		case a.NodeKey > b.NodeKey:
			return 1
		default:
			return 0
		}
	})

	var docs []Doc
	var docKeys []DocKey
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
			docKeys = docKeys[:0]
			docPaths = docPaths[:0]
			return
		}
		w.mu.Lock()
		for i, key := range docKeys {
			w.refs[key] = docRef{segID: segID, local: uint32(i)}
			w.paths[key] = docPaths[i]
		}
		w.stats.Batches++
		w.mu.Unlock()
		docs = docs[:0]
		docKeys = docKeys[:0]
		docPaths = docPaths[:0]
	}

	for _, key := range keys {
		if ctx.Err() != nil {
			break
		}
		op := ops[key]
		if !op.index {
			w.markDeleted(key)
			continue
		}
		if !w.InRoot(op.path) || w.excluded(op.path, false) {
			continue
		}
		text, reason, err := ReadTextFile(op.path, w.opts.MaxFileBytes)
		if err != nil || reason != SkipNone {
			w.mu.Lock()
			w.stats.Skipped[reason]++
			w.mu.Unlock()
			continue
		}
		mtime := op.mtime
		if mtime == 0 {
			if info, serr := os.Stat(op.path); serr == nil {
				mtime = info.ModTime().UnixNano()
			}
		}
		w.markDeleted(key)
		docs = append(docs, Doc{Key: key, MTime: mtime, Text: text})
		docKeys = append(docKeys, key)
		docPaths = append(docPaths, op.path)
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

func (w *Workspace) markDeleted(key DocKey) {
	w.mu.Lock()
	ref, ok := w.refs[key]
	if ok {
		delete(w.refs, key)
		w.stats.Deleted++
	}
	w.mu.Unlock()
	if ok {
		_ = w.store.MarkDead(ref.segID, []uint32{ref.local})
	}
}

var errRootRemoved = errors.New("l2search: root removed during scan")

// IndexScan walks root through the L1 subtree scanner and indexes every
// eligible file. Paths are composed from parent keys; the scanner's root
// record seeds the table. Index writes and the trailing compaction are
// serialized by mergeMu.
func (w *Workspace) IndexScan(ctx context.Context, root string) error {
	if w.opts.Scanner == nil {
		return errors.New("l2search: no subtree scanner configured")
	}
	w.mergeMu.Lock()
	err := w.indexScanLocked(ctx, root)
	w.mergeMu.Unlock()
	w.maybeMerge()
	return err
}

func (w *Workspace) indexScanLocked(ctx context.Context, root string) error {
	defer w.persistStats()
	abs, err := filepath.Abs(root)
	if err != nil {
		return fmt.Errorf("l2search: resolve root: %w", err)
	}
	dirPaths := make(map[uint64]string)

	var docs []Doc
	var docKeys []DocKey
	var docPaths []string
	flush := func() error {
		if len(docs) == 0 {
			return nil
		}
		if !w.InRoot(abs) {
			docs = docs[:0]
			docKeys = docKeys[:0]
			docPaths = docPaths[:0]
			return nil
		}
		segID, err := w.store.AddSegmentID(docs)
		if err != nil {
			return err
		}
		w.mu.Lock()
		for i, key := range docKeys {
			w.refs[key] = docRef{segID: segID, local: uint32(i)}
			w.paths[key] = docPaths[i]
		}
		w.stats.Batches++
		w.mu.Unlock()
		docs = docs[:0]
		docKeys = docKeys[:0]
		docPaths = docPaths[:0]
		return nil
	}
	skip := func(reason SkipReason) {
		w.mu.Lock()
		w.stats.Skipped[reason]++
		w.mu.Unlock()
	}

	scanErr := w.opts.Scanner.ScanSubtreeWithRoot(ctx, abs, func(rec ScanRecord) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !w.InRoot(abs) {
			return errRootRemoved
		}
		key := DocKey{VolID: rec.VolID, NodeKey: rec.NodeKey}
		if rec.IsDir {
			if rec.ParentKey == rec.NodeKey {
				dirPaths[rec.NodeKey] = abs
				return nil
			}
			parent := dirPaths[rec.ParentKey]
			if parent == "" {
				return nil
			}
			dirPaths[rec.NodeKey] = filepath.Join(parent, rec.Name)
			return nil
		}
		parent := dirPaths[rec.ParentKey]
		if parent == "" {
			skip(SkipNoPath)
			return nil
		}
		path := filepath.Join(parent, rec.Name)
		if w.excluded(path, false) {
			return nil
		}
		text, reason, rerr := ReadTextFile(path, w.opts.MaxFileBytes)
		if rerr != nil || reason != SkipNone {
			skip(reason)
			return nil
		}
		docs = append(docs, Doc{Key: key, MTime: rec.ModTime, Text: text})
		docKeys = append(docKeys, key)
		docPaths = append(docPaths, path)
		w.mu.Lock()
		w.stats.Indexed++
		w.mu.Unlock()
		if len(docs) >= w.opts.BatchDocs {
			return flush()
		}
		return nil
	})
	if scanErr != nil {
		_ = flush()
		return scanErr
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
func (w *Workspace) Search(term []byte, budget int) ([]DocKey, bool, error) {
	return w.store.Search(term, budget)
}

// maybeMerge compacts segments once the count or dead ratio crosses the
// thresholds, then rewrites document references to the merged segment.
func (w *Workspace) maybeMerge() {
	w.mergeMu.Lock()
	defer w.mergeMu.Unlock()
	if !w.store.NeedsMerge() {
		return
	}
	w.mu.Lock()
	keys := make([]DocKey, 0, len(w.refs))
	for key := range w.refs {
		keys = append(keys, key)
	}
	w.mu.Unlock()

	locations, err := w.store.MergeWithLocations()
	if err != nil {
		return
	}
	w.mu.Lock()
	for _, key := range keys {
		if loc, ok := locations[key]; ok {
			w.refs[key] = docRef{segID: loc.SegID, local: loc.Local}
		} else {
			delete(w.refs, key)
			delete(w.paths, key)
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

// IndexRoot indexes a root by walking the local filesystem. It exists for tests
// and callers without an L1 scanner; production indexing uses IndexScan so
// document keys match L1.
func (w *Workspace) IndexRoot(ctx context.Context, root string) error {
	w.mergeMu.Lock()
	err := w.indexRootLocked(ctx, root)
	w.mergeMu.Unlock()
	w.maybeMerge()
	return err
}

func (w *Workspace) indexRootLocked(ctx context.Context, root string) error {
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
		segID, err := w.store.AddSegmentID(docs)
		if err != nil {
			return err
		}
		w.mu.Lock()
		for i := range paths {
			key := docs[i].Key
			w.refs[key] = docRef{segID: segID, local: uint32(i)}
			w.paths[key] = paths[i]
		}
		w.stats.Batches++
		w.mu.Unlock()
		docs = docs[:0]
		paths = paths[:0]
		return nil
	}
	walkErr := filepath.WalkDir(abs, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
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
		var mtime int64
		if info, ierr := d.Info(); ierr == nil {
			mtime = info.ModTime().UnixNano()
		}
		key := DocKey{VolID: 1, NodeKey: stablePathKey(path)}
		docs = append(docs, Doc{Key: key, MTime: mtime, Text: text})
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
		_ = flush()
		return walkErr
	}
	return flush()
}

func stablePathKey(path string) uint64 {
	var h uint64 = 1469598103934665603
	for i := 0; i < len(path); i++ {
		h ^= uint64(path[i])
		h *= 1099511628211
	}
	return h
}
