package icons

import (
	"container/list"
	"image"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/egoist/mygo/ui"
)

const (
	// DefaultMaxExtEntries bounds the shared extension GPU bitmap cache.
	DefaultMaxExtEntries = 256
	// DefaultMaxFileEntries bounds the per-file async icon/thumbnail GPU bitmap cache.
	DefaultMaxFileEntries = 128
	// DefaultIdleDelay is the scroll-idle debounce before launching per-file extractions.
	DefaultIdleDelay = 80 * time.Millisecond
	// DefaultWorkers bounds concurrent off-UI-thread icon extraction goroutines.
	DefaultWorkers = 2
)

// Extractor extracts RGBA icons for file extensions (shared) and specific files (per-file).
type Extractor interface {
	ExtractExt(ext string, isDir bool) (image.Image, error)
	ExtractFile(absPath string) (image.Image, error)
}

// Config configures the GPU icon Atlas cache and async per-file extraction.
type Config struct {
	Extractor      Extractor
	MaxExtEntries  int
	MaxFileEntries int
	IdleDelay      time.Duration
	Workers        int
	OnReady        func()
}

type cacheEntry struct {
	key string
	bmp *ui.Bitmap
}

type lruCache struct {
	cap   int
	ll    *list.List
	items map[string]*list.Element
}

func newLRUCache(capacity int) *lruCache {
	if capacity <= 0 {
		capacity = 1
	}
	return &lruCache{
		cap:   capacity,
		ll:    list.New(),
		items: make(map[string]*list.Element, capacity),
	}
}

func (c *lruCache) get(key string) (*ui.Bitmap, bool) {
	if el, ok := c.items[key]; ok {
		c.ll.MoveToFront(el)
		return el.Value.(*cacheEntry).bmp, true
	}
	return nil, false
}

func (c *lruCache) put(key string, bmp *ui.Bitmap) {
	if el, ok := c.items[key]; ok {
		c.ll.MoveToFront(el)
		el.Value.(*cacheEntry).bmp = bmp
		return
	}
	el := c.ll.PushFront(&cacheEntry{key: key, bmp: bmp})
	c.items[key] = el
	for c.ll.Len() > c.cap {
		back := c.ll.Back()
		if back == nil {
			break
		}
		c.ll.Remove(back)
		ev := back.Value.(*cacheEntry)
		delete(c.items, ev.key)
	}
}

func (c *lruCache) clear() {
	c.ll.Init()
	clear(c.items)
}

func (c *lruCache) len() int {
	return c.ll.Len()
}

type fileExtractJob struct {
	gen  uint64
	path string
}

// Atlas manages bounded GPU icon bitmaps keyed by extension (shared across 95%+ files)
// plus scroll-idle debounced async per-file extraction for executables/images.
type Atlas struct {
	extractor Extractor
	idleDelay time.Duration
	onReady   func()

	mu         sync.Mutex
	extCache   *lruCache
	fileCache  *lruCache
	inFlight   map[string]struct{}
	idleTimer  *time.Timer
	gen        uint64
	closed     bool
	jobCh      chan fileExtractJob
	workerDone sync.WaitGroup
}

// NewAtlas constructs an Atlas using platform-default Extractor if cfg.Extractor is nil.
func NewAtlas(cfg Config) *Atlas {
	extCap := cfg.MaxExtEntries
	if extCap <= 0 {
		extCap = DefaultMaxExtEntries
	}
	fileCap := cfg.MaxFileEntries
	if fileCap <= 0 {
		fileCap = DefaultMaxFileEntries
	}
	idle := cfg.IdleDelay
	if idle <= 0 {
		idle = DefaultIdleDelay
	}
	workers := cfg.Workers
	if workers <= 0 {
		workers = DefaultWorkers
	}
	extractor := cfg.Extractor
	if extractor == nil {
		extractor = newPlatformExtractor()
	}

	a := &Atlas{
		extractor: extractor,
		idleDelay: idle,
		onReady:   cfg.OnReady,
		extCache:  newLRUCache(extCap),
		fileCache: newLRUCache(fileCap),
		inFlight:  make(map[string]struct{}),
		jobCh:     make(chan fileExtractJob, fileCap),
	}

	for i := 0; i < workers; i++ {
		a.workerDone.Add(1)
		go a.workerLoop()
	}
	return a
}

// SupportsPerFileIcon reports whether files of this extension carry distinct per-file icons.
func SupportsPerFileIcon(ext string) bool {
	switch strings.ToLower(strings.TrimPrefix(ext, ".")) {
	case "exe", "lnk", "app", "ico", "png", "jpg", "jpeg", "webp", "gif", "bmp":
		return true
	default:
		return false
	}
}

// IconFor returns a GPU bitmap immediately for the given row without blocking on per-file I/O.
// If a per-file icon has already been extracted and cached, it is returned; otherwise the
// shared extension/directory bitmap is returned (creating it once on first miss).
func (a *Atlas) IconFor(name, absPath string, isDir bool) *ui.Bitmap {
	if a == nil {
		return nil
	}
	ext := ""
	if !isDir {
		ext = strings.ToLower(filepath.Ext(name))
	}

	a.mu.Lock()
	if !isDir && absPath != "" && SupportsPerFileIcon(ext) {
		if bmp, ok := a.fileCache.get(absPath); ok {
			a.mu.Unlock()
			return bmp
		}
	}

	extKey := ext
	if isDir {
		extKey = "<dir>"
	}
	if bmp, ok := a.extCache.get(extKey); ok {
		a.mu.Unlock()
		return bmp
	}
	a.mu.Unlock()

	var img image.Image
	if a.extractor != nil {
		if extracted, err := a.extractor.ExtractExt(ext, isDir); err == nil && extracted != nil {
			img = extracted
		}
	}
	if img == nil {
		img = RenderBadge(ext, isDir)
	}
	bmp := ui.NewBitmap(img)

	a.mu.Lock()
	if !a.closed {
		if existing, ok := a.extCache.get(extKey); ok {
			a.mu.Unlock()
			return existing
		}
		a.extCache.put(extKey, bmp)
	}
	a.mu.Unlock()
	return bmp
}

// NotifyVisible notifies the Atlas of currently visible per-file candidate paths and resets
// the 80ms scroll-idle debounce timer. No per-file extractions launch while scrolling is active.
func (a *Atlas) NotifyVisible(perFilePaths []string) {
	if a == nil {
		return
	}
	paths := append([]string(nil), perFilePaths...)

	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return
	}
	if a.idleTimer != nil {
		a.idleTimer.Stop()
		a.idleTimer = nil
	}
	if len(paths) == 0 {
		return
	}
	gen := a.gen
	a.idleTimer = time.AfterFunc(a.idleDelay, func() {
		a.enqueueIdlePaths(gen, paths)
	})
}

func (a *Atlas) enqueueIdlePaths(gen uint64, paths []string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || a.gen != gen {
		return
	}
	for _, p := range paths {
		if p == "" {
			continue
		}
		if _, cached := a.fileCache.get(p); cached {
			continue
		}
		if _, busy := a.inFlight[p]; busy {
			continue
		}
		select {
		case a.jobCh <- fileExtractJob{gen: gen, path: p}:
			a.inFlight[p] = struct{}{}
		default:
		}
	}
}

func (a *Atlas) workerLoop() {
	defer a.workerDone.Done()
	for job := range a.jobCh {
		var img image.Image
		if a.extractor != nil {
			if extracted, err := a.extractor.ExtractFile(job.path); err == nil && extracted != nil {
				img = extracted
			}
		}

		notify := false
		a.mu.Lock()
		delete(a.inFlight, job.path)
		if !a.closed && a.gen == job.gen && img != nil {
			a.fileCache.put(job.path, ui.NewBitmap(img))
			notify = true
		}
		onReady := a.onReady
		a.mu.Unlock()

		if notify && onReady != nil {
			onReady()
		}
	}
}

// Clear releases all cached GPU icon bitmaps and cancels pending idle extractions.
// Invoked when the window is reclaimed after 30 seconds of inactivity.
func (a *Atlas) Clear() {
	if a == nil {
		return
	}
	a.mu.Lock()
	a.gen++
	if a.idleTimer != nil {
		a.idleTimer.Stop()
		a.idleTimer = nil
	}
	a.extCache.clear()
	a.fileCache.clear()
	clear(a.inFlight)
	a.mu.Unlock()
}

// Close clears caches and stops background worker goroutines.
func (a *Atlas) Close() {
	if a == nil {
		return
	}
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return
	}
	a.closed = true
	a.gen++
	if a.idleTimer != nil {
		a.idleTimer.Stop()
		a.idleTimer = nil
	}
	a.extCache.clear()
	a.fileCache.clear()
	clear(a.inFlight)
	close(a.jobCh)
	a.mu.Unlock()

	a.workerDone.Wait()
}

// Stats returns current cached extension and per-file icon counts.
func (a *Atlas) Stats() (extCount, fileCount int) {
	if a == nil {
		return 0, 0
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.extCache.len(), a.fileCache.len()
}
