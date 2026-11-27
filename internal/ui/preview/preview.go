package preview

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/egoist/mygo/ui"
)

const (
	// DefaultTimeout bounds every preview load so slow/network files never stall the UI.
	DefaultTimeout = 250 * time.Millisecond
	// DefaultMaxTextBytes caps text/code/Markdown preview reads at 64 KiB.
	DefaultMaxTextBytes = 64 * 1024
	// DefaultMaxImageSide caps decoded raster thumbnail dimensions at 512 px.
	DefaultMaxImageSide = 512
	// DefaultMaxArchiveEntries caps archive directory header listings.
	DefaultMaxArchiveEntries = 500
	// DefaultMaxDirEntries caps directory preview entry listings.
	DefaultMaxDirEntries = 100
)

// Kind classifies the loaded preview payload.
type Kind uint8

const (
	KindNone Kind = iota
	KindLoading
	KindText
	KindMarkdown
	KindImage
	KindArchive
	KindPDF
	KindDir
	KindBinary
	KindError
)

// Result is an immutable snapshot rendered by the UI preview pane.
type Result struct {
	Gen       uint64
	Path      string
	Kind      Kind
	Title     string
	Summary   string
	Text      string
	Truncated bool
	Entries   []string
	Image     *ui.Bitmap
	ImageW    int
	ImageH    int
	Error     string
}

// LoaderFunc loads a preview for absPath within ctx. Overridable in tests.
type LoaderFunc func(ctx context.Context, absPath string, isDir bool, cfg Config) Result

// Config configures the asynchronous non-blocking preview Manager.
type Config struct {
	Timeout           time.Duration
	MaxTextBytes      int64
	MaxImageSide      int
	MaxArchiveEntries int
	MaxDirEntries     int
	OnReady           func()
	Loader            LoaderFunc
}

func (c *Config) normalize() {
	if c.Timeout <= 0 {
		c.Timeout = DefaultTimeout
	}
	if c.MaxTextBytes <= 0 {
		c.MaxTextBytes = DefaultMaxTextBytes
	}
	if c.MaxImageSide <= 0 {
		c.MaxImageSide = DefaultMaxImageSide
	}
	if c.MaxArchiveEntries <= 0 {
		c.MaxArchiveEntries = DefaultMaxArchiveEntries
	}
	if c.MaxDirEntries <= 0 {
		c.MaxDirEntries = DefaultMaxDirEntries
	}
	if c.Loader == nil {
		c.Loader = defaultLoadPreview
	}
}

// Manager runs bounded, cancellable off-UI-thread preview loads for the selected result row.
type Manager struct {
	cfg Config

	mu     sync.Mutex
	gen    uint64
	cancel context.CancelFunc
	cur    Result
	closed bool
	wg     sync.WaitGroup
}

// NewManager constructs a non-blocking preview Manager.
func NewManager(cfg Config) *Manager {
	cfg.normalize()
	return &Manager{cfg: cfg}
}

// Select cancels any in-flight preview load and asynchronously loads absPath.
// Stale completions from superseded selections never overwrite the current state.
func (m *Manager) Select(absPath string, isDir bool) {
	if m == nil {
		return
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	if m.cancel != nil {
		m.cancel()
		m.cancel = nil
	}
	m.gen++
	gen := m.gen
	if strings.TrimSpace(absPath) == "" {
		m.cur = Result{Gen: gen, Kind: KindNone}
		onReady := m.cfg.OnReady
		m.mu.Unlock()
		if onReady != nil {
			onReady()
		}
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), m.cfg.Timeout)
	m.cancel = cancel
	m.cur = Result{
		Gen:   gen,
		Path:  absPath,
		Kind:  KindLoading,
		Title: filepath.Base(absPath),
	}
	onReady := m.cfg.OnReady
	m.wg.Add(1)
	m.mu.Unlock()

	if onReady != nil {
		onReady()
	}

	go func() {
		defer m.wg.Done()
		defer cancel()

		res := m.cfg.Loader(ctx, absPath, isDir, m.cfg)
		res.Gen = gen
		res.Path = absPath
		if res.Title == "" {
			res.Title = filepath.Base(absPath)
		}

		m.mu.Lock()
		if m.closed || m.gen != gen {
			m.mu.Unlock()
			return
		}
		m.cur = res
		cb := m.cfg.OnReady
		m.mu.Unlock()

		if cb != nil {
			cb()
		}
	}()
}

// Snapshot returns the latest preview Result safely for UI rendering.
func (m *Manager) Snapshot() Result {
	if m == nil {
		return Result{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cur
}

// Clear cancels any in-flight load and releases cached preview text/bitmaps (e.g. on 30s reclaim).
func (m *Manager) Clear() {
	if m == nil {
		return
	}
	m.mu.Lock()
	if m.cancel != nil {
		m.cancel()
		m.cancel = nil
	}
	m.gen++
	m.cur = Result{Gen: m.gen, Kind: KindNone}
	m.mu.Unlock()
}

// Close cancels pending work and waits for background preview goroutines to exit.
func (m *Manager) Close() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.closed = true
	if m.cancel != nil {
		m.cancel()
		m.cancel = nil
	}
	m.gen++
	m.cur = Result{}
	m.mu.Unlock()
	m.wg.Wait()
}

func defaultLoadPreview(ctx context.Context, absPath string, isDir bool, cfg Config) Result {
	if err := ctx.Err(); err != nil {
		return Result{Kind: KindError, Error: err.Error()}
	}
	info, err := os.Stat(absPath)
	if err != nil {
		return Result{Kind: KindError, Error: fmt.Sprintf("无法读取文件: %v", err)}
	}
	if isDir || info.IsDir() {
		return loadDirPreview(ctx, absPath, cfg.MaxDirEntries)
	}

	lower := strings.ToLower(absPath)
	ext := strings.ToLower(filepath.Ext(lower))

	switch {
	case ext == ".zip" || strings.HasSuffix(lower, ".tar.gz") || ext == ".tgz":
		return loadArchivePreview(ctx, absPath, cfg.MaxArchiveEntries)
	case ext == ".pdf":
		return loadPDFPreview(ctx, absPath, info.Size())
	case isImageExt(ext):
		return loadImagePreview(ctx, absPath, info.Size(), cfg.MaxImageSide)
	default:
		return loadTextPreview(ctx, absPath, ext, info.Size(), cfg.MaxTextBytes)
	}
}

func loadDirPreview(ctx context.Context, absPath string, maxEntries int) Result {
	f, err := os.Open(absPath)
	if err != nil {
		return Result{Kind: KindError, Error: fmt.Sprintf("无法读取目录: %v", err)}
	}
	defer f.Close()

	ents, err := f.ReadDir(maxEntries + 1)
	if err != nil && len(ents) == 0 {
		return Result{Kind: KindError, Error: fmt.Sprintf("无法读取目录项: %v", err)}
	}
	if err := ctx.Err(); err != nil {
		return Result{Kind: KindError, Error: err.Error()}
	}
	truncated := len(ents) > maxEntries
	if truncated {
		ents = ents[:maxEntries]
	}
	names := make([]string, 0, len(ents))
	for _, e := range ents {
		name := e.Name()
		if e.IsDir() {
			name += "/"
		}
		names = append(names, name)
	}
	summary := fmt.Sprintf("目录 · %d 项", len(names))
	if truncated {
		summary = fmt.Sprintf("目录 · 显示前 %d 项（已截断）", len(names))
	}
	return Result{
		Kind:      KindDir,
		Title:     filepath.Base(absPath),
		Summary:   summary,
		Entries:   names,
		Truncated: truncated,
	}
}
