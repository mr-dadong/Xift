package query

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/mr-dadong/Xift/internal/storage"
)

// SortOrder controls how matched search results are ordered before viewport slicing.
type SortOrder uint8

const (
	SortByRelevance SortOrder = iota
	SortByName
	SortBySize
	SortByModTime
)

// ParseSortOrder parses a CLI/API sort order string.
func ParseSortOrder(s string) (SortOrder, error) {
	switch s {
	case "", "relevance", "score":
		return SortByRelevance, nil
	case "name":
		return SortByName, nil
	case "size":
		return SortBySize, nil
	case "mtime", "dm", "time":
		return SortByModTime, nil
	default:
		return SortByRelevance, fmt.Errorf("query: unknown sort order %q (expected relevance, name, size, mtime)", s)
	}
}

// FrecencyProvider optionally supplies historical frecency boost scores (hook for Phase 2 P2-03).
type FrecencyProvider interface {
	Score(nodeID uint32, path string) float64
}

// SearchOptions configures query execution and viewport materialization.
type SearchOptions struct {
	Offset           int
	Limit            int
	Sort             SortOrder
	Desc             bool
	Workers          int
	PathSep          byte
	Now              time.Time
	FoldedMaxEntries int
	FoldedMaxDepth   int
	MaxRetained      int
}

func (o *SearchOptions) normalize() {
	if o.Offset < 0 {
		o.Offset = 0
	}
	if o.Limit <= 0 {
		o.Limit = 100
	}
	if o.Workers <= 0 {
		w := runtime.GOMAXPROCS(0)
		if w > 16 {
			w = 16
		}
		if w < 1 {
			w = 1
		}
		o.Workers = w
	}
	if o.PathSep == 0 {
		o.PathSep = '/'
	}
	if o.Now.IsZero() {
		o.Now = time.Now()
	}
	if o.FoldedMaxEntries <= 0 {
		o.FoldedMaxEntries = 100_000
	}
	if o.FoldedMaxDepth <= 0 {
		o.FoldedMaxDepth = 32
	}
	if o.MaxRetained <= 0 {
		o.MaxRetained = DefaultMaxRetained
	}
}

// ResultItem is a fully materialized viewport row returned to CLI, IPC, or UI callers.
type ResultItem struct {
	NodeID         uint32           `json:"node_id,omitempty"`
	Path           string           `json:"path"`
	Name           string           `json:"name"`
	Flags          uint16           `json:"flags,omitempty"`
	ExtID          uint16           `json:"ext_id,omitempty"`
	VolID          uint16           `json:"vol_id,omitempty"`
	IsDir          bool             `json:"is_dir"`
	Folded         bool             `json:"folded,omitempty"`
	System         bool             `json:"system,omitempty"`
	FromFoldedScan bool             `json:"from_folded_scan,omitempty"`
	Size           int64            `json:"size"`
	ModTime        int64            `json:"mtime"`
	Score          float64          `json:"score"`
	Highlights     []HighlightRange `json:"highlights,omitempty"`
}

// SearchResult holds query execution metadata and the materialized viewport slice.
type SearchResult struct {
	Query       string        `json:"query"`
	Total       int           `json:"total"`
	Returned    int           `json:"returned"`
	Offset      int           `json:"offset"`
	Limit       int           `json:"limit"`
	Cost        time.Duration `json:"-"`
	CostMs      float64       `json:"cost_ms"`
	Incremental bool          `json:"incremental"`
	ColdReads   int           `json:"-"`
	Warnings    []string      `json:"warnings,omitempty"`
	Items       []ResultItem  `json:"results"`
}

// EngineOption customizes query.Engine construction.
type EngineOption func(*Engine)

// WithFrecency attaches an optional FrecencyProvider for relevance ranking.
func WithFrecency(fp FrecencyProvider) EngineOption {
	return func(e *Engine) {
		e.frecency = fp
	}
}

// Engine executes unified DSL queries over a Tier 1 storage.Engine using multi-core
// lock-free hot-column scanning and on-demand viewport cold-column materialization.
type Engine struct {
	store    *storage.Engine
	frecency FrecencyProvider
}

// NewEngine creates a query Engine backed by store.
func NewEngine(store *storage.Engine, opts ...EngineOption) *Engine {
	e := &Engine{store: store}
	for _, opt := range opts {
		if opt != nil {
			opt(e)
		}
	}
	return e
}

// NewSession creates a stateful query Session supporting <2ms incremental narrowing
// and automatic cancellation of superseded in-flight queries.
func (e *Engine) NewSession() *Session {
	return &Session{engine: e}
}

// Start parses rawQuery, scans and ranks matching nodes, and returns a ResultSet holding
// a point-in-time snapshot for on-demand Viewport materialization. Callers MUST call Close().
func (e *Engine) Start(ctx context.Context, rawQuery string, opts SearchOptions) (*ResultSet, error) {
	if e == nil || e.store == nil {
		return nil, fmt.Errorf("query: nil storage engine")
	}
	opts.normalize()
	start := time.Now()

	q, err := ParseAt(rawQuery, opts.Now)
	if err != nil {
		return nil, err
	}

	snap := e.store.AcquireSnapshot()
	plan := compileQuery(q, e.store.ExtPool())

	if len(q.InDirs) > 0 {
		rs, err := e.startFoldedSearch(ctx, snap, q, plan, opts, start)
		snap.Release()
		return rs, err
	}

	cands, coldReads, _, err := scanSnapshot(ctx, snap, plan, opts.Workers)
	if err != nil {
		snap.Release()
		return nil, err
	}
	rs, err := e.rankCandidates(ctx, snap, q, plan, cands, coldReads, false, opts, start)
	if err != nil {
		snap.Release()
		return nil, err
	}
	return rs, nil
}

// Search parses rawQuery and executes a single stateless search against the latest storage snapshot.
func (e *Engine) Search(ctx context.Context, rawQuery string, opts SearchOptions) (*SearchResult, error) {
	opts.normalize()
	start := time.Now()

	rs, err := e.Start(ctx, rawQuery, opts)
	if err != nil {
		return nil, err
	}
	defer rs.Close()

	items, err := rs.Viewport(ctx, opts.Offset, opts.Limit)
	if err != nil {
		return nil, err
	}
	return rs.toSearchResult(items, opts.Offset, opts.Limit, start), nil
}
