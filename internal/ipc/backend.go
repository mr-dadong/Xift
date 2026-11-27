package ipc

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/mr-dadong/Xift/internal/query"
	"github.com/mr-dadong/Xift/internal/storage"
)

// QueryBackend adapts storage.Engine + query.Engine to the IPC Backend interface.
type QueryBackend struct {
	store          *storage.Engine
	engine         *query.Engine
	totalColdReads atomic.Int64
}

// NewQueryBackend creates a production IPC Backend over store and engine.
func NewQueryBackend(store *storage.Engine, engine *query.Engine) *QueryBackend {
	return &QueryBackend{
		store:  store,
		engine: engine,
	}
}

// NewSession returns a per-connection SessionBackend preserving incremental query refinement.
func (b *QueryBackend) NewSession() SessionBackend {
	return &querySessionBackend{
		parent: b,
		sess:   b.engine.NewSession(),
	}
}

// Status returns the current index live node count.
func (b *QueryBackend) Status() BackendStatus {
	var live uint64
	if b.store != nil {
		live = b.store.LiveCount()
	}
	return BackendStatus{LiveCount: live}
}

// TotalColdReads returns the cumulative cold-column reads observed across all results created by this backend.
func (b *QueryBackend) TotalColdReads() int64 {
	return b.totalColdReads.Load()
}

type querySessionBackend struct {
	parent *QueryBackend
	sess   *query.Session
}

func (s *querySessionBackend) Start(ctx context.Context, req QueryRequest) (Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, err := query.ParseAt(req.Text, time.Now()); err != nil {
		return nil, &RPCError{Code: ErrQueryParse, Message: err.Error()}
	}
	rs, err := s.sess.Start(ctx, req.Text, query.SearchOptions{
		Sort:    query.SortOrder(req.Sort),
		Desc:    req.Desc,
		PathSep: req.PathSep,
	})
	if err != nil {
		return nil, err
	}
	initialCold := int64(rs.ColdReads())
	if initialCold > 0 {
		s.parent.totalColdReads.Add(initialCold)
	}
	return &queryResultAdapter{
		parent:        s.parent,
		rs:            rs,
		lastColdReads: initialCold,
	}, nil
}

func (s *querySessionBackend) Close() {
	s.sess.Cancel()
	s.sess.Reset()
}

type queryResultAdapter struct {
	parent        *QueryBackend
	rs            *query.ResultSet
	lastColdReads int64
}

func (a *queryResultAdapter) Total() uint64      { return uint64(a.rs.Total()) }
func (a *queryResultAdapter) Retained() uint32   { return uint32(a.rs.Retained()) }
func (a *queryResultAdapter) Truncated() bool    { return a.rs.Truncated() }
func (a *queryResultAdapter) Incremental() bool  { return a.rs.Incremental() }
func (a *queryResultAdapter) Warnings() []string { return a.rs.Warnings() }
func (a *queryResultAdapter) Close()             { a.rs.Close() }

func (a *queryResultAdapter) CostMicros() uint32 {
	us := a.rs.Cost().Microseconds()
	if us < 0 {
		return 0
	}
	if us > 0xFFFFFFFF {
		return 0xFFFFFFFF
	}
	return uint32(us)
}

func (a *queryResultAdapter) Viewport(ctx context.Context, offset, limit int) ([]ViewportRow, error) {
	before := int64(a.rs.ColdReads())
	items, err := a.rs.Viewport(ctx, offset, limit)
	after := int64(a.rs.ColdReads())
	if delta := after - before; delta > 0 {
		a.parent.totalColdReads.Add(delta)
	}
	if err != nil {
		return nil, err
	}
	rows := make([]ViewportRow, len(items))
	for i := range items {
		it := &items[i]
		flags := it.Flags
		if it.IsDir {
			flags |= storage.FlagDir
		}
		if it.Folded {
			flags |= storage.FlagFolded
		}
		if it.System {
			flags |= storage.FlagSystem
		}
		if it.FromFoldedScan {
			flags |= RowFlagFromFoldedScan
		}
		var hls []HighlightRange
		if len(it.Highlights) > 0 {
			hls = make([]HighlightRange, len(it.Highlights))
			for j, h := range it.Highlights {
				hls[j] = HighlightRange{Start: uint32(h.Start), End: uint32(h.End)}
			}
		}
		rows[i] = ViewportRow{
			NodeID:     it.NodeID,
			Flags:      flags,
			ExtID:      it.ExtID,
			VolID:      it.VolID,
			Size:       it.Size,
			ModTime:    it.ModTime,
			Score:      it.Score,
			Path:       it.Path,
			Name:       it.Name,
			Highlights: hls,
		}
	}
	return rows, nil
}
