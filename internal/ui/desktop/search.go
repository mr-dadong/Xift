package desktop

import (
	"context"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/mr-dadong/Xift/internal/ipc"
)

const (
	// DefaultPageSize is the default viewport fetch batch size (50-100 rows).
	DefaultPageSize uint16 = 80
	// MaxPageSize is the hard upper bound per FetchViewport RPC.
	MaxPageSize uint16 = 100

	defaultRequestTimeout = 10 * time.Second
)

// SearchClient abstracts the IPC client methods used by SearchController.
type SearchClient interface {
	QueryStart(ctx context.Context, text string, sort uint8, desc bool, pathSep byte) (*ipc.QueryResultMeta, error)
	FetchViewport(ctx context.Context, sessionID uint64, offset uint32, limit uint16) (*ipc.ViewportRows, error)
	Cancel(sessionID uint64) error
	Close() error
}

// DialFunc establishes a SearchClient connection to the local Daemon.
type DialFunc func(ctx context.Context) (SearchClient, error)

// NewIPCDialer returns a DialFunc that connects to the local Xift Daemon over
// the authenticated OS IPC transport (Windows Named Pipe + DACL / Unix Domain Socket + peer cred).
func NewIPCDialer(endpoint, clientName string) DialFunc {
	if clientName == "" {
		clientName = "xift-ui"
	}
	return func(ctx context.Context) (SearchClient, error) {
		return ipc.DialWithName(ctx, endpoint, clientName)
	}
}

// SearchConfig configures a SearchController.
type SearchConfig struct {
	Dial           DialFunc
	PageSize       uint16
	PathSep        byte
	RequestTimeout time.Duration
	OnChange       func()
}

// SearchSnapshot is an immutable view of the current search state for the UI thread.
type SearchSnapshot struct {
	QueryText    string
	Sort         uint8
	Desc         bool
	SessionID    uint64
	Total        uint64
	Retained     uint32
	Truncated    bool
	CostMicros   uint32
	Warnings     []string
	PageOffset   uint32
	Rows         []ipc.ViewportRow
	Loading      bool
	Disconnected bool
	Error        string
}

// RowAt returns the materialized row for global virtual index idx if it resides
// in the currently loaded page.
func (s SearchSnapshot) RowAt(idx int) (ipc.ViewportRow, bool) {
	if idx < 0 {
		return ipc.ViewportRow{}, false
	}
	u := uint32(idx)
	if u < s.PageOffset {
		return ipc.ViewportRow{}, false
	}
	local := int(u - s.PageOffset)
	if local < 0 || local >= len(s.Rows) {
		return ipc.ViewportRow{}, false
	}
	return s.Rows[local], true
}

// SearchController executes IPC handshake, QueryStart, and FetchViewport in background
// goroutines while isolating UI state by (queryGen, pageSeq) so stale responses can never
// overwrite newer queries or pages.
type SearchController struct {
	dial           DialFunc
	pageSize       uint16
	pathSep        byte
	requestTimeout time.Duration
	onChange       func()

	mu           sync.Mutex
	client       SearchClient
	closed       bool
	queryText    string
	sort         uint8
	desc         bool
	queryGen     uint64
	pageSeq      uint64
	sessionID    uint64
	total        uint64
	retained     uint32
	truncated    bool
	costMicros   uint32
	warnings     []string
	pageOffset   uint32
	rows         []ipc.ViewportRow
	loading      bool
	disconnected bool
	errMsg       string

	queryCancel context.CancelFunc
	pageCancel  context.CancelFunc
}

// NewSearchController creates a SearchController with safe defaults.
func NewSearchController(cfg SearchConfig) *SearchController {
	ps := clampPageSize(cfg.PageSize)
	sep := cfg.PathSep
	if sep == 0 {
		sep = byte(os.PathSeparator)
	}
	timeout := cfg.RequestTimeout
	if timeout <= 0 {
		timeout = defaultRequestTimeout
	}
	return &SearchController{
		dial:           cfg.Dial,
		pageSize:       ps,
		pathSep:        sep,
		requestTimeout: timeout,
		onChange:       cfg.OnChange,
	}
}

func clampPageSize(limit uint16) uint16 {
	if limit == 0 {
		return DefaultPageSize
	}
	if limit > MaxPageSize {
		return MaxPageSize
	}
	return limit
}

// SetQuery starts a new background query, superseding any in-flight query or page fetch.
func (s *SearchController) SetQuery(text string, sort uint8, desc bool) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	if s.queryCancel != nil {
		s.queryCancel()
		s.queryCancel = nil
	}
	if s.pageCancel != nil {
		s.pageCancel()
		s.pageCancel = nil
	}

	s.queryText = text
	s.sort = sort
	s.desc = desc
	s.queryGen++
	gen := s.queryGen
	s.pageSeq++
	pseq := s.pageSeq
	s.sessionID = 0
	s.loading = true
	s.errMsg = ""

	ctx, cancel := context.WithTimeout(context.Background(), s.requestTimeout)
	s.queryCancel = cancel
	s.mu.Unlock()

	s.notify()
	go s.runQuery(ctx, cancel, gen, pseq, text, sort, desc)
}

// Reconnect re-establishes the IPC connection if needed and re-runs the retained query text.
func (s *SearchController) Reconnect() {
	s.mu.Lock()
	text := s.queryText
	sort := s.sort
	desc := s.desc
	s.mu.Unlock()
	s.SetQuery(text, sort, desc)
}

// Disconnect cancels any active query session and closes the IPC connection while preserving
// the current query text, sort, and desc for subsequent Reconnect calls.
func (s *SearchController) Disconnect() {
	s.mu.Lock()
	if s.queryCancel != nil {
		s.queryCancel()
		s.queryCancel = nil
	}
	if s.pageCancel != nil {
		s.pageCancel()
		s.pageCancel = nil
	}
	s.queryGen++
	s.pageSeq++
	client := s.client
	sid := s.sessionID
	s.client = nil
	s.sessionID = 0
	s.total = 0
	s.retained = 0
	s.truncated = false
	s.costMicros = 0
	s.warnings = nil
	s.pageOffset = 0
	s.rows = nil
	s.loading = false
	s.disconnected = true
	s.mu.Unlock()

	if client != nil {
		if sid != 0 {
			_ = client.Cancel(sid)
		}
		_ = client.Close()
	}
	s.notify()
}

// Close permanently shuts down the SearchController and closes any live IPC connection.
func (s *SearchController) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	if s.queryCancel != nil {
		s.queryCancel()
		s.queryCancel = nil
	}
	if s.pageCancel != nil {
		s.pageCancel()
		s.pageCancel = nil
	}
	client := s.client
	sid := s.sessionID
	s.client = nil
	s.sessionID = 0
	s.rows = nil
	s.mu.Unlock()

	if client != nil {
		if sid != 0 {
			_ = client.Cancel(sid)
		}
		_ = client.Close()
	}
}

// RequestPage fetches a single viewport page starting at offset with at most MaxPageSize rows.
func (s *SearchController) RequestPage(offset uint32, limit uint16) {
	s.mu.Lock()
	if s.closed || s.sessionID == 0 || s.retained == 0 || s.client == nil {
		s.mu.Unlock()
		return
	}

	reqLimit := clampPageSize(limit)
	if limit == 0 {
		reqLimit = s.pageSize
	}
	if offset >= s.retained {
		s.mu.Unlock()
		return
	}
	remaining := s.retained - offset
	if uint32(reqLimit) > remaining {
		reqLimit = uint16(remaining)
	}

	// Skip redundant fetch if the exact page is already loaded and not loading.
	if !s.loading && s.pageOffset == offset && uint32(len(s.rows)) == uint32(reqLimit) {
		s.mu.Unlock()
		return
	}

	if s.pageCancel != nil {
		s.pageCancel()
		s.pageCancel = nil
	}
	s.pageSeq++
	gen := s.queryGen
	pseq := s.pageSeq
	sid := s.sessionID
	client := s.client
	s.loading = true

	ctx, cancel := context.WithTimeout(context.Background(), s.requestTimeout)
	s.pageCancel = cancel
	s.mu.Unlock()

	go s.runFetchPage(ctx, cancel, client, gen, pseq, sid, offset, reqLimit)
}

func (s *SearchController) ensureClient(ctx context.Context) (SearchClient, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, errors.New("desktop: search controller closed")
	}
	if s.client != nil {
		c := s.client
		s.mu.Unlock()
		return c, nil
	}
	dial := s.dial
	s.mu.Unlock()

	if dial == nil {
		return nil, errors.New("desktop: no IPC dialer configured")
	}
	c, err := dial(ctx)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = c.Close()
		return nil, errors.New("desktop: search controller closed")
	}
	if s.client != nil {
		existing := s.client
		s.mu.Unlock()
		_ = c.Close()
		return existing, nil
	}
	s.client = c
	s.mu.Unlock()
	return c, nil
}

func (s *SearchController) dropClient(c SearchClient) {
	s.mu.Lock()
	if s.client == c {
		s.client = nil
		s.sessionID = 0
	}
	s.mu.Unlock()
	if c != nil {
		_ = c.Close()
	}
}

func (s *SearchController) runQuery(ctx context.Context, cancel context.CancelFunc, gen, pseq uint64, text string, sort uint8, desc bool) {
	defer cancel()

	client, err := s.ensureClient(ctx)
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			return
		}
		s.commitError(gen, pseq, err, true)
		return
	}

	meta, err := client.QueryStart(ctx, text, sort, desc, s.pathSep)
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			return
		}
		isRPC := false
		var rpcErr *ipc.RPCError
		if errors.As(err, &rpcErr) {
			isRPC = true
		}
		if !isRPC {
			s.dropClient(client)
		}
		s.commitError(gen, pseq, err, !isRPC)
		return
	}

	s.mu.Lock()
	if s.closed || gen != s.queryGen {
		s.mu.Unlock()
		return
	}
	s.sessionID = meta.SessionID
	s.total = meta.Total
	s.retained = meta.Retained
	s.truncated = meta.Truncated
	s.costMicros = meta.CostMicros
	s.warnings = append([]string(nil), meta.Warnings...)
	s.disconnected = false
	s.errMsg = ""
	if meta.Retained == 0 {
		s.pageOffset = 0
		s.rows = nil
		s.loading = false
		s.mu.Unlock()
		s.notify()
		return
	}
	fetchLimit := s.pageSize
	if uint32(fetchLimit) > meta.Retained {
		fetchLimit = uint16(meta.Retained)
	}
	s.mu.Unlock()

	s.runFetchPage(ctx, nil, client, gen, pseq, meta.SessionID, 0, fetchLimit)
}

func (s *SearchController) runFetchPage(ctx context.Context, cancel context.CancelFunc, client SearchClient, gen, pseq, sid uint64, offset uint32, limit uint16) {
	if cancel != nil {
		defer cancel()
	}

	vp, err := client.FetchViewport(ctx, sid, offset, limit)
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			return
		}
		isRPC := false
		var rpcErr *ipc.RPCError
		if errors.As(err, &rpcErr) {
			isRPC = true
		}
		if !isRPC {
			s.dropClient(client)
		}
		s.commitError(gen, pseq, err, !isRPC)
		return
	}

	s.mu.Lock()
	if s.closed || gen != s.queryGen || pseq != s.pageSeq || vp.SessionID != s.sessionID {
		s.mu.Unlock()
		return
	}
	s.pageOffset = vp.Offset
	s.rows = vp.Rows
	s.loading = false
	s.disconnected = false
	s.errMsg = ""
	s.mu.Unlock()

	s.notify()
}

func (s *SearchController) commitError(gen, pseq uint64, err error, disconnected bool) {
	s.mu.Lock()
	if s.closed || gen != s.queryGen || pseq != s.pageSeq {
		s.mu.Unlock()
		return
	}
	s.loading = false
	s.disconnected = disconnected
	s.errMsg = err.Error()
	// Never keep stale rows visible as results of a failed/disconnected query.
	s.rows = nil
	s.pageOffset = 0
	if disconnected {
		s.retained = 0
		s.total = 0
		s.truncated = false
		s.sessionID = 0
	}
	s.mu.Unlock()

	s.notify()
}

// Snapshot returns a copy of the current search state safe for rendering on the UI thread.
func (s *SearchController) Snapshot() SearchSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	rowsCopy := make([]ipc.ViewportRow, len(s.rows))
	copy(rowsCopy, s.rows)
	warnCopy := make([]string, len(s.warnings))
	copy(warnCopy, s.warnings)

	return SearchSnapshot{
		QueryText:    s.queryText,
		Sort:         s.sort,
		Desc:         s.desc,
		SessionID:    s.sessionID,
		Total:        s.total,
		Retained:     s.retained,
		Truncated:    s.truncated,
		CostMicros:   s.costMicros,
		Warnings:     warnCopy,
		PageOffset:   s.pageOffset,
		Rows:         rowsCopy,
		Loading:      s.loading,
		Disconnected: s.disconnected,
		Error:        s.errMsg,
	}
}

func (s *SearchController) notify() {
	if s.onChange != nil {
		s.onChange()
	}
}
