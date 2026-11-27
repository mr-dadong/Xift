package ipc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// DefaultMaxConns is the maximum number of concurrent authenticated IPC clients.
	DefaultMaxConns = 32

	// DefaultIdleTimeout is the inactivity duration after which an active session is released.
	DefaultIdleTimeout = 5 * time.Minute

	// MinViewportLimit and MaxViewportLimit bound FetchViewport.Limit.
	MinViewportLimit = 1
	MaxViewportLimit = 500

	handshakeTimeout      = 5 * time.Second
	maxBusyRejectHandlers = 16
	maxBusyHelloPayload   = 4096
	maxInFlightPerConn    = 16
)

// Timer abstracts time.Timer so idle timeout expiration can be tested deterministically.
type Timer interface {
	Stop() bool
	Reset(d time.Duration) bool
}

type stdTimer struct{ t *time.Timer }

func (s *stdTimer) Stop() bool                     { return s.t.Stop() }
func (s *stdTimer) Reset(d time.Duration) bool     { return s.t.Reset(d) }
func stdAfterFunc(d time.Duration, f func()) Timer { return &stdTimer{t: time.AfterFunc(d, f)} }

// QueryRequest holds the normalized parameters from a QueryStart frame.
type QueryRequest struct {
	Text    string
	Sort    uint8
	Desc    bool
	PathSep byte
}

// BackendStatus reports index-level statistics for StatusReply.
type BackendStatus struct {
	LiveCount uint64
}

// Result is a session-scoped ranked result set supporting viewport paging.
type Result interface {
	Total() uint64
	Retained() uint32
	Truncated() bool
	Incremental() bool
	CostMicros() uint32
	Warnings() []string
	Viewport(ctx context.Context, offset, limit int) ([]ViewportRow, error)
	Close()
}

// SessionBackend executes queries for a single client connection (preserving incremental cache).
type SessionBackend interface {
	Start(ctx context.Context, req QueryRequest) (Result, error)
	Close()
}

// Backend supplies per-connection query sessions and service status.
type Backend interface {
	NewSession() SessionBackend
	Status() BackendStatus
}

// ServerOptions configures IPC Server resource limits and timer injection.
type ServerOptions struct {
	MaxConns    int
	IdleTimeout time.Duration
	AfterFunc   func(d time.Duration, f func()) Timer
}

// Server serves the Xift binary viewport RPC protocol over a Listener.
type Server struct {
	backend     Backend
	maxConns    int
	idleTimeout time.Duration
	afterFunc   func(d time.Duration, f func()) Timer

	mu       sync.Mutex
	listener Listener
	conns    map[*connState]struct{}
	closed   bool

	activeSessions atomic.Int64
	rejectedConns  atomic.Uint64
	busySem        chan struct{}
	wg             sync.WaitGroup
}

// NewServer creates a new IPC RPC Server backed by backend.
func NewServer(backend Backend, opts ServerOptions) *Server {
	maxConns := opts.MaxConns
	if maxConns <= 0 {
		maxConns = DefaultMaxConns
	}
	idle := opts.IdleTimeout
	if idle <= 0 {
		idle = DefaultIdleTimeout
	}
	af := opts.AfterFunc
	if af == nil {
		af = stdAfterFunc
	}
	return &Server{
		backend:     backend,
		maxConns:    maxConns,
		idleTimeout: idle,
		afterFunc:   af,
		conns:       make(map[*connState]struct{}),
		busySem:     make(chan struct{}, maxBusyRejectHandlers),
	}
}

// Serve accepts connections on ln until ln is closed or Shutdown is called.
func (s *Server) Serve(ln Listener) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrListenerClosed
	}
	s.listener = ln
	s.mu.Unlock()

	for {
		conn, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed || errors.Is(err, ErrListenerClosed) {
				return nil
			}
			return err
		}

		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			_ = conn.Close()
			return nil
		}
		if len(s.conns) >= s.maxConns {
			select {
			case s.busySem <- struct{}{}:
				s.wg.Add(1)
				s.mu.Unlock()
				go s.rejectBusy(conn)
			default:
				s.mu.Unlock()
				_ = conn.Close()
			}
			continue
		}
		cs := newConnState(s, conn)
		s.conns[cs] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()

		go s.serveConn(cs)
	}
}

func (s *Server) rejectBusy(conn net.Conn) {
	defer func() {
		<-s.busySem
		s.wg.Done()
	}()
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(500 * time.Millisecond))
	frame, err := readFrameWithMax(conn, maxBusyHelloPayload)
	reqID := uint32(0)
	if err == nil {
		reqID = frame.ReqID
	}
	mt, pl, merr := EncodeMessage(&ErrorResp{
		Code:    ErrBusy,
		Message: fmt.Sprintf("server busy: max %d concurrent connections reached", s.maxConns),
	})
	if merr == nil {
		_ = WriteFrame(conn, mt, reqID, pl)
	}
}

// Shutdown stops accepting new connections, closes all active connections, cancels in-flight
// queries, releases all session snapshots, and waits until all handlers exit or ctx expires.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.closed = true
	ln := s.listener
	conns := make([]*connState, 0, len(s.conns))
	for cs := range s.conns {
		conns = append(conns, cs)
	}
	s.mu.Unlock()

	if ln != nil {
		_ = ln.Close()
	}
	for _, cs := range conns {
		cs.close()
	}

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) totalRejected() uint64 {
	s.mu.Lock()
	ln := s.listener
	s.mu.Unlock()
	total := s.rejectedConns.Load()
	if ln != nil {
		total += ln.RejectedCount()
	}
	return total
}

func (s *Server) removeConn(cs *connState) {
	s.mu.Lock()
	delete(s.conns, cs)
	s.mu.Unlock()
}

type activeSession struct {
	sessionID uint64
	token     uint64
	cancel    context.CancelFunc
	ready     chan struct{}

	mu     sync.RWMutex
	result Result
	closed bool
}

func (as *activeSession) setResult(r Result) bool {
	as.mu.Lock()
	defer as.mu.Unlock()
	if as.closed {
		return false
	}
	as.result = r
	return true
}

func (as *activeSession) viewport(ctx context.Context, offset, limit int) ([]ViewportRow, bool, error) {
	as.mu.RLock()
	defer as.mu.RUnlock()
	if as.closed || as.result == nil {
		return nil, false, nil
	}
	rows, err := as.result.Viewport(ctx, offset, limit)
	return rows, true, err
}

func (as *activeSession) release() {
	if as.cancel != nil {
		as.cancel()
	}
	as.mu.Lock()
	if !as.closed {
		as.closed = true
		if as.result != nil {
			as.result.Close()
			as.result = nil
		}
	}
	as.mu.Unlock()
}

type connState struct {
	server *Server
	conn   net.Conn
	sb     SessionBackend

	connCtx    context.Context
	connCancel context.CancelFunc
	closeOnce  sync.Once

	writeMu sync.Mutex

	mu        sync.Mutex
	active    *activeSession
	seqToken  uint64
	idleTimer Timer

	reqSem chan struct{}
	wg     sync.WaitGroup
}

func newConnState(s *Server, conn net.Conn) *connState {
	ctx, cancel := context.WithCancel(context.Background())
	return &connState{
		server:     s,
		conn:       conn,
		sb:         s.backend.NewSession(),
		connCtx:    ctx,
		connCancel: cancel,
		reqSem:     make(chan struct{}, maxInFlightPerConn),
	}
}

func (cs *connState) close() {
	cs.closeOnce.Do(func() {
		cs.connCancel()
		_ = cs.conn.Close()
	})
}

func (cs *connState) writeMessage(reqID uint32, msg any) error {
	mt, pl, err := EncodeMessage(msg)
	if err != nil {
		return err
	}
	cs.writeMu.Lock()
	defer cs.writeMu.Unlock()
	return WriteFrame(cs.conn, mt, reqID, pl)
}

func (cs *connState) writeError(reqID uint32, code ErrCode, msg string) {
	_ = cs.writeMessage(reqID, &ErrorResp{Code: code, Message: msg})
}

func (cs *connState) clearActiveLocked() *activeSession {
	old := cs.active
	if old != nil {
		cs.active = nil
		cs.server.activeSessions.Add(-1)
	}
	if cs.idleTimer != nil {
		cs.idleTimer.Stop()
		cs.idleTimer = nil
	}
	return old
}

func (cs *connState) resetIdleTimerLocked(token uint64) {
	if cs.idleTimer != nil {
		cs.idleTimer.Stop()
	}
	cs.idleTimer = cs.server.afterFunc(cs.server.idleTimeout, func() {
		cs.mu.Lock()
		var toRelease *activeSession
		if cs.active != nil && cs.active.token == token {
			toRelease = cs.clearActiveLocked()
		}
		cs.mu.Unlock()
		if toRelease != nil {
			toRelease.release()
		}
	})
}

func (s *Server) serveConn(cs *connState) {
	defer s.wg.Done()
	defer s.removeConn(cs)
	defer func() {
		cs.close()
		cs.mu.Lock()
		old := cs.clearActiveLocked()
		cs.mu.Unlock()
		if old != nil {
			old.release()
		}
		cs.wg.Wait()
		if cs.sb != nil {
			cs.sb.Close()
		}
	}()

	// Step 1: Mandatory Hello handshake with bounded deadline to prevent slowloris slot exhaustion.
	_ = cs.conn.SetDeadline(time.Now().Add(handshakeTimeout))
	first, err := readFrameWithMax(cs.conn, maxBusyHelloPayload)
	if err != nil {
		return
	}
	if first.Type != MsgHello {
		cs.writeError(first.ReqID, ErrBadRequest, "expected Hello as first message")
		return
	}
	msg, err := DecodeMessage(first.Type, first.Payload)
	if err != nil {
		cs.writeError(first.ReqID, ErrBadRequest, err.Error())
		return
	}
	hello := msg.(*HelloReq)
	if hello.MajorVersion != ProtocolMajorVersion {
		cs.writeError(first.ReqID, ErrVersionMismatch, fmt.Sprintf("unsupported protocol major version %d (server supports %d)", hello.MajorVersion, ProtocolMajorVersion))
		return
	}
	if err := cs.writeMessage(first.ReqID, &HelloAck{
		MajorVersion: ProtocolMajorVersion,
		DaemonPID:    uint32(os.Getpid()),
	}); err != nil {
		return
	}
	_ = cs.conn.SetDeadline(time.Time{})

	// Step 2: Main request dispatch loop.
	for {
		frame, err := ReadFrame(cs.conn)
		if err != nil {
			return
		}
		if frame.Version != ProtocolMajorVersion {
			cs.writeError(frame.ReqID, ErrVersionMismatch, fmt.Sprintf("unsupported frame version %d", frame.Version))
			return
		}
		decoded, err := DecodeMessage(frame.Type, frame.Payload)
		if err != nil {
			// Malformed payload closes this connection immediately without affecting others.
			return
		}

		switch req := decoded.(type) {
		case *QueryStartReq:
			cs.handleQueryStart(frame.ReqID, req)
		case *FetchViewportReq:
			cs.handleFetchViewport(frame.ReqID, req)
		case *CancelReq:
			cs.handleCancel(req)
		case *StatusReq:
			cs.handleStatus(frame.ReqID)
		default:
			cs.writeError(frame.ReqID, ErrBadRequest, "unexpected message type")
		}
	}
}

func (cs *connState) handleQueryStart(reqID uint32, req *QueryStartReq) {
	runCtx, cancel := context.WithCancel(cs.connCtx)

	cs.mu.Lock()
	old := cs.clearActiveLocked()
	cs.seqToken++
	token := cs.seqToken
	sess := &activeSession{
		sessionID: req.SessionID,
		token:     token,
		cancel:    cancel,
		ready:     make(chan struct{}),
	}
	cs.active = sess
	cs.server.activeSessions.Add(1)
	cs.resetIdleTimerLocked(token)
	cs.mu.Unlock()

	if old != nil {
		old.release()
	}

	select {
	case cs.reqSem <- struct{}{}:
	case <-cs.connCtx.Done():
		return
	}

	cs.wg.Add(1)
	go func() {
		defer func() {
			<-cs.reqSem
			cs.wg.Done()
		}()
		defer close(sess.ready)

		res, err := cs.sb.Start(runCtx, QueryRequest{
			Text:    req.Text,
			Sort:    req.Sort,
			Desc:    req.Desc,
			PathSep: req.PathSep,
		})
		if err != nil {
			wasCanceled := errors.Is(err, context.Canceled) || runCtx.Err() != nil
			cs.mu.Lock()
			if cs.active == sess {
				cs.clearActiveLocked()
			}
			cs.mu.Unlock()
			sess.release()

			if wasCanceled {
				cs.writeError(reqID, ErrCanceled, "query canceled")
				return
			}
			var rpcErr *RPCError
			if errors.As(err, &rpcErr) {
				cs.writeError(reqID, rpcErr.Code, rpcErr.Message)
				return
			}
			cs.writeError(reqID, ErrInternal, err.Error())
			return
		}

		// Capture metadata before installing res on sess so concurrent release cannot zero Retained/Truncated.
		metaResp := &QueryResultMeta{
			SessionID:   req.SessionID,
			Total:       res.Total(),
			Retained:    res.Retained(),
			CostMicros:  res.CostMicros(),
			Incremental: res.Incremental(),
			Truncated:   res.Truncated(),
			Warnings:    res.Warnings(),
		}

		cs.mu.Lock()
		isCurrent := cs.active == sess && runCtx.Err() == nil
		if isCurrent {
			if !sess.setResult(res) {
				isCurrent = false
			} else {
				cs.resetIdleTimerLocked(token)
			}
		}
		cs.mu.Unlock()

		if !isCurrent {
			res.Close()
			cs.writeError(reqID, ErrCanceled, "query superseded")
			return
		}

		if err := cs.writeMessage(reqID, metaResp); errors.Is(err, ErrPayloadTooLarge) {
			cs.writeError(reqID, ErrInternal, err.Error())
		}
	}()
}

func (cs *connState) handleFetchViewport(reqID uint32, req *FetchViewportReq) {
	cs.mu.Lock()
	sess := cs.active
	if sess == nil || sess.sessionID != req.SessionID {
		cs.mu.Unlock()
		cs.writeError(reqID, ErrStaleSession, fmt.Sprintf("session %d is not active", req.SessionID))
		return
	}
	token := sess.token
	cs.resetIdleTimerLocked(token)
	cs.mu.Unlock()

	limit := int(req.Limit)
	if limit < MinViewportLimit {
		limit = MinViewportLimit
	}
	if limit > MaxViewportLimit {
		limit = MaxViewportLimit
	}

	select {
	case cs.reqSem <- struct{}{}:
	case <-cs.connCtx.Done():
		return
	}

	cs.wg.Add(1)
	go func() {
		defer func() {
			<-cs.reqSem
			cs.wg.Done()
		}()
		select {
		case <-sess.ready:
		case <-cs.connCtx.Done():
			return
		}

		cs.mu.Lock()
		stillActive := cs.active == sess
		if stillActive {
			cs.resetIdleTimerLocked(token)
		}
		cs.mu.Unlock()
		if !stillActive {
			cs.writeError(reqID, ErrStaleSession, fmt.Sprintf("session %d expired or superseded", req.SessionID))
			return
		}

		rows, ok, err := sess.viewport(cs.connCtx, int(req.Offset), limit)
		if !ok {
			cs.writeError(reqID, ErrStaleSession, fmt.Sprintf("session %d is no longer available", req.SessionID))
			return
		}
		if err != nil {
			if errors.Is(err, context.Canceled) {
				cs.writeError(reqID, ErrCanceled, "viewport fetch canceled")
				return
			}
			cs.writeError(reqID, ErrInternal, err.Error())
			return
		}

		if err := cs.writeMessage(reqID, &ViewportRows{
			SessionID: req.SessionID,
			Offset:    req.Offset,
			Rows:      rows,
		}); errors.Is(err, ErrPayloadTooLarge) {
			cs.writeError(reqID, ErrInternal, err.Error())
		}
	}()
}

func (cs *connState) handleCancel(req *CancelReq) {
	cs.mu.Lock()
	var old *activeSession
	if cs.active != nil && (req.SessionID == 0 || cs.active.sessionID == req.SessionID) {
		old = cs.clearActiveLocked()
	}
	cs.mu.Unlock()
	if old != nil {
		old.release()
	}
}

func (cs *connState) handleStatus(reqID uint32) {
	st := cs.server.backend.Status()
	active := cs.server.activeSessions.Load()
	if active < 0 {
		active = 0
	}
	_ = cs.writeMessage(reqID, &StatusReply{
		ProtocolVersion: ProtocolMajorVersion,
		LiveCount:       st.LiveCount,
		ActiveSessions:  uint32(active),
		RejectedConns:   cs.server.totalRejected(),
	})
}
