package ipc

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// memListener is an in-memory net.Pipe-based Listener for deterministic unit testing.
type memListener struct {
	ch       chan net.Conn
	closed   atomic.Bool
	rejected atomic.Uint64
	once     sync.Once
}

func newMemListener() *memListener {
	return &memListener{ch: make(chan net.Conn, 64)}
}

func (l *memListener) Accept() (net.Conn, error) {
	c, ok := <-l.ch
	if !ok {
		return nil, ErrListenerClosed
	}
	return c, nil
}

func (l *memListener) Close() error {
	l.once.Do(func() {
		l.closed.Store(true)
		close(l.ch)
	})
	return nil
}

func (l *memListener) Addr() string          { return "mem://test" }
func (l *memListener) RejectedCount() uint64 { return l.rejected.Load() }

func (l *memListener) dial() (net.Conn, error) {
	if l.closed.Load() {
		return nil, ErrListenerClosed
	}
	c1, c2 := net.Pipe()
	select {
	case l.ch <- c1:
		return c2, nil
	default:
		_ = c1.Close()
		_ = c2.Close()
		return nil, errors.New("mem listener full")
	}
}

// fakeClock drives deterministic idle timeout expiration in unit tests.
type fakeTimer struct {
	mu      sync.Mutex
	stopped bool
	d       time.Duration
	fn      func()
	clk     *fakeClock
}

func (t *fakeTimer) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	was := !t.stopped
	t.stopped = true
	return was
}

func (t *fakeTimer) Reset(d time.Duration) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	was := !t.stopped
	t.stopped = false
	t.d = d
	return was
}

type fakeClock struct {
	mu     sync.Mutex
	timers []*fakeTimer
}

func (c *fakeClock) afterFunc(d time.Duration, fn func()) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	ft := &fakeTimer{d: d, fn: fn, clk: c}
	c.timers = append(c.timers, ft)
	return ft
}

func (c *fakeClock) fireAllExpired(elapsed time.Duration) {
	c.mu.Lock()
	list := append([]*fakeTimer(nil), c.timers...)
	c.mu.Unlock()
	for _, ft := range list {
		ft.mu.Lock()
		shouldFire := !ft.stopped && elapsed >= ft.d
		if shouldFire {
			ft.stopped = true
		}
		fn := ft.fn
		ft.mu.Unlock()
		if shouldFire && fn != nil {
			fn()
		}
	}
}

type fakeResult struct {
	total       uint64
	retained    uint32
	truncated   bool
	incremental bool
	costMicros  uint32
	warnings    []string
	rows        []ViewportRow
	closed      atomic.Bool
	lastLimit   atomic.Int32
}

func (r *fakeResult) Total() uint64      { return r.total }
func (r *fakeResult) Retained() uint32   { return r.retained }
func (r *fakeResult) Truncated() bool    { return r.truncated }
func (r *fakeResult) Incremental() bool  { return r.incremental }
func (r *fakeResult) CostMicros() uint32 { return r.costMicros }
func (r *fakeResult) Warnings() []string { return r.warnings }
func (r *fakeResult) Close()             { r.closed.Store(true) }
func (r *fakeResult) Viewport(ctx context.Context, offset, limit int) ([]ViewportRow, error) {
	r.lastLimit.Store(int32(limit))
	if offset >= len(r.rows) {
		return []ViewportRow{}, nil
	}
	end := offset + limit
	if end > len(r.rows) {
		end = len(r.rows)
	}
	out := make([]ViewportRow, end-offset)
	copy(out, r.rows[offset:end])
	return out, nil
}

type fakeBackend struct {
	liveCount uint64
	mu        sync.Mutex
	startHook func(ctx context.Context, req QueryRequest) (Result, error)
	results   []*fakeResult
}

func (b *fakeBackend) NewSession() SessionBackend { return b }
func (b *fakeBackend) Status() BackendStatus      { return BackendStatus{LiveCount: b.liveCount} }
func (b *fakeBackend) Close()                     {}

func (b *fakeBackend) Start(ctx context.Context, req QueryRequest) (Result, error) {
	b.mu.Lock()
	hook := b.startHook
	b.mu.Unlock()
	if hook != nil {
		return hook(ctx, req)
	}
	rows := make([]ViewportRow, 600)
	for i := range rows {
		rows[i] = ViewportRow{
			NodeID: uint32(i + 1),
			Path:   fmt.Sprintf("/dir/file_%04d.go", i),
			Name:   fmt.Sprintf("file_%04d.go", i),
			Size:   int64(i * 10),
		}
	}
	res := &fakeResult{
		total:    600,
		retained: 600,
		rows:     rows,
	}
	b.mu.Lock()
	b.results = append(b.results, res)
	b.mu.Unlock()
	return res, nil
}

func startTestServer(t *testing.T, backend Backend, opts ServerOptions) (*Server, *memListener) {
	t.Helper()
	ln := newMemListener()
	srv := NewServer(backend, opts)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	return srv, ln
}

func TestServerHelloVersionMismatch(t *testing.T) {
	_, ln := startTestServer(t, &fakeBackend{}, ServerOptions{})
	conn, err := ln.dial()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	mt, pl, err := EncodeMessage(&HelloReq{MajorVersion: 99, ClientName: "future"})
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteFrame(conn, mt, 1, pl); err != nil {
		t.Fatal(err)
	}
	frame, err := ReadFrame(conn)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if frame.Type != MsgError || frame.ReqID != 1 {
		t.Fatalf("got frame %+v, want MsgError reqID=1", frame)
	}
	msg, err := DecodeMessage(frame.Type, frame.Payload)
	if err != nil {
		t.Fatal(err)
	}
	eResp := msg.(*ErrorResp)
	if eResp.Code != ErrVersionMismatch {
		t.Fatalf("error code = %v, want VersionMismatch", eResp.Code)
	}
	// Connection must be closed afterwards.
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := ReadFrame(conn); err == nil {
		t.Fatalf("expected connection to be closed after VersionMismatch")
	}
}

func TestServerMalformedFrameIsolatesOffender(t *testing.T) {
	_, ln := startTestServer(t, &fakeBackend{liveCount: 42}, ServerOptions{})

	// Client A connects and sends an 8 MiB frame header.
	connA, err := ln.dial()
	if err != nil {
		t.Fatal(err)
	}
	defer connA.Close()
	cliA, err := newClientFromConn(context.Background(), connA, "clientA")
	if err != nil {
		t.Fatal(err)
	}
	defer cliA.Close()

	// Client B connects normally.
	connB, err := ln.dial()
	if err != nil {
		t.Fatal(err)
	}
	cliB, err := newClientFromConn(context.Background(), connB, "clientB")
	if err != nil {
		t.Fatal(err)
	}
	defer cliB.Close()

	// Send oversized header directly on connA.
	var badHdr [FrameHeaderSize]byte
	badHdr[0], badHdr[1] = FrameMagic0, FrameMagic1
	badHdr[2] = ProtocolMajorVersion
	badHdr[3] = byte(MsgQueryStart)
	binary.LittleEndian.PutUint32(badHdr[4:8], 999)
	binary.LittleEndian.PutUint32(badHdr[8:12], 8*1024*1024)
	_, _ = connA.Write(badHdr[:])

	// Client B must still work without interruption.
	st, err := cliB.Status(context.Background())
	if err != nil {
		t.Fatalf("client B Status failed after client A offender: %v", err)
	}
	if st.LiveCount != 42 {
		t.Fatalf("LiveCount = %d, want 42", st.LiveCount)
	}
}

func TestServerQuerySupersessionCancelsPreviousAndReleasesResult(t *testing.T) {
	started41 := make(chan struct{})
	b := &fakeBackend{}
	b.startHook = func(ctx context.Context, req QueryRequest) (Result, error) {
		if req.Text == "slow" {
			close(started41)
			<-ctx.Done()
			return nil, ctx.Err()
		}
		res := &fakeResult{total: 10, retained: 10, rows: []ViewportRow{{NodeID: 1, Name: "fast.go", Path: "/fast.go"}}}
		b.mu.Lock()
		b.results = append(b.results, res)
		b.mu.Unlock()
		return res, nil
	}
	_, ln := startTestServer(t, b, ServerOptions{})
	conn, err := ln.dial()
	if err != nil {
		t.Fatal(err)
	}
	cli, err := newClientFromConn(context.Background(), conn, "tester")
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	errCh := make(chan error, 1)
	go func() {
		_, err := cli.QueryStart(context.Background(), "slow", 0, false, '/')
		errCh <- err
	}()
	<-started41

	meta, err := cli.QueryStart(context.Background(), "fast", 0, false, '/')
	if err != nil {
		t.Fatalf("second QueryStart: %v", err)
	}
	if meta.Total != 10 {
		t.Fatalf("meta.Total = %d, want 10", meta.Total)
	}

	firstErr := <-errCh
	var rpcErr *RPCError
	if !errors.As(firstErr, &rpcErr) || rpcErr.Code != ErrCanceled {
		t.Fatalf("first QueryStart err = %v, want RPCError{Canceled}", firstErr)
	}
}

func TestServerStaleSessionLimitClampAndQueryParseRecoverable(t *testing.T) {
	b := &fakeBackend{}
	b.startHook = func(ctx context.Context, req QueryRequest) (Result, error) {
		if req.Text == "(unclosed" {
			return nil, &RPCError{Code: ErrQueryParse, Message: "unclosed paren"}
		}
		rows := make([]ViewportRow, 600)
		for i := range rows {
			rows[i] = ViewportRow{NodeID: uint32(i + 1), Name: fmt.Sprintf("f%d", i)}
		}
		res := &fakeResult{total: 600, retained: 600, rows: rows}
		b.mu.Lock()
		b.results = append(b.results, res)
		b.mu.Unlock()
		return res, nil
	}

	_, ln := startTestServer(t, b, ServerOptions{})
	conn, err := ln.dial()
	if err != nil {
		t.Fatal(err)
	}
	cli, err := newClientFromConn(context.Background(), conn, "tester")
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	// 1. Query syntax error returns QueryParse and keeps connection usable.
	_, err = cli.QueryStart(context.Background(), "(unclosed", 0, false, '/')
	var rpcErr *RPCError
	if !errors.As(err, &rpcErr) || rpcErr.Code != ErrQueryParse {
		t.Fatalf("expected QueryParse error, got %v", err)
	}

	// 2. Valid query succeeds on same connection.
	meta, err := cli.QueryStart(context.Background(), "valid", 0, false, '/')
	if err != nil {
		t.Fatalf("valid QueryStart after QueryParse: %v", err)
	}

	// 3. Limit > 500 is clamped to 500, and limit == 0 is clamped to 1.
	vp, err := cli.FetchViewport(context.Background(), meta.SessionID, 0, 999)
	if err != nil {
		t.Fatalf("FetchViewport(999): %v", err)
	}
	if len(vp.Rows) != MaxViewportLimit {
		t.Fatalf("len(vp.Rows) = %d, want %d", len(vp.Rows), MaxViewportLimit)
	}

	vpMin, err := cli.FetchViewport(context.Background(), meta.SessionID, 0, 0)
	if err != nil {
		t.Fatalf("FetchViewport(0): %v", err)
	}
	if len(vpMin.Rows) != MinViewportLimit {
		t.Fatalf("len(vpMin.Rows) = %d, want %d", len(vpMin.Rows), MinViewportLimit)
	}

	// 4. Stale SessionID returns StaleSession.
	_, err = cli.FetchViewport(context.Background(), meta.SessionID+999, 0, 10)
	if !errors.As(err, &rpcErr) || rpcErr.Code != ErrStaleSession {
		t.Fatalf("stale session err = %v, want StaleSession", err)
	}
}

func TestServerMaxConnsBusyAndIdleTimeoutAndShutdown(t *testing.T) {
	clk := &fakeClock{}
	b := &fakeBackend{liveCount: 100}
	srv, ln := startTestServer(t, b, ServerOptions{
		MaxConns:    DefaultMaxConns,
		IdleTimeout: DefaultIdleTimeout,
		AfterFunc:   clk.afterFunc,
	})
	ln.rejected.Store(3) // simulate 3 transport-level peer rejections

	// Open 32 concurrent clients.
	clients := make([]*Client, 0, DefaultMaxConns)
	for i := 0; i < DefaultMaxConns; i++ {
		c, err := ln.dial()
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		cli, err := newClientFromConn(context.Background(), c, fmt.Sprintf("c%d", i))
		if err != nil {
			t.Fatalf("handshake %d: %v", i, err)
		}
		clients = append(clients, cli)
	}
	defer func() {
		for _, cli := range clients {
			_ = cli.Close()
		}
	}()

	// 33rd client must receive ErrBusy.
	c33, err := ln.dial()
	if err != nil {
		t.Fatalf("dial 33rd: %v", err)
	}
	_, err = newClientFromConn(context.Background(), c33, "c33")
	var rpcErr *RPCError
	if !errors.As(err, &rpcErr) || rpcErr.Code != ErrBusy {
		t.Fatalf("33rd client err = %v, want RPCError{Busy}", err)
	}

	// Start a session on client 0 and verify Status counters.
	meta, err := clients[0].QueryStart(context.Background(), "hello", 0, false, '/')
	if err != nil {
		t.Fatalf("QueryStart: %v", err)
	}
	st, err := clients[0].Status(context.Background())
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.ActiveSessions != 1 || st.LiveCount != 100 || st.RejectedConns < 3 {
		t.Fatalf("unexpected Status: %+v", st)
	}

	// Advance fake clock past 5 minutes -> session must expire and release result.
	clk.fireAllExpired(DefaultIdleTimeout + time.Second)
	_, err = clients[0].FetchViewport(context.Background(), meta.SessionID, 0, 10)
	if !errors.As(err, &rpcErr) || rpcErr.Code != ErrStaleSession {
		t.Fatalf("FetchViewport after idle timeout: got %v, want StaleSession", err)
	}
	b.mu.Lock()
	if len(b.results) == 0 || !b.results[0].closed.Load() {
		b.mu.Unlock()
		t.Fatalf("expected expired session Result to be Closed")
	}
	b.mu.Unlock()

	// Start another session and verify Shutdown releases it.
	if _, err := clients[1].QueryStart(context.Background(), "world", 0, false, '/'); err != nil {
		t.Fatalf("QueryStart c1: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	b.mu.Lock()
	for i, r := range b.results {
		if !r.closed.Load() {
			b.mu.Unlock()
			t.Fatalf("result %d not closed after Shutdown", i)
		}
	}
	b.mu.Unlock()
}
