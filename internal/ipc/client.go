package ipc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
)

// Client is a concurrency-safe IPC RPC client supporting pipelined requests over one connection.
type Client struct {
	conn      net.Conn
	daemonPID uint32

	writeMu sync.Mutex

	mu       sync.Mutex
	pending  map[uint32]chan Frame
	closed   bool
	closedCh chan struct{}
	closeErr error

	nextReqID     atomic.Uint32
	nextSessionID atomic.Uint64
	readDone      chan struct{}
}

// Dial connects to the Daemon at endpoint (or DefaultEndpoint if empty), verifies server identity,
// and completes the Hello handshake.
func Dial(ctx context.Context, endpoint string) (*Client, error) {
	return DialWithName(ctx, endpoint, "xift")
}

// DialWithName connects to endpoint and identifies the client in Hello.
func DialWithName(ctx context.Context, endpoint, clientName string) (*Client, error) {
	conn, err := DialTransport(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	return newClientFromConn(ctx, conn, clientName)
}

func newClientFromConn(ctx context.Context, conn net.Conn, clientName string) (*Client, error) {
	c := &Client{
		conn:     conn,
		pending:  make(map[uint32]chan Frame),
		closedCh: make(chan struct{}),
		readDone: make(chan struct{}),
	}
	go c.readLoop()

	msg, err := c.call(ctx, &HelloReq{
		MajorVersion: ProtocolMajorVersion,
		ClientName:   clientName,
	})
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	ack, ok := msg.(*HelloAck)
	if !ok {
		_ = c.Close()
		return nil, fmt.Errorf("ipc: unexpected handshake response %T", msg)
	}
	c.daemonPID = ack.DaemonPID
	return c, nil
}

// DaemonPID returns the PID reported by the Daemon during HelloAck.
func (c *Client) DaemonPID() uint32 {
	return c.daemonPID
}

func (c *Client) readLoop() {
	defer close(c.readDone)
	for {
		frame, err := ReadFrame(c.conn)
		if err != nil {
			c.failAll(err)
			return
		}
		c.mu.Lock()
		ch, ok := c.pending[frame.ReqID]
		if ok {
			delete(c.pending, frame.ReqID)
		}
		c.mu.Unlock()
		if ok {
			ch <- frame
		}
	}
}

func (c *Client) failAll(err error) {
	c.mu.Lock()
	if !c.closed {
		c.closed = true
		c.closeErr = err
		close(c.closedCh)
	}
	c.mu.Unlock()
}

// Close closes the underlying transport connection and unblocks any pending requests.
func (c *Client) Close() error {
	c.failAll(net.ErrClosed)
	err := c.conn.Close()
	<-c.readDone
	return err
}

func (c *Client) call(ctx context.Context, msg any) (any, error) {
	mt, pl, err := EncodeMessage(msg)
	if err != nil {
		return nil, err
	}
	reqID := c.nextReqID.Add(1)
	ch := make(chan Frame, 1)

	c.mu.Lock()
	if c.closed {
		cerr := c.closeErr
		c.mu.Unlock()
		if cerr == nil {
			cerr = net.ErrClosed
		}
		return nil, cerr
	}
	c.pending[reqID] = ch
	c.mu.Unlock()

	c.writeMu.Lock()
	werr := WriteFrame(c.conn, mt, reqID, pl)
	c.writeMu.Unlock()
	if werr != nil {
		c.mu.Lock()
		delete(c.pending, reqID)
		c.mu.Unlock()
		return nil, werr
	}

	decodeResp := func(frame Frame) (any, error) {
		decoded, derr := DecodeMessage(frame.Type, frame.Payload)
		if derr != nil {
			return nil, derr
		}
		if eResp, ok := decoded.(*ErrorResp); ok {
			return nil, &RPCError{Code: eResp.Code, Message: eResp.Message}
		}
		return decoded, nil
	}

	select {
	case frame := <-ch:
		return decodeResp(frame)

	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pending, reqID)
		c.mu.Unlock()
		return nil, ctx.Err()

	case <-c.closedCh:
		// Prioritize a response frame (e.g. ErrBusy / ErrVersionMismatch) delivered right before close.
		select {
		case frame := <-ch:
			return decodeResp(frame)
		default:
		}
		c.mu.Lock()
		delete(c.pending, reqID)
		cerr := c.closeErr
		c.mu.Unlock()
		if cerr == nil {
			cerr = net.ErrClosed
		}
		return nil, cerr
	}
}

// QueryStart starts a new query session on this connection, superseding any prior session.
func (c *Client) QueryStart(ctx context.Context, text string, sort uint8, desc bool, pathSep byte) (*QueryResultMeta, error) {
	sid := c.nextSessionID.Add(1)
	msg, err := c.call(ctx, &QueryStartReq{
		SessionID: sid,
		Text:      text,
		Sort:      sort,
		Desc:      desc,
		PathSep:   pathSep,
	})
	if err != nil {
		return nil, err
	}
	meta, ok := msg.(*QueryResultMeta)
	if !ok {
		return nil, fmt.Errorf("ipc: unexpected QueryStart response %T", msg)
	}
	return meta, nil
}

// FetchViewport fetches rows [offset, offset+limit) from an active query session.
func (c *Client) FetchViewport(ctx context.Context, sessionID uint64, offset uint32, limit uint16) (*ViewportRows, error) {
	msg, err := c.call(ctx, &FetchViewportReq{
		SessionID: sessionID,
		Offset:    offset,
		Limit:     limit,
	})
	if err != nil {
		return nil, err
	}
	vp, ok := msg.(*ViewportRows)
	if !ok {
		return nil, fmt.Errorf("ipc: unexpected FetchViewport response %T", msg)
	}
	return vp, nil
}

// Cancel sends a one-way Cancel request to abort and release sessionID on the server.
func (c *Client) Cancel(sessionID uint64) error {
	mt, pl, err := EncodeMessage(&CancelReq{SessionID: sessionID})
	if err != nil {
		return err
	}
	reqID := c.nextReqID.Add(1)
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return WriteFrame(c.conn, mt, reqID, pl)
}

// Status queries Daemon health and counters.
func (c *Client) Status(ctx context.Context) (*StatusReply, error) {
	msg, err := c.call(ctx, &StatusReq{})
	if err != nil {
		return nil, err
	}
	st, ok := msg.(*StatusReply)
	if !ok {
		return nil, fmt.Errorf("ipc: unexpected Status response %T", msg)
	}
	return st, nil
}

// IsRPCError reports whether err is an *RPCError with the given code.
func IsRPCError(err error, code ErrCode) bool {
	var rpcErr *RPCError
	return errors.As(err, &rpcErr) && rpcErr.Code == code
}
