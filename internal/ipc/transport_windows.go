//go:build windows

package ipc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/windows"
)

const (
	pipeBufferSize         = 64 * 1024
	pipeOpenModeBase       = windows.PIPE_ACCESS_DUPLEX | windows.FILE_FLAG_OVERLAPPED
	pipeOpenModeFirst      = pipeOpenModeBase | windows.FILE_FLAG_FIRST_PIPE_INSTANCE
	pipeMode               = windows.PIPE_TYPE_BYTE | windows.PIPE_READMODE_BYTE | windows.PIPE_WAIT | windows.PIPE_REJECT_REMOTE_CLIENTS
	securitySQOSPresent    = 0x00100000
	securityIdentification = 0x00010000
)

type pipeAddr string

func (a pipeAddr) Network() string { return "pipe" }
func (a pipeAddr) String() string  { return string(a) }

func defaultEndpoint() (string, error) {
	sid, err := currentProcessUserSID()
	if err != nil {
		return "", err
	}
	return `\\.\pipe\xift-daemon-` + sid, nil
}

type winPipeListener struct {
	name       string
	nameUTF16  *uint16
	selfSID    string
	sd         *windows.SECURITY_DESCRIPTOR
	sa         *windows.SecurityAttributes
	verifier   func(peerID, selfID string) error
	closeEvent windows.Handle
	closed     atomic.Bool
	rejected   atomic.Uint64

	mu       sync.Mutex
	nextPipe windows.Handle
	acceptWg sync.WaitGroup
}

func listenWithOptions(opts ListenOptions) (Listener, error) {
	ep := opts.Endpoint
	if ep == "" {
		var err error
		ep, err = defaultEndpoint()
		if err != nil {
			return nil, err
		}
	}

	selfSID, err := currentProcessUserSID()
	if err != nil {
		return nil, err
	}
	sd, sa, err := sameUserSecurityDescriptor(selfSID)
	if err != nil {
		return nil, err
	}
	nameUTF16, err := windows.UTF16PtrFromString(ep)
	if err != nil {
		return nil, fmt.Errorf("ipc: invalid pipe name %q: %w", ep, err)
	}

	firstPipe, err := windows.CreateNamedPipe(
		nameUTF16,
		pipeOpenModeFirst,
		pipeMode,
		windows.PIPE_UNLIMITED_INSTANCES,
		pipeBufferSize,
		pipeBufferSize,
		0,
		sa,
	)
	runtime.KeepAlive(sd)
	if err != nil {
		if errors.Is(err, windows.ERROR_ACCESS_DENIED) || errors.Is(err, windows.ERROR_PIPE_BUSY) {
			return nil, ErrEndpointInUse
		}
		return nil, fmt.Errorf("ipc: CreateNamedPipe %q: %w", ep, err)
	}

	closeEv, err := windows.CreateEvent(nil, 1, 0, nil)
	if err != nil {
		_ = windows.CloseHandle(firstPipe)
		return nil, fmt.Errorf("ipc: CreateEvent: %w", err)
	}

	return &winPipeListener{
		name:       ep,
		nameUTF16:  nameUTF16,
		selfSID:    selfSID,
		sd:         sd,
		sa:         sa,
		verifier:   opts.peerVerifier,
		closeEvent: closeEv,
		nextPipe:   firstPipe,
	}, nil
}

func (l *winPipeListener) createPipeInstance() (windows.Handle, error) {
	h, err := windows.CreateNamedPipe(
		l.nameUTF16,
		pipeOpenModeBase,
		pipeMode,
		windows.PIPE_UNLIMITED_INSTANCES,
		pipeBufferSize,
		pipeBufferSize,
		0,
		l.sa,
	)
	runtime.KeepAlive(l.sd)
	if err != nil {
		return 0, err
	}
	return h, nil
}

func (l *winPipeListener) takeOrCreatePipe() (windows.Handle, error) {
	l.mu.Lock()
	if l.closed.Load() {
		l.mu.Unlock()
		return 0, ErrListenerClosed
	}
	h := l.nextPipe
	l.nextPipe = 0
	l.mu.Unlock()

	if h != 0 {
		return h, nil
	}
	return l.createPipeInstance()
}

func (l *winPipeListener) storeNextPipe(h windows.Handle) {
	l.mu.Lock()
	if l.closed.Load() || l.nextPipe != 0 {
		l.mu.Unlock()
		_ = windows.CloseHandle(h)
		return
	}
	l.nextPipe = h
	l.mu.Unlock()
}

func (l *winPipeListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	if l.closed.Load() {
		l.mu.Unlock()
		return nil, ErrListenerClosed
	}
	l.acceptWg.Add(1)
	l.mu.Unlock()
	defer l.acceptWg.Done()

	for {
		if l.closed.Load() {
			return nil, ErrListenerClosed
		}
		h, err := l.takeOrCreatePipe()
		if err != nil {
			if l.closed.Load() {
				return nil, ErrListenerClosed
			}
			return nil, err
		}

		if err := l.waitConnect(h); err != nil {
			// Ensure a replacement pipe instance exists before closing h so the pipe never drops to 0 instances.
			if !l.closed.Load() {
				if nextH, nerr := l.createPipeInstance(); nerr == nil {
					l.storeNextPipe(nextH)
				}
			}
			_ = windows.CloseHandle(h)
			if l.closed.Load() {
				return nil, ErrListenerClosed
			}
			if isTransientPipeConnectError(err) {
				continue
			}
			return nil, err
		}

		// Pre-create next pipe instance so concurrent clients don't hit ERROR_FILE_NOT_FOUND.
		if !l.closed.Load() {
			if nextH, nerr := l.createPipeInstance(); nerr == nil {
				l.storeNextPipe(nextH)
			}
		}

		clientSID, serr := namedPipeClientSID(h)
		if serr != nil || verifyPeer(clientSID, l.selfSID, l.verifier) != nil {
			l.rejected.Add(1)
			_ = windows.DisconnectNamedPipe(h)
			_ = windows.CloseHandle(h)
			continue
		}

		conn, cerr := newPipeConn(h, l.name)
		if cerr != nil {
			_ = windows.DisconnectNamedPipe(h)
			_ = windows.CloseHandle(h)
			return nil, cerr
		}
		return conn, nil
	}
}

func (l *winPipeListener) waitConnect(h windows.Handle) error {
	ev, err := windows.CreateEvent(nil, 1, 0, nil)
	if err != nil {
		return err
	}
	defer windows.CloseHandle(ev)

	ov := new(windows.Overlapped)
	ov.HEvent = ev

	var pinner runtime.Pinner
	pinner.Pin(ov)
	defer pinner.Unpin()

	err = windows.ConnectNamedPipe(h, ov)
	if err == nil || errors.Is(err, windows.ERROR_PIPE_CONNECTED) {
		return nil
	}
	if !errors.Is(err, windows.ERROR_IO_PENDING) {
		return err
	}

	handles := []windows.Handle{ev, l.closeEvent}
	event, werr := windows.WaitForMultipleObjects(handles, false, windows.INFINITE)
	if werr != nil {
		_ = windows.CancelIoEx(h, ov)
		var dummy uint32
		_ = windows.GetOverlappedResult(h, ov, &dummy, true)
		return werr
	}
	if event == windows.WAIT_OBJECT_0+1 {
		_ = windows.CancelIoEx(h, ov)
		var dummy uint32
		_ = windows.GetOverlappedResult(h, ov, &dummy, true)
		return ErrListenerClosed
	}

	var dummy uint32
	if err := windows.GetOverlappedResult(h, ov, &dummy, true); err != nil {
		return err
	}
	return nil
}

func (l *winPipeListener) Close() error {
	if l.closed.Swap(true) {
		return nil
	}
	_ = windows.SetEvent(l.closeEvent)

	l.mu.Lock()
	leftover := l.nextPipe
	l.nextPipe = 0
	l.mu.Unlock()
	if leftover != 0 {
		_ = windows.CloseHandle(leftover)
	}

	l.acceptWg.Wait()
	_ = windows.CloseHandle(l.closeEvent)
	return nil
}

func (l *winPipeListener) Addr() string {
	return l.name
}

func (l *winPipeListener) RejectedCount() uint64 {
	return l.rejected.Load()
}

func isTransientPipeConnectError(err error) bool {
	return errors.Is(err, windows.ERROR_NO_DATA) ||
		errors.Is(err, windows.ERROR_PIPE_NOT_CONNECTED) ||
		errors.Is(err, windows.ERROR_BROKEN_PIPE) ||
		errors.Is(err, windows.ERROR_OPERATION_ABORTED)
}

func dialTransportWithOptions(ctx context.Context, opts DialOptions) (net.Conn, error) {
	ep := opts.Endpoint
	if ep == "" {
		var err error
		ep, err = defaultEndpoint()
		if err != nil {
			return nil, err
		}
	}
	selfSID, err := currentProcessUserSID()
	if err != nil {
		return nil, err
	}
	nameUTF16, err := windows.UTF16PtrFromString(ep)
	if err != nil {
		return nil, fmt.Errorf("ipc: invalid pipe name %q: %w", ep, err)
	}

	flags := uint32(windows.FILE_FLAG_OVERLAPPED | securitySQOSPresent | securityIdentification)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		h, err := windows.CreateFile(
			nameUTF16,
			windows.GENERIC_READ|windows.GENERIC_WRITE,
			0,
			nil,
			windows.OPEN_EXISTING,
			flags,
			0,
		)
		if err == nil {
			serverSID, serr := namedPipeServerSID(h)
			if serr != nil {
				_ = windows.CloseHandle(h)
				return nil, fmt.Errorf("ipc: verify server identity: %w", serr)
			}
			if verr := verifyPeer(serverSID, selfSID, opts.peerVerifier); verr != nil {
				_ = windows.CloseHandle(h)
				return nil, verr
			}
			conn, cerr := newPipeConn(h, ep)
			if cerr != nil {
				_ = windows.CloseHandle(h)
				return nil, cerr
			}
			return conn, nil
		}

		if errors.Is(err, windows.ERROR_PIPE_BUSY) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(5 * time.Millisecond):
				continue
			}
		}
		return nil, err
	}
}

// pipeConn implements net.Conn over a Windows Overlapped Named Pipe handle with safe IRP cancellation.
type pipeConn struct {
	handle     windows.Handle
	addr       pipeAddr
	closeEvent windows.Handle

	stateMu   sync.Mutex
	closed    bool
	activeIO  int
	allIODone chan struct{}

	readMu        sync.Mutex
	writeMu       sync.Mutex
	readDeadline  atomic.Int64
	writeDeadline atomic.Int64
}

func newPipeConn(h windows.Handle, name string) (*pipeConn, error) {
	ev, err := windows.CreateEvent(nil, 1, 0, nil)
	if err != nil {
		return nil, err
	}
	return &pipeConn{
		handle:     h,
		addr:       pipeAddr(name),
		closeEvent: ev,
		allIODone:  make(chan struct{}),
	}, nil
}

func (c *pipeConn) beginIO() bool {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.closed {
		return false
	}
	c.activeIO++
	return true
}

func (c *pipeConn) endIO() {
	c.stateMu.Lock()
	c.activeIO--
	if c.closed && c.activeIO == 0 {
		close(c.allIODone)
	}
	c.stateMu.Unlock()
}

func (c *pipeConn) waitTimeoutMs(deadlineNano int64) (uint32, bool) {
	if deadlineNano == 0 {
		return windows.INFINITE, false
	}
	rem := time.Until(time.Unix(0, deadlineNano))
	if rem <= 0 {
		return 0, true
	}
	ms := rem.Milliseconds()
	if ms <= 0 {
		ms = 1
	}
	if ms > 0x7FFFFFFF {
		return 0x7FFFFFFF, false
	}
	return uint32(ms), false
}

func (c *pipeConn) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	c.readMu.Lock()
	defer c.readMu.Unlock()

	if !c.beginIO() {
		return 0, net.ErrClosed
	}
	defer c.endIO()

	timeoutMs, expired := c.waitTimeoutMs(c.readDeadline.Load())
	if expired {
		return 0, os.ErrDeadlineExceeded
	}

	ev, err := windows.CreateEvent(nil, 1, 0, nil)
	if err != nil {
		return 0, err
	}
	defer windows.CloseHandle(ev)

	ov := new(windows.Overlapped)
	ov.HEvent = ev

	// Copy into a pinned heap buffer so stack growth can never invalidate an in-flight IRP.
	buf := make([]byte, len(b))
	var pinner runtime.Pinner
	pinner.Pin(ov)
	pinner.Pin(&buf[0])
	defer pinner.Unpin()

	var done uint32
	err = windows.ReadFile(c.handle, buf, &done, ov)
	if err == nil {
		copy(b, buf[:done])
		return int(done), nil
	}
	if !errors.Is(err, windows.ERROR_IO_PENDING) {
		if errors.Is(err, windows.ERROR_BROKEN_PIPE) || errors.Is(err, windows.ERROR_PIPE_NOT_CONNECTED) {
			return 0, io.EOF
		}
		return 0, err
	}

	handles := []windows.Handle{ev, c.closeEvent}
	res, werr := windows.WaitForMultipleObjects(handles, false, timeoutMs)
	if werr != nil {
		_ = windows.CancelIoEx(c.handle, ov)
		_ = windows.GetOverlappedResult(c.handle, ov, &done, true)
		return 0, werr
	}

	switch res {
	case windows.WAIT_OBJECT_0:
		gerr := windows.GetOverlappedResult(c.handle, ov, &done, true)
		if done > 0 {
			copy(b, buf[:done])
		}
		if gerr != nil {
			if errors.Is(gerr, windows.ERROR_BROKEN_PIPE) || errors.Is(gerr, windows.ERROR_PIPE_NOT_CONNECTED) {
				if done > 0 {
					return int(done), nil
				}
				return 0, io.EOF
			}
			return int(done), gerr
		}
		return int(done), nil

	case windows.WAIT_OBJECT_0 + 1:
		_ = windows.CancelIoEx(c.handle, ov)
		_ = windows.GetOverlappedResult(c.handle, ov, &done, true)
		return 0, net.ErrClosed

	case uint32(windows.WAIT_TIMEOUT):
		_ = windows.CancelIoEx(c.handle, ov)
		gerr := windows.GetOverlappedResult(c.handle, ov, &done, true)
		if gerr == nil && done > 0 {
			copy(b, buf[:done])
			return int(done), nil
		}
		return 0, os.ErrDeadlineExceeded

	default:
		_ = windows.CancelIoEx(c.handle, ov)
		_ = windows.GetOverlappedResult(c.handle, ov, &done, true)
		return 0, fmt.Errorf("ipc: WaitForMultipleObjects returned %d", res)
	}
}

func (c *pipeConn) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if !c.beginIO() {
		return 0, net.ErrClosed
	}
	defer c.endIO()

	timeoutMs, expired := c.waitTimeoutMs(c.writeDeadline.Load())
	if expired {
		return 0, os.ErrDeadlineExceeded
	}

	ev, err := windows.CreateEvent(nil, 1, 0, nil)
	if err != nil {
		return 0, err
	}
	defer windows.CloseHandle(ev)

	ov := new(windows.Overlapped)
	ov.HEvent = ev

	buf := make([]byte, len(b))
	copy(buf, b)

	var pinner runtime.Pinner
	pinner.Pin(ov)
	pinner.Pin(&buf[0])
	defer pinner.Unpin()

	var done uint32
	err = windows.WriteFile(c.handle, buf, &done, ov)
	if err == nil {
		return int(done), nil
	}
	if !errors.Is(err, windows.ERROR_IO_PENDING) {
		return 0, err
	}

	handles := []windows.Handle{ev, c.closeEvent}
	res, werr := windows.WaitForMultipleObjects(handles, false, timeoutMs)
	if werr != nil {
		_ = windows.CancelIoEx(c.handle, ov)
		_ = windows.GetOverlappedResult(c.handle, ov, &done, true)
		return 0, werr
	}

	switch res {
	case windows.WAIT_OBJECT_0:
		gerr := windows.GetOverlappedResult(c.handle, ov, &done, true)
		return int(done), gerr

	case windows.WAIT_OBJECT_0 + 1:
		_ = windows.CancelIoEx(c.handle, ov)
		_ = windows.GetOverlappedResult(c.handle, ov, &done, true)
		return 0, net.ErrClosed

	case uint32(windows.WAIT_TIMEOUT):
		_ = windows.CancelIoEx(c.handle, ov)
		gerr := windows.GetOverlappedResult(c.handle, ov, &done, true)
		if gerr == nil && done == uint32(len(b)) {
			return int(done), nil
		}
		return int(done), os.ErrDeadlineExceeded

	default:
		_ = windows.CancelIoEx(c.handle, ov)
		_ = windows.GetOverlappedResult(c.handle, ov, &done, true)
		return 0, fmt.Errorf("ipc: WaitForMultipleObjects returned %d", res)
	}
}

func (c *pipeConn) Close() error {
	c.stateMu.Lock()
	if c.closed {
		c.stateMu.Unlock()
		return nil
	}
	c.closed = true
	hasActive := c.activeIO > 0
	if !hasActive {
		close(c.allIODone)
	}
	c.stateMu.Unlock()

	_ = windows.SetEvent(c.closeEvent)
	_ = windows.CancelIoEx(c.handle, nil)

	if hasActive {
		<-c.allIODone
	}

	err := windows.CloseHandle(c.handle)
	_ = windows.CloseHandle(c.closeEvent)
	return err
}

func (c *pipeConn) LocalAddr() net.Addr  { return c.addr }
func (c *pipeConn) RemoteAddr() net.Addr { return c.addr }

func (c *pipeConn) SetDeadline(t time.Time) error {
	_ = c.SetReadDeadline(t)
	_ = c.SetWriteDeadline(t)
	return nil
}

func (c *pipeConn) SetReadDeadline(t time.Time) error {
	if t.IsZero() {
		c.readDeadline.Store(0)
	} else {
		c.readDeadline.Store(t.UnixNano())
	}
	return nil
}

func (c *pipeConn) SetWriteDeadline(t time.Time) error {
	if t.IsZero() {
		c.writeDeadline.Store(0)
	} else {
		c.writeDeadline.Store(t.UnixNano())
	}
	return nil
}
