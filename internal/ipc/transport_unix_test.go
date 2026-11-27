//go:build unix

package ipc

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestUnixDefaultEndpoint(t *testing.T) {
	ep, err := DefaultEndpoint()
	if err != nil {
		t.Fatalf("DefaultEndpoint: %v", err)
	}
	if !strings.HasSuffix(ep, string(os.PathSeparator)+"daemon.sock") {
		t.Fatalf("unexpected default endpoint: %q", ep)
	}
}

func TestUnixRejectsLooseRuntimeDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "loose")
	if err := os.Mkdir(dir, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o777); err != nil {
		t.Fatal(err)
	}
	ep := filepath.Join(dir, "daemon.sock")
	if _, err := Listen(ep); !errors.Is(err, ErrInsecureDir) {
		t.Fatalf("Listen on 0777 dir: got %v, want ErrInsecureDir", err)
	}
}

func TestUnixRejectsSymlinkRuntimeDir(t *testing.T) {
	base := t.TempDir()
	realDir := filepath.Join(base, "real")
	if err := os.Mkdir(realDir, 0o700); err != nil {
		t.Fatal(err)
	}
	linkDir := filepath.Join(base, "link")
	if err := os.Symlink(realDir, linkDir); err != nil {
		t.Fatal(err)
	}
	ep := filepath.Join(linkDir, "daemon.sock")
	if _, err := Listen(ep); !errors.Is(err, ErrInsecureDir) {
		t.Fatalf("Listen on symlink dir: got %v, want ErrInsecureDir", err)
	}
}

func TestUnixStaleSocketReplacedAndDuplicateListenRejected(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "run")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	ep := filepath.Join(dir, "daemon.sock")

	// Create a stale socket file by listening and closing without removing (or closing raw listener).
	rawL, err := net.Listen("unix", ep)
	if err != nil {
		t.Fatal(err)
	}
	// Detach unlink on close so the socket file stays behind like a crashed daemon.
	if ul, ok := rawL.(*net.UnixListener); ok {
		ul.SetUnlinkOnClose(false)
	}
	_ = rawL.Close()

	// New Listen must recover the stale socket.
	l1, err := Listen(ep)
	if err != nil {
		t.Fatalf("Listen over stale socket: %v", err)
	}
	defer l1.Close()

	// Second Listen on the same live endpoint must fail with ErrEndpointInUse.
	if l2, err := Listen(ep); !errors.Is(err, ErrEndpointInUse) {
		if l2 != nil {
			_ = l2.Close()
		}
		t.Fatalf("second Listen: got %v, want ErrEndpointInUse", err)
	}
}

func TestUnixSameUserRoundTripAndPeerVerifierRejection(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "run")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	ep := filepath.Join(dir, "daemon.sock")

	// 1. Same-user connection succeeds.
	ln, err := Listen(ep)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()

	errCh := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer c.Close()
		buf := make([]byte, 4)
		if _, err := io.ReadFull(c, buf); err != nil {
			errCh <- err
			return
		}
		_, err = c.Write([]byte("pong"))
		errCh <- err
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cc, err := DialTransport(ctx, ep)
	if err != nil {
		t.Fatalf("DialTransport: %v", err)
	}
	if _, err := cc.Write([]byte("ping")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	reply := make([]byte, 4)
	if _, err := io.ReadFull(cc, reply); err != nil {
		t.Fatalf("ReadFull: %v", err)
	}
	_ = cc.Close()
	if string(reply) != "pong" {
		t.Fatalf("got %q, want pong", string(reply))
	}
	if err := <-errCh; err != nil {
		t.Fatalf("server echo: %v", err)
	}

	// 2. Client rejects impostor server when peerVerifier fails.
	if badConn, err := dialTransportWithOptions(ctx, DialOptions{
		Endpoint: ep,
		peerVerifier: func(peerID, selfID string) error {
			return ErrPeerIdentityMismatch
		},
	}); !errors.Is(err, ErrPeerIdentityMismatch) {
		if badConn != nil {
			_ = badConn.Close()
		}
		t.Fatalf("client impostor check: got %v, want ErrPeerIdentityMismatch", err)
	}

	// 3. Server rejects mismatched client without sending protocol response and increments RejectedCount.
	ep2 := filepath.Join(dir, "daemon2.sock")
	ln2, err := listenWithOptions(ListenOptions{
		Endpoint: ep2,
		peerVerifier: func(peerID, selfID string) error {
			return ErrPeerIdentityMismatch
		},
	})
	if err != nil {
		t.Fatalf("listenWithOptions: %v", err)
	}
	defer ln2.Close()

	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		c, err := ln2.Accept()
		if err == nil && c != nil {
			_ = c.Close()
		}
	}()

	rawConn, err := net.Dial("unix", ep2)
	if err != nil {
		t.Fatalf("raw dial: %v", err)
	}
	_ = rawConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var one [1]byte
	n, rerr := rawConn.Read(one[:])
	_ = rawConn.Close()
	if n != 0 || rerr == nil {
		t.Fatalf("expected immediate EOF/close without data, got n=%d err=%v", n, rerr)
	}
	_ = ln2.Close()
	<-acceptDone

	if ln2.RejectedCount() < 1 {
		t.Fatalf("RejectedCount() = %d, want >= 1", ln2.RejectedCount())
	}
}
