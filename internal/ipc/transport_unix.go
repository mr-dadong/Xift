//go:build unix

package ipc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

func defaultEndpoint() (string, error) {
	if runtime.GOOS == "linux" {
		if xdg := os.Getenv("XDG_RUNTIME_DIR"); xdg != "" {
			return filepath.Join(xdg, "xift", "daemon.sock"), nil
		}
	}
	dir := filepath.Join(os.TempDir(), fmt.Sprintf("xift-%d", os.Geteuid()))
	return filepath.Join(dir, "daemon.sock"), nil
}

func ensureSecureRuntimeDir(dir string) error {
	fi, err := os.Lstat(dir)
	created := false
	if os.IsNotExist(err) {
		parent := filepath.Dir(dir)
		if parent != "" && parent != dir {
			if mkErr := os.MkdirAll(parent, 0o755); mkErr != nil {
				return fmt.Errorf("ipc: create parent dir %q: %w", parent, mkErr)
			}
		}
		// Use os.Mkdir (not MkdirAll) for the leaf dir so a pre-planted symlink cannot be followed.
		if mkErr := os.Mkdir(dir, 0o700); mkErr != nil && !os.IsExist(mkErr) {
			return fmt.Errorf("ipc: create runtime dir %q: %w", dir, mkErr)
		}
		created = true
		fi, err = os.Lstat(dir)
	}
	if err != nil {
		return fmt.Errorf("ipc: stat runtime dir %q: %w", dir, err)
	}
	if fi.Mode()&os.ModeSymlink != 0 || !fi.IsDir() {
		return fmt.Errorf("%w: %s is not a real directory", ErrInsecureDir, dir)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok || st.Uid != uint32(os.Geteuid()) {
		return fmt.Errorf("%w: %s owner mismatch", ErrInsecureDir, dir)
	}
	if created && fi.Mode().Perm() != 0o700 {
		// Normalize umask on freshly created directory only after verifying it is not a symlink and owned by us.
		if chErr := os.Chmod(dir, 0o700); chErr != nil {
			return fmt.Errorf("ipc: chmod runtime dir %q: %w", dir, chErr)
		}
		fi, err = os.Lstat(dir)
		if err != nil {
			return fmt.Errorf("ipc: restat runtime dir %q: %w", dir, err)
		}
	}
	if fi.Mode().Perm() != 0o700 {
		return fmt.Errorf("%w: %s has mode %04o, want 0700", ErrInsecureDir, dir, fi.Mode().Perm())
	}
	return nil
}

type unixListener struct {
	ln       *net.UnixListener
	lockFile *os.File
	addr     string
	verifier func(peerID, selfID string) error
	closed   atomic.Bool
	rejected atomic.Uint64
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

	dir := filepath.Dir(ep)
	if err := ensureSecureRuntimeDir(dir); err != nil {
		return nil, err
	}

	// Acquire an exclusive non-blocking flock on a sibling lockfile inside the verified 0700 dir
	// to prevent concurrent Daemon startup races or unlinking a live socket on transient timeouts.
	lockPath := ep + ".lock"
	lockFD, err := unix.Open(lockPath, unix.O_CREAT|unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("ipc: open lockfile %q: %w", lockPath, err)
	}
	lockFile := os.NewFile(uintptr(lockFD), lockPath)
	if err := unix.Flock(int(lockFile.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = lockFile.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, ErrEndpointInUse
		}
		return nil, fmt.Errorf("ipc: flock %q: %w", lockPath, err)
	}

	if fi, err := os.Lstat(ep); err == nil {
		if fi.Mode()&os.ModeSocket == 0 {
			_ = lockFile.Close()
			return nil, fmt.Errorf("%w: non-socket file exists at %s", ErrEndpointInUse, ep)
		}
		// Probe in case a non-Xift listener is bound without our lockfile.
		probe, derr := net.DialTimeout("unix", ep, 200*time.Millisecond)
		if derr == nil {
			_ = probe.Close()
			_ = lockFile.Close()
			return nil, ErrEndpointInUse
		}
		if rerr := os.Remove(ep); rerr != nil && !os.IsNotExist(rerr) {
			_ = lockFile.Close()
			return nil, fmt.Errorf("ipc: remove stale socket %q: %w", ep, rerr)
		}
	}

	raw, err := net.Listen("unix", ep)
	if err != nil {
		_ = lockFile.Close()
		return nil, fmt.Errorf("ipc: listen unix %q: %w", ep, err)
	}
	if err := os.Chmod(ep, 0o600); err != nil {
		_ = raw.Close()
		_ = lockFile.Close()
		return nil, fmt.Errorf("ipc: chmod socket %q: %w", ep, err)
	}

	ul, ok := raw.(*net.UnixListener)
	if !ok {
		_ = raw.Close()
		_ = lockFile.Close()
		return nil, fmt.Errorf("ipc: unexpected listener type %T", raw)
	}
	return &unixListener{
		ln:       ul,
		lockFile: lockFile,
		addr:     ep,
		verifier: opts.peerVerifier,
	}, nil
}

func (l *unixListener) Accept() (net.Conn, error) {
	selfUID := strconv.Itoa(os.Geteuid())
	for {
		uc, err := l.ln.AcceptUnix()
		if err != nil {
			if l.closed.Load() {
				return nil, ErrListenerClosed
			}
			return nil, err
		}
		peerUID, perr := getPeerUID(uc)
		if perr != nil {
			l.rejected.Add(1)
			_ = uc.Close()
			continue
		}
		if verr := verifyPeer(strconv.FormatUint(uint64(peerUID), 10), selfUID, l.verifier); verr != nil {
			l.rejected.Add(1)
			_ = uc.Close()
			continue
		}
		return uc, nil
	}
}

func (l *unixListener) Close() error {
	l.closed.Store(true)
	err := l.ln.Close()
	if l.lockFile != nil {
		_ = unix.Flock(int(l.lockFile.Fd()), unix.LOCK_UN)
		_ = l.lockFile.Close()
	}
	return err
}

func (l *unixListener) Addr() string {
	return l.addr
}

func (l *unixListener) RejectedCount() uint64 {
	return l.rejected.Load()
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

	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", ep)
	if err != nil {
		return nil, err
	}
	uc, ok := conn.(*net.UnixConn)
	if !ok {
		_ = conn.Close()
		return nil, fmt.Errorf("ipc: unexpected unix conn type %T", conn)
	}

	peerUID, err := getPeerUID(uc)
	if err != nil {
		_ = uc.Close()
		return nil, fmt.Errorf("ipc: verify server identity: %w", err)
	}
	selfUID := strconv.Itoa(os.Geteuid())
	if err := verifyPeer(strconv.FormatUint(uint64(peerUID), 10), selfUID, opts.peerVerifier); err != nil {
		_ = uc.Close()
		return nil, err
	}
	return uc, nil
}
