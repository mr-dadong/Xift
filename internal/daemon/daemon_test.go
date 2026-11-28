package daemon

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/mr-dadong/Xift/internal/ipc"
	"github.com/mr-dadong/Xift/internal/query"
	"github.com/mr-dadong/Xift/internal/storage"
)

func seedDaemonTestIndex(t *testing.T, dataDir string, n int) {
	t.Helper()
	store, err := storage.Open(dataDir, storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	defer store.Close()

	b, err := store.BeginDenseSegment()
	if err != nil {
		t.Fatal(err)
	}
	rootID, err := b.AppendDense(1, 1, storage.RootParentIdx, "proj", storage.FlagDir, 0, 0, 1_700_000_000)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if _, err := b.AppendDense(1, uint64(i+2), rootID, fmt.Sprintf("doc_%03d.md", i), 0, 0, int64(100+i), int64(1_700_000_000+i)); err != nil {
			t.Fatal(err)
		}
	}
	meta, err := b.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CommitSegment(meta, nil); err != nil {
		t.Fatal(err)
	}
}

func uniqueDaemonEndpoint(t *testing.T) string {
	t.Helper()
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS == "windows" {
		return `\\.\pipe\xift-daemon-test-` + hex.EncodeToString(b[:])
	}
	dir := filepath.Join(t.TempDir(), "run")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(dir, "daemon.sock")
}

func TestDaemonRunSingleInstanceAndGracefulShutdown(t *testing.T) {
	dataDir := t.TempDir()
	seedDaemonTestIndex(t, dataDir, 50)
	ep := uniqueDaemonEndpoint(t)

	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan string, 1)
	done := make(chan error, 1)

	go func() {
		done <- Run(ctx, Config{
			DataDir:  dataDir,
			Endpoint: ep,
			OnReady: func(addr string) {
				ready <- addr
			},
		})
	}()

	select {
	case <-ready:
	case err := <-done:
		t.Fatalf("daemon exited early: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatalf("daemon did not become ready in time")
	}

	// 1. Second daemon on the same endpoint must fail fast with ErrEndpointInUse.
	err := Run(context.Background(), Config{
		DataDir:  t.TempDir(),
		Endpoint: ep,
	})
	if !errors.Is(err, ipc.ErrEndpointInUse) {
		t.Fatalf("second Daemon Run: got %v, want ipc.ErrEndpointInUse", err)
	}

	// 2. First daemon keeps serving queries and Status.
	dialCtx, dialCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer dialCancel()
	cli, err := ipc.Dial(dialCtx, ep)
	if err != nil {
		t.Fatalf("ipc.Dial: %v", err)
	}
	defer cli.Close()

	st, err := cli.Status(dialCtx)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.LiveCount != 51 { // root + 50 files
		t.Fatalf("LiveCount = %d, want 51", st.LiveCount)
	}

	meta, err := cli.QueryStart(dialCtx, "doc_", uint8(query.SortByName), false, '/')
	if err != nil {
		t.Fatalf("QueryStart: %v", err)
	}
	if meta.Total != 50 {
		t.Fatalf("Total = %d, want 50", meta.Total)
	}
	vp, err := cli.FetchViewport(dialCtx, meta.SessionID, 10, 5)
	if err != nil {
		t.Fatalf("FetchViewport: %v", err)
	}
	if len(vp.Rows) != 5 || vp.Rows[0].Name != "doc_010.md" {
		t.Fatalf("unexpected viewport rows: %+v", vp.Rows)
	}

	// 3. Cancelling ctx gracefully shuts down daemon, closes client connection, and returns nil.
	cancel()
	select {
	case runErr := <-done:
		if runErr != nil {
			t.Fatalf("Daemon Run after cancel returned %v, want nil", runErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Daemon Run did not exit after ctx cancel")
	}

	// Client connection should now fail.
	if _, err := cli.Status(context.Background()); err == nil {
		t.Fatalf("expected Status to fail after daemon shutdown")
	}
}

func TestDaemonCrossProcessQueryAndStatus(t *testing.T) {
	if os.Getenv("XIFT_TEST_DAEMON_HELPER") == "1" {
		dataDir := os.Getenv("XIFT_TEST_DATA_DIR")
		ep := os.Getenv("XIFT_TEST_ENDPOINT")
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = Run(ctx, Config{DataDir: dataDir, Endpoint: ep})
		os.Exit(0)
	}

	dataDir := t.TempDir()
	seedDaemonTestIndex(t, dataDir, 20)
	ep := uniqueDaemonEndpoint(t)

	cmd := exec.Command(os.Args[0], "-test.run=^TestDaemonCrossProcessQueryAndStatus$")
	cmd.Env = append(os.Environ(),
		"XIFT_TEST_DAEMON_HELPER=1",
		"XIFT_TEST_DATA_DIR="+dataDir,
		"XIFT_TEST_ENDPOINT="+ep,
	)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start helper daemon process: %v", err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	// Poll until helper daemon accepts connections.
	var cli *ipc.Client
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		c, err := ipc.Dial(ctx, ep)
		cancel()
		if err == nil {
			cli = c
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	if cli == nil {
		t.Fatalf("failed to connect to cross-process helper daemon at %s", ep)
	}
	defer cli.Close()

	if int(cli.DaemonPID()) != cmd.Process.Pid {
		t.Fatalf("DaemonPID = %d, want helper PID %d", cli.DaemonPID(), cmd.Process.Pid)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	st, err := cli.Status(ctx)
	if err != nil {
		t.Fatalf("cross-process Status: %v", err)
	}
	if st.LiveCount != 21 {
		t.Fatalf("cross-process LiveCount = %d, want 21", st.LiveCount)
	}

	meta, err := cli.QueryStart(ctx, "doc_01", uint8(query.SortByName), false, '/')
	if err != nil {
		t.Fatalf("cross-process QueryStart: %v", err)
	}
	if meta.Total != 10 { // doc_010 .. doc_019
		t.Fatalf("cross-process Total = %d, want 10", meta.Total)
	}
	vp, err := cli.FetchViewport(ctx, meta.SessionID, 0, 10)
	if err != nil {
		t.Fatalf("cross-process FetchViewport: %v", err)
	}
	if len(vp.Rows) != 10 || vp.Rows[0].Name != "doc_010.md" {
		t.Fatalf("unexpected cross-process rows: %+v", vp.Rows)
	}
}

func TestDaemonHonorsBlockCacheConfig(t *testing.T) {
	dataDir := t.TempDir()
	seedDaemonTestIndex(t, dataDir, 10)
	ep := uniqueDaemonEndpoint(t)

	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Config{
			DataDir:         dataDir,
			Endpoint:        ep,
			BlockCacheBytes: 1 << 20,
			OnReady:         func(addr string) { ready <- addr },
		})
	}()

	select {
	case <-ready:
	case err := <-done:
		t.Fatalf("daemon exited early: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("daemon did not become ready in time")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("daemon shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("daemon did not shut down")
	}
}
