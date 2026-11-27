package desktop

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/mr-dadong/Xift/internal/ipc"
	"github.com/mr-dadong/Xift/internal/query"
	"github.com/mr-dadong/Xift/internal/storage"
)

func testIPCEndpoint(t *testing.T) string {
	t.Helper()
	var b [8]byte
	if _, err := crand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS == "windows" {
		return `\\.\pipe\xift-ui-int-` + hex.EncodeToString(b[:])
	}
	dir := filepath.Join(t.TempDir(), "run")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(dir, "daemon.sock")
}

func startTestIPCServer(t *testing.T) (string, *ipc.Server) {
	t.Helper()
	store, err := storage.Open(t.TempDir(), storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	b, err := store.BeginDenseSegment()
	if err != nil {
		t.Fatalf("BeginDenseSegment: %v", err)
	}
	rootID, err := b.AppendDense(1, 1, storage.RootParentIdx, "docs", storage.FlagDir, 0, 0, 1_700_000_000)
	if err != nil {
		t.Fatal(err)
	}
	names := []string{
		"季度报告_2026.pdf",
		"财务报告_最终版.xlsx",
		"项目报告_摘要.md",
		"design_notes.txt",
		"archive_2025.zip",
	}
	for i, name := range names {
		if _, err := b.AppendDense(1, uint64(10+i), rootID, name, 0, 0, int64(1024*(i+1)), int64(1_700_000_100+i)); err != nil {
			t.Fatal(err)
		}
	}
	metaSeg, err := b.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CommitSegment(metaSeg, nil); err != nil {
		t.Fatal(err)
	}

	qe := query.NewEngine(store)
	backend := ipc.NewQueryBackend(store, qe)
	ep := testIPCEndpoint(t)
	ln, err := ipc.Listen(ep)
	if err != nil {
		t.Fatalf("ipc.Listen: %v", err)
	}
	srv := ipc.NewServer(backend, ipc.ServerOptions{})
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	return ep, srv
}

func TestSearchIntegrationQueryHideDisconnectAndReconnect(t *testing.T) {
	ep, _ := startTestIPCServer(t)
	clk := newFakeClock()

	ctrl := NewSearchController(SearchConfig{
		Dial:     NewIPCDialer(ep, "xift-ui-test"),
		PageSize: 50,
	})
	defer ctrl.Close()

	lc := NewLifecycle(LifecycleConfig{
		IdleDelay: 30 * time.Second,
		Clock:     clk,
		Callbacks: ResourceCallbacks{
			OnShowWindow: func(mode WindowMode, recreated bool) {
				if recreated && ctrl.Snapshot().QueryText != "" {
					ctrl.Reconnect()
				}
			},
			OnReclaimResources: func() {
				ctrl.Disconnect()
			},
		},
	})

	// 1. Show Launcher and run a real Chinese query over local IPC.
	lc.Show(ModeLauncher)
	ctrl.SetQuery("报告", uint8(query.SortByRelevance), false)

	waitForCondition(t, 3*time.Second, func() bool {
		s := ctrl.Snapshot()
		return !s.Loading && !s.Disconnected && s.Retained == 3 && len(s.Rows) == 3
	})

	// 2. Hide for 30s -> triggers reclaim -> cancels session & closes IPC connection,
	//    while preserving the query text for subsequent reconnect.
	lc.Hide()
	clk.Advance(30 * time.Second)

	snapAfterReclaim := ctrl.Snapshot()
	if !snapAfterReclaim.Disconnected {
		t.Fatalf("expected controller disconnected after 30s reclaim, got %+v", snapAfterReclaim)
	}
	if snapAfterReclaim.QueryText != "报告" {
		t.Fatalf("expected retained query text %q, got %q", "报告", snapAfterReclaim.QueryText)
	}
	if len(snapAfterReclaim.Rows) != 0 {
		t.Fatalf("expected rows cleared on disconnect, got %d", len(snapAfterReclaim.Rows))
	}

	// Verify daemon has 0 active sessions via an independent status probe.
	probeClient, err := ipc.DialWithName(context.Background(), ep, "probe")
	if err != nil {
		t.Fatalf("probe Dial: %v", err)
	}
	defer probeClient.Close()
	waitForCondition(t, 2*time.Second, func() bool {
		st, err := probeClient.Status(context.Background())
		return err == nil && st.ActiveSessions == 0
	})

	// 3. Re-show in Explorer -> reconnects IPC and re-runs the retained query automatically.
	lc.Show(ModeExplorer)
	waitForCondition(t, 3*time.Second, func() bool {
		s := ctrl.Snapshot()
		return !s.Loading && !s.Disconnected && s.Retained == 3 && len(s.Rows) == 3
	})

	// Verify real file paths are returned (no mock data).
	finalSnap := ctrl.Snapshot()
	for i, row := range finalSnap.Rows {
		if row.Path == "" || row.Name == "" {
			t.Fatalf("row %d missing real path/name: %+v", i, row)
		}
		if fmt.Sprintf("%s", row.Name) == "" {
			t.Fatalf("row %d empty name", i)
		}
	}
}
