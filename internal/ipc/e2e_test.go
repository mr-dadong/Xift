package ipc

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"fmt"
	mrand "math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/mr-dadong/Xift/internal/query"
	"github.com/mr-dadong/Xift/internal/storage"
)

func testUniqueEndpoint(t *testing.T) string {
	t.Helper()
	var b [8]byte
	if _, err := crand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS == "windows" {
		return `\\.\pipe\xift-e2e-` + hex.EncodeToString(b[:])
	}
	dir := filepath.Join(t.TempDir(), "run")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(dir, "daemon.sock")
}

func TestE2E1MNodesViewportSLAAndSnapshotStability(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 1M-node E2E SLA test in -short mode")
	}

	const totalFiles = 1_000_000
	dir := t.TempDir()
	store, err := storage.Open(dir, storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	defer store.Close()

	// Build 1,000,000-node synthetic Tier 1 dense segment.
	b, err := store.BeginDenseSegment()
	if err != nil {
		t.Fatalf("BeginDenseSegment: %v", err)
	}
	rootID, err := b.AppendDense(1, 1, storage.RootParentIdx, "workspace", storage.FlagDir, 0, 0, 1_700_000_000)
	if err != nil {
		t.Fatal(err)
	}
	// Create 100 directories so paths are realistic.
	dirIDs := make([]uint32, 100)
	for d := 0; d < 100; d++ {
		id, err := b.AppendDense(1, uint64(d+2), rootID, fmt.Sprintf("pkg_%03d", d), storage.FlagDir, 0, 0, 1_700_000_000)
		if err != nil {
			t.Fatal(err)
		}
		dirIDs[d] = id
	}
	// 1,000,000 files; 50,000 of them contain "report_" for rich paging.
	for i := 0; i < totalFiles; i++ {
		var name string
		if i%20 == 0 {
			name = fmt.Sprintf("report_%06d.go", i)
		} else {
			name = fmt.Sprintf("asset_%06d.bin", i)
		}
		if _, err := b.AppendDense(
			1,
			uint64(1000+i),
			dirIDs[i%100],
			name,
			0,
			0,
			int64((i%4096)+1),
			int64(1_700_000_000+i),
		); err != nil {
			t.Fatalf("AppendDense %d: %v", i, err)
		}
	}
	metaSeg, err := b.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if err := store.CommitSegment(metaSeg, nil); err != nil {
		t.Fatalf("CommitSegment: %v", err)
	}

	qe := query.NewEngine(store)
	backend := NewQueryBackend(store, qe)
	ep := testUniqueEndpoint(t)
	ln, err := Listen(ep)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	srv := NewServer(backend, ServerOptions{})
	go func() { _ = srv.Serve(ln) }()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cli, err := Dial(ctx, ep)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer cli.Close()

	// Warm up both IPC and in-process paths once.
	meta, err := cli.QueryStart(ctx, "report_", uint8(query.SortByRelevance), false, '/')
	if err != nil {
		t.Fatalf("QueryStart warmup: %v", err)
	}
	if meta.Total != 50_000 || meta.Retained != 50_000 {
		t.Fatalf("got total=%d retained=%d, want 50000/50000", meta.Total, meta.Retained)
	}

	// 1. 200 sequential 60-row FetchViewport calls at random offsets -> median < 2 ms, cold reads <= 60 each.
	rng := mrand.New(mrand.NewPCG(42, 99))
	vpLatencies := make([]time.Duration, 200)
	for i := 0; i < 200; i++ {
		off := uint32(rng.IntN(49_900))
		coldBefore := backend.TotalColdReads()
		t0 := time.Now()
		vp, err := cli.FetchViewport(ctx, meta.SessionID, off, 60)
		vpLatencies[i] = time.Since(t0)
		if err != nil {
			t.Fatalf("FetchViewport[%d] off=%d: %v", i, off, err)
		}
		if len(vp.Rows) != 60 {
			t.Fatalf("FetchViewport[%d] len=%d, want 60", i, len(vp.Rows))
		}
		coldDelta := backend.TotalColdReads() - coldBefore
		if coldDelta > 60 {
			t.Fatalf("FetchViewport[%d] coldReads delta=%d, want <= 60", i, coldDelta)
		}
	}
	slices.Sort(vpLatencies)
	medianVP := vpLatencies[len(vpLatencies)/2]
	p95VP := vpLatencies[len(vpLatencies)*95/100]
	t.Logf("E2E 1M-node FetchViewport(60 rows, 200 random offsets): median=%v, p95=%v (race=%v)", medianVP, p95VP, raceEnabled)
	if !raceEnabled && medianVP >= 2*time.Millisecond {
		t.Fatalf("FetchViewport median latency %v >= 2ms SLA", medianVP)
	}

	// 2. QueryStart extra overhead vs in-process query -> median < 1 ms.
	// Use a narrower query so we measure pure IPC + RPC framing overhead cleanly.
	const overheadRounds = 25
	inprocTimes := make([]time.Duration, overheadRounds)
	ipcTimes := make([]time.Duration, overheadRounds)
	localSess := qe.NewSession()
	for i := 0; i < overheadRounds; i++ {
		qText := fmt.Sprintf("report_%03d", i%10)
		localSess.Reset()
		t0 := time.Now()
		rs, err := localSess.Start(ctx, qText, query.SearchOptions{Sort: query.SortByRelevance, PathSep: '/'})
		inprocTimes[i] = time.Since(t0)
		if err != nil {
			t.Fatalf("in-proc Start: %v", err)
		}
		rs.Close()

		t1 := time.Now()
		_, err = cli.QueryStart(ctx, qText, uint8(query.SortByRelevance), false, '/')
		ipcTimes[i] = time.Since(t1)
		if err != nil {
			t.Fatalf("IPC QueryStart: %v", err)
		}
	}
	slices.Sort(inprocTimes)
	slices.Sort(ipcTimes)
	medInproc := inprocTimes[overheadRounds/2]
	medIPC := ipcTimes[overheadRounds/2]
	overhead := medIPC - medInproc
	t.Logf("E2E 1M-node QueryStart: inproc_median=%v, ipc_median=%v, overhead=%v (race=%v)", medInproc, medIPC, overhead, raceEnabled)
	if !raceEnabled && overhead >= 1*time.Millisecond {
		t.Fatalf("QueryStart IPC overhead %v >= 1ms SLA (inproc=%v, ipc=%v)", overhead, medInproc, medIPC)
	}

	// 3. Snapshot stability across live index deletions.
	stableMeta, err := cli.QueryStart(ctx, "report_000", uint8(query.SortByName), false, '/')
	if err != nil {
		t.Fatalf("QueryStart stable: %v", err)
	}
	beforeVP, err := cli.FetchViewport(ctx, stableMeta.SessionID, 0, 50)
	if err != nil {
		t.Fatalf("FetchViewport before delete: %v", err)
	}
	if len(beforeVP.Rows) == 0 {
		t.Fatalf("expected non-empty rows for report_000")
	}

	// Delete the first 100 matching report_000* nodes from storage.
	var delOps []storage.Mutation
	for i := 0; i < 1000; i += 20 {
		delOps = append(delOps, storage.Mutation{
			Op:      storage.OpDelete,
			VolID:   1,
			NodeKey: uint64(1000 + i),
		})
	}
	if err := store.ApplyBatch(delOps); err != nil {
		t.Fatalf("ApplyBatch delete: %v", err)
	}

	afterVP, err := cli.FetchViewport(ctx, stableMeta.SessionID, 0, 50)
	if err != nil {
		t.Fatalf("FetchViewport after delete: %v", err)
	}
	if len(afterVP.Rows) != len(beforeVP.Rows) {
		t.Fatalf("row count changed after delete: got %d, want %d", len(afterVP.Rows), len(beforeVP.Rows))
	}
	for i := range beforeVP.Rows {
		if afterVP.Rows[i].NodeID != beforeVP.Rows[i].NodeID || afterVP.Rows[i].Path != beforeVP.Rows[i].Path {
			t.Fatalf("row %d changed after delete: got %+v, want %+v", i, afterVP.Rows[i], beforeVP.Rows[i])
		}
	}
}
