package ipc

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mr-dadong/Xift/internal/query"
	"github.com/mr-dadong/Xift/internal/storage"
)

func TestReviewRegressionBusyBurstAndConcurrentShutdownNoPanic(t *testing.T) {
	ln := newMemListener()
	srv := NewServer(&fakeBackend{}, ServerOptions{MaxConns: 2})
	go func() { _ = srv.Serve(ln) }()

	// Fill the 2 active slots.
	var active []*Client
	for i := 0; i < 2; i++ {
		c, err := ln.dial()
		if err != nil {
			t.Fatal(err)
		}
		cli, err := newClientFromConn(context.Background(), c, "active")
		if err != nil {
			t.Fatal(err)
		}
		active = append(active, cli)
	}
	defer func() {
		for _, cli := range active {
			_ = cli.Close()
		}
	}()

	// Burst 32 extra connections concurrently with Shutdown to verify bounded rejectBusy and zero WaitGroup panic.
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := ln.dial()
			if err != nil {
				return
			}
			_, _ = newClientFromConn(context.Background(), c, "burst")
		}()
	}
	time.Sleep(10 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown under busy burst: %v", err)
	}
	wg.Wait()
}

func TestReviewRegressionClientPreservesRPCErrorOnImmediateClose(t *testing.T) {
	_, ln := startTestServer(t, &fakeBackend{}, ServerOptions{MaxConns: 1})

	// Occupy the single slot.
	c0, err := ln.dial()
	if err != nil {
		t.Fatal(err)
	}
	cli0, err := newClientFromConn(context.Background(), c0, "holder")
	if err != nil {
		t.Fatal(err)
	}
	defer cli0.Close()

	// Repeat 20 times to catch any select race between <-ch and <-closedCh when server closes immediately.
	for i := 0; i < 20; i++ {
		c, err := ln.dial()
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		_, err = newClientFromConn(context.Background(), c, "overflow")
		if !IsRPCError(err, ErrBusy) {
			t.Fatalf("iter %d: got %v, want RPCError{Busy} (must not drop to io.EOF)", i, err)
		}
	}
}

func TestReviewRegressionOversizedViewportRowsReturnsErrorNotHang(t *testing.T) {
	hugePath := strings.Repeat("a", 200*1024) // 200 KiB per row -> 30 rows = 6 MiB > 4 MiB MaxPayloadLen
	b := &fakeBackend{}
	b.startHook = func(ctx context.Context, req QueryRequest) (Result, error) {
		rows := make([]ViewportRow, 30)
		for i := range rows {
			rows[i] = ViewportRow{NodeID: uint32(i + 1), Path: hugePath, Name: "a"}
		}
		return &fakeResult{total: 30, retained: 30, rows: rows}, nil
	}
	_, ln := startTestServer(t, b, ServerOptions{})
	c, err := ln.dial()
	if err != nil {
		t.Fatal(err)
	}
	cli, err := newClientFromConn(context.Background(), c, "huge")
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	meta, err := cli.QueryStart(ctx, "huge", 0, false, '/')
	if err != nil {
		t.Fatalf("QueryStart: %v", err)
	}
	_, err = cli.FetchViewport(ctx, meta.SessionID, 0, 30)
	if !IsRPCError(err, ErrInternal) {
		t.Fatalf("expected RPCError{Internal} for oversized ViewportRows, got %v", err)
	}
}

func TestReviewRegressionFrameVersionMismatchAfterHandshake(t *testing.T) {
	_, ln := startTestServer(t, &fakeBackend{}, ServerOptions{})
	conn, err := ln.dial()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Valid Hello first.
	mt, pl, err := EncodeMessage(&HelloReq{MajorVersion: ProtocolMajorVersion, ClientName: "vtest"})
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteFrame(conn, mt, 1, pl); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadFrame(conn); err != nil {
		t.Fatal(err)
	}

	// Now send a Status frame with header version = 2.
	var hdr [FrameHeaderSize]byte
	hdr[0], hdr[1] = FrameMagic0, FrameMagic1
	hdr[2] = 2 // wrong frame version
	hdr[3] = byte(MsgStatus)
	binary.LittleEndian.PutUint32(hdr[4:8], 2)
	binary.LittleEndian.PutUint32(hdr[8:12], 0)
	if _, err := conn.Write(hdr[:]); err != nil {
		t.Fatal(err)
	}
	resp, err := ReadFrame(conn)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	msg, err := DecodeMessage(resp.Type, resp.Payload)
	if err != nil {
		t.Fatal(err)
	}
	eResp, ok := msg.(*ErrorResp)
	if !ok || eResp.Code != ErrVersionMismatch {
		t.Fatalf("expected VersionMismatch, got %#v", msg)
	}
}

func TestReviewRegressionRapidSupersessionRealBackend(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.Open(dir, storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	b, err := store.BeginDenseSegment()
	if err != nil {
		t.Fatal(err)
	}
	rootID, err := b.AppendDense(1, 1, storage.RootParentIdx, "root", storage.FlagDir, 0, 0, 1_700_000_000)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5000; i++ {
		if _, err := b.AppendDense(1, uint64(i+2), rootID, fmt.Sprintf("alpha_%05d.txt", i), 0, 0, 100, 1_700_000_000); err != nil {
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
	backend := NewQueryBackend(store, qe)
	_, ln := startTestServer(t, backend, ServerOptions{})
	c, err := ln.dial()
	if err != nil {
		t.Fatal(err)
	}
	cli, err := newClientFromConn(context.Background(), c, "rapid")
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	// Fire 10 pipelined rapid queries; the final one ("alpha_04999") must succeed without being canceled by an older goroutine.
	var wg sync.WaitGroup
	for i := 0; i < 9; i++ {
		wg.Add(1)
		go func(q string) {
			defer wg.Done()
			_, err := cli.QueryStart(context.Background(), q, 0, false, '/')
			if err != nil && !IsRPCError(err, ErrCanceled) && !errors.Is(err, context.Canceled) {
				t.Errorf("unexpected error on superseded query %q: %v", q, err)
			}
		}(fmt.Sprintf("alpha_%d", i))
	}
	time.Sleep(5 * time.Millisecond)
	finalMeta, err := cli.QueryStart(context.Background(), "alpha_04999", 0, false, '/')
	if err != nil {
		t.Fatalf("final QueryStart failed: %v", err)
	}
	if finalMeta.Total != 1 {
		t.Fatalf("finalMeta.Total = %d, want 1", finalMeta.Total)
	}
	wg.Wait()
}
