package ipc

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/mr-dadong/Xift/internal/filter"
	"github.com/mr-dadong/Xift/internal/query"
	"github.com/mr-dadong/Xift/internal/storage"
)

func testGovEndpoint(t *testing.T) string {
	t.Helper()
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS == "windows" {
		return `\\.\pipe\xift-gov-test-` + hex.EncodeToString(b[:])
	}
	dir := filepath.Join(t.TempDir(), "run")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(dir, "gov.sock")
}

func TestProtocolRecordOpenAndApplyFilterRuleRoundTrip(t *testing.T) {
	msgs := []any{
		&RecordOpenReq{Path: "/work/report.md"},
		&RecordOpenAck{},
		&ApplyFilterRuleReq{Action: FilterActionFold, TargetPath: "/proj/vendor", NodeID: 42, VolID: 1},
		&ApplyFilterRuleAck{AffectedCount: 50},
	}
	for _, m := range msgs {
		mt, pl, err := EncodeMessage(m)
		if err != nil {
			t.Fatalf("EncodeMessage(%T): %v", m, err)
		}
		got, err := DecodeMessage(mt, pl)
		if err != nil {
			t.Fatalf("DecodeMessage(%T): %v", m, err)
		}
		switch want := m.(type) {
		case *RecordOpenReq:
			g := got.(*RecordOpenReq)
			if g.Path != want.Path {
				t.Fatalf("RecordOpenReq mismatch: %+v vs %+v", g, want)
			}
		case *RecordOpenAck:
			if _, ok := got.(*RecordOpenAck); !ok {
				t.Fatalf("expected *RecordOpenAck, got %T", got)
			}
		case *ApplyFilterRuleReq:
			g := got.(*ApplyFilterRuleReq)
			if *g != *want {
				t.Fatalf("ApplyFilterRuleReq mismatch: %+v vs %+v", g, want)
			}
		case *ApplyFilterRuleAck:
			g := got.(*ApplyFilterRuleAck)
			if *g != *want {
				t.Fatalf("ApplyFilterRuleAck mismatch: %+v vs %+v", g, want)
			}
		}
	}
}

func TestIPCRecordOpenAndApplyFilterRuleClosedLoop(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.Open(dir, storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	defer store.Close()

	// Seed index with two equal-depth report.md files + a vendor directory + a .cache directory.
	ops := []storage.Mutation{
		{Op: storage.OpCreate, VolID: 1, NodeKey: 1, ParentKey: 0, Name: "proj", Flags: storage.FlagDir},
		{Op: storage.OpCreate, VolID: 1, NodeKey: 2, ParentKey: 1, Name: "bak", Flags: storage.FlagDir},
		{Op: storage.OpCreate, VolID: 1, NodeKey: 3, ParentKey: 1, Name: "work", Flags: storage.FlagDir},
		{Op: storage.OpCreate, VolID: 1, NodeKey: 4, ParentKey: 2, Name: "report.md"},
		{Op: storage.OpCreate, VolID: 1, NodeKey: 5, ParentKey: 3, Name: "report.md"},
		{Op: storage.OpCreate, VolID: 1, NodeKey: 10, ParentKey: 1, Name: "vendor", Flags: storage.FlagDir},
		{Op: storage.OpCreate, VolID: 1, NodeKey: 11, ParentKey: 10, Name: "dep.go"},
		{Op: storage.OpCreate, VolID: 1, NodeKey: 12, ParentKey: 10, Name: "dep2.go"},
		{Op: storage.OpCreate, VolID: 1, NodeKey: 20, ParentKey: 1, Name: ".cache", Flags: storage.FlagDir},
		{Op: storage.OpCreate, VolID: 1, NodeKey: 21, ParentKey: 20, Name: "blob.tmp"},
	}
	if err := store.ApplyBatch(ops); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}

	fs, err := query.OpenFrecencyStore(filepath.Join(dir, "frecency.db"), query.FrecencyOptions{})
	if err != nil {
		t.Fatalf("OpenFrecencyStore: %v", err)
	}
	rulesPath := filepath.Join(dir, "rules.json")
	pruner := filter.NewPruner(filter.DefaultRuleEngine(), nil)
	qe := query.NewEngine(store, query.WithFrecency(fs))
	backend := NewQueryBackend(store, qe, WithFrecencyStore(fs), WithPruner(pruner, rulesPath))

	srv := NewServer(backend, ServerOptions{})
	ln, err := Listen(testGovEndpoint(t))
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer srv.Shutdown(context.Background())
	go srv.Serve(ln)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli, err := Dial(ctx, ln.Addr())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer cli.Close()

	// 1. Invalid empty RecordOpen returns ErrBadRequest and keeps connection alive.
	if err := cli.RecordOpen(ctx, ""); !IsRPCError(err, ErrBadRequest) {
		t.Fatalf("expected ErrBadRequest for empty path, got %v", err)
	}

	// 2. Valid RecordOpen boosts proj/work/report.md above proj/bak/report.md.
	if err := cli.RecordOpen(ctx, "proj/work/report.md"); err != nil {
		t.Fatalf("RecordOpen: %v", err)
	}
	meta, err := cli.QueryStart(ctx, "report.md", uint8(query.SortByRelevance), false, '/')
	if err != nil {
		t.Fatalf("QueryStart: %v", err)
	}
	vp, err := cli.FetchViewport(ctx, meta.SessionID, 0, 10)
	if err != nil {
		t.Fatalf("FetchViewport: %v", err)
	}
	if len(vp.Rows) != 2 || vp.Rows[0].Path != "proj/work/report.md" {
		t.Fatalf("expected proj/work/report.md first after RecordOpen, got %+v", vp.Rows)
	}

	// 3. ApplyFilterRule(Fold) removes descendants while keeping folded shell.
	affected, err := cli.ApplyFilterRule(ctx, FilterActionFold, "proj/vendor", 0, 1)
	if err != nil {
		t.Fatalf("ApplyFilterRule(Fold): %v", err)
	}
	if affected != 2 {
		t.Fatalf("Fold affected=%d, want 2 descendants", affected)
	}
	meta2, err := cli.QueryStart(ctx, "dep.go", uint8(query.SortByRelevance), false, '/')
	if err != nil {
		t.Fatalf("QueryStart dep.go: %v", err)
	}
	if meta2.Total != 0 {
		t.Fatalf("expected dep.go removed after Fold, got total=%d", meta2.Total)
	}
	metaShell, err := cli.QueryStart(ctx, "vendor", uint8(query.SortByRelevance), false, '/')
	if err != nil {
		t.Fatalf("QueryStart vendor: %v", err)
	}
	vpShell, err := cli.FetchViewport(ctx, metaShell.SessionID, 0, 5)
	if err != nil {
		t.Fatalf("FetchViewport vendor: %v", err)
	}
	if len(vpShell.Rows) != 1 || (vpShell.Rows[0].Flags&storage.FlagFolded) == 0 {
		t.Fatalf("expected vendor shell with FlagFolded retained, got %+v", vpShell.Rows)
	}

	// 4. ApplyFilterRule(HardExclude) removes target dir AND descendants.
	affectedEx, err := cli.ApplyFilterRule(ctx, FilterActionHardExclude, "proj/.cache", 0, 1)
	if err != nil {
		t.Fatalf("ApplyFilterRule(HardExclude): %v", err)
	}
	if affectedEx != 2 {
		t.Fatalf("HardExclude affected=%d, want 2 (dir + child)", affectedEx)
	}
	metaEx, err := cli.QueryStart(ctx, ".cache", uint8(query.SortByRelevance), false, '/')
	if err != nil {
		t.Fatalf("QueryStart .cache: %v", err)
	}
	if metaEx.Total != 0 {
		t.Fatalf("expected .cache completely excluded, got total=%d", metaEx.Total)
	}
}
