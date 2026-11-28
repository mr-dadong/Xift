package ipc

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/mr-dadong/Xift/internal/l2search"
	"github.com/mr-dadong/Xift/internal/query"
	"github.com/mr-dadong/Xift/internal/storage"
)

type fakeL2Admin struct {
	roots []string
	stats l2search.Stats
}

func (f *fakeL2Admin) AddRoot(dir string) ([]string, error) {
	for _, r := range f.roots {
		if r == dir {
			return f.roots, nil
		}
	}
	f.roots = append(f.roots, dir)
	return f.roots, nil
}

func (f *fakeL2Admin) RemoveRoot(dir string) ([]string, error) {
	out := f.roots[:0]
	for _, r := range f.roots {
		if r != dir {
			out = append(out, r)
		}
	}
	f.roots = out
	return f.roots, nil
}

func (f *fakeL2Admin) Status() l2search.Stats { return f.stats }

// TestAdminRPCs covers the administrative JSON channel used by `xift l2` and
// `xift snapshot`, including unknown ops and malformed payloads.
func TestAdminRPCs(t *testing.T) {
	dataDir := t.TempDir()
	store, err := storage.Open(dataDir, storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	b, err := store.BeginDenseSegment()
	if err != nil {
		t.Fatal(err)
	}
	root, err := b.AppendDense(1, 1, storage.RootParentIdx, "proj", storage.FlagDir, 0, 0, 1_700_000_000)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.AppendDense(1, 2, root, "main.go", 0, 0, 128, 1_700_000_000); err != nil {
		t.Fatal(err)
	}
	meta, err := b.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CommitSegment(meta, nil); err != nil {
		t.Fatal(err)
	}

	l2 := &fakeL2Admin{stats: l2search.Stats{
		Roots:    []string{"D:/code"},
		Indexed:  7,
		Batches:  2,
		Paused:   true,
		DiskUsed: 4096,
		DiskCap:  1 << 20,
		Skipped:  map[l2search.SkipReason]int{l2search.SkipBinary: 3},
	}}
	backend := NewQueryBackend(store, query.NewEngine(store), WithL2Service(l2))
	ep := testUniqueEndpoint(t)
	ln, err := Listen(ep)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	srv := NewServer(backend, ServerOptions{})
	go func() { _ = srv.Serve(ln) }()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cli, err := Dial(ctx, ep)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer cli.Close()

	out, err := cli.Admin(ctx, "l2.add", []byte(`{"dir":"D:/code"}`))
	if err != nil {
		t.Fatalf("l2.add: %v", err)
	}
	if !strings.Contains(string(out), "D:/code") {
		t.Fatalf("l2.add response = %s", out)
	}

	out, err = cli.Admin(ctx, "l2.status", nil)
	if err != nil {
		t.Fatalf("l2.status: %v", err)
	}
	var st L2Status
	if err := json.Unmarshal(out, &st); err != nil {
		t.Fatalf("l2.status json: %v (%s)", err, out)
	}
	if st.Indexed != 7 || !st.Paused || st.Skipped["binary"] != 3 || st.DiskCap != 1<<20 {
		t.Fatalf("l2.status = %+v", st)
	}

	pkgDir := t.TempDir()
	out, err = cli.Admin(ctx, "snapshot.export", []byte(`{"volume_id":1,"out_dir":"`+strings.ReplaceAll(pkgDir, `\`, `\\`)+`","label":"proj"}`))
	if err != nil {
		t.Fatalf("snapshot.export: %v", err)
	}
	var pkg storage.SnapshotPackage
	if err := json.Unmarshal(out, &pkg); err != nil {
		t.Fatalf("export json: %v (%s)", err, out)
	}
	if pkg.Nodes != 2 {
		t.Fatalf("export package nodes = %d", pkg.Nodes)
	}

	out, err = cli.Admin(ctx, "snapshot.import", []byte(`{"package":"`+strings.ReplaceAll(pkgDir, `\`, `\\`)+`"}`))
	if err != nil {
		t.Fatalf("snapshot.import: %v", err)
	}
	var info storage.SnapshotInfo
	if err := json.Unmarshal(out, &info); err != nil {
		t.Fatalf("import json: %v (%s)", err, out)
	}
	if info.ID != 1 || info.Nodes != 2 {
		t.Fatalf("info = %+v", info)
	}

	out, err = cli.Admin(ctx, "snapshot.list", nil)
	if err != nil {
		t.Fatalf("snapshot.list: %v", err)
	}
	var list struct {
		Snapshots []storage.SnapshotInfo `json:"snapshots"`
	}
	if err := json.Unmarshal(out, &list); err != nil {
		t.Fatal(err)
	}
	if len(list.Snapshots) != 1 {
		t.Fatalf("snapshots = %+v", list.Snapshots)
	}

	if _, err := cli.Admin(ctx, "snapshot.remove", []byte(`{"id":1}`)); err != nil {
		t.Fatalf("snapshot.remove: %v", err)
	}
	if got := len(store.Snapshots()); got != 0 {
		t.Fatalf("snapshots after remove = %d", got)
	}

	if _, err := cli.Admin(ctx, "nope", nil); !IsRPCError(err, ErrBadRequest) {
		t.Fatalf("unknown op err = %v, want BadRequest", err)
	}
	if _, err := cli.Admin(ctx, "l2.add", []byte(`{`)); !IsRPCError(err, ErrBadRequest) {
		t.Fatalf("malformed payload err = %v, want BadRequest", err)
	}
}
