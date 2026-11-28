package ipc

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

type fakeWatchAdmin struct{ status WatchStatus }

func (f fakeWatchAdmin) WatchStatus() WatchStatus { return f.status }

// TestWatchStatus verifies the watch.status admin op returns the supervisor
// state and stays available with no volumes.
func TestWatchStatus(t *testing.T) {
	backend := NewQueryBackend(nil, nil, WithWatchService(fakeWatchAdmin{status: WatchStatus{
		Enabled: true,
		Volumes: []VolumeWatchStatus{
			{VolID: 1, Root: `C:\`, State: "watching", Cursor: 42},
			{VolID: 2, Root: `D:\`, State: "retrying", LastErr: "denied", Rebuilt: 1},
		},
	}}))
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
	payload, err := cli.Admin(ctx, "watch.status", nil)
	if err != nil {
		t.Fatalf("watch.status: %v", err)
	}
	var got WatchStatus
	if err := json.Unmarshal(payload, &got); err != nil {
		t.Fatalf("decode: %v (%s)", err, payload)
	}
	if !got.Enabled || len(got.Volumes) != 2 || got.Volumes[1].State != "retrying" || got.Volumes[1].Rebuilt != 1 {
		t.Fatalf("watch status = %+v", got)
	}

	// No service attached: explicit disabled response, not an error.
	backend2 := NewQueryBackend(nil, nil)
	ep2 := testUniqueEndpoint(t)
	ln2, err := Listen(ep2)
	if err != nil {
		t.Fatalf("Listen 2: %v", err)
	}
	srv2 := NewServer(backend2, ServerOptions{})
	go func() { _ = srv2.Serve(ln2) }()
	defer func() {
		c, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel2()
		_ = srv2.Shutdown(c)
	}()
	cli2, err := Dial(ctx, ep2)
	if err != nil {
		t.Fatalf("Dial 2: %v", err)
	}
	defer cli2.Close()
	payload, err = cli2.Admin(ctx, "watch.status", nil)
	if err != nil {
		t.Fatalf("watch.status without service: %v", err)
	}
	if err := json.Unmarshal(payload, &got); err != nil {
		t.Fatalf("decode 2: %v", err)
	}
	if got.Enabled || len(got.Volumes) != 0 {
		t.Fatalf("disabled watch status = %+v", got)
	}
}
