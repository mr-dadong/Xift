package daemon

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/mr-dadong/Xift/internal/provider"
	"github.com/mr-dadong/Xift/internal/query"
	"github.com/mr-dadong/Xift/internal/storage"
)

// fakeWatchProvider feeds events from a channel and records the cursor each
// volume subscribed from.
type fakeWatchProvider struct {
	provider.FSProvider
	events chan provider.FileEvent

	mu         sync.Mutex
	since      map[uint16]uint64
	fullScans  int
	watchErrs  []error
	scanRecs   []provider.ScanRecord
	scanCursor uint64
	scanGate   chan struct{}
	errVols    map[uint16]error
	attempts   map[uint16]int
	watchDone  chan uint16
}

func newFakeWatchProvider() *fakeWatchProvider {
	return &fakeWatchProvider{
		events:    make(chan provider.FileEvent, 32),
		since:     make(map[uint16]uint64),
		errVols:   make(map[uint16]error),
		attempts:  make(map[uint16]int),
		watchDone: make(chan uint16, 32),
	}
}

func (f *fakeWatchProvider) Watch(ctx context.Context, vol provider.VolumeInfo, sinceCursor uint64, out chan<- provider.FileEvent) error {
	defer func() {
		select {
		case f.watchDone <- vol.VolID:
		default:
		}
	}()
	f.mu.Lock()
	f.since[vol.VolID] = sinceCursor
	f.attempts[vol.VolID]++
	var early error
	if err, ok := f.errVols[vol.VolID]; ok {
		early = err
	}
	if early == nil && len(f.watchErrs) > 0 {
		early = f.watchErrs[0]
		f.watchErrs = f.watchErrs[1:]
	}
	f.mu.Unlock()
	if early != nil {
		return early
	}
	for {
		select {
		case ev := <-f.events:
			if ev.VolID != 0 && ev.VolID != vol.VolID {
				continue
			}
			ev.VolID = vol.VolID
			select {
			case out <- ev:
			case <-ctx.Done():
				return ctx.Err()
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (f *fakeWatchProvider) FullScan(_ context.Context, _ provider.VolumeInfo, writer provider.SegmentWriter) (uint64, error) {
	f.mu.Lock()
	f.fullScans++
	recs := append([]provider.ScanRecord(nil), f.scanRecs...)
	cursor := f.scanCursor
	gate := f.scanGate
	f.mu.Unlock()
	if gate != nil {
		<-gate
	}
	for _, rec := range recs {
		if err := writer.WriteRecord(rec); err != nil {
			return 0, err
		}
	}
	return cursor, nil
}

func (f *fakeWatchProvider) fullScanCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fullScans
}

func (f *fakeWatchProvider) attemptsFor(volID uint16) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attempts[volID]
}

func (f *fakeWatchProvider) sinceFor(volID uint16) (uint64, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.since[volID]
	return c, ok
}

func seedLiveRoot(t *testing.T, store *storage.Engine) {
	t.Helper()
	b, err := store.BeginDenseSegment()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.AppendDense(1, 1, storage.RootParentIdx, "root", storage.FlagDir, 0, 0, 1_700_000_000); err != nil {
		t.Fatal(err)
	}
	meta, err := b.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CommitSegment(meta, nil); err != nil {
		t.Fatal(err)
	}
}

// TestLiveWatchDeliversEvents verifies kernel events reach the L1 snapshot
// through the batcher without any disk polling.
func TestLiveWatchDeliversEvents(t *testing.T) {
	dataDir := t.TempDir()
	store, err := storage.Open(dataDir, storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	defer store.Close()
	seedLiveRoot(t, store)

	prov := newFakeWatchProvider()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w, err := startLiveWatch(ctx, store, WatchConfig{
		Provider: prov,
		Volumes:  []provider.VolumeInfo{{VolID: 1, RootPath: t.TempDir()}},
		Enabled:  true,
		Window:   20 * time.Millisecond,
	}, nil)
	if err != nil {
		t.Fatalf("startLiveWatch: %v", err)
	}
	defer w.Close()

	qe := query.NewEngine(store)
	prov.events <- provider.FileEvent{
		Type: provider.EventCreate, NodeKey: 2, ParentKey: 1, NewName: "live.txt", Size: 16, ModTime: 1_700_000_001, Cursor: 10,
	}
	waitForL2(t, 2*time.Second, func() bool {
		res, qerr := qe.Search(ctx, "live", query.SearchOptions{Limit: 10})
		return qerr == nil && res.Total == 1 && res.Items[0].Name == "live.txt"
	})

	prov.events <- provider.FileEvent{
		Type: provider.EventDelete, NodeKey: 2, Cursor: 11,
	}
	waitForL2(t, 2*time.Second, func() bool {
		res, qerr := qe.Search(ctx, "live", query.SearchOptions{Limit: 10})
		return qerr == nil && res.Total == 0
	})
}
