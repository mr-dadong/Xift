package desktop

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mr-dadong/Xift/internal/ipc"
)

type fakeSearchClient struct {
	mu           sync.Mutex
	queryStartFn func(ctx context.Context, text string, sort uint8, desc bool, pathSep byte) (*ipc.QueryResultMeta, error)
	fetchFn      func(ctx context.Context, sessionID uint64, offset uint32, limit uint16) (*ipc.ViewportRows, error)
	cancelCount  int
	closeCount   int
	lastLimit    uint16
}

func (f *fakeSearchClient) QueryStart(ctx context.Context, text string, sort uint8, desc bool, pathSep byte) (*ipc.QueryResultMeta, error) {
	if f.queryStartFn != nil {
		return f.queryStartFn(ctx, text, sort, desc, pathSep)
	}
	return &ipc.QueryResultMeta{SessionID: 1}, nil
}

func (f *fakeSearchClient) FetchViewport(ctx context.Context, sessionID uint64, offset uint32, limit uint16) (*ipc.ViewportRows, error) {
	f.mu.Lock()
	f.lastLimit = limit
	f.mu.Unlock()
	if f.fetchFn != nil {
		return f.fetchFn(ctx, sessionID, offset, limit)
	}
	return &ipc.ViewportRows{SessionID: sessionID, Offset: offset}, nil
}

func (f *fakeSearchClient) Cancel(sessionID uint64) error {
	f.mu.Lock()
	f.cancelCount++
	f.mu.Unlock()
	return nil
}

func (f *fakeSearchClient) Close() error {
	f.mu.Lock()
	f.closeCount++
	f.mu.Unlock()
	return nil
}

func waitForCondition(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v", timeout)
}

func TestSearchDropsStaleQueryResponse(t *testing.T) {
	releaseFirst := make(chan struct{})
	firstStarted := make(chan struct{})

	client := &fakeSearchClient{
		queryStartFn: func(ctx context.Context, text string, sort uint8, desc bool, pathSep byte) (*ipc.QueryResultMeta, error) {
			if text == "old" {
				close(firstStarted)
				<-releaseFirst
				return &ipc.QueryResultMeta{SessionID: 1, Total: 50, Retained: 50}, nil
			}
			return &ipc.QueryResultMeta{SessionID: 2, Total: 2, Retained: 2}, nil
		},
		fetchFn: func(ctx context.Context, sessionID uint64, offset uint32, limit uint16) (*ipc.ViewportRows, error) {
			if sessionID == 1 {
				return &ipc.ViewportRows{
					SessionID: 1,
					Offset:    offset,
					Rows:      []ipc.ViewportRow{{NodeID: 10, Name: "old.txt", Path: "/old.txt"}},
				}, nil
			}
			return &ipc.ViewportRows{
				SessionID: 2,
				Offset:    offset,
				Rows: []ipc.ViewportRow{
					{NodeID: 20, Name: "new1.txt", Path: "/new1.txt"},
					{NodeID: 21, Name: "new2.txt", Path: "/new2.txt"},
				},
			}, nil
		},
	}

	ctrl := NewSearchController(SearchConfig{
		Dial: func(ctx context.Context) (SearchClient, error) { return client, nil },
	})
	defer ctrl.Close()

	ctrl.SetQuery("old", 0, false)
	<-firstStarted
	ctrl.SetQuery("new", 0, false)

	waitForCondition(t, 2*time.Second, func() bool {
		snap := ctrl.Snapshot()
		return !snap.Loading && len(snap.Rows) == 2 && snap.Rows[0].Name == "new1.txt"
	})

	// Release stale first query response and ensure it cannot overwrite the current page.
	close(releaseFirst)
	time.Sleep(40 * time.Millisecond)

	snap := ctrl.Snapshot()
	if snap.QueryText != "new" || snap.Retained != 2 || len(snap.Rows) != 2 || snap.Rows[0].Name != "new1.txt" {
		t.Fatalf("stale query overwrote current state: %+v", snap)
	}
}

func TestSearchDropsStalePageResponse(t *testing.T) {
	releasePage1 := make(chan struct{})
	page1Started := make(chan struct{})

	client := &fakeSearchClient{
		queryStartFn: func(ctx context.Context, text string, sort uint8, desc bool, pathSep byte) (*ipc.QueryResultMeta, error) {
			return &ipc.QueryResultMeta{SessionID: 7, Total: 300, Retained: 300}, nil
		},
		fetchFn: func(ctx context.Context, sessionID uint64, offset uint32, limit uint16) (*ipc.ViewportRows, error) {
			if offset == 80 {
				close(page1Started)
				<-releasePage1
				return &ipc.ViewportRows{
					SessionID: sessionID,
					Offset:    80,
					Rows:      []ipc.ViewportRow{{NodeID: 80, Name: "stale-page-80.txt"}},
				}, nil
			}
			return &ipc.ViewportRows{
				SessionID: sessionID,
				Offset:    offset,
				Rows:      []ipc.ViewportRow{{NodeID: offset, Name: "current-page.txt"}},
			}, nil
		},
	}

	ctrl := NewSearchController(SearchConfig{
		Dial:     func(ctx context.Context) (SearchClient, error) { return client, nil },
		PageSize: 80,
	})
	defer ctrl.Close()

	ctrl.SetQuery("report", 0, false)
	waitForCondition(t, 2*time.Second, func() bool {
		snap := ctrl.Snapshot()
		return !snap.Loading && snap.Retained == 300 && len(snap.Rows) == 1
	})

	ctrl.RequestPage(80, 80)
	<-page1Started
	ctrl.RequestPage(160, 80)

	waitForCondition(t, 2*time.Second, func() bool {
		snap := ctrl.Snapshot()
		return !snap.Loading && snap.PageOffset == 160 && len(snap.Rows) == 1 && snap.Rows[0].NodeID == 160
	})

	// Release slow page 80; must be ignored because page 160 is newer.
	close(releasePage1)
	time.Sleep(40 * time.Millisecond)

	snap := ctrl.Snapshot()
	if snap.PageOffset != 160 || len(snap.Rows) != 1 || snap.Rows[0].NodeID != 160 {
		t.Fatalf("stale page overwrote current viewport: %+v", snap)
	}
}

func TestSearchLastPageClampingTruncatedAndMax100Rows(t *testing.T) {
	var requestedLimits []uint16
	var mu sync.Mutex

	client := &fakeSearchClient{
		queryStartFn: func(ctx context.Context, text string, sort uint8, desc bool, pathSep byte) (*ipc.QueryResultMeta, error) {
			return &ipc.QueryResultMeta{
				SessionID: 9,
				Total:     50000,
				Retained:  125,
				Truncated: true,
			}, nil
		},
		fetchFn: func(ctx context.Context, sessionID uint64, offset uint32, limit uint16) (*ipc.ViewportRows, error) {
			mu.Lock()
			requestedLimits = append(requestedLimits, limit)
			mu.Unlock()
			rows := make([]ipc.ViewportRow, limit)
			for i := range rows {
				rows[i] = ipc.ViewportRow{NodeID: offset + uint32(i), Name: "item"}
			}
			return &ipc.ViewportRows{SessionID: sessionID, Offset: offset, Rows: rows}, nil
		},
	}

	// Asking for PageSize=250 must still clamp per-page fetch to <= 100.
	ctrl := NewSearchController(SearchConfig{
		Dial:     func(ctx context.Context) (SearchClient, error) { return client, nil },
		PageSize: 250,
	})
	defer ctrl.Close()

	ctrl.SetQuery("log", 0, false)
	waitForCondition(t, 2*time.Second, func() bool {
		snap := ctrl.Snapshot()
		return !snap.Loading && len(snap.Rows) == 100
	})

	snap := ctrl.Snapshot()
	if snap.Total != 50000 || snap.Retained != 125 || !snap.Truncated {
		t.Fatalf("expected Total=50000 Retained=125 Truncated=true, got %+v", snap)
	}

	// Request last partial page starting at offset 100 -> only 25 rows remain.
	ctrl.RequestPage(100, 100)
	waitForCondition(t, 2*time.Second, func() bool {
		s := ctrl.Snapshot()
		return !s.Loading && s.PageOffset == 100 && len(s.Rows) == 25
	})

	mu.Lock()
	defer mu.Unlock()
	if len(requestedLimits) < 2 || requestedLimits[0] != 100 || requestedLimits[1] != 25 {
		t.Fatalf("expected clamped limits [100, 25], got %v", requestedLimits)
	}
}

func TestSearchDisconnectClearsOldRowsAndShowsRetryableError(t *testing.T) {
	failNext := false
	var mu sync.Mutex

	client := &fakeSearchClient{
		queryStartFn: func(ctx context.Context, text string, sort uint8, desc bool, pathSep byte) (*ipc.QueryResultMeta, error) {
			mu.Lock()
			fail := failNext
			mu.Unlock()
			if fail {
				return nil, errors.New("broken pipe")
			}
			return &ipc.QueryResultMeta{SessionID: 3, Total: 1, Retained: 1}, nil
		},
		fetchFn: func(ctx context.Context, sessionID uint64, offset uint32, limit uint16) (*ipc.ViewportRows, error) {
			return &ipc.ViewportRows{
				SessionID: sessionID,
				Offset:    0,
				Rows:      []ipc.ViewportRow{{NodeID: 1, Name: "a.txt", Path: "/a.txt"}},
			}, nil
		},
	}

	ctrl := NewSearchController(SearchConfig{
		Dial: func(ctx context.Context) (SearchClient, error) { return client, nil },
	})
	defer ctrl.Close()

	ctrl.SetQuery("a", 0, false)
	waitForCondition(t, 2*time.Second, func() bool {
		s := ctrl.Snapshot()
		return !s.Loading && len(s.Rows) == 1
	})

	mu.Lock()
	failNext = true
	mu.Unlock()

	ctrl.SetQuery("b", 0, false)
	waitForCondition(t, 2*time.Second, func() bool {
		s := ctrl.Snapshot()
		return !s.Loading && s.Disconnected && s.Error != ""
	})

	snap := ctrl.Snapshot()
	if len(snap.Rows) != 0 || snap.Retained != 0 {
		t.Fatalf("disconnected error must not display old rows as new results: %+v", snap)
	}
}
