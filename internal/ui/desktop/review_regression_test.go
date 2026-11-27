package desktop

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mr-dadong/Xift/internal/ipc"
)

func TestReviewRegressionTraySupervisorNoDeadlockOnConcurrentHiddenAndQuit(t *testing.T) {
	for iter := 0; iter < 20; iter++ {
		clk := newFakeClock()
		var last *fakeWorkerProc
		var mu sync.Mutex

		sup := NewTraySupervisor(TraySupervisorConfig{
			IdleDelay: 30 * time.Second,
			Clock:     clk,
			Spawner: func(mode WindowMode) (WorkerProcess, error) {
				fw := newFakeWorkerProc(2000 + iter)
				mu.Lock()
				last = fw
				mu.Unlock()
				return fw, nil
			},
		})

		sup.ShowLauncher()

		done := make(chan struct{})
		go func() {
			defer close(done)
			mu.Lock()
			fw := last
			mu.Unlock()
			if fw != nil {
				fw.emitEvent(WorkerEvent{Evt: "hidden"})
			}
		}()

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				_ = sup.Stats()
			}
		}()
		go func() {
			defer wg.Done()
			sup.Quit()
		}()

		waitCh := make(chan struct{})
		go func() {
			wg.Wait()
			<-done
			close(waitCh)
		}()

		select {
		case <-waitCh:
		case <-time.After(3 * time.Second):
			t.Fatalf("iter %d: deadlock detected between monitorWorker hidden event, Stats, and Quit", iter)
		}
	}
}

func TestReviewRegressionTraySupervisorConcurrentSetWarningsAndShow(t *testing.T) {
	clk := newFakeClock()
	sup := NewTraySupervisor(TraySupervisorConfig{
		IdleDelay: 30 * time.Second,
		Clock:     clk,
		Spawner: func(mode WindowMode) (WorkerProcess, error) {
			return newFakeWorkerProc(3001), nil
		},
	})
	defer sup.Quit()

	sup.ShowLauncher()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				sup.SetWarnings([]string{fmt.Sprintf("warn-%d-%d", id, j)})
				sup.ShowExplorer()
				sup.ShowLauncher()
			}
		}(i)
	}
	wg.Wait()
}

func TestReviewRegressionTraySupervisorPreReclaimReshowPreservesLiveQuery(t *testing.T) {
	clk := newFakeClock()
	var workers []*fakeWorkerProc
	sup := NewTraySupervisor(TraySupervisorConfig{
		IdleDelay: 30 * time.Second,
		Clock:     clk,
		Spawner: func(mode WindowMode) (WorkerProcess, error) {
			fw := newFakeWorkerProc(4000 + len(workers))
			workers = append(workers, fw)
			return fw, nil
		},
	})
	defer sup.Quit()

	sup.ShowLauncher()
	w1 := workers[0]
	w1.emitEvent(WorkerEvent{Evt: "query", Query: "旧查询"})
	waitForCondition(t, time.Second, func() bool { return sup.RetainedQuery() == "旧查询" })

	// Pre-30s mode switch to live worker must NOT send stale retainedQuery.
	sup.ShowExplorer()
	waitForCondition(t, time.Second, func() bool {
		cmds := w1.commands()
		return len(cmds) >= 2 && cmds[len(cmds)-1].Mode == "explorer"
	})
	cmds := w1.commands()
	lastCmd := cmds[len(cmds)-1]
	if lastCmd.Query != "" {
		t.Fatalf("live worker show command must not overwrite live query, got %q", lastCmd.Query)
	}

	// After 30s reclaim, respawned worker MUST receive retainedQuery.
	sup.Hide()
	clk.Advance(30 * time.Second)
	waitForCondition(t, time.Second, func() bool { return !sup.Stats().WorkerAlive })

	sup.ShowLauncher()
	if len(workers) != 2 {
		t.Fatalf("expected second worker after reclaim, got %d", len(workers))
	}
	w2 := workers[1]
	waitForCondition(t, time.Second, func() bool { return len(w2.commands()) >= 1 })
	if got := w2.commands()[0].Query; got != "旧查询" {
		t.Fatalf("respawned worker should restore retainedQuery=%q, got %q", "旧查询", got)
	}
}

func TestReviewRegressionEnsureViewportLoadedStraddlingPageBoundary(t *testing.T) {
	const totalRows = 200
	var mu sync.Mutex
	var requestedOffsets []uint32

	fc := &fakeSearchClient{
		queryStartFn: func(ctx context.Context, text string, sort uint8, desc bool, pathSep byte) (*ipc.QueryResultMeta, error) {
			return &ipc.QueryResultMeta{SessionID: 7, Total: totalRows, Retained: totalRows}, nil
		},
		fetchFn: func(ctx context.Context, sessionID uint64, offset uint32, limit uint16) (*ipc.ViewportRows, error) {
			mu.Lock()
			requestedOffsets = append(requestedOffsets, offset)
			mu.Unlock()
			rows := make([]ipc.ViewportRow, 0, limit)
			for i := uint32(0); i < uint32(limit) && offset+i < totalRows; i++ {
				idx := offset + i
				rows = append(rows, ipc.ViewportRow{
					Name: fmt.Sprintf("f%03d.txt", idx),
					Path: fmt.Sprintf("/tmp/f%03d.txt", idx),
				})
			}
			return &ipc.ViewportRows{SessionID: sessionID, Offset: offset, Rows: rows}, nil
		},
	}

	app := NewApp(AppConfig{
		Host:   &fakeWindowHost{},
		Opener: &fakeFileOpener{},
		Dial: func(ctx context.Context) (SearchClient, error) {
			return fc, nil
		},
	})
	defer app.Quit()

	app.SetQueryInput("f")
	waitForCondition(t, time.Second, func() bool {
		snap := app.SearchSnapshot()
		return !snap.Loading && len(snap.Rows) == int(DefaultPageSize)
	})

	// Straddle the 80-row boundary: firstVis=70 (loaded), lastVis=90 (not loaded).
	app.EnsureViewportLoaded(app.SearchSnapshot(), 70, 90)
	waitForCondition(t, time.Second, func() bool {
		snap := app.SearchSnapshot()
		if snap.Loading {
			return false
		}
		_, ok70 := snap.RowAt(70)
		_, ok90 := snap.RowAt(90)
		return ok70 && ok90
	})
}

func TestReviewRegressionSearchTimeoutClearsLoadingAndReportsError(t *testing.T) {
	fc := &fakeSearchClient{
		queryStartFn: func(ctx context.Context, text string, sort uint8, desc bool, pathSep byte) (*ipc.QueryResultMeta, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	ctrl := NewSearchController(SearchConfig{
		RequestTimeout: 40 * time.Millisecond,
		Dial: func(ctx context.Context) (SearchClient, error) {
			return fc, nil
		},
	})
	defer ctrl.Close()

	ctrl.SetQuery("slow", 0, false)
	waitForCondition(t, time.Second, func() bool {
		snap := ctrl.Snapshot()
		return !snap.Loading && snap.Disconnected && snap.Error != ""
	})
}
