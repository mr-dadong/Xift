package desktop

import (
	"bufio"
	"encoding/json"
	"io"
	"sync"
	"testing"
	"time"
)

type fakeWorkerProc struct {
	pid        int
	inR        *io.PipeReader
	inW        *io.PipeWriter
	outR       *io.PipeReader
	outW       *io.PipeWriter
	done       chan struct{}
	closeOnce  sync.Once
	mu         sync.Mutex
	cmds       []WorkerCommand
	killed     bool
	quitRecvCh chan struct{}
}

func newFakeWorkerProc(pid int) *fakeWorkerProc {
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	fw := &fakeWorkerProc{
		pid:        pid,
		inR:        inR,
		inW:        inW,
		outR:       outR,
		outW:       outW,
		done:       make(chan struct{}),
		quitRecvCh: make(chan struct{}),
	}
	go fw.readLoop()
	return fw
}

func (f *fakeWorkerProc) readLoop() {
	defer f.finish()
	sc := bufio.NewScanner(f.inR)
	for sc.Scan() {
		var cmd WorkerCommand
		if err := json.Unmarshal(sc.Bytes(), &cmd); err != nil {
			continue
		}
		f.mu.Lock()
		f.cmds = append(f.cmds, cmd)
		f.mu.Unlock()
		if cmd.Cmd == "quit" {
			select {
			case <-f.quitRecvCh:
			default:
				close(f.quitRecvCh)
			}
			return
		}
	}
}

func (f *fakeWorkerProc) emitEvent(evt WorkerEvent) {
	b, _ := json.Marshal(evt)
	b = append(b, '\n')
	_, _ = f.outW.Write(b)
}

func (f *fakeWorkerProc) finish() {
	f.closeOnce.Do(func() {
		_ = f.outW.Close()
		_ = f.inR.Close()
		close(f.done)
	})
}

func (f *fakeWorkerProc) PID() int              { return f.pid }
func (f *fakeWorkerProc) Stdin() io.WriteCloser { return f.inW }
func (f *fakeWorkerProc) Stdout() io.ReadCloser { return f.outR }
func (f *fakeWorkerProc) Wait() error {
	<-f.done
	return nil
}
func (f *fakeWorkerProc) Kill() error {
	f.mu.Lock()
	f.killed = true
	f.mu.Unlock()
	f.finish()
	return nil
}

func (f *fakeWorkerProc) commands() []WorkerCommand {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]WorkerCommand(nil), f.cmds...)
}

func TestTraySupervisorReusesWorkerBefore30sAndReclaimsAfter30s(t *testing.T) {
	clk := newFakeClock()
	var (
		mu      sync.Mutex
		workers []*fakeWorkerProc
	)

	sup := NewTraySupervisor(TraySupervisorConfig{
		IdleDelay: 30 * time.Second,
		Clock:     clk,
		Spawner: func(mode WindowMode) (WorkerProcess, error) {
			mu.Lock()
			defer mu.Unlock()
			w := newFakeWorkerProc(1000 + len(workers) + 1)
			workers = append(workers, w)
			return w, nil
		},
	})
	defer sup.Quit()

	// 1. Show Launcher -> spawns worker #1.
	sup.ShowLauncher()
	waitForCondition(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(workers) == 1 && len(workers[0].commands()) == 1
	})

	// Simulate user typing query in worker #1 and pressing Esc (hidden).
	workers[0].emitEvent(WorkerEvent{Evt: "query", Query: "财务报告 ext:pdf"})
	workers[0].emitEvent(WorkerEvent{Evt: "hidden"})
	waitForCondition(t, 2*time.Second, func() bool {
		return sup.RetainedQuery() == "财务报告 ext:pdf" && sup.ActiveMode() == ModeNone
	})

	// 2. Re-show at 29s -> reuses worker #1 without spawning worker #2.
	clk.Advance(29 * time.Second)
	sup.ShowExplorer()
	waitForCondition(t, 2*time.Second, func() bool {
		return len(workers[0].commands()) >= 2
	})
	mu.Lock()
	countAfter29s := len(workers)
	mu.Unlock()
	if countAfter29s != 1 {
		t.Fatalf("expected worker reuse before 30s, got %d workers spawned", countAfter29s)
	}

	// 3. Hide again and advance full 30s -> sends quit and terminates worker #1.
	sup.Hide()
	clk.Advance(30 * time.Second)

	select {
	case <-workers[0].done:
	case <-time.After(2 * time.Second):
		t.Fatalf("worker #1 did not exit on 30s reclaim")
	}

	st := sup.Stats()
	if st.WorkerAlive || st.WorkersSpawned != 1 || st.WorkersExited != 1 {
		t.Fatalf("unexpected supervisor stats after reclaim: %+v", st)
	}

	// 4. Re-show after reclaim -> spawns worker #2 and forwards retained query text.
	sup.ShowLauncher()
	waitForCondition(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(workers) == 2 && len(workers[1].commands()) == 1
	})

	cmd := workers[1].commands()[0]
	if cmd.Cmd != "show" || cmd.Mode != "launcher" || cmd.Query != "财务报告 ext:pdf" {
		t.Fatalf("new worker did not receive retained query: %+v", cmd)
	}
}
