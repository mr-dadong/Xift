package desktop

import (
	"bufio"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"
)

// WorkerProcess abstracts a running window-worker subprocess for deterministic testing.
type WorkerProcess interface {
	PID() int
	Stdin() io.WriteCloser
	Stdout() io.ReadCloser
	Wait() error
	Kill() error
}

// WorkerSpawner launches a fresh window-worker subprocess for the requested initial mode.
type WorkerSpawner func(mode WindowMode) (WorkerProcess, error)

// TraySupervisorConfig configures the zero-GPU tray host supervisor.
type TraySupervisorConfig struct {
	IdleDelay time.Duration
	Clock     Clock
	Spawner   WorkerSpawner
	OnError   func(err error)
}

// SupervisorStats reports worker subprocess lifecycle counters and current PID.
type SupervisorStats struct {
	ActiveMode     WindowMode
	WorkerAlive    bool
	WorkerPID      int
	WorkersSpawned uint64
	WorkersExited  uint64
}

// TraySupervisor runs inside the zero-GPU tray host process. It owns the 30-second
// idle reclamation Lifecycle, retains the user's latest query text across worker
// restarts, and terminates the GPU worker subprocess on reclamation so the OS
// reclaims 100% of graphics driver memory.
type TraySupervisor struct {
	spawner   WorkerSpawner
	onError   func(error)
	lifecycle *Lifecycle

	mu             sync.Mutex
	worker         WorkerProcess
	workerStdin    io.WriteCloser
	workerDone     chan struct{}
	retainedQuery  string
	warnings       []string
	workersSpawned uint64
	workersExited  uint64
}

// NewTraySupervisor constructs a TraySupervisor that manages on-demand window workers.
func NewTraySupervisor(cfg TraySupervisorConfig) *TraySupervisor {
	s := &TraySupervisor{
		spawner: cfg.Spawner,
		onError: cfg.OnError,
	}

	s.lifecycle = NewLifecycle(LifecycleConfig{
		IdleDelay: cfg.IdleDelay,
		Clock:     cfg.Clock,
		Callbacks: ResourceCallbacks{
			OnShowWindow: func(mode WindowMode, recreated bool) {
				s.handleShow(mode, recreated)
			},
			OnHideWindow: func(mode WindowMode) {
				s.handleHide()
			},
			OnReclaimResources: func() {
				s.stopWorker()
			},
		},
	})

	return s
}

// ShowLauncher displays or focuses the Launcher window.
func (s *TraySupervisor) ShowLauncher() {
	s.lifecycle.Show(ModeLauncher)
}

// ShowExplorer displays or focuses the Explorer window.
func (s *TraySupervisor) ShowExplorer() {
	s.lifecycle.Show(ModeExplorer)
}

// Hide hides the active window and arms the 30-second idle worker reclamation timer.
func (s *TraySupervisor) Hide() {
	s.lifecycle.Hide()
}

// Quit stops timers and shuts down any live window worker subprocess.
func (s *TraySupervisor) Quit() {
	s.lifecycle.Quit()
}

// ActiveMode returns the currently visible window mode.
func (s *TraySupervisor) ActiveMode() WindowMode {
	return s.lifecycle.ActiveMode()
}

// RetainedQuery returns the latest query text retained across hide/reclaim cycles.
func (s *TraySupervisor) RetainedQuery() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.retainedQuery
}

// SetWarnings updates platform shortcut/tray warnings and pushes them to any live worker.
func (s *TraySupervisor) SetWarnings(warnings []string) {
	s.mu.Lock()
	s.warnings = append([]string(nil), warnings...)
	w := s.workerStdin
	ws := append([]string(nil), s.warnings...)
	s.mu.Unlock()

	if w != nil {
		_ = WriteJSONLine(w, WorkerCommand{Cmd: "warnings", Warnings: ws})
	}
}

// Stats returns a snapshot of worker process lifecycle state.
func (s *TraySupervisor) Stats() SupervisorStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	pid := 0
	if s.worker != nil {
		pid = s.worker.PID()
	}
	return SupervisorStats{
		ActiveMode:     s.lifecycle.ActiveMode(),
		WorkerAlive:    s.worker != nil,
		WorkerPID:      pid,
		WorkersSpawned: s.workersSpawned,
		WorkersExited:  s.workersExited,
	}
}

func (s *TraySupervisor) handleShow(mode WindowMode, recreated bool) {
	s.mu.Lock()
	w := s.worker
	stdin := s.workerStdin
	query := s.retainedQuery
	warnings := append([]string(nil), s.warnings...)
	s.mu.Unlock()

	if w == nil || recreated {
		var err error
		w, stdin, err = s.spawnWorker(mode)
		if err != nil {
			if s.onError != nil {
				s.onError(err)
			}
			return
		}
	}

	if stdin != nil {
		if err := WriteJSONLine(stdin, WorkerCommand{
			Cmd:      "show",
			Mode:     mode.String(),
			Query:    query,
			Warnings: warnings,
		}); err != nil {
			// Worker pipe broke; respawn once cleanly.
			s.stopWorker()
			w2, stdin2, serr := s.spawnWorker(mode)
			if serr != nil {
				if s.onError != nil {
					s.onError(serr)
				}
				return
			}
			_ = w2
			_ = WriteJSONLine(stdin2, WorkerCommand{
				Cmd:      "show",
				Mode:     mode.String(),
				Query:    query,
				Warnings: warnings,
			})
		}
	}
}

func (s *TraySupervisor) handleHide() {
	s.mu.Lock()
	stdin := s.workerStdin
	s.mu.Unlock()
	if stdin != nil {
		_ = WriteJSONLine(stdin, WorkerCommand{Cmd: "hide"})
	}
}

func (s *TraySupervisor) spawnWorker(mode WindowMode) (WorkerProcess, io.WriteCloser, error) {
	if s.spawner == nil {
		return nil, nil, io.ErrClosedPipe
	}
	proc, err := s.spawner(mode)
	if err != nil {
		return nil, nil, err
	}

	done := make(chan struct{})
	s.mu.Lock()
	s.worker = proc
	s.workerStdin = proc.Stdin()
	s.workerDone = done
	s.workersSpawned++
	stdin := s.workerStdin
	s.mu.Unlock()

	go s.monitorWorker(proc, done)
	return proc, stdin, nil
}

func (s *TraySupervisor) monitorWorker(proc WorkerProcess, done chan struct{}) {
	defer close(done)

	sc := bufio.NewScanner(proc.Stdout())
	sc.Buffer(make([]byte, 0, 4096), 256*1024)
	for sc.Scan() {
		var evt WorkerEvent
		if err := unmarshalJSONLine(sc.Bytes(), &evt); err != nil {
			continue
		}
		switch evt.Evt {
		case "query":
			s.mu.Lock()
			s.retainedQuery = evt.Query
			s.mu.Unlock()
		case "hidden":
			s.lifecycle.Hide()
		}
	}

	_ = proc.Wait()

	s.mu.Lock()
	if s.worker == proc {
		s.worker = nil
		s.workerStdin = nil
		s.workerDone = nil
	}
	s.workersExited++
	s.mu.Unlock()
}

func (s *TraySupervisor) stopWorker() {
	s.mu.Lock()
	proc := s.worker
	stdin := s.workerStdin
	done := s.workerDone
	s.worker = nil
	s.workerStdin = nil
	s.workerDone = nil
	s.mu.Unlock()

	if proc == nil {
		return
	}

	if stdin != nil {
		_ = WriteJSONLine(stdin, WorkerCommand{Cmd: "quit"})
		_ = stdin.Close()
	}

	if done != nil {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			_ = proc.Kill()
			<-done
		}
	}
}

type execWorkerProc struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser
}

func (p *execWorkerProc) PID() int {
	if p.cmd == nil || p.cmd.Process == nil {
		return 0
	}
	return p.cmd.Process.Pid
}

func (p *execWorkerProc) Stdin() io.WriteCloser { return p.stdin }
func (p *execWorkerProc) Stdout() io.ReadCloser { return p.stdout }
func (p *execWorkerProc) Wait() error           { return p.cmd.Wait() }
func (p *execWorkerProc) Kill() error {
	if p.cmd != nil && p.cmd.Process != nil {
		return p.cmd.Process.Kill()
	}
	return nil
}

// NewSelfExecWorkerSpawner returns a WorkerSpawner that launches the current xift-ui
// executable in --window-worker mode with stdin/stdout JSON-line control pipes.
func NewSelfExecWorkerSpawner(endpoint string) WorkerSpawner {
	return func(mode WindowMode) (WorkerProcess, error) {
		exe, err := os.Executable()
		if err != nil {
			return nil, err
		}
		args := []string{"-window-worker", "-mode", mode.String()}
		if endpoint != "" {
			args = append(args, "-endpoint", endpoint)
		}
		cmd := exec.Command(exe, args...)
		cmd.Stderr = os.Stderr
		stdin, err := cmd.StdinPipe()
		if err != nil {
			return nil, err
		}
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			_ = stdin.Close()
			return nil, err
		}
		if err := cmd.Start(); err != nil {
			_ = stdin.Close()
			_ = stdout.Close()
			return nil, err
		}
		return &execWorkerProc{cmd: cmd, stdin: stdin, stdout: stdout}, nil
	}
}
