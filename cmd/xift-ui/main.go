package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/egoist/mygo"
	"github.com/mr-dadong/Xift/internal/ui/desktop"
	"github.com/mr-dadong/Xift/internal/ui/platform"
)

func main() {
	var (
		endpoint     string
		startMode    string
		idleReclaim  time.Duration
		windowWorker bool
	)
	flag.StringVar(&endpoint, "endpoint", "", "local Daemon IPC endpoint override (default: per-user endpoint)")
	flag.StringVar(&startMode, "mode", "launcher", "initial window mode: launcher, explorer, or tray")
	flag.DurationVar(&idleReclaim, "idle-reclaim", desktop.DefaultIdleReclaimDelay, "idle hidden duration before releasing window GPU resources")
	flag.BoolVar(&windowWorker, "window-worker", false, "internal mode: run GPU window worker controlled by tray supervisor")
	flag.Parse()

	if windowWorker {
		runWindowWorker(endpoint)
		return
	}
	runTraySupervisor(endpoint, startMode, idleReclaim)
}

// runTraySupervisor runs the zero-GPU tray and global-hotkey host process.
// It never creates a MyGo GPU window directly; instead it spawns an on-demand
// window worker subprocess and terminates it after 30s hidden so the OS reclaims
// 100% of graphics driver memory.
func runTraySupervisor(endpoint, startMode string, idleReclaim time.Duration) {
	if !mygo.App.RequestSingleInstanceLock() {
		fmt.Fprintln(os.Stderr, "xift-ui: another instance is already running")
		os.Exit(0)
	}

	mygo.App.OnWindowAllClosed(func() {})

	adapter := platform.NewAdapter()
	supervisor := desktop.NewTraySupervisor(desktop.TraySupervisorConfig{
		IdleDelay: idleReclaim,
		Spawner:   desktop.NewSelfExecWorkerSpawner(endpoint),
		OnError: func(err error) {
			log.Printf("xift-ui: window worker error: %v", err)
		},
	})

	mygo.App.OnSecondInstance(func(_ []string, _ string) {
		supervisor.ShowLauncher()
	})

	mygo.App.OnQuit(func() {
		supervisor.Quit()
		adapter.Close()
	})

	mygo.App.WhenReady(func() {
		st := adapter.Start(platform.Callbacks{
			OnShowLauncher: func() {
				supervisor.ShowLauncher()
			},
			OnShowExplorer: func() {
				supervisor.ShowExplorer()
			},
			OnQuit: func() {
				supervisor.Quit()
				adapter.Close()
				mygo.App.Quit()
			},
			OnWarningsChanged: func(warnings []string) {
				supervisor.SetWarnings(warnings)
			},
		})
		if len(st.Warnings) > 0 {
			supervisor.SetWarnings(st.Warnings)
			for _, w := range st.Warnings {
				log.Printf("xift-ui: %s", w)
			}
		}

		switch strings.ToLower(strings.TrimSpace(startMode)) {
		case "explorer":
			supervisor.ShowExplorer()
		case "tray", "hidden":
			// Stay resident in the tray with zero GPU initialization.
		default:
			supervisor.ShowLauncher()
		}
	})

	if err := mygo.App.Run(); err != nil {
		log.Fatal(err)
	}
}

// runWindowWorker runs the on-demand MyGo GPU window subprocess controlled over stdin/stdout.
func runWindowWorker(endpoint string) {
	mygo.App.OnWindowAllClosed(func() {})

	var outMu sync.Mutex
	emit := func(evt desktop.WorkerEvent) {
		outMu.Lock()
		defer outMu.Unlock()
		_ = desktop.WriteJSONLine(os.Stdout, evt)
	}

	adapter := platform.NewAdapter()
	host := desktop.NewMyGoWindowHost()
	app := desktop.NewApp(desktop.AppConfig{
		Host:   host,
		Opener: adapter,
		Dial:   desktop.NewIPCDialer(endpoint, "xift-ui"),
		// Idle reclamation is owned by the tray supervisor, which terminates this worker.
		IdleDelay: 24 * time.Hour,
		OnQueryChanged: func(text string) {
			emit(desktop.WorkerEvent{Evt: "query", Query: text})
		},
		OnHidden: func() {
			emit(desktop.WorkerEvent{Evt: "hidden"})
		},
	})

	mygo.App.OnQuit(func() {
		app.Quit()
		st := host.GPUStats()
		emit(desktop.WorkerEvent{
			Evt:       "gpu",
			PID:       os.Getpid(),
			Created:   st.Created,
			Destroyed: st.Destroyed,
			Live:      st.Live,
		})
	})

	mygo.App.WhenReady(func() {
		emit(desktop.WorkerEvent{Evt: "ready", PID: os.Getpid()})

		go func() {
			sc := bufio.NewScanner(os.Stdin)
			sc.Buffer(make([]byte, 0, 4096), 256*1024)
			for sc.Scan() {
				var cmd desktop.WorkerCommand
				if err := json.Unmarshal(sc.Bytes(), &cmd); err != nil {
					continue
				}
				c := cmd
				switch c.Cmd {
				case "show":
					mygo.RunOnMain(func() {
						if len(c.Warnings) > 0 {
							app.SetShortcutWarnings(c.Warnings)
						}
						if c.Query != "" {
							app.SetQueryInput(c.Query)
						}
						if c.Mode == "explorer" {
							app.ShowExplorer()
						} else {
							app.ShowLauncher()
						}
					})
				case "hide":
					mygo.RunOnMain(func() {
						app.Hide()
					})
				case "warnings":
					mygo.RunOnMain(func() {
						app.SetShortcutWarnings(c.Warnings)
					})
				case "quit":
					mygo.RunOnMain(func() {
						app.Quit()
						mygo.App.Quit()
					})
					return
				}
			}
			// Parent supervisor closed stdin or exited: terminate cleanly to avoid orphan processes.
			mygo.RunOnMain(func() {
				app.Quit()
				mygo.App.Quit()
			})
		}()
	})

	if err := mygo.App.Run(); err != nil {
		log.Fatal(err)
	}
}
