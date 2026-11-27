package desktop

import (
	"sync"
	"time"
)

// DefaultIdleReclaimDelay is the continuous hidden duration after which
// window-specific GPU and rendering resources are released.
const DefaultIdleReclaimDelay = 30 * time.Second

// WindowMode identifies the currently active desktop search window form.
type WindowMode uint8

const (
	ModeNone WindowMode = iota
	ModeLauncher
	ModeExplorer
)

func (m WindowMode) String() string {
	switch m {
	case ModeLauncher:
		return "launcher"
	case ModeExplorer:
		return "explorer"
	default:
		return "none"
	}
}

// Timer is the minimal handle returned by Clock.AfterFunc.
type Timer interface {
	Stop() bool
}

// Clock abstracts time.AfterFunc for deterministic lifecycle testing.
type Clock interface {
	AfterFunc(d time.Duration, fn func()) Timer
}

type realClock struct{}

func (realClock) AfterFunc(d time.Duration, fn func()) Timer {
	return time.AfterFunc(d, fn)
}

// ResourceCallbacks receives serialized lifecycle state transitions.
// Callbacks must not re-enter Lifecycle methods synchronously.
type ResourceCallbacks struct {
	OnShowWindow       func(mode WindowMode, recreated bool)
	OnHideWindow       func(mode WindowMode)
	OnReclaimResources func()
}

// LifecycleConfig configures a Lifecycle controller.
type LifecycleConfig struct {
	IdleDelay time.Duration
	Clock     Clock
	Callbacks ResourceCallbacks
}

// LifecycleStats reports GPU/window context creation and reclamation counts.
type LifecycleStats struct {
	ActiveMode   WindowMode
	ContextAlive bool
	CreateCount  uint64
	ReclaimCount uint64
}

// Lifecycle serializes window show, hide, 30s idle reclamation, and quit transitions.
// Only a timer matching the latest hidden generation can trigger resource reclamation,
// and each live window context is destroyed at most once.
type Lifecycle struct {
	mu           sync.Mutex
	idleDelay    time.Duration
	clock        Clock
	callbacks    ResourceCallbacks
	activeMode   WindowMode
	lastMode     WindowMode
	contextAlive bool
	hideGen      uint64
	timer        Timer
	quit         bool
	createCount  uint64
	reclaimCount uint64
}

// NewLifecycle constructs a Lifecycle controller with defaults for omitted fields.
func NewLifecycle(cfg LifecycleConfig) *Lifecycle {
	delay := cfg.IdleDelay
	if delay <= 0 {
		delay = DefaultIdleReclaimDelay
	}
	clk := cfg.Clock
	if clk == nil {
		clk = realClock{}
	}
	return &Lifecycle{
		idleDelay: delay,
		clock:     clk,
		callbacks: cfg.Callbacks,
	}
}

// Show displays or switches to the requested window mode, canceling any pending
// idle reclamation timer and recreating the rendering context if previously released.
func (l *Lifecycle) Show(mode WindowMode) bool {
	if mode == ModeNone {
		return false
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.quit {
		return false
	}

	if l.timer != nil {
		l.timer.Stop()
		l.timer = nil
	}
	l.hideGen++

	recreated := !l.contextAlive
	if recreated {
		l.contextAlive = true
		l.createCount++
	}
	l.activeMode = mode
	l.lastMode = mode

	if l.callbacks.OnShowWindow != nil {
		l.callbacks.OnShowWindow(mode, recreated)
	}
	return recreated
}

// Hide hides the currently visible window and arms a fresh idle reclamation timer.
// Duplicate calls while already hidden are ignored without resetting the countdown.
func (l *Lifecycle) Hide() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.quit || l.activeMode == ModeNone {
		return
	}

	prev := l.activeMode
	l.activeMode = ModeNone

	if l.callbacks.OnHideWindow != nil {
		l.callbacks.OnHideWindow(prev)
	}

	if !l.contextAlive {
		return
	}

	if l.timer != nil {
		l.timer.Stop()
		l.timer = nil
	}
	l.hideGen++
	gen := l.hideGen
	l.timer = l.clock.AfterFunc(l.idleDelay, func() {
		l.fireReclaim(gen)
	})
}

func (l *Lifecycle) fireReclaim(gen uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.quit || l.activeMode != ModeNone || !l.contextAlive || gen != l.hideGen {
		return
	}

	l.timer = nil
	l.contextAlive = false
	l.reclaimCount++
	if l.callbacks.OnReclaimResources != nil {
		l.callbacks.OnReclaimResources()
	}
}

// Quit stops any pending timer and releases live window resources at most once.
func (l *Lifecycle) Quit() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.quit {
		return
	}
	l.quit = true
	l.hideGen++

	if l.timer != nil {
		l.timer.Stop()
		l.timer = nil
	}
	l.activeMode = ModeNone

	if l.contextAlive {
		l.contextAlive = false
		l.reclaimCount++
		if l.callbacks.OnReclaimResources != nil {
			l.callbacks.OnReclaimResources()
		}
	}
}

// ActiveMode returns the currently visible window mode, or ModeNone when hidden.
func (l *Lifecycle) ActiveMode() WindowMode {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.activeMode
}

// LastMode returns the most recently shown window mode.
func (l *Lifecycle) LastMode() WindowMode {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastMode
}

// ContextAlive reports whether window-specific rendering resources are currently live.
func (l *Lifecycle) ContextAlive() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.contextAlive
}

// Stats returns a consistent snapshot of lifecycle counters.
func (l *Lifecycle) Stats() LifecycleStats {
	l.mu.Lock()
	defer l.mu.Unlock()
	return LifecycleStats{
		ActiveMode:   l.activeMode,
		ContextAlive: l.contextAlive,
		CreateCount:  l.createCount,
		ReclaimCount: l.reclaimCount,
	}
}
