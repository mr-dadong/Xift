package desktop

import (
	"sync"
	"testing"
	"time"
)

type fakeTimer struct {
	clock   *fakeClock
	fireAt  time.Time
	fn      func()
	stopped bool
	fired   bool
}

func (t *fakeTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	if t.stopped || t.fired {
		return false
	}
	t.stopped = true
	return true
}

type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) AfterFunc(d time.Duration, fn func()) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{
		clock:  c,
		fireAt: c.now.Add(d),
		fn:     fn,
	}
	c.timers = append(c.timers, t)
	return t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	var due []*fakeTimer
	for _, t := range c.timers {
		if !t.stopped && !t.fired && !t.fireAt.After(c.now) {
			t.fired = true
			due = append(due, t)
		}
	}
	c.mu.Unlock()
	for _, t := range due {
		t.fn()
	}
}

// FireAllPendingStale simulates OS/runtime timer callbacks that had already been queued
// before Stop() took effect.
func (c *fakeClock) FireAllCallbacks() {
	c.mu.Lock()
	fns := make([]func(), 0, len(c.timers))
	for _, t := range c.timers {
		if !t.fired {
			t.fired = true
			fns = append(fns, t.fn)
		}
	}
	c.mu.Unlock()
	for _, fn := range fns {
		fn()
	}
}

func TestLifecycleReclaimAfter30SecondsAndRecreate(t *testing.T) {
	clk := newFakeClock()
	var (
		showCalls    int
		recreateCnt  int
		hideCalls    int
		reclaimCalls int
	)

	lc := NewLifecycle(LifecycleConfig{
		IdleDelay: 30 * time.Second,
		Clock:     clk,
		Callbacks: ResourceCallbacks{
			OnShowWindow: func(mode WindowMode, recreated bool) {
				showCalls++
				if recreated {
					recreateCnt++
				}
			},
			OnHideWindow: func(mode WindowMode) {
				hideCalls++
			},
			OnReclaimResources: func() {
				reclaimCalls++
			},
		},
	})

	lc.Show(ModeLauncher)
	if showCalls != 1 || recreateCnt != 1 || !lc.ContextAlive() {
		t.Fatalf("expected initial show & create, got show=%d recreate=%d alive=%v", showCalls, recreateCnt, lc.ContextAlive())
	}

	lc.Hide()
	if hideCalls != 1 || lc.ActiveMode() != ModeNone || !lc.ContextAlive() {
		t.Fatalf("expected hidden with live context, got hide=%d mode=%v alive=%v", hideCalls, lc.ActiveMode(), lc.ContextAlive())
	}

	clk.Advance(29 * time.Second)
	if reclaimCalls != 0 || !lc.ContextAlive() {
		t.Fatalf("must not reclaim before 30s, got reclaim=%d alive=%v", reclaimCalls, lc.ContextAlive())
	}

	clk.Advance(1 * time.Second)
	if reclaimCalls != 1 || lc.ContextAlive() {
		t.Fatalf("expected reclaim at 30s, got reclaim=%d alive=%v", reclaimCalls, lc.ContextAlive())
	}

	// Showing again after reclaim must recreate the rendering context.
	lc.Show(ModeExplorer)
	if showCalls != 2 || recreateCnt != 2 || !lc.ContextAlive() || lc.ActiveMode() != ModeExplorer {
		t.Fatalf("expected recreate on show after reclaim, got show=%d recreate=%d alive=%v mode=%v",
			showCalls, recreateCnt, lc.ContextAlive(), lc.ActiveMode())
	}
}

func TestLifecycleShowAt29SecondsCancelsOldTimerAndRestartsCountdown(t *testing.T) {
	clk := newFakeClock()
	var (
		reclaimCalls int
		recreateCnt  int
	)

	lc := NewLifecycle(LifecycleConfig{
		IdleDelay: 30 * time.Second,
		Clock:     clk,
		Callbacks: ResourceCallbacks{
			OnShowWindow: func(mode WindowMode, recreated bool) {
				if recreated {
					recreateCnt++
				}
			},
			OnReclaimResources: func() {
				reclaimCalls++
			},
		},
	})

	lc.Show(ModeLauncher)
	lc.Hide()
	clk.Advance(29 * time.Second)

	// Re-show at 29s; context is still alive so no recreation.
	lc.Show(ModeLauncher)
	if recreateCnt != 1 {
		t.Fatalf("expected no recreation before 30s, got recreateCnt=%d", recreateCnt)
	}

	// Cross original 30s mark AND simulate stale callback delivery.
	clk.Advance(5 * time.Second)
	clk.FireAllCallbacks()
	if reclaimCalls != 0 || !lc.ContextAlive() {
		t.Fatalf("stale timer must not reclaim visible window, got reclaim=%d alive=%v", reclaimCalls, lc.ContextAlive())
	}

	// Hide again: countdown must restart from 0 (full 30s).
	lc.Hide()
	clk.Advance(29 * time.Second)
	if reclaimCalls != 0 || !lc.ContextAlive() {
		t.Fatalf("restarted countdown must not fire at 29s, got reclaim=%d alive=%v", reclaimCalls, lc.ContextAlive())
	}

	clk.Advance(1 * time.Second)
	if reclaimCalls != 1 || lc.ContextAlive() {
		t.Fatalf("restarted countdown must fire at 30s, got reclaim=%d alive=%v", reclaimCalls, lc.ContextAlive())
	}
}

func TestLifecycleDuplicateHideIsIdempotent(t *testing.T) {
	clk := newFakeClock()
	var (
		hideCalls    int
		reclaimCalls int
	)

	lc := NewLifecycle(LifecycleConfig{
		IdleDelay: 30 * time.Second,
		Clock:     clk,
		Callbacks: ResourceCallbacks{
			OnHideWindow: func(mode WindowMode) {
				hideCalls++
			},
			OnReclaimResources: func() {
				reclaimCalls++
			},
		},
	})

	lc.Show(ModeExplorer)
	lc.Hide()
	clk.Advance(15 * time.Second)

	// Duplicate Hide while already hidden must not trigger OnHideWindow again
	// or reset the countdown, and reclaim must happen only once.
	lc.Hide()
	lc.Hide()
	if hideCalls != 1 {
		t.Fatalf("duplicate Hide should be ignored, got hideCalls=%d", hideCalls)
	}

	clk.Advance(15 * time.Second)
	if reclaimCalls != 1 {
		t.Fatalf("expected single reclaim at 30s, got reclaimCalls=%d", reclaimCalls)
	}

	// Another Hide after reclaim must still not trigger a second reclaim.
	lc.Hide()
	clk.Advance(60 * time.Second)
	clk.FireAllCallbacks()
	if reclaimCalls != 1 {
		t.Fatalf("window resources must be destroyed only once, got reclaimCalls=%d", reclaimCalls)
	}
}

func TestLifecycleQuitWithPendingCallbackDestroysOnce(t *testing.T) {
	clk := newFakeClock()
	var reclaimCalls int

	lc := NewLifecycle(LifecycleConfig{
		IdleDelay: 30 * time.Second,
		Clock:     clk,
		Callbacks: ResourceCallbacks{
			OnReclaimResources: func() {
				reclaimCalls++
			},
		},
	})

	lc.Show(ModeLauncher)
	lc.Hide()
	clk.Advance(10 * time.Second)

	// Quit while timer is pending: must release live context exactly once.
	lc.Quit()
	if reclaimCalls != 1 || lc.ContextAlive() {
		t.Fatalf("expected single reclaim on Quit, got reclaim=%d alive=%v", reclaimCalls, lc.ContextAlive())
	}

	// Late/pending timer callback firing after Quit must be ignored.
	clk.Advance(30 * time.Second)
	clk.FireAllCallbacks()
	lc.Quit() // Idempotent Quit
	if reclaimCalls != 1 {
		t.Fatalf("reclaim must happen only once across Quit and pending callbacks, got %d", reclaimCalls)
	}
}
