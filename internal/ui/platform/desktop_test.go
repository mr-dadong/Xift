package platform

import (
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

type fakeShortcutBackend struct {
	mu          sync.Mutex
	rejectAcc   map[string]error
	registered  map[string]func()
	onKeysHooks map[string]func(string)
	unregAll    int
}

func newFakeShortcutBackend() *fakeShortcutBackend {
	return &fakeShortcutBackend{
		rejectAcc:   make(map[string]error),
		registered:  make(map[string]func()),
		onKeysHooks: make(map[string]func(string)),
	}
}

func (f *fakeShortcutBackend) Register(acc, desc string, onKeys func(string), fn func()) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err, bad := f.rejectAcc[acc]; bad {
		return err
	}
	f.registered[acc] = fn
	if onKeys != nil {
		f.onKeysHooks[acc] = onKeys
	}
	return nil
}

func (f *fakeShortcutBackend) UnregisterAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unregAll++
	f.registered = make(map[string]func())
}

type fakeTrayHandle struct {
	destroyed bool
}

func (h *fakeTrayHandle) Destroy() { h.destroyed = true }

type fakeTrayBackend struct {
	failErr error
	created int
	handle  *fakeTrayHandle
}

func (f *fakeTrayBackend) Create(callbacks TrayCallbacks) (TrayHandle, error) {
	if f.failErr != nil {
		return nil, f.failErr
	}
	f.created++
	f.handle = &fakeTrayHandle{}
	return f.handle, nil
}

type fakeShellBackend struct {
	mu           sync.Mutex
	openedArgs   []string
	revealedArgs []string
}

func (f *fakeShellBackend) OpenPath(absPath string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.openedArgs = append(f.openedArgs, absPath)
	return nil
}

func (f *fakeShellBackend) ShowItemInFolder(absPath string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.revealedArgs = append(f.revealedArgs, absPath)
	return nil
}

func TestPlatformHotkeyRejectionReportsSpecificBindingAndKeepsTray(t *testing.T) {
	sc := newFakeShortcutBackend()
	sc.rejectAcc[DefaultLauncherShortcut] = errors.New("hotkey already registered by another app")
	tray := &fakeTrayBackend{}
	shell := &fakeShellBackend{}

	adapter := NewAdapterWithBackends(sc, tray, shell)
	defer adapter.Close()

	res := adapter.Start(Callbacks{
		OnShowLauncher: func() {},
		OnShowExplorer: func() {},
		OnQuit:         func() {},
	})

	if res.LauncherBound {
		t.Fatalf("must not claim Launcher hotkey succeeded when OS rejected it")
	}
	if !res.ExplorerBound {
		t.Fatalf("expected Explorer hotkey to remain bound")
	}
	if !res.TrayAvailable || tray.created != 1 {
		t.Fatalf("expected tray to remain available for window fallback, res=%+v", res)
	}
	joined := strings.Join(res.Warnings, "; ")
	if !strings.Contains(joined, DefaultLauncherShortcut) {
		t.Fatalf("expected warning to name rejected binding %q, got %q", DefaultLauncherShortcut, joined)
	}
}

func TestPlatformWaylandPortalDeclineUpdatesBindingStatus(t *testing.T) {
	sc := newFakeShortcutBackend()
	tray := &fakeTrayBackend{}
	shell := &fakeShellBackend{}

	var (
		mu       sync.Mutex
		warnings []string
	)
	adapter := NewAdapterWithBackends(sc, tray, shell)
	defer adapter.Close()

	res := adapter.Start(Callbacks{
		OnShowLauncher: func() {},
		OnShowExplorer: func() {},
		OnQuit:         func() {},
		OnWarningsChanged: func(w []string) {
			mu.Lock()
			warnings = append([]string(nil), w...)
			mu.Unlock()
		},
	})
	if !res.LauncherBound {
		t.Fatalf("expected initial portal request accepted before user answer")
	}

	// Simulate Wayland GlobalShortcuts portal user declining Alt+Space (OnKeys("")).
	hook := sc.onKeysHooks[DefaultLauncherShortcut]
	if hook == nil {
		t.Fatalf("expected OnKeys callback registered for Wayland portal support")
	}
	hook("")

	st := adapter.Status()
	if st.LauncherBound {
		t.Fatalf("portal decline must clear LauncherBound")
	}
	mu.Lock()
	gotWarnings := strings.Join(warnings, "; ")
	mu.Unlock()
	if !strings.Contains(gotWarnings, DefaultLauncherShortcut) {
		t.Fatalf("expected warning naming declined shortcut %q, got %q", DefaultLauncherShortcut, gotWarnings)
	}
}

func TestPlatformTrayFallbackDoesNotCrashAndReportsWarning(t *testing.T) {
	sc := newFakeShortcutBackend()
	tray := &fakeTrayBackend{failErr: errors.New("StatusNotifierWatcher unavailable")}
	shell := &fakeShellBackend{}

	adapter := NewAdapterWithBackends(sc, tray, shell)
	defer adapter.Close()

	res := adapter.Start(Callbacks{
		OnShowLauncher: func() {},
		OnShowExplorer: func() {},
		OnQuit:         func() {},
	})

	if res.TrayAvailable {
		t.Fatalf("expected TrayAvailable=false when tray host fails")
	}
	if !res.LauncherBound || !res.ExplorerBound {
		t.Fatalf("hotkeys should still work when tray falls back: %+v", res)
	}
	if len(res.Warnings) == 0 {
		t.Fatalf("expected fallback warning when tray creation fails")
	}
}

func TestPlatformUnicodeAndSpacesPathPassedAsSingleArgument(t *testing.T) {
	sc := newFakeShortcutBackend()
	tray := &fakeTrayBackend{}
	shell := &fakeShellBackend{}

	adapter := NewAdapterWithBackends(sc, tray, shell)
	defer adapter.Close()

	// Create a valid absolute path with Unicode, spaces, and shell metacharacters.
	absPath := filepath.Join(t.TempDir(), "测试 目录 & (2026)", "季度 报告 ; echo pwned.pdf")

	if err := adapter.OpenFile(absPath); err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	if err := adapter.RevealInFolder(absPath); err != nil {
		t.Fatalf("RevealInFolder failed: %v", err)
	}

	if len(shell.openedArgs) != 1 || shell.openedArgs[0] != absPath {
		t.Fatalf("OpenFile must pass exact single absolute path argument, got %v", shell.openedArgs)
	}
	if len(shell.revealedArgs) != 1 || shell.revealedArgs[0] != absPath {
		t.Fatalf("RevealInFolder must pass exact single absolute path argument, got %v", shell.revealedArgs)
	}

	// Relative or empty paths must be rejected.
	if err := adapter.OpenFile("relative/path.txt"); err == nil {
		t.Fatalf("expected error for relative path")
	}
	if err := adapter.RevealInFolder(""); err == nil {
		t.Fatalf("expected error for empty path")
	}
}
