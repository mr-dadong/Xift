package platform

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
)

const (
	// DefaultLauncherShortcut invokes the floating Launcher quick-search panel.
	DefaultLauncherShortcut = "Alt+Space"
	// DefaultExplorerShortcut invokes the full Explorer multi-column window.
	DefaultExplorerShortcut = "Ctrl+Alt+Space"
)

// ErrInvalidPath is returned when a file operation is given an empty or non-absolute path.
var ErrInvalidPath = errors.New("platform: file operation requires a non-empty absolute local path")

// ShortcutBackend abstracts OS global shortcut registration.
type ShortcutBackend interface {
	Register(acc, description string, onKeys func(boundKeys string), fn func()) error
	UnregisterAll()
}

// TrayCallbacks defines the actions exposed by the system tray / menu bar icon.
type TrayCallbacks struct {
	OnShowLauncher func()
	OnShowExplorer func()
	OnQuit         func()
}

// TrayHandle represents a live OS tray icon that can be destroyed on exit.
type TrayHandle interface {
	Destroy()
}

// TrayBackend abstracts OS tray / menu bar creation.
type TrayBackend interface {
	Create(callbacks TrayCallbacks) (TrayHandle, error)
}

// ShellBackend abstracts ordinary-user file opening and folder reveal operations.
// Implementations MUST pass absPath as a single direct argument and NEVER invoke a shell command string.
type ShellBackend interface {
	OpenPath(absPath string) error
	ShowItemInFolder(absPath string) error
}

// Callbacks wires platform hotkey and tray events into the desktop App.
type Callbacks struct {
	OnShowLauncher    func()
	OnShowExplorer    func()
	OnQuit            func()
	OnWarningsChanged func(warnings []string)
}

// Status reports the current state of global shortcuts, tray availability, and user warnings.
type Status struct {
	LauncherBound   bool
	LauncherKeys    string
	ExplorerBound   bool
	ExplorerKeys    string
	TrayAvailable   bool
	SessionProtocol string
	Warnings        []string
}

// Adapter coordinates global hotkeys, system tray, and ordinary-user file operations.
type Adapter struct {
	shortcuts ShortcutBackend
	tray      TrayBackend
	shell     ShellBackend

	mu            sync.Mutex
	trayHandle    TrayHandle
	status        Status
	launcherWarn  string
	explorerWarn  string
	trayWarn      string
	onWarnChanged func([]string)
}

// NewAdapter constructs the native OS desktop adapter for the current platform.
func NewAdapter() *Adapter {
	return newPlatformAdapter()
}

// NewAdapterWithBackends constructs an Adapter with injected backends for unit testing.
func NewAdapterWithBackends(sc ShortcutBackend, tray TrayBackend, shell ShellBackend) *Adapter {
	return &Adapter{
		shortcuts: sc,
		tray:      tray,
		shell:     shell,
	}
}

// Start registers the system tray and default global hotkeys.
// Failures are reported honestly in Status.Warnings without crashing or claiming success.
func (a *Adapter) Start(cb Callbacks) Status {
	a.mu.Lock()
	a.onWarnChanged = cb.OnWarningsChanged
	a.launcherWarn = ""
	a.explorerWarn = ""
	a.trayWarn = ""
	a.status = Status{
		SessionProtocol: detectSessionProtocol(),
	}
	a.mu.Unlock()

	if a.tray != nil {
		h, err := a.tray.Create(TrayCallbacks{
			OnShowLauncher: cb.OnShowLauncher,
			OnShowExplorer: cb.OnShowExplorer,
			OnQuit:         cb.OnQuit,
		})
		a.mu.Lock()
		if err != nil {
			a.status.TrayAvailable = false
			a.trayWarn = fmt.Sprintf("系统托盘不可用（%v），已回退为窗口/快捷键入口", err)
		} else {
			a.trayHandle = h
			a.status.TrayAvailable = true
		}
		a.mu.Unlock()
	}

	if a.shortcuts != nil {
		a.registerOne(true, DefaultLauncherShortcut, "Xift Launcher Quick Search", cb.OnShowLauncher)
		a.registerOne(false, DefaultExplorerShortcut, "Xift Explorer Search Window", cb.OnShowExplorer)
	}

	a.mu.Lock()
	a.rebuildWarningsLocked()
	st := a.copyStatusLocked()
	a.mu.Unlock()
	return st
}

func (a *Adapter) registerOne(isLauncher bool, acc, desc string, fn func()) {
	label := "Explorer"
	if isLauncher {
		label = "Launcher"
	}

	onKeys := func(bound string) {
		a.mu.Lock()
		if strings.TrimSpace(bound) == "" {
			if isLauncher {
				a.status.LauncherBound = false
				a.status.LauncherKeys = ""
				a.launcherWarn = fmt.Sprintf("%s 全局快捷键 %s 被桌面拒绝或未授权，请改用托盘入口", label, acc)
			} else {
				a.status.ExplorerBound = false
				a.status.ExplorerKeys = ""
				a.explorerWarn = fmt.Sprintf("%s 全局快捷键 %s 被桌面拒绝或未授权，请改用托盘入口", label, acc)
			}
		} else {
			if isLauncher {
				a.status.LauncherBound = true
				a.status.LauncherKeys = bound
				a.launcherWarn = ""
			} else {
				a.status.ExplorerBound = true
				a.status.ExplorerKeys = bound
				a.explorerWarn = ""
			}
		}
		a.rebuildWarningsLocked()
		warnings := append([]string(nil), a.status.Warnings...)
		cb := a.onWarnChanged
		a.mu.Unlock()

		if cb != nil {
			cb(warnings)
		}
	}

	err := a.shortcuts.Register(acc, desc, onKeys, fn)
	a.mu.Lock()
	defer a.mu.Unlock()
	if err != nil {
		warn := fmt.Sprintf("%s 全局快捷键 %s 注册失败（%v），请使用托盘入口打开", label, acc, err)
		if isLauncher {
			a.status.LauncherBound = false
			a.status.LauncherKeys = ""
			a.launcherWarn = warn
		} else {
			a.status.ExplorerBound = false
			a.status.ExplorerKeys = ""
			a.explorerWarn = warn
		}
		return
	}
	if isLauncher {
		a.status.LauncherBound = true
		a.status.LauncherKeys = acc
	} else {
		a.status.ExplorerBound = true
		a.status.ExplorerKeys = acc
	}
}

func (a *Adapter) rebuildWarningsLocked() {
	var ws []string
	if a.launcherWarn != "" {
		ws = append(ws, a.launcherWarn)
	}
	if a.explorerWarn != "" {
		ws = append(ws, a.explorerWarn)
	}
	if a.trayWarn != "" {
		ws = append(ws, a.trayWarn)
	}
	a.status.Warnings = ws
}

func (a *Adapter) copyStatusLocked() Status {
	st := a.status
	st.Warnings = append([]string(nil), a.status.Warnings...)
	return st
}

// Status returns a consistent snapshot of the adapter's current state.
func (a *Adapter) Status() Status {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.copyStatusLocked()
}

// Close unregisters global shortcuts and destroys the tray icon.
func (a *Adapter) Close() {
	if a.shortcuts != nil {
		a.shortcuts.UnregisterAll()
	}
	a.mu.Lock()
	h := a.trayHandle
	a.trayHandle = nil
	a.mu.Unlock()
	if h != nil {
		h.Destroy()
	}
}

// ValidateAbsolutePath verifies that p is a non-empty absolute local path with no NUL bytes.
func ValidateAbsolutePath(p string) (string, error) {
	if p == "" || strings.ContainsRune(p, 0) {
		return "", ErrInvalidPath
	}
	if !filepath.IsAbs(p) {
		return "", ErrInvalidPath
	}
	return p, nil
}

// OpenFile opens absPath with the OS default application as a single direct argument.
func (a *Adapter) OpenFile(absPath string) error {
	clean, err := ValidateAbsolutePath(absPath)
	if err != nil {
		return err
	}
	if a.shell == nil {
		return errors.New("platform: shell backend not configured")
	}
	return a.shell.OpenPath(clean)
}

// RevealInFolder opens the OS file manager and highlights absPath as a single direct argument.
func (a *Adapter) RevealInFolder(absPath string) error {
	clean, err := ValidateAbsolutePath(absPath)
	if err != nil {
		return err
	}
	if a.shell == nil {
		return errors.New("platform: shell backend not configured")
	}
	return a.shell.ShowItemInFolder(clean)
}
