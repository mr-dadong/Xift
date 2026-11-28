package desktop

import (
	"sync"
	"sync/atomic"

	"github.com/egoist/mygo"
	"github.com/egoist/mygo/transfer"
	"github.com/egoist/mygo/ui"
	"github.com/mr-dadong/Xift/internal/ui/platform"
)

// GPUContextStats records window-specific MyGo rendering context creations and destructions.
type GPUContextStats struct {
	Created   uint64
	Destroyed uint64
	Live      uint64
}

// WindowHost abstracts native window creation, visibility, and GPU context destruction.
type WindowHost interface {
	Show(mode WindowMode, recreated bool, view func(c *ui.Context), onUserClose func())
	Hide()
	Destroy()
	Invalidate()
	Update(fn func())
	GPUStats() GPUContextStats
}

// MyGoWindowHost manages a single active MyGo native window and its GPU rendering context.
// Switching between Launcher (panel) and Explorer (full window) replaces the native window
// so at most one GPU swap chain / rendering context exists at any time.
type MyGoWindowHost struct {
	mu      sync.Mutex
	win     *mygo.Window
	curMode WindowMode

	created   atomic.Uint64
	destroyed atomic.Uint64
	live      atomic.Uint64
}

// NewMyGoWindowHost creates a WindowHost backed by MyGo native GPU windows.
func NewMyGoWindowHost() *MyGoWindowHost {
	return &MyGoWindowHost{}
}

func windowOptionsForMode(mode WindowMode, view func(c *ui.Context)) mygo.WindowOptions {
	if mode == ModeLauncher {
		return mygo.WindowOptions{
			Title:       "Xift Launcher",
			Width:       760,
			Height:      480,
			MinWidth:    560,
			MinHeight:   320,
			AlwaysOnTop: true,
			Panel:       true,
			Content:     ui.View(view),
		}
	}
	return mygo.WindowOptions{
		Title:     "Xift Explorer",
		Width:     1120,
		Height:    720,
		MinWidth:  760,
		MinHeight: 460,
		Content:   ui.View(view),
	}
}

// Show displays the requested window mode on the MyGo main thread.
func (h *MyGoWindowHost) Show(mode WindowMode, recreated bool, view func(c *ui.Context), onUserClose func()) {
	mygo.RunOnMain(func() {
		h.mu.Lock()
		existing := h.win
		existingMode := h.curMode
		h.mu.Unlock()

		// Reuse existing window when mode matches and it is still alive.
		if !recreated && existing != nil && !existing.IsDestroyed() && existingMode == mode {
			existing.Show()
			existing.Focus()
			existing.Invalidate()
			return
		}

		// Destroy any previous window (e.g. switching between Launcher and Explorer)
		// outside h.mu so native callbacks cannot self-deadlock.
		if existing != nil {
			h.mu.Lock()
			if h.win == existing {
				h.win = nil
				h.curMode = ModeNone
			}
			h.mu.Unlock()
			if !existing.IsDestroyed() {
				existing.Destroy()
				h.destroyed.Add(1)
				if h.live.Load() > 0 {
					h.live.Add(^uint64(0))
				}
			}
		}

		w := mygo.NewWindow(windowOptionsForMode(mode, view))
		w.OnClose(func(e *mygo.CloseEvent) {
			e.PreventDefault()
			if onUserClose != nil {
				onUserClose()
			}
		})
		h.mu.Lock()
		h.win = w
		h.curMode = mode
		h.mu.Unlock()

		h.created.Add(1)
		h.live.Add(1)
		w.Show()
		w.Focus()
	})
}

// Hide hides the currently active window without destroying its GPU context yet.
func (h *MyGoWindowHost) Hide() {
	mygo.RunOnMain(func() {
		h.mu.Lock()
		w := h.win
		h.mu.Unlock()
		if w != nil && !w.IsDestroyed() {
			w.Hide()
		}
	})
}

// Destroy tears down the active MyGo window and releases its window-specific GPU context.
func (h *MyGoWindowHost) Destroy() {
	mygo.RunOnMain(func() {
		h.mu.Lock()
		w := h.win
		h.win = nil
		h.curMode = ModeNone
		h.mu.Unlock()

		if w != nil && !w.IsDestroyed() {
			w.Destroy()
			h.destroyed.Add(1)
			if h.live.Load() > 0 {
				h.live.Add(^uint64(0))
			}
		}
	})
}

// Invalidate schedules a redraw on the active window on the MyGo main thread.
func (h *MyGoWindowHost) Invalidate() {
	mygo.RunOnMain(func() {
		h.mu.Lock()
		w := h.win
		h.mu.Unlock()
		if w != nil && !w.IsDestroyed() {
			w.Invalidate()
		}
	})
}

// Update runs fn on the UI thread and invalidates the active window.
func (h *MyGoWindowHost) Update(fn func()) {
	mygo.RunOnMain(func() {
		if fn != nil {
			fn()
		}
		h.mu.Lock()
		w := h.win
		h.mu.Unlock()
		if w != nil && !w.IsDestroyed() {
			w.Invalidate()
		}
	})
}

// GPUStats returns the cumulative window-specific GPU context counters.
func (h *MyGoWindowHost) GPUStats() GPUContextStats {
	return GPUContextStats{
		Created:   h.created.Load(),
		Destroyed: h.destroyed.Load(),
		Live:      h.live.Load(),
	}
}

// PopupContextMenu displays a native OS popup menu anchored to the active window.
func (h *MyGoWindowHost) PopupContextMenu(items []ContextMenuItem) {
	if len(items) == 0 {
		return
	}
	mygo.RunOnMain(func() {
		h.mu.Lock()
		w := h.win
		h.mu.Unlock()
		if w == nil || w.IsDestroyed() {
			return
		}
		menuItems := make([]*mygo.MenuItem, 0, len(items))
		for _, it := range items {
			if it.Separator {
				menuItems = append(menuItems, mygo.Separator())
				continue
			}
			cb := it.OnClick
			menuItems = append(menuItems, &mygo.MenuItem{
				ID:       it.ID,
				Label:    it.Label,
				Disabled: it.Disabled,
				Click: func(_ *mygo.MenuItem, _ *mygo.Window) {
					if cb != nil {
						go cb()
					}
				},
			})
		}
		m := mygo.NewMenu(menuItems)
		m.Popup(w)
	})
}

// StartFileDrag initiates a cross-platform native external file drag session for absPath.
func (h *MyGoWindowHost) StartFileDrag(absPath string) error {
	cleanPath, err := platform.ValidateAbsolutePath(absPath)
	if err != nil {
		return err
	}
	data, err := transfer.FileData(cleanPath)
	if err != nil {
		return err
	}
	var dragErr error
	mygo.RunOnMain(func() {
		h.mu.Lock()
		w := h.win
		h.mu.Unlock()
		if w == nil || w.IsDestroyed() {
			return
		}
		dragErr = w.StartDataDrag(data, mygo.DragOptions{})
	})
	return dragErr
}

// WriteClipboardText writes plain text to the system clipboard on the UI main thread.
func (h *MyGoWindowHost) WriteClipboardText(text string) {
	mygo.RunOnMain(func() {
		mygo.Clipboard.WriteText(text)
	})
}
