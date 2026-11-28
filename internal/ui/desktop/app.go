package desktop

import (
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/egoist/mygo/ui"
	"github.com/mr-dadong/Xift/internal/ipc"
	"github.com/mr-dadong/Xift/internal/query"
	"github.com/mr-dadong/Xift/internal/storage"
	"github.com/mr-dadong/Xift/internal/ui/icons"
	"github.com/mr-dadong/Xift/internal/ui/preview"
)

// FileOpener performs ordinary-user file operations on selected search results.
type FileOpener interface {
	OpenFile(absPath string) error
	RevealInFolder(absPath string) error
}

// AppConfig configures the dual-mode desktop search application.
type AppConfig struct {
	Host           WindowHost
	Opener         FileOpener
	Dial           DialFunc
	Clock          Clock
	IdleDelay      time.Duration
	PageSize       uint16
	PathSep        byte
	OnQueryChanged func(text string)
	OnHidden       func()
}

// App coordinates the single shared SearchController, Lifecycle state machine,
// icon Atlas, non-blocking Preview manager, and the Launcher / Explorer MyGo views.
type App struct {
	host           WindowHost
	opener         FileOpener
	search         *SearchController
	lifecycle      *Lifecycle
	icons          *icons.Atlas
	previewMgr     *preview.Manager
	onQueryChanged func(string)
	onHidden       func()

	mu                   sync.Mutex
	queryInput           string
	lastSubmitted        string
	uiQueryBuf           string // Persistent pointer owned by the UI thread for ui.TextInput
	uiQueryDirtyFromExt  bool
	sortOrder            uint8
	sortDesc             bool
	selectedIdx          int
	focusSearchNextFrame bool
	previewOpen          bool
	lastPreviewPath      string
	lastVisFirst         int
	lastVisLast          int
	lastVisSessionID     uint64
	lastVisPageOffset    uint32
	lastVisRowCount      int
	shortcutWarnings     []string
	statusMsg            string
	listState            ui.ListState
}

// NewApp constructs the dual-mode desktop application controller.
func NewApp(cfg AppConfig) *App {
	a := &App{
		host:           cfg.Host,
		opener:         cfg.Opener,
		sortOrder:      uint8(query.SortByRelevance),
		selectedIdx:    -1,
		lastVisFirst:   -1,
		lastVisLast:    -1,
		onQueryChanged: cfg.OnQueryChanged,
		onHidden:       cfg.OnHidden,
	}
	a.listState.RowHeight = 30

	invalidate := func() {
		if a.host != nil {
			a.host.Invalidate()
		}
	}

	a.icons = icons.NewAtlas(icons.Config{
		OnReady: invalidate,
	})
	a.previewMgr = preview.NewManager(preview.Config{
		OnReady: invalidate,
	})

	a.search = NewSearchController(SearchConfig{
		Dial:     cfg.Dial,
		PageSize: cfg.PageSize,
		PathSep:  cfg.PathSep,
		OnChange: func() {
			a.syncPreviewForSelection()
			invalidate()
		},
	})

	a.lifecycle = NewLifecycle(LifecycleConfig{
		IdleDelay: cfg.IdleDelay,
		Clock:     cfg.Clock,
		Callbacks: ResourceCallbacks{
			OnShowWindow: func(mode WindowMode, recreated bool) {
				a.mu.Lock()
				a.focusSearchNextFrame = true
				q := a.queryInput
				a.mu.Unlock()

				if a.host != nil {
					a.host.Show(mode, recreated, a.renderView, func() {
						a.Hide()
					})
				}
				if recreated {
					if q != "" {
						a.search.Reconnect()
					}
				}
			},
			OnHideWindow: func(mode WindowMode) {
				if a.host != nil {
					a.host.Hide()
				}
				if a.onHidden != nil {
					a.onHidden()
				}
			},
			OnReclaimResources: func() {
				a.search.Disconnect()
				a.icons.Clear()
				a.previewMgr.Clear()
				a.mu.Lock()
				a.lastPreviewPath = ""
				a.lastVisFirst = -1
				a.lastVisLast = -1
				a.lastVisSessionID = 0
				a.lastVisPageOffset = 0
				a.lastVisRowCount = 0
				a.mu.Unlock()
				if a.host != nil {
					a.host.Destroy()
				}
			},
		},
	})

	return a
}

// ShowLauncher displays or focuses the Launcher quick-search panel.
func (a *App) ShowLauncher() {
	a.lifecycle.Show(ModeLauncher)
}

// ShowExplorer displays or focuses the Explorer multi-column window while preserving query state.
func (a *App) ShowExplorer() {
	a.lifecycle.Show(ModeExplorer)
}

// SwitchMode switches between Launcher and Explorer while preserving query text and selection.
func (a *App) SwitchMode(mode WindowMode) {
	a.lifecycle.Show(mode)
}

// Hide hides the active window and starts the 30-second idle reclamation timer.
func (a *App) Hide() {
	a.lifecycle.Hide()
}

// HandleEscape hides the active window.
func (a *App) HandleEscape() {
	a.lifecycle.Hide()
}

// Quit stops timers, releases rendering resources, and closes IPC connections.
func (a *App) Quit() {
	a.lifecycle.Quit()
	a.search.Close()
	a.icons.Close()
	a.previewMgr.Close()
}

// TogglePreview toggles the non-blocking right-hand preview pane.
func (a *App) TogglePreview() {
	a.mu.Lock()
	a.previewOpen = !a.previewOpen
	open := a.previewOpen
	if !open {
		a.lastPreviewPath = ""
	}
	a.mu.Unlock()

	if !open {
		a.previewMgr.Clear()
	} else {
		a.syncPreviewForSelection()
	}
	if a.host != nil {
		a.host.Invalidate()
	}
}

// PreviewOpen reports whether the split preview pane is currently visible.
func (a *App) PreviewOpen() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.previewOpen
}

// PreviewSnapshot returns the latest non-blocking preview snapshot.
func (a *App) PreviewSnapshot() preview.Result {
	return a.previewMgr.Snapshot()
}

// IconStats returns the current cached extension and per-file icon counts.
func (a *App) IconStats() (int, int) {
	return a.icons.Stats()
}

func (a *App) syncPreviewForSelection() {
	if !a.PreviewOpen() {
		return
	}
	row, ok := a.selectedRow()
	a.mu.Lock()
	if !ok {
		if a.lastPreviewPath != "" {
			a.lastPreviewPath = ""
			a.mu.Unlock()
			a.previewMgr.Clear()
			return
		}
		a.mu.Unlock()
		return
	}
	if row.Path == a.lastPreviewPath {
		a.mu.Unlock()
		return
	}
	a.lastPreviewPath = row.Path
	a.mu.Unlock()

	isDir := (row.Flags & storage.FlagDir) != 0
	a.previewMgr.Select(row.Path, isDir)
}

// ActiveMode returns the currently visible window mode.
func (a *App) ActiveMode() WindowMode {
	return a.lifecycle.ActiveMode()
}

// LifecycleStats returns lifecycle state and context counters.
func (a *App) LifecycleStats() LifecycleStats {
	return a.lifecycle.Stats()
}

// GPUStats returns window-specific GPU context counters from the underlying host.
func (a *App) GPUStats() GPUContextStats {
	if a.host == nil {
		return GPUContextStats{}
	}
	return a.host.GPUStats()
}

// SetShortcutWarnings stores user-visible notices when the OS rejects global shortcuts.
func (a *App) SetShortcutWarnings(warnings []string) {
	a.mu.Lock()
	a.shortcutWarnings = append([]string(nil), warnings...)
	a.mu.Unlock()
	if a.host != nil {
		a.host.Invalidate()
	}
}

// ShortcutWarnings returns a copy of current global shortcut warnings.
func (a *App) ShortcutWarnings() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.shortcutWarnings...)
}

// QueryInput returns the current shared search input text.
func (a *App) QueryInput() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.queryInput
}

// SetQueryInput updates the shared query text and starts a background IPC search if changed.
func (a *App) SetQueryInput(text string) {
	a.setQueryInternal(text, true)
}

func (a *App) setQueryInternal(text string, fromExternal bool) {
	a.mu.Lock()
	changed := text != a.lastSubmitted
	a.queryInput = text
	if fromExternal {
		a.uiQueryDirtyFromExt = true
	}
	if changed {
		a.lastSubmitted = text
		a.selectedIdx = 0
		a.statusMsg = ""
	}
	sortOrder := a.sortOrder
	sortDesc := a.sortDesc
	a.mu.Unlock()

	if changed {
		if !fromExternal {
			a.listState.ScrollTo(0, ui.Start)
		} else if a.host != nil {
			a.host.Update(func() {
				a.listState.ScrollTo(0, ui.Start)
			})
		}
		a.search.SetQuery(text, sortOrder, sortDesc)
		if a.onQueryChanged != nil {
			a.onQueryChanged(text)
		}
	}
}

// syncUIQueryBeforeRender copies any external query update (e.g. from tray supervisor
// on worker restart) into the persistent UI-thread buffer before ui.TextInput renders.
func (a *App) syncUIQueryBeforeRender() {
	a.mu.Lock()
	if a.uiQueryDirtyFromExt {
		a.uiQueryBuf = a.queryInput
		a.uiQueryDirtyFromExt = false
	}
	a.mu.Unlock()
}

// syncUIQueryAfterRender submits the query if ui.TextInput mutated a.uiQueryBuf.
func (a *App) syncUIQueryAfterRender() {
	if a.uiQueryBuf != a.QueryInput() {
		a.setQueryInternal(a.uiQueryBuf, false)
	}
}

// SetSort updates the Explorer sort order and re-runs the active query.
func (a *App) SetSort(sortOrder uint8, desc bool) {
	a.mu.Lock()
	a.sortOrder = sortOrder
	a.sortDesc = desc
	text := a.queryInput
	a.selectedIdx = 0
	a.mu.Unlock()

	a.listState.ScrollTo(0, ui.Start)
	a.search.SetQuery(text, sortOrder, desc)
}

// RetrySearch reconnects to the Daemon and re-executes the current query.
func (a *App) RetrySearch() {
	a.search.Reconnect()
}

// SearchSnapshot returns the latest immutable search snapshot.
func (a *App) SearchSnapshot() SearchSnapshot {
	return a.search.Snapshot()
}

// SelectRow sets the active virtual row index.
func (a *App) SelectRow(idx int) {
	snap := a.search.Snapshot()
	a.mu.Lock()
	if idx < 0 || uint32(idx) >= snap.Retained {
		a.selectedIdx = -1
		a.mu.Unlock()
		a.syncPreviewForSelection()
		return
	}
	a.selectedIdx = idx
	a.mu.Unlock()
	a.syncPreviewForSelection()
}

// ClearSelection clears the active row selection.
func (a *App) ClearSelection() {
	a.mu.Lock()
	a.selectedIdx = -1
	a.mu.Unlock()
}

// SelectedIndex returns the currently selected virtual row index, or -1 if none.
func (a *App) SelectedIndex() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.selectedIdx
}

// MoveSelection moves the selected row by delta within [0, Retained-1] and fetches
// the target viewport page if needed.
func (a *App) MoveSelection(delta int) {
	snap := a.search.Snapshot()
	if snap.Retained == 0 {
		a.ClearSelection()
		return
	}

	a.mu.Lock()
	cur := a.selectedIdx
	maxIdx := int(snap.Retained) - 1
	if cur < 0 {
		if delta >= 0 {
			cur = 0
		} else {
			cur = maxIdx
		}
	} else {
		cur += delta
		if cur < 0 {
			cur = 0
		}
		if cur > maxIdx {
			cur = maxIdx
		}
	}
	a.selectedIdx = cur
	a.mu.Unlock()

	a.listState.ScrollTo(cur, ui.Center)
	a.ensureIndexLoaded(snap, cur)
	a.syncPreviewForSelection()
	if a.host != nil {
		a.host.Invalidate()
	}
}

func (a *App) selectedRow() (ipc.ViewportRow, bool) {
	a.mu.Lock()
	idx := a.selectedIdx
	a.mu.Unlock()
	if idx < 0 {
		return ipc.ViewportRow{}, false
	}
	snap := a.search.Snapshot()
	if uint32(idx) >= snap.Retained {
		return ipc.ViewportRow{}, false
	}
	row, ok := snap.RowAt(idx)
	if !ok || row.Path == "" {
		return ipc.ViewportRow{}, false
	}
	return row, true
}

// OpenSelected opens the currently selected file via the ordinary-user FileOpener.
// When there is no valid selection, no file operation is performed.
func (a *App) OpenSelected() error {
	row, ok := a.selectedRow()
	if !ok || a.opener == nil {
		return nil
	}
	err := a.opener.OpenFile(row.Path)
	if err != nil {
		a.setStatus("打开失败: " + err.Error())
	} else {
		a.setStatus("已打开: " + row.Name)
		a.search.RecordOpen(row.Path)
	}
	return err
}

// RevealSelected reveals the currently selected file in its containing folder.
// When there is no valid selection, no file operation is performed.
func (a *App) RevealSelected() error {
	row, ok := a.selectedRow()
	if !ok || a.opener == nil {
		return nil
	}
	err := a.opener.RevealInFolder(row.Path)
	if err != nil {
		a.setStatus("定位失败: " + err.Error())
	} else {
		a.setStatus("已定位: " + filepath.Dir(row.Path))
		a.search.RecordOpen(row.Path)
	}
	return err
}

func (a *App) setStatus(msg string) {
	a.mu.Lock()
	a.statusMsg = msg
	a.mu.Unlock()
	if a.host != nil {
		a.host.Invalidate()
	}
}

func (a *App) statusMessage() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.statusMsg
}

func (a *App) ensureIndexLoaded(snap SearchSnapshot, idx int) {
	if idx < 0 || snap.Retained == 0 || uint32(idx) >= snap.Retained {
		return
	}
	if _, ok := snap.RowAt(idx); ok {
		return
	}
	pageSize := uint32(DefaultPageSize)
	pageStart := (uint32(idx) / pageSize) * pageSize
	a.search.RequestPage(pageStart, DefaultPageSize)
}

// EnsureViewportLoaded requests the page covering [firstVis, lastVis] if it is not yet loaded.
func (a *App) EnsureViewportLoaded(snap SearchSnapshot, firstVis, lastVis int) {
	if snap.Retained == 0 || snap.Loading || snap.Disconnected {
		return
	}
	if firstVis < 0 {
		firstVis = 0
	}
	if uint32(firstVis) >= snap.Retained {
		return
	}
	_, firstOK := snap.RowAt(firstVis)
	lastIdx := lastVis
	if lastIdx >= int(snap.Retained) {
		lastIdx = int(snap.Retained) - 1
	}
	lastOK := true
	if lastIdx >= firstVis {
		_, lastOK = snap.RowAt(lastIdx)
	}
	if firstOK && lastOK {
		return
	}

	// Request an unaligned single page starting at firstVis so a viewport straddling
	// a DefaultPageSize boundary is covered in one <= MaxPageSize fetch instead of
	// re-requesting the already-loaded lower aligned page.
	needed := lastIdx - firstVis + 1
	reqLimit := DefaultPageSize
	if needed > int(reqLimit) {
		if needed > int(MaxPageSize) {
			reqLimit = MaxPageSize
		} else {
			reqLimit = uint16(needed)
		}
	}
	a.search.RequestPage(uint32(firstVis), reqLimit)
}

func (a *App) renderView(c *ui.Context) {
	mode := a.lifecycle.ActiveMode()
	if mode == ModeExplorer {
		a.renderExplorer(c)
		return
	}
	a.renderLauncher(c)
}

func (a *App) registerCommonShortcuts(c *ui.Context) {
	c.OnWindowShortcut(0, ui.KeyEscape, func() {
		a.HandleEscape()
	})
	c.OnWindowShortcut(0, ui.KeyDown, func() {
		a.MoveSelection(1)
	})
	c.OnWindowShortcut(0, ui.KeyUp, func() {
		a.MoveSelection(-1)
	})
	c.OnWindowShortcut(0, ui.KeyEnter, func() {
		_ = a.OpenSelected()
	})
	c.OnWindowShortcut(ui.Ctrl, ui.KeyEnter, func() {
		_ = a.RevealSelected()
	})
	c.OnWindowShortcut(0, ui.KeyTab, func() {
		a.TogglePreview()
	})
}

func (a *App) notifyVisibleIcons(snap SearchSnapshot, firstVis, lastVis int) {
	if len(snap.Rows) == 0 || snap.Retained == 0 || firstVis < 0 {
		return
	}
	a.mu.Lock()
	if firstVis == a.lastVisFirst &&
		lastVis == a.lastVisLast &&
		snap.SessionID == a.lastVisSessionID &&
		snap.PageOffset == a.lastVisPageOffset &&
		len(snap.Rows) == a.lastVisRowCount {
		a.mu.Unlock()
		return
	}
	a.lastVisFirst = firstVis
	a.lastVisLast = lastVis
	a.lastVisSessionID = snap.SessionID
	a.lastVisPageOffset = snap.PageOffset
	a.lastVisRowCount = len(snap.Rows)
	a.mu.Unlock()

	end := lastVis
	if end >= int(snap.Retained) {
		end = int(snap.Retained) - 1
	}
	var perFile []string
	for i := firstVis; i <= end; i++ {
		if row, ok := snap.RowAt(i); ok && (row.Flags&storage.FlagDir) == 0 {
			if icons.SupportsPerFileIcon(filepath.Ext(row.Name)) && row.Path != "" {
				perFile = append(perFile, row.Path)
			}
		}
	}
	a.icons.NotifyVisible(perFile)
}

func formatSize(size int64, isDir bool) string {
	if isDir {
		return "<DIR>"
	}
	if size < 0 {
		return "-"
	}
	const (
		kb = 1024
		mb = 1024 * kb
		gb = 1024 * mb
	)
	switch {
	case size >= gb:
		return fmt.Sprintf("%.1f GB", float64(size)/float64(gb))
	case size >= mb:
		return fmt.Sprintf("%.1f MB", float64(size)/float64(mb))
	case size >= kb:
		return fmt.Sprintf("%.1f KB", float64(size)/float64(kb))
	default:
		return fmt.Sprintf("%d B", size)
	}
}

func formatModTime(sec int64) string {
	if sec <= 0 {
		return "-"
	}
	return time.Unix(sec, 0).Format("2006-01-02 15:04")
}
