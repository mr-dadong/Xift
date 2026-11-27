package desktop

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/egoist/mygo/ui"
	"github.com/mr-dadong/Xift/internal/ipc"
)

type fakeWindowHost struct {
	mu          sync.Mutex
	showCalls   int
	recreateCnt int
	hideCalls   int
	destroyCnt  int
	lastMode    WindowMode
}

func (h *fakeWindowHost) Show(mode WindowMode, recreated bool, view func(c *ui.Context), onUserClose func()) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.showCalls++
	if recreated {
		h.recreateCnt++
	}
	h.lastMode = mode
}

func (h *fakeWindowHost) Hide() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.hideCalls++
}

func (h *fakeWindowHost) Destroy() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.destroyCnt++
}

func (h *fakeWindowHost) Invalidate() {}
func (h *fakeWindowHost) Update(fn func()) {
	if fn != nil {
		fn()
	}
}
func (h *fakeWindowHost) GPUStats() GPUContextStats { return GPUContextStats{} }

type fakeFileOpener struct {
	mu      sync.Mutex
	opened  []string
	reveals []string
}

func (f *fakeFileOpener) OpenFile(absPath string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.opened = append(f.opened, absPath)
	return nil
}

func (f *fakeFileOpener) RevealInFolder(absPath string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reveals = append(f.reveals, absPath)
	return nil
}

func newTestAppWithRows(t *testing.T, rows []ipc.ViewportRow) (*App, *fakeWindowHost, *fakeFileOpener, *fakeClock) {
	t.Helper()
	client := &fakeSearchClient{
		queryStartFn: func(ctx context.Context, text string, sort uint8, desc bool, pathSep byte) (*ipc.QueryResultMeta, error) {
			return &ipc.QueryResultMeta{
				SessionID: 1,
				Total:     uint64(len(rows)),
				Retained:  uint32(len(rows)),
			}, nil
		},
		fetchFn: func(ctx context.Context, sessionID uint64, offset uint32, limit uint16) (*ipc.ViewportRows, error) {
			end := int(offset) + int(limit)
			if end > len(rows) {
				end = len(rows)
			}
			var slice []ipc.ViewportRow
			if int(offset) < len(rows) {
				slice = append(slice, rows[offset:end]...)
			}
			return &ipc.ViewportRows{
				SessionID: sessionID,
				Offset:    offset,
				Rows:      slice,
			}, nil
		},
	}

	host := &fakeWindowHost{}
	opener := &fakeFileOpener{}
	clk := newFakeClock()
	app := NewApp(AppConfig{
		Host:   host,
		Opener: opener,
		Clock:  clk,
		Dial:   func(ctx context.Context) (SearchClient, error) { return client, nil },
	})
	t.Cleanup(func() { app.Quit() })
	return app, host, opener, clk
}

func TestAppSwitchModePreservesQueryText(t *testing.T) {
	rows := []ipc.ViewportRow{
		{NodeID: 1, Name: "报告_1.pdf", Path: `C:\docs\报告_1.pdf`},
		{NodeID: 2, Name: "报告_2.pdf", Path: `C:\docs\报告_2.pdf`},
	}
	app, host, _, _ := newTestAppWithRows(t, rows)

	app.ShowLauncher()
	app.SetQueryInput("报告 ext:pdf")
	waitForCondition(t, 2*time.Second, func() bool {
		return len(app.SearchSnapshot().Rows) == 2
	})

	app.ShowExplorer()
	if app.ActiveMode() != ModeExplorer {
		t.Fatalf("expected ModeExplorer, got %v", app.ActiveMode())
	}
	if app.QueryInput() != "报告 ext:pdf" {
		t.Fatalf("query text lost on switch to Explorer: got %q", app.QueryInput())
	}
	if host.lastMode != ModeExplorer {
		t.Fatalf("expected host lastMode Explorer, got %v", host.lastMode)
	}
}

func TestAppArrowNavigationAndFileActions(t *testing.T) {
	rows := []ipc.ViewportRow{
		{NodeID: 1, Name: "报告_1.pdf", Path: `C:\docs\报告_1.pdf`},
		{NodeID: 2, Name: "报告_2.pdf", Path: `C:\docs\报告_2.pdf`},
		{NodeID: 3, Name: "报告_3.pdf", Path: `C:\docs\报告_3.pdf`},
	}
	app, _, opener, _ := newTestAppWithRows(t, rows)

	app.ShowLauncher()
	app.SetQueryInput("报告")
	waitForCondition(t, 2*time.Second, func() bool {
		return len(app.SearchSnapshot().Rows) == 3
	})

	// Move down to second row (index 1) and open / reveal.
	app.SelectRow(0)
	app.MoveSelection(1)
	if app.SelectedIndex() != 1 {
		t.Fatalf("expected selected index 1, got %d", app.SelectedIndex())
	}
	if err := app.OpenSelected(); err != nil {
		t.Fatalf("OpenSelected: %v", err)
	}
	if err := app.RevealSelected(); err != nil {
		t.Fatalf("RevealSelected: %v", err)
	}

	if len(opener.opened) != 1 || opener.opened[0] != `C:\docs\报告_2.pdf` {
		t.Fatalf("unexpected opened paths: %v", opener.opened)
	}
	if len(opener.reveals) != 1 || opener.reveals[0] != `C:\docs\报告_2.pdf` {
		t.Fatalf("unexpected revealed paths: %v", opener.reveals)
	}

	// Arrow up clamps at 0.
	app.MoveSelection(-5)
	if app.SelectedIndex() != 0 {
		t.Fatalf("expected clamped index 0, got %d", app.SelectedIndex())
	}
}

func TestAppNoSelectionExecutesNoFileOperation(t *testing.T) {
	app, _, opener, _ := newTestAppWithRows(t, nil)

	app.ShowLauncher()
	app.ClearSelection()
	if err := app.OpenSelected(); err != nil {
		t.Fatalf("OpenSelected with no selection returned error: %v", err)
	}
	if err := app.RevealSelected(); err != nil {
		t.Fatalf("RevealSelected with no selection returned error: %v", err)
	}
	if len(opener.opened) != 0 || len(opener.reveals) != 0 {
		t.Fatalf("expected no file operations without selection, opened=%v reveals=%v", opener.opened, opener.reveals)
	}
}

func TestAppEscapeHidesAndDuplicateInvokeOnlyFocuses(t *testing.T) {
	app, host, _, _ := newTestAppWithRows(t, nil)

	app.ShowLauncher()
	app.ShowLauncher()
	if host.recreateCnt != 1 {
		t.Fatalf("duplicate ShowLauncher must not recreate window, got recreateCnt=%d", host.recreateCnt)
	}
	if host.showCalls != 2 || app.ActiveMode() != ModeLauncher {
		t.Fatalf("expected second ShowLauncher to focus existing window, showCalls=%d mode=%v", host.showCalls, app.ActiveMode())
	}

	app.HandleEscape()
	if app.ActiveMode() != ModeNone || host.hideCalls != 1 {
		t.Fatalf("Escape must hide active window, mode=%v hideCalls=%d", app.ActiveMode(), host.hideCalls)
	}
}

func TestAppTextInputTriggersRealSearch(t *testing.T) {
	rows := []ipc.ViewportRow{
		{NodeID: 1, Name: "报告_1.pdf", Path: `C:\docs\报告_1.pdf`},
	}
	app, _, _, _ := newTestAppWithRows(t, rows)
	app.ShowLauncher()

	tt := ui.NewTester(app.renderView, 760, 480)
	tt.Frame()
	tt.Type("报告")
	tt.Frame()

	waitForCondition(t, 2*time.Second, func() bool {
		return app.QueryInput() == "报告" && len(app.SearchSnapshot().Rows) == 1
	})

	tt.Frame()
	if !tt.HasText("报告_1.pdf") {
		t.Fatalf("expected rendered Launcher view to show search hit, texts=%v", tt.Texts())
	}
}
