package desktop

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/egoist/mygo/ui"
	"github.com/mr-dadong/Xift/internal/ipc"
	"github.com/mr-dadong/Xift/internal/ui/platform"
	"github.com/mr-dadong/Xift/internal/ui/preview"
)

type p203MockHost struct {
	mu          sync.Mutex
	menuItems   []ContextMenuItem
	dragPaths   []string
	copiedTexts []string
	invalidates int
	destroyed   int
	lastView    func(c *ui.Context)
}

func (h *p203MockHost) Show(_ WindowMode, _ bool, view func(c *ui.Context), _ func()) {
	h.mu.Lock()
	h.lastView = view
	h.mu.Unlock()
}
func (h *p203MockHost) Hide() {}
func (h *p203MockHost) Destroy() {
	h.mu.Lock()
	h.destroyed++
	h.mu.Unlock()
}
func (h *p203MockHost) Invalidate() {
	h.mu.Lock()
	h.invalidates++
	h.mu.Unlock()
}
func (h *p203MockHost) Update(fn func()) {
	if fn != nil {
		fn()
	}
}
func (h *p203MockHost) GPUStats() GPUContextStats { return GPUContextStats{} }
func (h *p203MockHost) PopupContextMenu(items []ContextMenuItem) {
	h.mu.Lock()
	h.menuItems = append([]ContextMenuItem(nil), items...)
	h.mu.Unlock()
}
func (h *p203MockHost) StartFileDrag(absPath string) error {
	h.mu.Lock()
	h.dragPaths = append(h.dragPaths, absPath)
	h.mu.Unlock()
	return nil
}
func (h *p203MockHost) WriteClipboardText(text string) {
	h.mu.Lock()
	h.copiedTexts = append(h.copiedTexts, text)
	h.mu.Unlock()
}

type p203MockClient struct {
	mu           sync.Mutex
	rows         []ipc.ViewportRow
	recordOpens  []string
	appliedRules []ipc.ApplyFilterRuleReq
	queryCount   int
}

func (c *p203MockClient) QueryStart(_ context.Context, _ string, _ uint8, _ bool, _ byte) (*ipc.QueryResultMeta, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queryCount++
	return &ipc.QueryResultMeta{
		SessionID: uint64(c.queryCount),
		Total:     uint64(len(c.rows)),
		Retained:  uint32(len(c.rows)),
	}, nil
}

func (c *p203MockClient) FetchViewport(_ context.Context, sid uint64, offset uint32, limit uint16) (*ipc.ViewportRows, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	end := int(offset) + int(limit)
	if end > len(c.rows) {
		end = len(c.rows)
	}
	if int(offset) >= len(c.rows) {
		return &ipc.ViewportRows{SessionID: sid, Offset: offset}, nil
	}
	out := make([]ipc.ViewportRow, end-int(offset))
	copy(out, c.rows[offset:end])
	return &ipc.ViewportRows{SessionID: sid, Offset: offset, Rows: out}, nil
}

func (c *p203MockClient) Cancel(_ uint64) error { return nil }
func (c *p203MockClient) Close() error          { return nil }

func (c *p203MockClient) RecordOpen(_ context.Context, path string) error {
	c.mu.Lock()
	c.recordOpens = append(c.recordOpens, path)
	c.mu.Unlock()
	return nil
}

func (c *p203MockClient) ApplyFilterRule(_ context.Context, action uint8, targetPath string, nodeID uint32, volID uint16) (uint32, error) {
	c.mu.Lock()
	c.appliedRules = append(c.appliedRules, ipc.ApplyFilterRuleReq{
		Action:     action,
		TargetPath: targetPath,
		NodeID:     nodeID,
		VolID:      volID,
	})
	c.mu.Unlock()
	return 12, nil
}

type p203MockOpener struct {
	mu       sync.Mutex
	opened   []string
	revealed []string
}

func (o *p203MockOpener) OpenFile(p string) error {
	o.mu.Lock()
	o.opened = append(o.opened, p)
	o.mu.Unlock()
	return nil
}

func (o *p203MockOpener) RevealInFolder(p string) error {
	o.mu.Lock()
	o.revealed = append(o.revealed, p)
	o.mu.Unlock()
	return nil
}

func TestBuildHighlightSpansDefensiveUTF8Slicing(t *testing.T) {
	base := ui.Hex("#ffffff")
	hl := ui.Hex("#fde047")
	bg := ui.Hex("#422006")

	// Multi-byte Chinese filename with valid and out-of-bounds/mid-rune ranges.
	name := "文档架构设计.md"
	spans := BuildHighlightSpans(name, []ipc.HighlightRange{
		{Start: 0, End: 12},   // "文档架构"
		{Start: 13, End: 999}, // Mid-rune start (13 is inside '设' bytes 12..15) and OOB end
	}, base, hl, bg)

	var joined string
	for _, s := range spans {
		joined += s.Text
	}
	if joined != name {
		t.Fatalf("joined spans = %q, want %q (spans=%+v)", joined, name, spans)
	}
	if spans[0].Text != "文档架构" || spans[0].Weight != 600 {
		t.Fatalf("expected first span highlighted '文档架构', got %+v", spans[0])
	}
}

func TestP203EndToEndHighlightsPreviewContextMenuDragAndRecordOpen(t *testing.T) {
	dir := t.TempDir()
	sampleFile := filepath.Join(dir, "dist", "bundle.js")
	if err := os.MkdirAll(filepath.Dir(sampleFile), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sampleFile, []byte("console.log('xift');\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	host := &p203MockHost{}
	opener := &p203MockOpener{}
	cli := &p203MockClient{
		rows: []ipc.ViewportRow{
			{
				NodeID: 101,
				VolID:  1,
				Name:   "bundle.js",
				Path:   sampleFile,
				Highlights: []ipc.HighlightRange{
					{Start: 0, End: 6},
				},
			},
			{
				NodeID: 102,
				VolID:  1,
				Name:   "relative.txt",
				Path:   "relative/not-abs.txt",
			},
		},
	}

	app := NewApp(AppConfig{
		Host:   host,
		Opener: opener,
		Dial: func(ctx context.Context) (SearchClient, error) {
			return cli, nil
		},
		IdleDelay: 30 * time.Second,
	})
	defer app.Quit()

	app.ShowLauncher()
	app.SetQueryInput("bundle")

	// Wait for query rows to populate.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if app.SearchSnapshot().Retained == 2 && len(app.SearchSnapshot().Rows) == 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Render one frame via ui.NewTester to verify RichText + Icon + Preview pane render cleanly.
	tester := ui.NewTester(app.renderView, 900, 520)
	tester.Frame()

	// 1. Toggle Preview on row 0 -> loads real sampleFile non-blockingly.
	app.SelectRow(0)
	app.TogglePreview()
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if app.PreviewSnapshot().Kind == preview.KindText {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if app.PreviewSnapshot().Kind != preview.KindText {
		t.Fatalf("expected preview KindText, got %+v", app.PreviewSnapshot())
	}
	tester.Frame() // Render with preview pane open

	// 2. OpenSelected triggers FileOpener AND async RecordOpen.
	if err := app.OpenSelected(); err != nil {
		t.Fatalf("OpenSelected: %v", err)
	}
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		cli.mu.Lock()
		n := len(cli.recordOpens)
		cli.mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cli.mu.Lock()
	if len(cli.recordOpens) != 1 || cli.recordOpens[0] != sampleFile {
		t.Fatalf("expected RecordOpen(%q), got %+v", sampleFile, cli.recordOpens)
	}
	cli.mu.Unlock()

	// 3. Context menu: verify all 7 actions and trigger Copy Path + Fold Parent Directory.
	app.ShowContextMenuForRow(0)
	host.mu.Lock()
	items := append([]ContextMenuItem(nil), host.menuItems...)
	host.mu.Unlock()

	byID := make(map[string]ContextMenuItem)
	for _, it := range items {
		if it.ID != "" {
			byID[it.ID] = it
		}
	}
	for _, id := range []string{"open", "reveal", "copy-path", "copy-name", "toggle-preview", "fold-dir", "exclude-path"} {
		if _, ok := byID[id]; !ok {
			t.Fatalf("missing context menu item %q", id)
		}
	}
	byID["copy-path"].OnClick()
	host.mu.Lock()
	if len(host.copiedTexts) != 1 || host.copiedTexts[0] != sampleFile {
		t.Fatalf("expected copied path %q, got %+v", sampleFile, host.copiedTexts)
	}
	host.mu.Unlock()

	byID["fold-dir"].OnClick()
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		cli.mu.Lock()
		n := len(cli.appliedRules)
		cli.mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cli.mu.Lock()
	if len(cli.appliedRules) != 1 || cli.appliedRules[0].Action != ipc.FilterActionFold || cli.appliedRules[0].TargetPath != filepath.Dir(sampleFile) {
		t.Fatalf("unexpected appliedRules: %+v", cli.appliedRules)
	}
	cli.mu.Unlock()

	// 4. External file drag-out: valid absolute path succeeds, relative path rejected.
	if err := app.StartDragForRow(0); err != nil {
		t.Fatalf("StartDragForRow(0) failed: %v", err)
	}
	host.mu.Lock()
	if len(host.dragPaths) != 1 || host.dragPaths[0] != sampleFile {
		t.Fatalf("expected drag path %q, got %+v", sampleFile, host.dragPaths)
	}
	host.mu.Unlock()

	if err := app.StartDragForRow(1); err != platform.ErrInvalidPath {
		t.Fatalf("expected ErrInvalidPath for relative row, got %v", err)
	}

	// 5. Root drive parent must disable fold-dir to avoid folding an entire volume.
	rootFile := filepath.Join(filepath.VolumeName(sampleFile)+string(filepath.Separator), "boot.ini")
	rootMenu := app.BuildRowContextMenu(ipc.ViewportRow{Name: "boot.ini", Path: rootFile})
	for _, it := range rootMenu {
		if it.ID == "fold-dir" && !it.Disabled {
			t.Fatalf("expected fold-dir disabled for volume-root file %q", rootFile)
		}
	}
}
