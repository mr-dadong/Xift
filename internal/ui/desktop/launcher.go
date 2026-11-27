package desktop

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/egoist/mygo/ui"
)

func (a *App) renderLauncher(c *ui.Context) {
	a.registerCommonShortcuts(c)
	a.syncUIQueryBeforeRender()

	a.mu.Lock()
	wantFocus := a.focusSearchNextFrame
	a.focusSearchNextFrame = false
	selIdx := a.selectedIdx
	warnings := append([]string(nil), a.shortcutWarnings...)
	statusMsg := a.statusMsg
	a.mu.Unlock()

	// Catch edits made by MyGo's TextInput handler between frames.
	a.syncUIQueryAfterRender()

	snap := a.search.Snapshot()
	count := int(snap.Retained)
	if count == 0 && selIdx >= 0 {
		a.ClearSelection()
		selIdx = -1
	} else if count > 0 && selIdx >= count {
		a.SelectRow(count - 1)
		selIdx = count - 1
	}

	firstVis, lastVis := a.listState.Visible()
	a.EnsureViewportLoaded(snap, firstVis, lastVis)

	ui.Column(c).Fill().Padding(14).Gap(10).Background(ui.Hex("#14171f")).Children(func() {
		// Search bar
		ui.Row(c).FillWidth().Gap(8).Children(func() {
			input := ui.TextInput(c, &a.uiQueryBuf).
				Placeholder("搜索文件（支持中文与 DSL，如：报告 ext:pdf）").
				Grow(1)
			if wantFocus {
				input.Focus()
			}
			if ui.Button(c, "在 Explorer 中打开").Clicked() {
				a.syncUIQueryAfterRender()
				a.ShowExplorer()
			}
		})

		a.syncUIQueryAfterRender()

		// Global shortcut conflict banner (only when OS rejected a binding)
		if len(warnings) > 0 {
			ui.Row(c).FillWidth().Padding(6, 10).Radius(6).Background(ui.Hex("#3b2a14")).Children(func() {
				ui.Text(c, strings.Join(warnings, " | ")).FontSize(12).TextColor(ui.Hex("#fcd34d")).SingleLine()
			})
		}

		// Connection / query error banner with retry button
		if snap.Disconnected || snap.Error != "" {
			ui.Row(c).FillWidth().Padding(8, 10).Gap(10).Radius(6).Background(ui.Hex("#3a1c20")).Children(func() {
				msg := snap.Error
				if msg == "" {
					msg = "未连接到本地 Xift Daemon"
				}
				ui.Text(c, msg).Grow(1).FontSize(12).TextColor(ui.Hex("#fca5a5")).SingleLine()
				if ui.Button(c, "重试连接").Clicked() {
					a.RetrySearch()
				}
			})
		}

		// Virtualized result list bounded strictly by snap.Retained
		if count == 0 {
			emptyText := "输入关键词以实时搜索本地索引"
			if snap.Loading {
				emptyText = "正在查询..."
			} else if snap.QueryText != "" && snap.Error == "" && !snap.Disconnected {
				emptyText = "未找到匹配文件"
			}
			ui.Column(c).Fill().Grow(1).Padding(24).Children(func() {
				ui.Text(c, emptyText).FontSize(13).TextColor(ui.Hex("#9ca3af"))
			})
		} else {
			ui.List(c, &a.listState, count, func(i int) {
				row, ok := snap.RowAt(i)
				bg := ui.Hex("#14171f")
				if i == selIdx {
					bg = ui.Hex("#254b73")
				} else if i%2 == 1 {
					bg = ui.Hex("#191d27")
				}

				r := ui.Row(c).FillWidth().Height(30).Padding(0, 10).Gap(10).Background(bg).Children(func() {
					if !ok {
						ui.Text(c, "加载中...").FontSize(12).TextColor(ui.Hex("#6b7280")).SingleLine()
						return
					}
					ui.Text(c, row.Name).Width(260).FontSize(13).TextColor(ui.Hex("#f3f4f6")).SingleLine()
					ui.Text(c, filepath.Dir(row.Path)).Grow(1).FontSize(12).TextColor(ui.Hex("#9ca3af")).SingleLine()
				})
				if r.RowClicked() {
					a.SelectRow(i)
				}
			}).Fill().Grow(1)
		}

		// Compact status / shortcut hint footer
		footer := fmt.Sprintf("↑/↓ 选择 · Enter 打开 · Ctrl+Enter 定位目录 · Esc 隐藏")
		if snap.Retained > 0 {
			if snap.Truncated {
				footer = fmt.Sprintf("可浏览 %d / 共 %d 条（已截断） · %s", snap.Retained, snap.Total, footer)
			} else {
				footer = fmt.Sprintf("共 %d 条 · %s", snap.Retained, footer)
			}
		}
		if statusMsg != "" {
			footer = statusMsg + "  |  " + footer
		}
		ui.Text(c, footer).FontSize(11).TextColor(ui.Hex("#9ca3af")).SingleLine()
	})
}
