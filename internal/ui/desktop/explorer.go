package desktop

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/egoist/mygo/transfer"
	"github.com/egoist/mygo/ui"
	"github.com/mr-dadong/Xift/internal/query"
	"github.com/mr-dadong/Xift/internal/storage"
	"github.com/mr-dadong/Xift/internal/ui/platform"
)

func (a *App) renderExplorer(c *ui.Context) {
	a.registerCommonShortcuts(c)
	a.syncUIQueryBeforeRender()

	a.mu.Lock()
	wantFocus := a.focusSearchNextFrame
	a.focusSearchNextFrame = false
	selIdx := a.selectedIdx
	curSort := a.sortOrder
	curDesc := a.sortDesc
	previewOpen := a.previewOpen
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
	a.notifyVisibleIcons(snap, firstVis, lastVis)

	ui.Column(c).Fill().Padding(12).Gap(8).Background(ui.Hex("#16181d")).Children(func() {
		// Top search & DSL bar
		ui.Row(c).FillWidth().Gap(8).Children(func() {
			input := ui.TextInput(c, &a.uiQueryBuf).
				Placeholder("搜索或 DSL 筛选（例：wdjg ext:pdf size:>1mb dm:7d !temp）").
				Grow(1)
			if wantFocus {
				input.Focus()
			}
			if ui.Button(c, "打开").Clicked() {
				_ = a.OpenSelected()
			}
			if ui.Button(c, "定位目录").Clicked() {
				_ = a.RevealSelected()
			}
			previewBtn := "预览"
			if previewOpen {
				previewBtn = "收起预览"
			}
			if ui.Button(c, previewBtn).Clicked() {
				a.TogglePreview()
			}
			if ui.Button(c, "快搜窗").Clicked() {
				a.syncUIQueryAfterRender()
				a.ShowLauncher()
			}
		})

		a.syncUIQueryAfterRender()

		// Sort controls
		ui.Row(c).FillWidth().Gap(6).Children(func() {
			ui.Text(c, "排序:").FontSize(12).TextColor(ui.Hex("#9ca3af")).SingleLine()
			sortButtons := []struct {
				label string
				order uint8
			}{
				{"相关度", uint8(query.SortByRelevance)},
				{"名称", uint8(query.SortByName)},
				{"大小", uint8(query.SortBySize)},
				{"修改时间", uint8(query.SortByModTime)},
			}
			for _, sb := range sortButtons {
				lbl := sb.label
				if curSort == sb.order {
					if curDesc {
						lbl += " ↓"
					} else {
						lbl += " ↑"
					}
				}
				if ui.Button(c, lbl).Clicked() {
					if curSort == sb.order {
						a.SetSort(sb.order, !curDesc)
					} else {
						a.SetSort(sb.order, false)
					}
				}
			}
		})

		if len(warnings) > 0 {
			ui.Row(c).FillWidth().Padding(6, 10).Radius(6).Background(ui.Hex("#3b2a14")).Children(func() {
				ui.Text(c, strings.Join(warnings, " | ")).FontSize(12).TextColor(ui.Hex("#fcd34d")).SingleLine()
			})
		}

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

		// Main split body: 4-column table + optional non-blocking preview pane
		ui.Row(c).Fill().Grow(1).Gap(10).Children(func() {
			ui.Column(c).Fill().Grow(1).Gap(4).Children(func() {
				// Four-column header: Name, Path, Size, ModTime
				ui.Row(c).FillWidth().Padding(5, 10).Gap(12).Background(ui.Hex("#1f232c")).Children(func() {
					ui.Text(c, "名称").Width(240).FontSize(12).TextColor(ui.Hex("#9ca3af")).SingleLine()
					ui.Text(c, "路径").Grow(1).FontSize(12).TextColor(ui.Hex("#9ca3af")).SingleLine()
					ui.Text(c, "大小").Width(92).FontSize(12).TextColor(ui.Hex("#9ca3af")).SingleLine()
					ui.Text(c, "修改时间").Width(136).FontSize(12).TextColor(ui.Hex("#9ca3af")).SingleLine()
				})

				// Virtualized 4-column list bounded strictly by snap.Retained
				if count == 0 {
					emptyText := "输入关键词或 DSL 条件以检索文件"
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
						bg := ui.Hex("#16181d")
						if i == selIdx {
							bg = ui.Hex("#264f78")
						} else if i%2 == 1 {
							bg = ui.Hex("#1a1d24")
						}

						r := ui.Row(c).FillWidth().Height(30).Padding(0, 10).Gap(10).Background(bg).Children(func() {
							if !ok {
								ui.Text(c, "加载视口中...").FontSize(12).TextColor(ui.Hex("#6b7280")).SingleLine()
								return
							}
							isDir := (row.Flags & storage.FlagDir) != 0
							if bmp := a.icons.IconFor(row.Name, row.Path, isDir); bmp != nil {
								ui.Image(c, bmp).Size(18, 18)
							}
							spans := BuildHighlightSpans(row.Name, row.Highlights, ui.Hex("#f3f4f6"), ui.Hex("#fde047"), ui.Hex("#422006"))
							ui.RichText(c, spans...).Width(220).FontSize(13).SingleLine()
							ui.Text(c, filepath.Dir(row.Path)).Grow(1).FontSize(12).TextColor(ui.Hex("#9ca3af")).SingleLine()
							ui.Text(c, formatSize(row.Size, isDir)).Width(92).FontSize(12).TextColor(ui.Hex("#d1d5db")).SingleLine()
							ui.Text(c, formatModTime(row.ModTime)).Width(136).FontSize(12).TextColor(ui.Hex("#9ca3af")).SingleLine()
						})
						if ok {
							if cleanPath, err := platform.ValidateAbsolutePath(row.Path); err == nil {
								dragPath := cleanPath
								r.DragDataFrom(func() transfer.Data {
									d, err := transfer.FileData(dragPath)
									if err != nil {
										return transfer.Data{}
									}
									return d
								})
							}
						}
						if r.RowClicked() {
							a.SelectRow(i)
						}
						if r.RowDoubleClicked() {
							a.SelectRow(i)
							_ = a.OpenSelected()
						}
						if r.RightClicked() {
							a.ShowContextMenuForRow(i)
						}
					}).Fill().Grow(1)
				}
			})

			if previewOpen {
				renderPreviewPane(c, a.previewMgr.Snapshot())
			}
		})

		// Footer status bar
		summary := fmt.Sprintf("可浏览: %d 条", snap.Retained)
		if snap.Truncated {
			summary = fmt.Sprintf("可浏览: %d / 总命中: %d 条（仅保留前 %d 条）", snap.Retained, snap.Total, snap.Retained)
		} else if snap.Total > 0 {
			summary = fmt.Sprintf("共 %d 条 | 耗时 %.2f ms | 当前页 [%d..%d] | Tab 预览 · 右键菜单",
				snap.Retained,
				float64(snap.CostMicros)/1000.0,
				snap.PageOffset,
				snap.PageOffset+uint32(len(snap.Rows)),
			)
		}
		if statusMsg != "" {
			summary = statusMsg + "  |  " + summary
		}
		ui.Text(c, summary).FontSize(12).TextColor(ui.Hex("#9ca3af")).SingleLine()
	})
}
