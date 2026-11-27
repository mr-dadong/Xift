package main

import (
	"fmt"
	"log"
	"time"

	"github.com/egoist/mygo"
	"github.com/egoist/mygo/ui"
	"github.com/mr-dadong/Xift/internal/ui/spike"
)

const defaultRowCount = 3_000_000

type spikeApp struct {
	dataset   *spike.VirtualDataset
	metrics   *spike.MetricsTracker
	listState ui.ListState

	query       string
	lastQuery   string
	selectedRow int
}

func newSpikeApp() *spikeApp {
	app := &spikeApp{
		dataset:     spike.NewVirtualDataset(defaultRowCount),
		metrics:     spike.NewMetricsTracker(250 * time.Millisecond),
		selectedRow: -1,
	}
	app.listState.RowHeight = 28
	return app
}

func (a *spikeApp) view(c *ui.Context) {
	a.metrics.RecordFrame(time.Now())

	if a.query != a.lastQuery {
		a.dataset.SetQuery(a.query)
		a.lastQuery = a.query
		a.selectedRow = -1
		a.listState.ScrollTo(0, ui.Start)
	}

	count := a.dataset.Count()
	snap := a.metrics.Snapshot()
	firstVis, lastVis := a.listState.Visible()

	ui.Column(c).Fill().Padding(12).Gap(8).Background(ui.Hex("#16181d")).Children(func() {
		// Top search & jump toolbar
		ui.Row(c).FillWidth().Gap(8).Children(func() {
			ui.TextInput(c, &a.query).
				Placeholder("输入中文/英文测试 IME 与实时过滤（如：设计文档、财务报表、report）").
				Grow(1)

			if ui.Button(c, "清空").Clicked() {
				a.query = ""
			}
			if ui.Button(c, "顶部 #0").Clicked() && count > 0 {
				a.listState.ScrollTo(0, ui.Start)
			}
			if ui.Button(c, "中部 #150万").Clicked() && count > 0 {
				a.listState.ScrollTo(count/2, ui.Start)
			}
			if ui.Button(c, "底部 #300万").Clicked() && count > 0 {
				a.listState.ScrollToEnd()
			}
		})

		// Real-time HUD telemetry bar
		hudText := fmt.Sprintf(
			"命中: %d / %d   |   视口行: [%d .. %d]   |   FPS: %.1f (帧耗时 %.2f ms)   |   HeapAlloc: %.1f MB / Sys: %.1f MB",
			count,
			a.dataset.Total(),
			firstVis,
			lastVis,
			snap.FPS,
			snap.LastFrameMs,
			float64(snap.HeapAllocBytes)/(1<<20),
			float64(snap.SysBytes)/(1<<20),
		)
		ui.Row(c).FillWidth().Padding(6, 10).Radius(6).Background(ui.Hex("#20242c")).Children(func() {
			ui.Text(c, hudText).FontSize(12).TextColor(ui.Hex("#93c5fd")).SingleLine()
		})

		// Column header
		ui.Row(c).FillWidth().Padding(4, 10).Gap(12).Background(ui.Hex("#1d2028")).Children(func() {
			ui.Text(c, "行号").Width(88).FontSize(12).TextColor(ui.Hex("#9ca3af")).SingleLine()
			ui.Text(c, "文件名").Width(280).FontSize(12).TextColor(ui.Hex("#9ca3af")).SingleLine()
			ui.Text(c, "目录路径").Grow(1).FontSize(12).TextColor(ui.Hex("#9ca3af")).SingleLine()
			ui.Text(c, "大小").Width(80).FontSize(12).TextColor(ui.Hex("#9ca3af")).SingleLine()
		})

		// Virtualized 3M row list
		ui.List(c, &a.listState, count, func(i int) {
			row, ok := a.dataset.Row(i)
			if !ok {
				return
			}
			bg := ui.Hex("#16181d")
			if i == a.selectedRow {
				bg = ui.Hex("#264f78")
			} else if i%2 == 1 {
				bg = ui.Hex("#1a1d24")
			}

			r := ui.Row(c).FillWidth().Height(28).Padding(0, 10).Gap(12).Background(bg).Children(func() {
				ui.Text(c, fmt.Sprintf("#%07d", row.ID)).Width(88).FontSize(12).TextColor(ui.Hex("#6b7280")).SingleLine()
				ui.Text(c, row.Name).Width(280).FontSize(13).TextColor(ui.Hex("#f3f4f6")).SingleLine()
				ui.Text(c, row.Dir).Grow(1).FontSize(12).TextColor(ui.Hex("#9ca3af")).SingleLine()
				ui.Text(c, row.SizeText).Width(80).FontSize(12).TextColor(ui.Hex("#d1d5db")).SingleLine()
			})
			if r.RowClicked() {
				a.selectedRow = i
			}
		}).Fill().Grow(1)

		// Footer hint / selected row echo
		footer := "提示：请拖动滚动条或点击顶部/中部/底部跳转测试 60 FPS 与末尾行精度；在搜索框切换微软拼音测试中文 IME 组合态与回删。"
		if a.selectedRow >= 0 {
			if row, ok := a.dataset.Row(a.selectedRow); ok {
				footer = fmt.Sprintf("已选中可见第 %d 行 -> #%07d  %s\\%s (%s)", a.selectedRow, row.ID, row.Dir, row.Name, row.SizeText)
			}
		}
		ui.Text(c, footer).FontSize(12).TextColor(ui.Hex("#9ca3af")).SingleLine()
	})
}

func main() {
	app := newSpikeApp()

	mygo.App.WhenReady(func() {
		mygo.NewWindow(mygo.WindowOptions{
			Title:  "Xift P0-01 Spike — MyGo ui.List 300万虚拟列表 & 中文 IME 压测",
			Width:  1120,
			Height: 720,
			Content: ui.View(app.view),
		})
	})

	if err := mygo.App.Run(); err != nil {
		log.Fatal(err)
	}
}
