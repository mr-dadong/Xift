package desktop

import (
	"strings"

	"github.com/egoist/mygo/ui"
	"github.com/mr-dadong/Xift/internal/ui/preview"
)

const maxPreviewRenderLines = 180

func renderPreviewPane(c *ui.Context, res preview.Result) {
	ui.Column(c).Width(320).FillHeight().Padding(10).Gap(8).Radius(6).Background(ui.Hex("#11141b")).Children(func() {
		title := res.Title
		if title == "" {
			title = "预览"
		}
		ui.Text(c, title).FontSize(13).TextColor(ui.Hex("#f3f4f6")).SingleLine()
		if res.Summary != "" {
			ui.Text(c, res.Summary).FontSize(11).TextColor(ui.Hex("#9ca3af")).SingleLine()
		}

		switch res.Kind {
		case preview.KindNone:
			ui.Text(c, "选择文件以查看预览（Tab 开关）").FontSize(12).TextColor(ui.Hex("#6b7280"))
		case preview.KindLoading:
			ui.Text(c, "正在加载预览...").FontSize(12).TextColor(ui.Hex("#93c5fd"))
		case preview.KindError:
			ui.Text(c, res.Error).FontSize(12).TextColor(ui.Hex("#fca5a5"))
		case preview.KindBinary:
			ui.Text(c, "二进制或超大文件不显示正文").FontSize(12).TextColor(ui.Hex("#9ca3af"))
		case preview.KindImage:
			if res.Image != nil {
				w := float32(res.ImageW)
				h := float32(res.ImageH)
				const maxW float32 = 296
				const maxH float32 = 340
				if w > maxW && w > 0 {
					h = h * (maxW / w)
					w = maxW
				}
				if h > maxH && h > 0 {
					w = w * (maxH / h)
					h = maxH
				}
				if w < 1 {
					w = 1
				}
				if h < 1 {
					h = 1
				}
				ui.Image(c, res.Image).Size(w, h)
			}
		case preview.KindArchive, preview.KindDir:
			limit := len(res.Entries)
			if limit > maxPreviewRenderLines {
				limit = maxPreviewRenderLines
			}
			ui.Column(c).Fill().Grow(1).Gap(2).Children(func() {
				for i := 0; i < limit; i++ {
					ui.Text(c, res.Entries[i]).FontSize(11).TextColor(ui.Hex("#d1d5db")).SingleLine()
				}
				if len(res.Entries) > limit {
					ui.Text(c, "...").FontSize(11).TextColor(ui.Hex("#6b7280")).SingleLine()
				}
			})
		case preview.KindText, preview.KindMarkdown, preview.KindPDF:
			lines := strings.Split(res.Text, "\n")
			if len(lines) > maxPreviewRenderLines {
				lines = lines[:maxPreviewRenderLines]
			}
			ui.Column(c).Fill().Grow(1).Gap(1).Children(func() {
				for _, ln := range lines {
					ln = strings.TrimRight(ln, "\r")
					ui.Text(c, ln).FontSize(11).TextColor(ui.Hex("#e5e7eb")).SingleLine()
				}
			})
		}
	})
}
