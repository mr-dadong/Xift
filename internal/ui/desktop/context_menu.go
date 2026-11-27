package desktop

import (
	"fmt"
	"path/filepath"

	"github.com/mr-dadong/Xift/internal/ipc"
	"github.com/mr-dadong/Xift/internal/storage"
	"github.com/mr-dadong/Xift/internal/ui/platform"
)

// ContextMenuItem describes one entry in the cross-platform native popup context menu.
type ContextMenuItem struct {
	ID        string
	Label     string
	Separator bool
	Disabled  bool
	OnClick   func()
}

// ContextMenuHost is an optional WindowHost extension for showing native popup menus.
type ContextMenuHost interface {
	PopupContextMenu(items []ContextMenuItem)
}

// DragHost is an optional WindowHost extension for initiating native external file drags.
type DragHost interface {
	StartFileDrag(absPath string) error
}

// ClipboardHost is an optional WindowHost extension for writing plain text to the OS clipboard.
type ClipboardHost interface {
	WriteClipboardText(text string)
}

// BuildRowContextMenu constructs the 7 core actions for a search result row.
func (a *App) BuildRowContextMenu(row ipc.ViewportRow) []ContextMenuItem {
	isDir := (row.Flags & storage.FlagDir) != 0
	foldTarget := filepath.Dir(row.Path)
	foldLabel := "折叠所属目录"
	var foldNodeID uint32
	if isDir {
		foldTarget = row.Path
		foldLabel = "折叠此目录"
		foldNodeID = row.NodeID
	}
	foldDisabled := foldTarget == "" || foldTarget == "." || foldTarget == filepath.Dir(foldTarget)

	previewLabel := "打开预览 (Tab)"
	if a.PreviewOpen() {
		previewLabel = "关闭预览 (Tab)"
	}

	return []ContextMenuItem{
		{
			ID:    "open",
			Label: "打开",
			OnClick: func() {
				_ = a.OpenSelected()
			},
		},
		{
			ID:    "reveal",
			Label: "在资源管理器中显示",
			OnClick: func() {
				_ = a.RevealSelected()
			},
		},
		{
			ID:    "copy-path",
			Label: "复制完整路径",
			OnClick: func() {
				a.CopyText(row.Path, "已复制路径")
			},
		},
		{
			ID:    "copy-name",
			Label: "复制文件名",
			OnClick: func() {
				a.CopyText(row.Name, "已复制文件名")
			},
		},
		{Separator: true},
		{
			ID:    "toggle-preview",
			Label: previewLabel,
			OnClick: func() {
				a.TogglePreview()
			},
		},
		{Separator: true},
		{
			ID:       "fold-dir",
			Label:    foldLabel,
			Disabled: foldDisabled,
			OnClick: func() {
				a.applyGovernanceRule(ipc.FilterActionFold, foldTarget, foldNodeID, row.VolID, "已折叠目录")
			},
		},
		{
			ID:       "exclude-path",
			Label:    "永久排除此路径",
			Disabled: row.Path == "",
			OnClick: func() {
				a.applyGovernanceRule(ipc.FilterActionHardExclude, row.Path, row.NodeID, row.VolID, "已永久排除")
			},
		},
	}
}

// ShowContextMenuForRow selects idx and displays the native popup context menu for that row.
func (a *App) ShowContextMenuForRow(idx int) {
	a.SelectRow(idx)
	row, ok := a.selectedRow()
	if !ok {
		return
	}
	cmh, ok := a.host.(ContextMenuHost)
	if !ok || cmh == nil {
		return
	}
	cmh.PopupContextMenu(a.BuildRowContextMenu(row))
}

// StartDragForRow validates the row's local path and initiates a native OS external file drag.
func (a *App) StartDragForRow(idx int) error {
	a.SelectRow(idx)
	row, ok := a.selectedRow()
	if !ok {
		return platform.ErrInvalidPath
	}
	cleanPath, err := platform.ValidateAbsolutePath(row.Path)
	if err != nil {
		return err
	}
	dh, ok := a.host.(DragHost)
	if !ok || dh == nil {
		return nil
	}
	return dh.StartFileDrag(cleanPath)
}

// CopyText copies text to the system clipboard via the WindowHost if supported.
func (a *App) CopyText(text, successMsg string) {
	if text == "" {
		return
	}
	if ch, ok := a.host.(ClipboardHost); ok && ch != nil {
		ch.WriteClipboardText(text)
		a.setStatus(successMsg)
	}
}

func (a *App) applyGovernanceRule(action uint8, targetPath string, nodeID uint32, volID uint16, okPrefix string) {
	a.setStatus("正在应用规则...")
	a.search.ApplyFilterRule(action, targetPath, nodeID, volID, func(affected uint32, err error) {
		if err != nil {
			a.setStatus("规则应用失败: " + err.Error())
			return
		}
		a.setStatus(fmt.Sprintf("%s: %s（清理 %d 项）", okPrefix, filepath.Base(targetPath), affected))
		a.search.Reconnect()
	})
}
