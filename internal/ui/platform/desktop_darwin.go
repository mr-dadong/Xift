//go:build darwin

package platform

import (
	"github.com/egoist/mygo"
	trayassets "github.com/mr-dadong/Xift/assets/icons/tray"
)

func detectSessionProtocol() string {
	return "cocoa"
}

func newPlatformAdapter() *Adapter {
	return NewAdapterWithBackends(
		mygoShortcutBackend{},
		mygoTrayBackend{id: "xift-desktop", toolTip: "Xift Desktop Search", templateIcon: true},
		mygoShellBackend{},
	)
}

type mygoShortcutBackend struct{}

func (mygoShortcutBackend) Register(acc, description string, onKeys func(string), fn func()) error {
	return mygo.GlobalShortcut.RegisterWith(acc, mygo.GlobalShortcutOptions{
		Description: description,
		OnKeys:      onKeys,
	}, fn)
}

func (mygoShortcutBackend) UnregisterAll() {
	mygo.GlobalShortcut.UnregisterAll()
}

type mygoTrayBackend struct {
	id           string
	toolTip      string
	templateIcon bool
}

func (b mygoTrayBackend) Create(cb TrayCallbacks) (TrayHandle, error) {
	menu := mygo.NewMenu([]*mygo.MenuItem{
		{
			Label: "Open Launcher",
			Click: func(_ *mygo.MenuItem, _ *mygo.Window) {
				if cb.OnShowLauncher != nil {
					cb.OnShowLauncher()
				}
			},
		},
		{
			Label: "Open Explorer",
			Click: func(_ *mygo.MenuItem, _ *mygo.Window) {
				if cb.OnShowExplorer != nil {
					cb.OnShowExplorer()
				}
			},
		},
		mygo.Separator(),
		{
			Label: "Quit Xift",
			Click: func(_ *mygo.MenuItem, _ *mygo.Window) {
				if cb.OnQuit != nil {
					cb.OnQuit()
				}
			},
		},
	})

	t, err := mygo.NewTray(mygo.TrayOptions{
		ID:             b.id,
		Icon:           trayassets.Dark32PNG,
		IconIsTemplate: b.templateIcon,
		ToolTip:        b.toolTip,
		Menu:           menu,
	})
	if err != nil {
		return nil, err
	}
	t.OnClick(func() {
		if cb.OnShowLauncher != nil {
			cb.OnShowLauncher()
		}
	})
	return t, nil
}

type mygoShellBackend struct{}

func (mygoShellBackend) OpenPath(absPath string) error {
	return mygo.Shell.OpenPath(absPath)
}

func (mygoShellBackend) ShowItemInFolder(absPath string) error {
	mygo.Shell.ShowItemInFolder(absPath)
	return nil
}
