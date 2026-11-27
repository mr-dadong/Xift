//go:build windows

package platform

import (
	"github.com/mr-dadong/Xift/internal/filter"
	"github.com/mr-dadong/Xift/internal/provider"
	"github.com/mr-dadong/Xift/internal/provider/windows"
)

// NewDefaultProvider returns the native Pure Go Windows FSProvider.
func NewDefaultProvider(pruner *filter.Pruner, opts provider.Options) (provider.FSProvider, error) {
	return windows.New(pruner, opts), nil
}
