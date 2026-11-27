//go:build linux

package platform

import (
	"github.com/mr-dadong/Xift/internal/filter"
	"github.com/mr-dadong/Xift/internal/provider"
	"github.com/mr-dadong/Xift/internal/provider/linux"
)

// NewDefaultProvider returns the native Pure Go Linux FSProvider.
func NewDefaultProvider(pruner *filter.Pruner, opts provider.Options) (provider.FSProvider, error) {
	return linux.New(pruner, opts), nil
}
