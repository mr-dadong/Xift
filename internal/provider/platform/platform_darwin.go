//go:build darwin

package platform

import (
	"github.com/mr-dadong/Xift/internal/filter"
	"github.com/mr-dadong/Xift/internal/provider"
	"github.com/mr-dadong/Xift/internal/provider/darwin"
)

// NewDefaultProvider returns the native Pure Go macOS FSProvider.
func NewDefaultProvider(pruner *filter.Pruner, opts provider.Options) (provider.FSProvider, error) {
	return darwin.New(pruner, opts), nil
}
