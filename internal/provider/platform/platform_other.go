//go:build !windows && !darwin && !linux

package platform

import (
	"github.com/mr-dadong/Xift/internal/filter"
	"github.com/mr-dadong/Xift/internal/provider"
)

// NewDefaultProvider returns provider.ErrUnsupportedOS on unsupported operating systems.
func NewDefaultProvider(_ *filter.Pruner, _ provider.Options) (provider.FSProvider, error) {
	return nil, provider.ErrUnsupportedOS
}
