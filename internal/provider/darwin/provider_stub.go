//go:build !darwin

package darwin

import (
	"context"

	"github.com/mr-dadong/Xift/internal/filter"
	"github.com/mr-dadong/Xift/internal/provider"
)

// Provider is a non-macOS compile-time stub returning provider.ErrUnsupportedOS.
type Provider struct{}

// New creates a stub macOS Provider on non-macOS platforms.
func New(_ *filter.Pruner, _ provider.Options) *Provider {
	return &Provider{}
}

func (p *Provider) EnumerateVolumes(context.Context) ([]provider.VolumeInfo, error) {
	return nil, provider.ErrUnsupportedOS
}

func (p *Provider) FullScan(context.Context, provider.VolumeInfo, provider.SegmentWriter) (uint64, error) {
	return 0, provider.ErrUnsupportedOS
}

func (p *Provider) Watch(context.Context, provider.VolumeInfo, uint64, chan<- provider.FileEvent) error {
	return provider.ErrUnsupportedOS
}

func (p *Provider) ScanSubtree(context.Context, provider.VolumeInfo, string, uint64, provider.SegmentWriter) error {
	return provider.ErrUnsupportedOS
}

func (p *Provider) ScanSubtreeWithRoot(context.Context, provider.VolumeInfo, string, provider.SegmentWriter) error {
	return provider.ErrUnsupportedOS
}
