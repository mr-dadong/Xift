//go:build darwin

package icons

import "image"

// darwinExtractor provides zero-CGO icon extraction on macOS with graceful badge fallback.
type darwinExtractor struct{}

func newPlatformExtractor() Extractor {
	return &darwinExtractor{}
}

func (d *darwinExtractor) ExtractExt(ext string, isDir bool) (image.Image, error) {
	return RenderBadge(ext, isDir), nil
}

func (d *darwinExtractor) ExtractFile(absPath string) (image.Image, error) {
	return nil, nil
}
