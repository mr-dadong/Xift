//go:build !windows && !darwin

package icons

import "image"

// linuxExtractor provides zero-CGO icon extraction on Linux/BSD with graceful badge fallback.
type linuxExtractor struct{}

func newPlatformExtractor() Extractor {
	return &linuxExtractor{}
}

func (l *linuxExtractor) ExtractExt(ext string, isDir bool) (image.Image, error) {
	return RenderBadge(ext, isDir), nil
}

func (l *linuxExtractor) ExtractFile(absPath string) (image.Image, error) {
	return nil, nil
}
