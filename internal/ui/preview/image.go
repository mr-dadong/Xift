package preview

import (
	"bytes"
	"context"
	"fmt"
	"image"
	"image/draw"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"os"
	"path/filepath"

	"github.com/egoist/mygo/ui"
	_ "golang.org/x/image/bmp"
	_ "golang.org/x/image/webp"
)

const (
	maxImageFileBytes = 20 * 1024 * 1024 // 20 MiB raw file cap
	maxImagePixels    = 64_000_000       // 64 MP decompression-bomb pixel cap
)

func isImageExt(ext string) bool {
	switch ext {
	case ".png", ".jpg", ".jpeg", ".gif", ".bmp", ".webp":
		return true
	default:
		return false
	}
}

func loadImagePreview(ctx context.Context, absPath string, totalSize int64, maxSide int) Result {
	if err := ctx.Err(); err != nil {
		return Result{Kind: KindError, Error: err.Error()}
	}
	if totalSize > maxImageFileBytes {
		return Result{
			Kind:    KindBinary,
			Title:   filepath.Base(absPath),
			Summary: fmt.Sprintf("图片过大（%.1f MB，超过 20 MB 预览上限）", float64(totalSize)/(1024*1024)),
		}
	}
	data, err := os.ReadFile(absPath)
	if err != nil {
		return Result{Kind: KindError, Error: fmt.Sprintf("无法读取图片: %v", err)}
	}
	if err := ctx.Err(); err != nil {
		return Result{Kind: KindError, Error: err.Error()}
	}

	cfg, format, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil {
		return Result{Kind: KindError, Error: fmt.Sprintf("图片头解析失败: %v", err)}
	}
	if cfg.Width <= 0 || cfg.Height <= 0 || int64(cfg.Width)*int64(cfg.Height) > maxImagePixels {
		return Result{
			Kind:    KindBinary,
			Title:   filepath.Base(absPath),
			Summary: fmt.Sprintf("图片尺寸过大（%d × %d，超过安全预览像素上限）", cfg.Width, cfg.Height),
		}
	}

	src, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return Result{Kind: KindError, Error: fmt.Sprintf("图片解码失败: %v", err)}
	}
	bounds := src.Bounds()
	origW, origH := bounds.Dx(), bounds.Dy()
	scaled := downscaleImage(src, maxSide)
	sb := scaled.Bounds()

	return Result{
		Kind:    KindImage,
		Title:   filepath.Base(absPath),
		Summary: fmt.Sprintf("图片 (%s) · %d × %d · %d B", format, origW, origH, totalSize),
		Image:   ui.NewBitmap(scaled),
		ImageW:  sb.Dx(),
		ImageH:  sb.Dy(),
	}
}

// downscaleImage scales src proportionally so max(width, height) <= maxSide.
func downscaleImage(src image.Image, maxSide int) *image.RGBA {
	b := src.Bounds()
	w, h := b.Dx(), b.Dy()
	if w <= 0 || h <= 0 {
		return image.NewRGBA(image.Rect(0, 0, 1, 1))
	}
	if w <= maxSide && h <= maxSide {
		dst := image.NewRGBA(image.Rect(0, 0, w, h))
		draw.Draw(dst, dst.Bounds(), src, b.Min, draw.Src)
		return dst
	}

	var nw, nh int
	if w >= h {
		nw = maxSide
		nh = max(1, (h*maxSide)/w)
	} else {
		nh = maxSide
		nw = max(1, (w*maxSide)/h)
	}

	dst := image.NewRGBA(image.Rect(0, 0, nw, nh))
	for y := 0; y < nh; y++ {
		sy := b.Min.Y + (y*h)/nh
		for x := 0; x < nw; x++ {
			sx := b.Min.X + (x*w)/nw
			dst.Set(x, y, src.At(sx, sy))
		}
	}
	return dst
}
