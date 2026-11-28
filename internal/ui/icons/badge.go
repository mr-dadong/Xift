package icons

import (
	"image"
	"image/color"
	"strings"
)

const iconSize = 20

// RenderBadge renders a crisp 20x20 pure-Go RGBA fallback icon color-coded by file type.
func RenderBadge(ext string, isDir bool) *image.RGBA {
	img := image.NewRGBA(image.Rect(0, 0, iconSize, iconSize))
	bg := badgeColor(ext, isDir)
	border := darkenColor(bg, 0.72)

	if isDir {
		// Draw a recognizable folder silhouette with top tab.
		for y := 3; y < 17; y++ {
			for x := 2; x < 18; x++ {
				if y < 6 && x >= 10 {
					continue
				}
				isEdge := y == 3 || y == 16 || x == 2 || x == 17 || (y == 5 && x >= 10) || (y < 6 && x == 9)
				if isEdge {
					img.SetRGBA(x, y, border)
				} else {
					img.SetRGBA(x, y, bg)
				}
			}
		}
		return img
	}

	// Rounded document tile with a folded top-right corner.
	for y := 2; y < 18; y++ {
		for x := 3; x < 17; x++ {
			// Cut top-right corner for document fold effect.
			if x >= 13 && y <= 5 && (x-13)+(5-y) > 2 {
				continue
			}
			isEdge := y == 2 || y == 17 || x == 3 || x == 16 || (x >= 13 && y <= 5 && (x-13)+(5-y) == 2)
			if isEdge {
				img.SetRGBA(x, y, border)
			} else if y >= 7 && y <= 8 && x >= 6 && x <= 13 {
				img.SetRGBA(x, y, color.RGBA{R: 255, G: 255, B: 255, A: 220})
			} else if y >= 11 && y <= 12 && x >= 6 && x <= 11 {
				img.SetRGBA(x, y, color.RGBA{R: 255, G: 255, B: 255, A: 185})
			} else {
				img.SetRGBA(x, y, bg)
			}
		}
	}
	return img
}

func badgeColor(ext string, isDir bool) color.RGBA {
	if isDir {
		return color.RGBA{R: 245, G: 158, B: 11, A: 255} // Amber folder
	}
	switch strings.ToLower(strings.TrimPrefix(ext, ".")) {
	case "go", "rs", "ts", "tsx", "js", "jsx", "py", "c", "h", "cpp", "java", "kt", "swift", "sh", "ps1":
		return color.RGBA{R: 56, G: 189, B: 248, A: 255} // Sky code
	case "md", "txt", "log", "json", "yaml", "yml", "toml", "xml", "csv", "ini":
		return color.RGBA{R: 99, G: 102, B: 241, A: 255} // Indigo text/doc
	case "pdf":
		return color.RGBA{R: 239, G: 68, B: 68, A: 255} // Red PDF
	case "png", "jpg", "jpeg", "gif", "webp", "bmp", "svg", "ico":
		return color.RGBA{R: 34, G: 197, B: 94, A: 255} // Green image
	case "zip", "tar", "gz", "tgz", "7z", "rar", "xz":
		return color.RGBA{R: 168, G: 85, B: 247, A: 255} // Purple archive
	case "exe", "msi", "app", "dmg", "deb", "rpm", "lnk":
		return color.RGBA{R: 249, G: 115, B: 22, A: 255} // Orange binary
	case "mp3", "wav", "flac", "mp4", "mkv", "mov", "avi":
		return color.RGBA{R: 236, G: 72, B: 153, A: 255} // Pink media
	default:
		return color.RGBA{R: 100, G: 116, B: 139, A: 255} // Slate generic
	}
}

func darkenColor(c color.RGBA, factor float64) color.RGBA {
	return color.RGBA{
		R: uint8(float64(c.R) * factor),
		G: uint8(float64(c.G) * factor),
		B: uint8(float64(c.B) * factor),
		A: c.A,
	}
}
