package preview

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func loadArchivePreview(ctx context.Context, absPath string, maxEntries int) Result {
	lower := strings.ToLower(absPath)
	if strings.HasSuffix(lower, ".zip") {
		return loadZipPreview(ctx, absPath, maxEntries)
	}
	return loadTarGzPreview(ctx, absPath, maxEntries)
}

func loadZipPreview(ctx context.Context, absPath string, maxEntries int) Result {
	zr, err := zip.OpenReader(absPath)
	if err != nil {
		return Result{Kind: KindError, Error: fmt.Sprintf("无法解析 ZIP 目录头: %v", err)}
	}
	defer zr.Close()

	total := len(zr.File)
	limit := total
	truncated := false
	if limit > maxEntries {
		limit = maxEntries
		truncated = true
	}

	entries := make([]string, 0, limit)
	for i := 0; i < limit; i++ {
		if (i&63) == 0 && ctx.Err() != nil {
			return Result{Kind: KindError, Error: ctx.Err().Error()}
		}
		f := zr.File[i]
		entries = append(entries, fmt.Sprintf("%s (%d B)", f.Name, f.UncompressedSize64))
	}
	summary := fmt.Sprintf("ZIP 压缩包 · 共 %d 项", total)
	if truncated {
		summary = fmt.Sprintf("ZIP 压缩包 · 共 %d 项（仅展示前 %d 项）", total, maxEntries)
	}
	return Result{
		Kind:      KindArchive,
		Title:     filepath.Base(absPath),
		Summary:   summary,
		Entries:   entries,
		Truncated: truncated,
	}
}

const maxTarGzDecompressedBytes = 16 * 1024 * 1024 // 16 MiB decompressed cap

type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c *ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

func loadTarGzPreview(ctx context.Context, absPath string, maxEntries int) Result {
	f, err := os.Open(absPath)
	if err != nil {
		return Result{Kind: KindError, Error: fmt.Sprintf("无法打开压缩包: %v", err)}
	}
	defer f.Close()

	gz, err := gzip.NewReader(&ctxReader{ctx: ctx, r: f})
	if err != nil {
		return Result{Kind: KindError, Error: fmt.Sprintf("无法解析 gzip 头: %v", err)}
	}
	defer gz.Close()

	bounded := &ctxReader{ctx: ctx, r: io.LimitReader(gz, maxTarGzDecompressedBytes)}
	tr := tar.NewReader(bounded)
	entries := make([]string, 0, min(maxEntries, 64))
	truncated := false
	for {
		if err := ctx.Err(); err != nil {
			if len(entries) > 0 {
				truncated = true
				break
			}
			return Result{Kind: KindError, Error: err.Error()}
		}
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			if len(entries) > 0 {
				truncated = true
				break
			}
			return Result{Kind: KindError, Error: fmt.Sprintf("无法解析 tar 目录头: %v", err)}
		}
		if len(entries) >= maxEntries {
			truncated = true
			break
		}
		entries = append(entries, fmt.Sprintf("%s (%d B)", hdr.Name, hdr.Size))
	}
	summary := fmt.Sprintf("TAR.GZ 压缩包 · %d 项", len(entries))
	if truncated {
		summary = fmt.Sprintf("TAR.GZ 压缩包 · 仅展示前 %d 项（已截断）", len(entries))
	}
	return Result{
		Kind:      KindArchive,
		Title:     filepath.Base(absPath),
		Summary:   summary,
		Entries:   entries,
		Truncated: truncated,
	}
}
