package preview

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"unicode/utf8"
)

func loadTextPreview(ctx context.Context, absPath, ext string, totalSize int64, maxBytes int64) Result {
	if err := ctx.Err(); err != nil {
		return Result{Kind: KindError, Error: err.Error()}
	}
	f, err := os.Open(absPath)
	if err != nil {
		return Result{Kind: KindError, Error: fmt.Sprintf("无法打开文件: %v", err)}
	}
	defer f.Close()

	buf, err := io.ReadAll(io.LimitReader(f, maxBytes+1))
	if err != nil {
		return Result{Kind: KindError, Error: fmt.Sprintf("读取失败: %v", err)}
	}
	if err := ctx.Err(); err != nil {
		return Result{Kind: KindError, Error: err.Error()}
	}

	truncated := int64(len(buf)) > maxBytes
	if truncated {
		buf = buf[:maxBytes]
		// Trim at most 3 trailing bytes ONLY if they form an incomplete multi-byte UTF-8 rune,
		// without draining non-UTF-8 (e.g. GBK / Latin-1) text files to empty.
		buf = trimIncompleteTrailingUTF8(buf)
	}

	sniffLen := len(buf)
	if sniffLen > 4096 {
		sniffLen = 4096
	}
	if bytes.IndexByte(buf[:sniffLen], 0) >= 0 {
		return Result{
			Kind:    KindBinary,
			Title:   filepath.Base(absPath),
			Summary: fmt.Sprintf("二进制文件 · %d B（不显示原始字节）", totalSize),
		}
	}

	kind := KindText
	label := "文本/代码"
	if ext == ".md" || ext == ".markdown" {
		kind = KindMarkdown
		label = "Markdown"
	}
	summary := fmt.Sprintf("%s · %d B", label, totalSize)
	if truncated {
		summary = fmt.Sprintf("%s · %d B（仅预览前 %d KiB）", label, totalSize, maxBytes/1024)
	}
	return Result{
		Kind:      kind,
		Title:     filepath.Base(absPath),
		Summary:   summary,
		Text:      string(buf),
		Truncated: truncated,
	}
}

func trimIncompleteTrailingUTF8(buf []byte) []byte {
	n := len(buf)
	if n == 0 {
		return buf
	}
	// Check at most the last 3 bytes for a UTF-8 lead byte whose rune was cut off.
	for i := 1; i <= 3 && i <= n; i++ {
		tail := buf[n-i:]
		if utf8.RuneStart(tail[0]) {
			if !utf8.FullRune(tail) {
				return buf[:n-i]
			}
			return buf
		}
	}
	return buf
}
