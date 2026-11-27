package preview

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const pdfInspectChunk = 64 * 1024

func loadPDFPreview(ctx context.Context, absPath string, totalSize int64) Result {
	if err := ctx.Err(); err != nil {
		return Result{Kind: KindError, Error: err.Error()}
	}
	f, err := os.Open(absPath)
	if err != nil {
		return Result{Kind: KindError, Error: fmt.Sprintf("无法打开 PDF: %v", err)}
	}
	defer f.Close()

	head, err := io.ReadAll(io.LimitReader(f, pdfInspectChunk))
	if err != nil {
		return Result{Kind: KindError, Error: fmt.Sprintf("读取 PDF 头失败: %v", err)}
	}
	if err := ctx.Err(); err != nil {
		return Result{Kind: KindError, Error: err.Error()}
	}

	var tail []byte
	if totalSize > pdfInspectChunk {
		off := totalSize - pdfInspectChunk
		if off < 0 {
			off = 0
		}
		if _, err := f.Seek(off, io.SeekStart); err == nil {
			tail, _ = io.ReadAll(io.LimitReader(f, pdfInspectChunk))
		}
	}

	version := parsePDFVersion(head)
	pages := parsePDFPageCount(head)
	if pages == 0 && len(tail) > 0 {
		pages = parsePDFPageCount(tail)
	}
	title := parsePDFInfoField(head, "/Title")
	if title == "" && len(tail) > 0 {
		title = parsePDFInfoField(tail, "/Title")
	}
	author := parsePDFInfoField(head, "/Author")
	if author == "" && len(tail) > 0 {
		author = parsePDFInfoField(tail, "/Author")
	}

	var lines []string
	if version != "" {
		lines = append(lines, "PDF 版本: "+version)
	}
	if pages > 0 {
		lines = append(lines, fmt.Sprintf("页数估计: %d 页", pages))
	}
	if title != "" {
		lines = append(lines, "标题: "+title)
	}
	if author != "" {
		lines = append(lines, "作者: "+author)
	}
	lines = append(lines, fmt.Sprintf("大小: %d B", totalSize))

	summary := "PDF 文档"
	if pages > 0 {
		summary = fmt.Sprintf("PDF %s · %d 页 · %d B", version, pages, totalSize)
	} else if version != "" {
		summary = fmt.Sprintf("PDF %s · %d B", version, totalSize)
	}
	return Result{
		Kind:    KindPDF,
		Title:   filepath.Base(absPath),
		Summary: summary,
		Text:    strings.Join(lines, "\n"),
	}
}

func parsePDFVersion(data []byte) string {
	idx := bytes.Index(data, []byte("%PDF-"))
	if idx < 0 || idx+8 > len(data) {
		return ""
	}
	end := idx + 5
	for end < len(data) && end < idx+12 && (data[end] == '.' || (data[end] >= '0' && data[end] <= '9')) {
		end++
	}
	return string(data[idx+5 : end])
}

func parsePDFPageCount(data []byte) int {
	key := []byte("/Count")
	best := 0
	for off := 0; off < len(data); {
		idx := bytes.Index(data[off:], key)
		if idx < 0 {
			break
		}
		p := off + idx + len(key)
		for p < len(data) && (data[p] == ' ' || data[p] == '\t' || data[p] == '\r' || data[p] == '\n') {
			p++
		}
		start := p
		for p < len(data) && data[p] >= '0' && data[p] <= '9' {
			p++
		}
		if p > start {
			if n, err := strconv.Atoi(string(data[start:p])); err == nil && n > best {
				best = n
			}
		}
		off = p + 1
	}
	return best
}

func parsePDFInfoField(data []byte, key string) string {
	idx := bytes.Index(data, []byte(key))
	if idx < 0 {
		return ""
	}
	p := idx + len(key)
	for p < len(data) && (data[p] == ' ' || data[p] == '\t') {
		p++
	}
	if p >= len(data) || data[p] != '(' {
		return ""
	}
	p++
	var out []byte
	for p < len(data) && len(out) < 256 {
		if data[p] == '\\' && p+1 < len(data) {
			p++
			out = append(out, data[p])
			p++
			continue
		}
		if data[p] == ')' {
			break
		}
		out = append(out, data[p])
		p++
	}
	return strings.TrimSpace(string(out))
}
