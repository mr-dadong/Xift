package spike

import (
	"fmt"
	"strings"
)

// RowData represents a single viewport-rendered virtual file entry.
type RowData struct {
	ID       int
	Name     string
	Dir      string
	Ext      string
	SizeText string
}

type patternTemplate struct {
	dir        string
	stem       string
	ext        string
	searchText string // pre-lowercased dir + " " + stem + "." + ext for O(P) query matching
}

var patternPool = buildPatternPool()

func buildPatternPool() []patternTemplate {
	dirs := []string{
		`D:\Work\项目文档\架构设计`,
		`D:\Work\研发中心\产品需求`,
		`D:\Code\github\Xift\internal\storage`,
		`D:\Code\github\Xift\internal\query`,
		`D:\Code\github\Xift\internal\provider`,
		`C:\Users\dev\Documents\年度报告`,
		`E:\Archive\财务报表\2026`,
		`D:\Workspace\设计规范\UI组件`,
	}
	stems := []struct {
		stem string
		ext  string
	}{
		{"系统设计文档", "md"},
		{"技术架构报告", "pdf"},
		{"季度财务报表", "xlsx"},
		{"核心索引源码", "go"},
		{"性能优化记录", "txt"},
		{"annual_report", "pdf"},
		{"engine_benchmark", "go"},
		{"release_notes", "md"},
	}

	pool := make([]patternTemplate, 0, len(dirs)*len(stems))
	for _, d := range dirs {
		for _, s := range stems {
			full := strings.ToLower(d + " " + s.stem + "." + s.ext)
			pool = append(pool, patternTemplate{
				dir:        d,
				stem:       s.stem,
				ext:        s.ext,
				searchText: full,
			})
		}
	}
	return pool
}

// VirtualDataset provides O(1) deterministic access and zero-allocation-scale filtering
// over millions of synthetic file rows without pre-allocating row strings on the Go heap.
type VirtualDataset struct {
	total           int
	query           string
	matchedPatterns []int // nil when query is empty (all patterns active)
	filteredCount   int
}

// NewVirtualDataset creates a virtual dataset with the specified total number of rows.
func NewVirtualDataset(total int) *VirtualDataset {
	if total < 0 {
		total = 0
	}
	return &VirtualDataset{
		total:         total,
		filteredCount: total,
	}
}

// Total returns the unfiltered dataset size.
func (d *VirtualDataset) Total() int {
	return d.total
}

// Count returns the current number of visible rows matching the active query.
func (d *VirtualDataset) Count() int {
	return d.filteredCount
}

// Query returns the current filter query string.
func (d *VirtualDataset) Query() string {
	return d.query
}

// SetQuery updates the active filter query in O(P) time (P = len(patternPool) = 64)
// without scanning or allocating across the full 3M dataset.
func (d *VirtualDataset) SetQuery(q string) {
	if q == d.query {
		return
	}
	d.query = q
	trimmed := strings.TrimSpace(q)
	if trimmed == "" {
		d.matchedPatterns = nil
		d.filteredCount = d.total
		return
	}

	needle := strings.ToLower(trimmed)
	matched := make([]int, 0, len(patternPool))
	for idx, tpl := range patternPool {
		if strings.Contains(tpl.searchText, needle) {
			matched = append(matched, idx)
		}
	}
	d.matchedPatterns = matched
	if len(matched) == 0 {
		d.filteredCount = 0
		return
	}

	period := len(patternPool)
	fullCycles := d.total / period
	remainder := d.total % period
	count := fullCycles * len(matched)
	for _, slot := range matched {
		if slot < remainder {
			count++
		}
	}
	d.filteredCount = count
}

// Row materializes the display data for visible row index i on demand in O(1) time.
func (d *VirtualDataset) Row(i int) (RowData, bool) {
	if i < 0 || i >= d.filteredCount {
		return RowData{}, false
	}

	var origID int
	if d.matchedPatterns == nil {
		origID = i
	} else {
		m := len(d.matchedPatterns)
		period := len(patternPool)
		origID = (i/m)*period + d.matchedPatterns[i%m]
	}

	tpl := patternPool[origID%len(patternPool)]
	sizeBytes := uint64(((origID*7919 + 104729) % 52428800) + 512)
	return RowData{
		ID:       origID,
		Name:     fmt.Sprintf("%s_%07d.%s", tpl.stem, origID, tpl.ext),
		Dir:      tpl.dir,
		Ext:      tpl.ext,
		SizeText: formatSize(sizeBytes),
	}, true
}

func formatSize(b uint64) string {
	switch {
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(b)/float64(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
