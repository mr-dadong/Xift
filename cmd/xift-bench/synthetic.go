package main

import "fmt"

// syntheticTokens are mixed into file names so inverted-index postings have a
// realistic document-frequency spread (including CJK names for the Pinyin
// namespace).
var (
	asciiTokens = []string{
		"report", "annual", "invoice", "design", "archive",
		"backup", "photo", "notes", "draft", "final",
	}
	extensions = []string{"md", "txt", "pdf", "png", "go", "json", "log", "docx"}
	cjkNames   = []string{"年度报告", "设计文档", "会议纪要", "合同附件"}
)

// syntheticCounts describes one synthetic corpus.
type syntheticCounts struct {
	Dirs  int
	Files int
}

// totalNodes includes the volume root.
func (c syntheticCounts) totalNodes() int { return 1 + c.Dirs + c.Files }

// syntheticRow is one generated node. NodeKey is 1-based; ParentKey is 0 for the
// volume root, 1 for directories, and the owning directory's key for files.
type syntheticRow struct {
	NodeKey   uint64
	ParentKey uint64
	Name      string
	IsDir     bool
	Size      int64
	ModTime   int64
}

// streamSynthetic emits the corpus in insertion order: root, directories, then
// files spread across directories. Emission order matches dense nodeID order.
func streamSynthetic(c syntheticCounts, baseMTime int64, emit func(row syntheticRow)) {
	emit(syntheticRow{NodeKey: 1, ParentKey: 0, Name: "root", IsDir: true, ModTime: baseMTime})

	for i := 0; i < c.Dirs; i++ {
		emit(syntheticRow{
			NodeKey:   uint64(i + 2),
			ParentKey: 1,
			Name:      fmt.Sprintf("dir_%06d", i),
			IsDir:     true,
			ModTime:   baseMTime + int64(i),
		})
	}

	dirCount := c.Dirs
	if dirCount == 0 {
		dirCount = 1
	}
	for i := 0; i < c.Files; i++ {
		dirKey := uint64(2 + i%dirCount)
		if c.Dirs == 0 {
			dirKey = 1
		}
		var name string
		if i%10 == 0 {
			name = fmt.Sprintf("%s_%08d.%s", cjkNames[i%len(cjkNames)], i, extensions[i%len(extensions)])
		} else {
			name = fmt.Sprintf("%s_%08d_%s.%s", asciiTokens[i%len(asciiTokens)], i, asciiTokens[(i/len(asciiTokens))%len(asciiTokens)], extensions[i%len(extensions)])
		}
		emit(syntheticRow{
			NodeKey:   uint64(2 + c.Dirs + i),
			ParentKey: dirKey,
			Name:      name,
			Size:      int64((i * 37) % 10_000_000),
			ModTime:   baseMTime + int64(i),
		})
	}
}
