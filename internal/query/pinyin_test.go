package query

import (
	"testing"
)

func TestPinyinInitialsBasicAndPolyphonic(t *testing.T) {
	cases := []struct {
		name     string
		needle   string
		wantHit  bool
		wantSpan string
	}{
		{"年度财务报告.pdf", "ndcwbg", true, "年度财务报告"},
		{"产品设计文档.docx", "cpsj", true, "产品设计"},
		{"文档架构设计.md", "wdjg", true, "文档架构"},
		{"银行卡.txt", "yhk", true, "银行卡"},    // 行 -> h (polyphonic x/h)
		{"行走日志.txt", "xzrz", true, "行走日志"}, // 行 -> x (polyphonic x/h)
		{"重绘.go", "zh", true, "重绘"},        // 重 -> z (polyphonic c/z)
		{"重构计划.md", "cgjh", true, "重构计划"},  // 重 -> c (polyphonic c/z)
		{"音乐播放器.mp3", "yy", true, "音乐"},    // 乐 -> y (polyphonic l/y)
		{"快乐时光.jpg", "kl", true, "快乐"},     // 乐 -> l (polyphonic l/y)
		{"Xift文档架构.md", "xiftwdjg", true, "Xift文档架构"},
		{"Xift文档架构.md", "wdjg", true, "文档架构"},
		{"文档架构.md", "xyz", false, ""},
		{"readme.md", "rm", false, ""}, // Pure ASCII does not match as Pinyin
	}

	for _, tc := range cases {
		tier, ranges := MatchPinyin([]byte(tc.name), tc.needle)
		gotHit := tier != TierNone
		if gotHit != tc.wantHit {
			t.Errorf("MatchPinyin(%q, %q) hit=%v (tier=%v), want %v", tc.name, tc.needle, gotHit, tier, tc.wantHit)
			continue
		}
		if !tc.wantHit {
			continue
		}
		if tier != TierPinyin {
			t.Errorf("MatchPinyin(%q, %q) tier=%v, want TierPinyin", tc.name, tc.needle, tier)
		}
		if len(ranges) == 0 {
			t.Fatalf("MatchPinyin(%q, %q) returned empty highlight ranges", tc.name, tc.needle)
		}
		var sliced string
		for _, r := range ranges {
			if r.Start < 0 || r.End > len(tc.name) || r.Start >= r.End {
				t.Fatalf("MatchPinyin(%q, %q) invalid range %+v", tc.name, tc.needle, r)
			}
			sliced += tc.name[r.Start:r.End]
		}
		if sliced != tc.wantSpan {
			t.Errorf("MatchPinyin(%q, %q) highlighted %q, want %q (ranges=%+v)", tc.name, tc.needle, sliced, tc.wantSpan, ranges)
		}
	}
}
