package query

import "testing"

func TestMatchNameTiersAndHighlights(t *testing.T) {
	cases := []struct {
		name     string
		file     string
		needle   string
		wantTier MatchTier
		wantSub  string
	}{
		{"exact", "report.pdf", "report.pdf", TierExact, "report.pdf"},
		{"exact-case", "Report.PDF", "report.pdf", TierExact, "Report.PDF"},
		{"prefix", "report_2026.pdf", "report", TierPrefix, "report"},
		{"word-boundary-underscore", "annual_report_2026.pdf", "report", TierWordBoundary, "report"},
		{"word-boundary-camel", "XiftQueryEngine.go", "query", TierWordBoundary, "Query"},
		{"substring", "myreportfile.txt", "report", TierSubstring, "report"},
		{"no-match", "readme.md", "report", TierNone, ""},
		{"unicode-cjk", "设计文档_2026.md", "文档", TierSubstring, "文档"},
		{"unicode-case", "Über_Report.txt", "über", TierPrefix, "Über"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			nonASCII := false
			for i := 0; i < len(tc.file); i++ {
				if tc.file[i] >= 0x80 {
					nonASCII = true
					break
				}
			}
			tier, start, end := MatchName([]byte(tc.file), nonASCII, []byte(tc.needle), tc.needle)
			if tier != tc.wantTier {
				t.Fatalf("MatchName(%q, %q) tier=%v, want %v", tc.file, tc.needle, tier, tc.wantTier)
			}
			if tc.wantTier == TierNone {
				return
			}
			gotSub := tc.file[start:end]
			if gotSub != tc.wantSub {
				t.Fatalf("highlight slice %q[%d:%d] = %q, want %q", tc.file, start, end, gotSub, tc.wantSub)
			}
		})
	}
}

func TestMatchWildcard(t *testing.T) {
	cases := []struct {
		file    string
		pattern string
		want    bool
	}{
		{"main.go", "*.go", true},
		{"MAIN.GO", "*.go", true},
		{"main.rs", "*.go", false},
		{"test_1.txt", "test_?.txt", true},
		{"test_12.txt", "test_?.txt", false},
		{"a_b_c_d.go", "a*b*c*.go", true},
		{"设计_v2.md", "设计_?.md", false},
		{"设计_2.md", "设计_?.md", true},
		// Pathological multi-star pattern must finish instantaneously without exponential backtracking.
		{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab", "*a*a*a*a*a*a*a*a*c", false},
	}

	for _, tc := range cases {
		nonASCII := false
		for i := 0; i < len(tc.file); i++ {
			if tc.file[i] >= 0x80 {
				nonASCII = true
				break
			}
		}
		if got := MatchWildcard([]byte(tc.file), nonASCII, tc.pattern); got != tc.want {
			t.Errorf("MatchWildcard(%q, %q) = %v, want %v", tc.file, tc.pattern, got, tc.want)
		}
	}
}

func TestMergeHighlights(t *testing.T) {
	in := []HighlightRange{
		{Start: 6, End: 10},
		{Start: 0, End: 4},
		{Start: 3, End: 7},
		{Start: 12, End: 15},
	}
	got := MergeHighlights(in)
	if len(got) != 2 || got[0] != (HighlightRange{0, 10}) || got[1] != (HighlightRange{12, 15}) {
		t.Fatalf("unexpected merged highlights: %+v", got)
	}
}

func BenchmarkMatchASCIIFoldZeroAlloc(b *testing.B) {
	hay := []byte("XiftQueryEngineImplementation_2026.go")
	needle := []byte("query")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		tier, _, _ := MatchName(hay, false, needle, "query")
		if tier == TierNone {
			b.Fatal("expected match")
		}
	}
}
