package lsm_test

import (
	"math/rand"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"unicode"

	"github.com/mr-dadong/Xift/internal/query"
	"github.com/mr-dadong/Xift/internal/storage/lsm"
	"github.com/mr-dadong/Xift/internal/textkey"
)

// foldString lowercases every rune, mirroring the matcher's UTF-8 path.
func foldString(s string) string {
	var b strings.Builder
	for _, r := range s {
		b.WriteRune(unicode.ToLower(r))
	}
	return b.String()
}

func isAllASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return false
		}
	}
	return true
}

func genName(rng *rand.Rand) string {
	cjk := []rune("设计文档重长乐调传藏差朝称单都会给还降解卡率奇强圈省盛识说宿弹提系校曾折查厦角觉秘泊辟便参乘仇臭畜攒禅地得度佛盖龟核和华稽贾劲句扛落绿埋脉模娜难炮胖瀑栖齐区任塞杉数遂拓万尾蔚无鲜纤巷削血殷佣与员约钥择粘着爪综帧栈栅硅钛锂炫飙睿昊鑫淼岚琪瑾瑜璇璐婷妍怡昕曦皓祺筱茜莹菲萱薇轩辰铭锐雯霖韵鸿麟啰嗯哒喵熵阈幂锚契嵌溯汰渲曳弈驿溢翊耦缀粽")
	asciiWords := []string{"report", "data", "img", "kernel", "notes", "blob", "final", "draft", "v2", "2026", "xift"}
	var b strings.Builder
	n := 1 + rng.Intn(4)
	for i := 0; i < n; i++ {
		switch rng.Intn(3) {
		case 0:
			b.WriteString(asciiWords[rng.Intn(len(asciiWords))])
		case 1:
			for j := 0; j < 1+rng.Intn(3); j++ {
				b.WriteRune(cjk[rng.Intn(len(cjk))])
			}
		default:
			b.WriteString(asciiWords[rng.Intn(len(asciiWords))])
			b.WriteRune(cjk[rng.Intn(len(cjk))])
		}
		if i < n-1 && rng.Intn(2) == 0 {
			b.WriteByte('_')
		}
	}
	switch rng.Intn(4) {
	case 0:
		b.WriteString(".go")
	case 1:
		b.WriteString(".md")
	case 2:
		b.WriteString(".png")
	}
	return b.String()
}

// derivedSubstringNeedle extracts a random 3..6 rune window from name,
// respecting UTF-8 rune boundaries.
func derivedSubstringNeedle(rng *rand.Rand, name string) (string, bool) {
	runes := []rune(name)
	if len(runes) < 3 {
		return "", false
	}
	maxLen := min(6, len(runes))
	w := 3 + rng.Intn(maxLen-2)
	o := rng.Intn(len(runes) - w + 1)
	return string(runes[o : o+w]), true
}

// derivedPinyinNeedle maps a window of name runes to a needle of possible
// initials (CJK) or literal lowercase ASCII chars (non-CJK), guaranteeing a
// Pinyin-tier match when the window contains at least one CJK rune.
func derivedPinyinNeedle(rng *rand.Rand, name string) (string, bool) {
	runes := []rune(name)
	if len(runes) < 3 {
		return "", false
	}
	w := 3 + rng.Intn(min(6, len(runes))-2)
	if w > len(runes) {
		w = len(runes)
	}
	o := rng.Intn(len(runes) - w + 1)
	window := runes[o : o+w]
	var b strings.Builder
	hasCJK := false
	var opts [26]byte
	for _, r := range window {
		n := fillOptions(r, opts[:])
		if n == 0 {
			return "", false // window unmatchable by construction
		}
		if textkey.PinyinInitialsMask(r) != 0 {
			hasCJK = true
			b.WriteByte(opts[rng.Intn(n)])
		} else {
			b.WriteByte(opts[0])
		}
	}
	if !hasCJK {
		return "", false
	}
	return b.String(), true
}

func fillOptions(r rune, dst []byte) int {
	if mask := textkey.PinyinInitialsMask(r); mask != 0 {
		n := 0
		for i := 0; i < 26; i++ {
			if mask&(1<<uint(i)) != 0 {
				dst[n] = byte('a' + i)
				n++
			}
		}
		return n
	}
	if lr := unicode.ToLower(r); lr >= 0x20 && lr < 0x7f {
		dst[0] = byte(lr)
		return 1
	}
	return 0
}

// TestInvertedNoFalseNegative verifies the core Tier 2 correctness property:
// for any name and any needle the real query matcher would accept
// (case-insensitive substring or Pinyin initials), every trigram of the needle
// is present in the name's indexed gram set — inverted candidate generation
// can never miss a true hit. Needles are derived from the names themselves so
// both branches fire on guaranteed true matches.
func TestInvertedNoFalseNegative(t *testing.T) {
	rng := rand.New(rand.NewSource(2026))

	names := make([]string, 0, 400)
	seen := make(map[string]struct{})
	for len(names) < 400 {
		n := genName(rng)
		if _, dup := seen[n]; !dup {
			seen[n] = struct{}{}
			names = append(names, n)
		}
	}

	dir := t.TempDir()
	b := lsm.NewInvertedBuilder()
	gramSets := make([][]uint64, len(names))
	for i, n := range names {
		keys := textkey.AppendTrigramKeys([]byte(n), nil)
		keys = textkey.AppendPinyinTrigramKeys([]byte(n), keys)
		slices.Sort(keys)
		keys = slices.Compact(keys)
		gramSets[i] = keys
		if err := b.AddName(keys, uint32(i)); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(dir, "trigram.idx")
	if _, err := b.Finish(path, 1<<30); err != nil {
		t.Fatal(err)
	}

	ix, err := lsm.OpenInvertedIndex(path, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ix.Close()

	subChecked, pyChecked := 0, 0
	for iter := 0; iter < 4000; iter++ {
		idx := rng.Intn(len(names))
		name := names[idx]
		grams := gramSets[idx]

		var needle string
		var pinyin bool
		switch rng.Intn(2) {
		case 0:
			n, ok := derivedSubstringNeedle(rng, name)
			needle, pinyin = n, false
			if !ok {
				continue
			}
		default:
			n, ok := derivedPinyinNeedle(rng, name)
			needle, pinyin = n, true
			if !ok {
				continue
			}
		}
		if len(needle) < 3 {
			continue
		}

		if !pinyin {
			if !strings.Contains(foldString(name), foldString(needle)) {
				t.Fatalf("derived substring needle %q should hit %q", needle, name)
			}
			subChecked++
			for _, g := range textkey.AppendTrigramKeys([]byte(needle), nil) {
				if !slices.Contains(grams, g) {
					t.Fatalf("substring hit %q / %q: gram %#x not indexed", name, needle, g)
				}
				raw, ok, err := ix.LookupPosting(1, 0, g)
				if err != nil || !ok {
					t.Fatalf("substring %q / %q: gram %#x lookup ok=%v err=%v", name, needle, g, ok, err)
				}
				v, err := lsm.ParseRoaring(raw)
				if err != nil {
					t.Fatal(err)
				}
				if !v.Contains(uint32(idx)) {
					t.Fatalf("substring %q / %q: gram %#x posting misses localIdx %d", name, needle, g, idx)
				}
			}
		} else {
			if !isAllASCII(needle) || !query.MatchPinyinFast([]byte(name), []byte(needle)) {
				t.Fatalf("derived pinyin needle %q should hit %q", needle, name)
			}
			pyChecked++
			for i := 0; i+3 <= len(needle); i++ {
				g := textkey.GramNamespacePinyin |
					uint64(needle[i])<<16 | uint64(needle[i+1])<<8 | uint64(needle[i+2])
				if !slices.Contains(grams, g) {
					t.Fatalf("pinyin hit %q / %q: gram %#x not indexed", name, needle, g)
				}
				raw, ok, err := ix.LookupPosting(1, 0, g)
				if err != nil || !ok {
					t.Fatalf("pinyin %q / %q: gram %#x lookup ok=%v err=%v", name, needle, g, ok, err)
				}
				v, err := lsm.ParseRoaring(raw)
				if err != nil {
					t.Fatal(err)
				}
				if !v.Contains(uint32(idx)) {
					t.Fatalf("pinyin %q / %q: gram %#x posting misses localIdx %d", name, needle, g, idx)
				}
			}
		}
	}
	if subChecked < 100 || pyChecked < 100 {
		t.Fatalf("insufficient coverage: substring=%d pinyin=%d", subChecked, pyChecked)
	}
}
