package query

import (
	"slices"
	"unicode"
	"unicode/utf8"
)

// HighlightRange is a half-open UTF-8 byte interval [Start, End) in a filename.
type HighlightRange struct {
	Start int `json:"start"`
	End   int `json:"end"`
}

// MatchTier ranks filename match quality from lowest to highest.
type MatchTier uint8

const (
	TierNone MatchTier = iota
	TierWildcard
	TierFuzzy
	TierPinyin
	TierSubstring
	TierWordBoundary
	TierPrefix
	TierExact
)

// Score returns the base relevance score contribution of MatchTier.
func (t MatchTier) Score() float64 {
	switch t {
	case TierExact:
		return 1000
	case TierPrefix:
		return 750
	case TierWordBoundary:
		return 550
	case TierSubstring:
		return 350
	case TierPinyin:
		return 260
	case TierFuzzy:
		return 180
	case TierWildcard:
		return 150
	default:
		return 0
	}
}

// MatchNameWithFallback tries direct substring matching first (zero-alloc for ASCII),
// then falls back to Mandarin Pinyin initials (for non-ASCII names with ASCII queries)
// and finally ordered discrete Fuzzy subsequence matching (for queries of length >= 2).
func MatchNameWithFallback(name []byte, isNonASCII bool, needleLower []byte, needleLowerStr string) MatchTier {
	if tier, _, _ := MatchName(name, isNonASCII, needleLower, needleLowerStr); tier != TierNone {
		return tier
	}
	if isNonASCII && isAllASCII(needleLower) && MatchPinyinFast(name, needleLower) {
		return TierPinyin
	}
	if len(needleLower) >= 2 && MatchFuzzyFast(name, isNonASCII, needleLower, needleLowerStr) {
		return TierFuzzy
	}
	return TierNone
}

// MatchNameHighlights computes the best MatchTier and exact UTF-8 byte HighlightRanges
// across direct substring, Mandarin Pinyin initials, and Fuzzy subsequence matching.
func MatchNameHighlights(name []byte, isNonASCII bool, needleLower []byte, needleLowerStr string) (MatchTier, []HighlightRange) {
	if tier, s, e := MatchName(name, isNonASCII, needleLower, needleLowerStr); tier != TierNone && e > s {
		return tier, []HighlightRange{{Start: s, End: e}}
	}
	if isNonASCII && isAllASCII(needleLower) {
		if tier, ranges := MatchPinyin(name, needleLowerStr); tier != TierNone {
			return tier, ranges
		}
	}
	if len(needleLower) >= 2 {
		if tier, ranges := MatchFuzzy(name, isNonASCII, needleLower, needleLowerStr); tier != TierNone {
			return tier, ranges
		}
	}
	return TierNone, nil
}

func asciiLower(b byte) byte {
	if b >= 'A' && b <= 'Z' {
		return b + ('a' - 'A')
	}
	return b
}

func isWordSepByte(b byte) bool {
	return b == '_' || b == '-' || b == '.' || b == ' ' || b == '/' || b == '\\'
}

func isASCIIUpper(b byte) bool { return b >= 'A' && b <= 'Z' }
func isASCIILower(b byte) bool { return b >= 'a' && b <= 'z' }

func isWordBoundaryASCII(hay []byte, pos int) bool {
	if pos <= 0 {
		return true
	}
	prev := hay[pos-1]
	if isWordSepByte(prev) {
		return true
	}
	// CamelCase boundary: lowercase followed by uppercase (e.g., XiftQuery -> Query).
	if isASCIILower(prev) && pos < len(hay) && isASCIIUpper(hay[pos]) {
		return true
	}
	return false
}

// MatchName performs case-insensitive substring matching of needleLower against name,
// returning the best MatchTier and the exact UTF-8 byte span [start, end) in name.
// When both name and needleLower are ASCII, this function executes with zero heap allocations.
func MatchName(name []byte, isNonASCII bool, needleLower []byte, needleLowerStr string) (MatchTier, int, int) {
	if len(needleLower) == 0 {
		return TierNone, 0, 0
	}
	if !isNonASCII && isAllASCII(needleLower) {
		return matchNameASCII(name, needleLower)
	}
	return matchNameUTF8(name, needleLowerStr)
}

func isAllASCII(b []byte) bool {
	for i := 0; i < len(b); i++ {
		if b[i] >= 0x80 {
			return false
		}
	}
	return true
}

func matchNameASCII(hay, needleLower []byte) (MatchTier, int, int) {
	hn := len(hay)
	nn := len(needleLower)
	if nn == 0 || nn > hn {
		return TierNone, 0, 0
	}

	first := needleLower[0]
	bestTier := TierNone
	bestPos := -1

	limit := hn - nn
	for i := 0; i <= limit; i++ {
		if asciiLower(hay[i]) != first {
			continue
		}
		matched := true
		for j := 1; j < nn; j++ {
			if asciiLower(hay[i+j]) != needleLower[j] {
				matched = false
				break
			}
		}
		if !matched {
			continue
		}
		var tier MatchTier
		switch {
		case i == 0 && nn == hn:
			return TierExact, 0, hn
		case i == 0:
			return TierPrefix, 0, nn
		case isWordBoundaryASCII(hay, i):
			tier = TierWordBoundary
		default:
			tier = TierSubstring
		}
		if tier > bestTier {
			bestTier = tier
			bestPos = i
			if bestTier == TierWordBoundary {
				// Cannot beat WordBoundary after pos > 0.
				return bestTier, bestPos, bestPos + nn
			}
		}
	}
	if bestPos < 0 {
		return TierNone, 0, 0
	}
	return bestTier, bestPos, bestPos + nn
}

func matchNameUTF8(hay []byte, needleLower string) (MatchTier, int, int) {
	if len(needleLower) == 0 || len(hay) == 0 {
		return TierNone, 0, 0
	}

	// Decode needle runes once on a small stack buffer when possible.
	var nBuf [64]rune
	nRunes := nBuf[:0]
	for _, r := range needleLower {
		nRunes = append(nRunes, unicode.ToLower(r))
	}
	nn := len(nRunes)
	if nn == 0 {
		return TierNone, 0, 0
	}

	bestTier := TierNone
	bestStart := 0
	bestEnd := 0

	var prevRune rune
	hasPrev := false
	for off := 0; off < len(hay); {
		r, sz := utf8.DecodeRune(hay[off:])
		if unicode.ToLower(r) == nRunes[0] {
			cur := off + sz
			matched := true
			for j := 1; j < nn; j++ {
				if cur >= len(hay) {
					matched = false
					break
				}
				hr, hsz := utf8.DecodeRune(hay[cur:])
				if unicode.ToLower(hr) != nRunes[j] {
					matched = false
					break
				}
				cur += hsz
			}
			if matched {
				var tier MatchTier
				switch {
				case off == 0 && cur == len(hay):
					return TierExact, 0, len(hay)
				case off == 0:
					return TierPrefix, 0, cur
				case hasPrev && (isWordSepRune(prevRune) || (unicode.IsLower(prevRune) && unicode.IsUpper(r))):
					tier = TierWordBoundary
				default:
					tier = TierSubstring
				}
				if tier > bestTier {
					bestTier = tier
					bestStart = off
					bestEnd = cur
					if bestTier == TierWordBoundary {
						return bestTier, bestStart, bestEnd
					}
				}
			}
		}
		prevRune = r
		hasPrev = true
		off += sz
	}

	if bestTier == TierNone {
		return TierNone, 0, 0
	}
	return bestTier, bestStart, bestEnd
}

func isWordSepRune(r rune) bool {
	return r == '_' || r == '-' || r == '.' || r == ' ' || r == '/' || r == '\\'
}

// MatchWildcard performs non-backtracking linear/quadratic-safe case-insensitive wildcard
// matching supporting '*' (0+ chars) and '?' (1 char).
func MatchWildcard(name []byte, isNonASCII bool, patternLower string) bool {
	if !isNonASCII && isAllASCII([]byte(patternLower)) {
		return matchWildcardASCII(name, patternLower)
	}
	return matchWildcardUTF8(name, patternLower)
}

func matchWildcardASCII(text []byte, pat string) bool {
	ti, pi := 0, 0
	starIdx, matchIdx := -1, 0
	tn, pn := len(text), len(pat)

	for ti < tn {
		if pi < pn && (pat[pi] == '?' || pat[pi] == asciiLower(text[ti])) {
			ti++
			pi++
		} else if pi < pn && pat[pi] == '*' {
			starIdx = pi
			matchIdx = ti
			pi++
		} else if starIdx != -1 {
			pi = starIdx + 1
			matchIdx++
			ti = matchIdx
		} else {
			return false
		}
	}
	for pi < pn && pat[pi] == '*' {
		pi++
	}
	return pi == pn
}

func matchWildcardUTF8(text []byte, pat string) bool {
	var tBuf [128]rune
	tRunes := tBuf[:0]
	for off := 0; off < len(text); {
		r, sz := utf8.DecodeRune(text[off:])
		tRunes = append(tRunes, unicode.ToLower(r))
		off += sz
	}
	var pBuf [64]rune
	pRunes := pBuf[:0]
	for _, r := range pat {
		pRunes = append(pRunes, unicode.ToLower(r))
	}

	ti, pi := 0, 0
	starIdx, matchIdx := -1, 0
	tn, pn := len(tRunes), len(pRunes)

	for ti < tn {
		if pi < pn && (pRunes[pi] == '?' || pRunes[pi] == tRunes[ti]) {
			ti++
			pi++
		} else if pi < pn && pRunes[pi] == '*' {
			starIdx = pi
			matchIdx = ti
			pi++
		} else if starIdx != -1 {
			pi = starIdx + 1
			matchIdx++
			ti = matchIdx
		} else {
			return false
		}
	}
	for pi < pn && pRunes[pi] == '*' {
		pi++
	}
	return pi == pn
}

// MergeHighlights sorts and merges overlapping or adjacent HighlightRanges.
func MergeHighlights(ranges []HighlightRange) []HighlightRange {
	if len(ranges) <= 1 {
		return ranges
	}
	slices.SortFunc(ranges, func(a, b HighlightRange) int {
		if a.Start != b.Start {
			if a.Start < b.Start {
				return -1
			}
			return 1
		}
		if a.End < b.End {
			return -1
		}
		if a.End > b.End {
			return 1
		}
		return 0
	})
	out := ranges[:1]
	for _, r := range ranges[1:] {
		last := &out[len(out)-1]
		if r.Start <= last.End {
			if r.End > last.End {
				last.End = r.End
			}
		} else {
			out = append(out, r)
		}
	}
	return out
}
