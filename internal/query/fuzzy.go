package query

import (
	"unicode"
	"unicode/utf8"
)

// MatchFuzzyFast performs a zero-allocation word-boundary-aware ordered subsequence check.
// Single-character terms never trigger Fuzzy fallback. To avoid noisy false positives
// (such as "doc_01" matching "doc_001.md"), the first matched character and any character
// matched after a gap MUST land on a word boundary (start of name, after '_', '-', '.',
// ' ', '/', '\\', or a CamelCase transition).
func MatchFuzzyFast(name []byte, isNonASCII bool, needleLower []byte, needleLowerStr string) bool {
	nn := len(needleLower)
	if nn < 2 || nn > 32 || len(name) < nn {
		return false
	}
	if !isNonASCII && isAllASCII(needleLower) {
		return matchFuzzyFastASCII(name, needleLower)
	}
	return matchFuzzyFastUTF8(name, needleLowerStr)
}

func matchFuzzyFastASCII(name, needleLower []byte) bool {
	hn := len(name)
	nn := len(needleLower)
	first := needleLower[0]

	// Start only at word boundaries matching needleLower[0].
	for start := 0; start <= hn-nn; start++ {
		if asciiLower(name[start]) != first || !isWordBoundaryASCII(name, start) {
			continue
		}
		ni := 1
		prev := start
		for i := start + 1; i < hn && ni < nn; i++ {
			if asciiLower(name[i]) != needleLower[ni] {
				continue
			}
			// Contiguous continuation OR word boundary jump.
			if i == prev+1 || isWordBoundaryASCII(name, i) {
				prev = i
				ni++
			}
		}
		if ni == nn {
			return true
		}
	}
	return false
}

func matchFuzzyFastUTF8(name []byte, needleLower string) bool {
	var nBuf [32]rune
	nRunes := nBuf[:0]
	for _, r := range needleLower {
		nRunes = append(nRunes, unicode.ToLower(r))
		if len(nRunes) >= 32 {
			break
		}
	}
	nn := len(nRunes)
	if nn < 2 {
		return false
	}

	var prevR rune
	hasPrev := false
	for start := 0; start < len(name); {
		r, sz := utf8.DecodeRune(name[start:])
		isWB := !hasPrev || isWordSepRune(prevR) || (unicode.IsLower(prevR) && unicode.IsUpper(r))
		if isWB && unicode.ToLower(r) == nRunes[0] {
			ni := 1
			cur := start + sz
			lastMatchEnd := cur
			pR := r
			for cur < len(name) && ni < nn {
				hr, hsz := utf8.DecodeRune(name[cur:])
				if unicode.ToLower(hr) == nRunes[ni] {
					wb := isWordSepRune(pR) || (unicode.IsLower(pR) && unicode.IsUpper(hr))
					if cur == lastMatchEnd || wb {
						lastMatchEnd = cur + hsz
						ni++
					}
				}
				pR = hr
				cur += hsz
			}
			if ni == nn {
				return true
			}
		}
		prevR = r
		hasPrev = true
		start += sz
	}
	return false
}

// MatchFuzzy performs word-boundary-aware ordered subsequence matching and returns
// TierFuzzy along with merged, sorted UTF-8 byte HighlightRanges.
func MatchFuzzy(name []byte, isNonASCII bool, needleLower []byte, needleLowerStr string) (MatchTier, []HighlightRange) {
	nn := len(needleLower)
	if nn < 2 || nn > 32 || len(name) < nn {
		return TierNone, nil
	}
	if !isNonASCII && isAllASCII(needleLower) {
		return matchFuzzyASCII(name, needleLower)
	}
	return matchFuzzyUTF8(name, needleLowerStr)
}

func matchFuzzyASCII(name, needleLower []byte) (MatchTier, []HighlightRange) {
	hn := len(name)
	nn := len(needleLower)
	first := needleLower[0]

	for start := 0; start <= hn-nn; start++ {
		if asciiLower(name[start]) != first || !isWordBoundaryASCII(name, start) {
			continue
		}
		ranges := make([]HighlightRange, 0, nn)
		ranges = append(ranges, HighlightRange{Start: start, End: start + 1})
		ni := 1
		prev := start
		for i := start + 1; i < hn && ni < nn; i++ {
			if asciiLower(name[i]) != needleLower[ni] {
				continue
			}
			if i == prev+1 || isWordBoundaryASCII(name, i) {
				ranges = append(ranges, HighlightRange{Start: i, End: i + 1})
				prev = i
				ni++
			}
		}
		if ni == nn {
			return TierFuzzy, MergeHighlights(ranges)
		}
	}
	return TierNone, nil
}

func matchFuzzyUTF8(name []byte, needleLower string) (MatchTier, []HighlightRange) {
	var nBuf [32]rune
	nRunes := nBuf[:0]
	for _, r := range needleLower {
		nRunes = append(nRunes, unicode.ToLower(r))
		if len(nRunes) >= 32 {
			break
		}
	}
	nn := len(nRunes)
	if nn < 2 {
		return TierNone, nil
	}

	var prevR rune
	hasPrev := false
	for start := 0; start < len(name); {
		r, sz := utf8.DecodeRune(name[start:])
		isWB := !hasPrev || isWordSepRune(prevR) || (unicode.IsLower(prevR) && unicode.IsUpper(r))
		if isWB && unicode.ToLower(r) == nRunes[0] {
			ranges := make([]HighlightRange, 0, nn)
			ranges = append(ranges, HighlightRange{Start: start, End: start + sz})
			ni := 1
			cur := start + sz
			lastMatchEnd := cur
			pR := r
			for cur < len(name) && ni < nn {
				hr, hsz := utf8.DecodeRune(name[cur:])
				if unicode.ToLower(hr) == nRunes[ni] {
					wb := isWordSepRune(pR) || (unicode.IsLower(pR) && unicode.IsUpper(hr))
					if cur == lastMatchEnd || wb {
						ranges = append(ranges, HighlightRange{Start: cur, End: cur + hsz})
						lastMatchEnd = cur + hsz
						ni++
					}
				}
				pR = hr
				cur += hsz
			}
			if ni == nn {
				return TierFuzzy, MergeHighlights(ranges)
			}
		}
		prevR = r
		hasPrev = true
		start += sz
	}
	return TierNone, nil
}
