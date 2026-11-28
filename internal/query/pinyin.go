package query

import (
	"unicode"
	"unicode/utf8"

	"github.com/mr-dadong/Xift/internal/textkey"
)

const (
	cjkUnifiedStart = textkey.CJKUnifiedStart
	cjkUnifiedEnd   = textkey.CJKUnifiedEnd
)

func pinyinBit(ch byte) uint32 { return textkey.PinyinBit(ch) }

// PinyinInitialsMask returns the a..z bitmask for a CJK rune, or 0 if not mapped.
func PinyinInitialsMask(r rune) uint32 { return textkey.PinyinInitialsMask(r) }

// runeMatchesPinyinChar reports whether rune r matches lowercase ASCII byte ch,
// returning (matched, usedPinyin).
func runeMatchesPinyinChar(r rune, ch byte) (bool, bool) {
	if r >= cjkUnifiedStart && r <= cjkUnifiedEnd {
		bit := pinyinBit(ch)
		if bit != 0 && (textkey.PinyinInitialsMask(r)&bit) != 0 {
			return true, true
		}
		return false, false
	}
	if r < 0x80 && asciiLower(byte(r)) == ch {
		return true, false
	}
	if unicode.ToLower(r) == rune(ch) {
		return true, false
	}
	return false, false
}

// MatchPinyinFast is a zero-allocation hot-path check for whether name matches
// a pure-ASCII lowercase query via Mandarin Pinyin initials (at least one CJK
// character must match by Pinyin initial).
func MatchPinyinFast(name []byte, needleLower []byte) bool {
	nn := len(needleLower)
	if nn == 0 || len(name) == 0 {
		return false
	}
	for start := 0; start < len(name); {
		r, sz := utf8.DecodeRune(name[start:])
		ok, usedPy := runeMatchesPinyinChar(r, needleLower[0])
		if ok {
			cur := start + sz
			j := 1
			for j < nn && cur < len(name) {
				hr, hsz := utf8.DecodeRune(name[cur:])
				m, py := runeMatchesPinyinChar(hr, needleLower[j])
				if !m {
					break
				}
				if py {
					usedPy = true
				}
				cur += hsz
				j++
			}
			if j == nn && usedPy {
				return true
			}
		}
		start += sz
	}
	return false
}

// MatchPinyin matches a pure-ASCII lowercase query against name using Mandarin
// Pinyin initials, returning TierPinyin and exact UTF-8 byte highlight ranges.
func MatchPinyin(name []byte, needleLower string) (MatchTier, []HighlightRange) {
	nn := len(needleLower)
	if nn == 0 || len(name) == 0 || !isAllASCII([]byte(needleLower)) {
		return TierNone, nil
	}
	for start := 0; start < len(name); {
		r, sz := utf8.DecodeRune(name[start:])
		ok, usedPy := runeMatchesPinyinChar(r, needleLower[0])
		if ok {
			cur := start + sz
			j := 1
			for j < nn && cur < len(name) {
				hr, hsz := utf8.DecodeRune(name[cur:])
				m, py := runeMatchesPinyinChar(hr, needleLower[j])
				if !m {
					break
				}
				if py {
					usedPy = true
				}
				cur += hsz
				j++
			}
			if j == nn && usedPy {
				return TierPinyin, []HighlightRange{{Start: start, End: cur}}
			}
		}
		start += sz
	}
	return TierNone, nil
}
