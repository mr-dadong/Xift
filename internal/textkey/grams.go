package textkey

import (
	"unicode"
	"unicode/utf8"
)

// GramNamespacePinyin is the high bit distinguishing Pinyin-initial gram keys
// from original-rune gram keys inside one dictionary.
const GramNamespacePinyin = uint64(1) << 63

// foldRune mirrors the query matcher's case folding (unicode.ToLower per rune).
func foldRune(r rune) rune { return unicode.ToLower(r) }

// fillRuneOptions writes the possible ASCII letter options for rune r into
// dst (capacity >= 26) and returns the count:
//   - CJK runes with a Pinyin mapping expand to every possible initial
//     (polyphones included), mirroring the matcher's mask semantics;
//   - other runes contribute their lowercase form when it is printable ASCII;
//   - runes with no options (unmapped CJK, non-ASCII non-CJK) yield 0, which
//     makes any window containing them unmatchable — identical to the matcher.
func fillRuneOptions(r rune, dst []byte) int {
	if mask := PinyinInitialsMask(r); mask != 0 {
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

// AppendTrigramKeys appends the original-namespace trigram keys of name:
// every window of 3 consecutive runes, each rune folded with unicode.ToLower.
// Names with fewer than 3 runes produce no keys. Callers deduplicate per name.
func AppendTrigramKeys(name []byte, dst []uint64) []uint64 {
	var runes [3]rune
	n := 0
	for i := 0; i < len(name); {
		r, sz := utf8.DecodeRune(name[i:])
		i += sz
		runes[n%3] = foldRune(r)
		n++
		if n >= 3 {
			// Window ending at this rune.
			s := n - 3
			dst = append(dst, uint64(runes[s%3])<<42|uint64(runes[(s+1)%3])<<21|uint64(runes[(s+2)%3]))
		}
	}
	return dst
}

// AppendBigramKeys appends the original-namespace bigram keys of name.
func AppendBigramKeys(name []byte, dst []uint64) []uint64 {
	var runes [2]rune
	n := 0
	for i := 0; i < len(name); {
		r, sz := utf8.DecodeRune(name[i:])
		i += sz
		runes[n%2] = foldRune(r)
		n++
		if n >= 2 {
			s := n - 2
			dst = append(dst, uint64(runes[s%2])<<21|uint64(runes[(s+1)%2]))
		}
	}
	return dst
}

// hasPinyinRune reports whether name contains at least one rune with a
// Pinyin-initial mapping (matcher precondition for the Pinyin tier).
func hasPinyinRune(name []byte) bool {
	for i := 0; i < len(name); {
		r, sz := utf8.DecodeRune(name[i:])
		i += sz
		if PinyinInitialsMask(r) != 0 {
			return true
		}
	}
	return false
}

// AppendPinyinTrigramKeys appends Pinyin-namespace trigram keys for name,
// expanding polyphonic initials: a window matches a query trigram when each
// query letter is a possible initial of the corresponding name rune. Names
// without any CJK rune produce no keys (the matcher requires at least one
// Pinyin-matched rune).
func AppendPinyinTrigramKeys(name []byte, dst []uint64) []uint64 {
	if !hasPinyinRune(name) {
		return dst
	}
	var runes []rune
	for i := 0; i < len(name); {
		r, sz := utf8.DecodeRune(name[i:])
		i += sz
		runes = append(runes, r)
	}
	var opts [3][26]byte
	var optN [3]int
	for w := 0; w+3 <= len(runes); w++ {
		any := true
		for p := 0; p < 3; p++ {
			optN[p] = fillRuneOptions(runes[w+p], opts[p][:])
			if optN[p] == 0 {
				any = false
				break
			}
		}
		if !any {
			continue
		}
		for i0 := 0; i0 < optN[0]; i0++ {
			for i1 := 0; i1 < optN[1]; i1++ {
				for i2 := 0; i2 < optN[2]; i2++ {
					key := GramNamespacePinyin |
						uint64(opts[0][i0])<<16 | uint64(opts[1][i1])<<8 | uint64(opts[2][i2])
					dst = append(dst, key)
				}
			}
		}
	}
	return dst
}

// AppendPinyinBigramKeys appends Pinyin-namespace bigram keys for name with
// polyphone expansion.
func AppendPinyinBigramKeys(name []byte, dst []uint64) []uint64 {
	if !hasPinyinRune(name) {
		return dst
	}
	var runes []rune
	for i := 0; i < len(name); {
		r, sz := utf8.DecodeRune(name[i:])
		i += sz
		runes = append(runes, r)
	}
	var opts [2][26]byte
	var optN [2]int
	for w := 0; w+2 <= len(runes); w++ {
		any := true
		for p := 0; p < 2; p++ {
			optN[p] = fillRuneOptions(runes[w+p], opts[p][:])
			if optN[p] == 0 {
				any = false
				break
			}
		}
		if !any {
			continue
		}
		for i0 := 0; i0 < optN[0]; i0++ {
			for i1 := 0; i1 < optN[1]; i1++ {
				key := GramNamespacePinyin |
					uint64(opts[0][i0])<<8 | uint64(opts[1][i1])
				dst = append(dst, key)
			}
		}
	}
	return dst
}
