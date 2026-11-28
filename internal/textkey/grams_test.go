package textkey

import (
	"slices"
	"testing"
)

func origKey(r0, r1, r2 rune) uint64 {
	return uint64(r0)<<42 | uint64(r1)<<21 | uint64(r2)
}

func pyKey(b0, b1, b2 byte) uint64 {
	if b2 == 0 {
		return GramNamespacePinyin | uint64(b0)<<8 | uint64(b1)
	}
	return GramNamespacePinyin | uint64(b0)<<16 | uint64(b1)<<8 | uint64(b2)
}

func TestTrigramKeysWindows(t *testing.T) {
	keys := AppendTrigramKeys([]byte("abcd"), nil)
	if len(keys) != 2 {
		t.Fatalf("trigram keys = %d, want 2", len(keys))
	}
	if keys[0] != origKey('a', 'b', 'c') || keys[1] != origKey('b', 'c', 'd') {
		t.Fatalf("keys = %#x", keys)
	}
}

func TestTrigramKeysFoldAndShort(t *testing.T) {
	// ASCII uppercase folds to lowercase.
	keys := AppendTrigramKeys([]byte("ABC"), nil)
	if len(keys) != 1 || keys[0] != origKey('a', 'b', 'c') {
		t.Fatalf("fold keys = %#x", keys)
	}
	// Unicode fold.
	keys = AppendTrigramKeys([]byte("ÁBÇ"), nil)
	if len(keys) != 1 || keys[0] != origKey('á', 'b', 'ç') {
		t.Fatalf("unicode fold keys = %#x", keys)
	}
	// Fewer than 3 runes -> no keys.
	if keys := AppendTrigramKeys([]byte("ab"), nil); len(keys) != 0 {
		t.Fatalf("short keys = %#x", keys)
	}
	if keys := AppendTrigramKeys(nil, nil); len(keys) != 0 {
		t.Fatalf("empty keys = %#x", keys)
	}
	// CJK name: each rune is one position.
	keys = AppendTrigramKeys([]byte("设计文档"), nil)
	if len(keys) != 2 {
		t.Fatalf("cjk keys = %d, want 2", len(keys))
	}
	if keys[0] != origKey('设', '计', '文') || keys[1] != origKey('计', '文', '档') {
		t.Fatalf("cjk keys = %#x", keys)
	}
}

func TestBigramKeys(t *testing.T) {
	keys := AppendBigramKeys([]byte("abcd"), nil)
	want := []uint64{
		uint64('a')<<21 | uint64('b'),
		uint64('b')<<21 | uint64('c'),
		uint64('c')<<21 | uint64('d'),
	}
	if !slices.Equal(keys, want) {
		t.Fatalf("bigram keys = %#x, want %#x", keys, want)
	}
	if keys := AppendBigramKeys([]byte("a"), nil); len(keys) != 0 {
		t.Fatalf("single rune bigram = %#x", keys)
	}
}

func TestPinyinTrigramKeys(t *testing.T) {
	// 设计文档 -> windows (s,j,w) and (j,w,d).
	keys := AppendPinyinTrigramKeys([]byte("设计文档"), nil)
	want := []uint64{pyKey('s', 'j', 'w'), pyKey('j', 'w', 'd')}
	if !slices.Equal(keys, want) {
		t.Fatalf("pinyin keys = %#x, want %#x", keys, want)
	}
}

func TestPinyinPolyphoneExpansion(t *testing.T) {
	// 重 has initials z and c; window (重,设,计) expands to (z,s,j) and (c,s,j).
	keys := AppendPinyinTrigramKeys([]byte("重设计"), nil)
	want := []uint64{pyKey('c', 's', 'j'), pyKey('z', 's', 'j')}
	if !slices.Equal(keys, want) {
		t.Fatalf("polyphone keys = %#x, want %#x", keys, want)
	}
}

func TestPinyinMixedRunes(t *testing.T) {
	// 第2版: window (第,2,版) -> (d,2,b). Digit passes through as itself.
	keys := AppendPinyinTrigramKeys([]byte("第2版"), nil)
	want := []uint64{pyKey('d', '2', 'b')}
	if !slices.Equal(keys, want) {
		t.Fatalf("mixed keys = %#x, want %#x", keys, want)
	}
}

func TestPinyinPureASCIIExcluded(t *testing.T) {
	if keys := AppendPinyinTrigramKeys([]byte("abcd"), nil); len(keys) != 0 {
		t.Fatalf("pure ASCII pinyin keys = %#x", keys)
	}
	// Names without any mappable CJK rune produce nothing.
	if keys := AppendPinyinTrigramKeys([]byte("ァィゥ"), nil); len(keys) != 0 {
		t.Fatalf("kana pinyin keys = %#x", keys)
	}
}

func TestPinyinBigramKeys(t *testing.T) {
	keys := AppendPinyinBigramKeys([]byte("设计"), nil)
	want := []uint64{pyKey('s', 'j', 0)}
	if !slices.Equal(keys, want) {
		t.Fatalf("pinyin bigram keys = %#x, want %#x", keys, want)
	}
	// Polyphone bigram: 重设 -> (z,s) and (c,s).
	keys = AppendPinyinBigramKeys([]byte("重设"), nil)
	want = []uint64{pyKey('c', 's', 0), pyKey('z', 's', 0)}
	if !slices.Equal(keys, want) {
		t.Fatalf("polyphone bigram = %#x, want %#x", keys, want)
	}
}

func TestUnmappedCJKBreaksWindow(t *testing.T) {
	// A rune with no options makes its windows vanish: find a CJK rune in
	// range with zero mask (e.g. a rare ideograph not in GB2312 level-1 and
	// not in the curated extras). U+4DB0 (ext area edge) is within
	// U+4E00..U+9FFF? No -- use U+9FC3 (uncommon). Verify it has no mask
	// first; if it happens to be mapped, pick another.
	r := rune(0x9FC3)
	if PinyinInitialsMask(r) != 0 {
		r = rune(0x4DBF) // not in range
		if PinyinInitialsMask(r) != 0 {
			t.Skip("could not find unmapped CJK rune")
		}
	}
	name := "设" + string(r) + "计"
	keys := AppendPinyinTrigramKeys([]byte(name), nil)
	// All windows contain the unmapped rune -> no keys.
	if len(keys) != 0 {
		t.Fatalf("windows through unmapped rune produced %d keys", len(keys))
	}
}
