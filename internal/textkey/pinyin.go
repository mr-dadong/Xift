// Package textkey provides shared text normalization and n-gram key
// extraction used by both the query matcher and the Tier 2 inverted-index
// builder, guaranteeing that index-time and query-time gram keys are derived
// from exactly the same folding and Pinyin-initial rules.
package textkey

import (
	"unicode/utf8"

	"golang.org/x/text/encoding/simplifiedchinese"
)

const (
	// CJKUnifiedStart/End bound the CJK Unified Ideographs block covered by
	// the Pinyin initial table.
	CJKUnifiedStart = 0x4E00
	CJKUnifiedEnd   = 0x9FFF
	cjkUnifiedCount = CJKUnifiedEnd - CJKUnifiedStart + 1
)

// pinyinInitialMask stores a 26-bit a..z initial bitmask for each CJK Unified Ideograph
// in U+4E00..U+9FFF. Zero Go pointers (noscan), ~84 KiB static data segment.
var pinyinInitialMask [cjkUnifiedCount]uint32

func init() {
	initPinyinTable()
}

// PinyinBit converts a letter byte to its 26-bit mask bit (0 for non-letters).
func PinyinBit(ch byte) uint32 {
	if ch >= 'a' && ch <= 'z' {
		return 1 << uint(ch-'a')
	}
	if ch >= 'A' && ch <= 'Z' {
		return 1 << uint(ch-'A')
	}
	return 0
}

func initPinyinTable() {
	// GB2312 Level-1 Hanzi (3,755 most common Chinese characters, 0xB0A1..0xD7F9)
	// are strictly ordered by Mandarin Pinyin initial.
	type boundary struct {
		code    uint16
		initial byte
	}
	boundaries := [...]boundary{
		{0xB0A1, 'a'},
		{0xB0C5, 'b'},
		{0xB2C1, 'c'},
		{0xB4EE, 'd'},
		{0xB6EA, 'e'},
		{0xB7A2, 'f'},
		{0xB8C1, 'g'},
		{0xB9FE, 'h'},
		{0xBBF7, 'j'},
		{0xBFA6, 'k'},
		{0xC0AC, 'l'},
		{0xC2E8, 'm'},
		{0xC4C3, 'n'},
		{0xC5B6, 'o'},
		{0xC5BE, 'p'},
		{0xC6DA, 'q'},
		{0xC8BB, 'r'},
		{0xC8F6, 's'},
		{0xCBFA, 't'},
		{0xCDDA, 'w'},
		{0xCEF4, 'x'},
		{0xD1B9, 'y'},
		{0xD4D1, 'z'},
	}

	dec := simplifiedchinese.GB18030.NewDecoder()
	var gb [2]byte
	var utf8Buf [8]byte
	bIdx := 0

	for hi := 0xB0; hi <= 0xD7; hi++ {
		loEnd := 0xFE
		if hi == 0xD7 {
			loEnd = 0xF9
		}
		for lo := 0xA1; lo <= loEnd; lo++ {
			code := uint16(hi<<8 | lo)
			for bIdx+1 < len(boundaries) && code >= boundaries[bIdx+1].code {
				bIdx++
			}
			gb[0] = byte(hi)
			gb[1] = byte(lo)
			dec.Reset()
			nDst, _, err := dec.Transform(utf8Buf[:], gb[:], true)
			if err != nil || nDst == 0 {
				continue
			}
			r, _ := utf8.DecodeRune(utf8Buf[:nDst])
			if r >= CJKUnifiedStart && r <= CJKUnifiedEnd {
				pinyinInitialMask[r-CJKUnifiedStart] |= PinyinBit(boundaries[bIdx].initial)
			}
		}
	}

	// Curated polyphonic readings and common Level-2 / modern tech Hanzi.
	extras := []struct {
		r        rune
		initials string
	}{
		// Common polyphonic Hanzi (multi-initial)
		{'行', "xh"},
		{'重', "zc"},
		{'长', "cz"},
		{'乐', "ly"},
		{'调', "dt"},
		{'传', "cz"},
		{'藏', "cz"},
		{'差', "cz"},
		{'朝', "cz"},
		{'称', "cs"},
		{'单', "dsc"},
		{'都', "dz"},
		{'给', "gj"},
		{'还', "hx"},
		{'会', "hk"},
		{'降', "jx"},
		{'解', "jx"},
		{'卡', "kq"},
		{'率', "ls"},
		{'奇', "qj"},
		{'强', "qj"},
		{'圈', "qj"},
		{'省', "sx"},
		{'盛', "sc"},
		{'识', "sz"},
		{'说', "sy"},
		{'宿', "sx"},
		{'弹', "td"},
		{'提', "td"},
		{'系', "xj"},
		{'校', "xj"},
		{'曾', "zc"},
		{'折', "zs"},
		{'查', "cz"},
		{'厦', "xs"},
		{'角', "jq"},
		{'觉', "jj"},
		{'秘', "mb"},
		{'泊', "bp"},
		{'辟', "bp"},
		{'便', "bp"},
		{'参', "cs"},
		{'乘', "cs"},
		{'仇', "cq"},
		{'臭', "cx"},
		{'畜', "cx"},
		{'攒', "cz"},
		{'禅', "cs"},
		{'地', "d"},
		{'得', "d"},
		{'度', "d"},
		{'佛', "f"},
		{'盖', "g"},
		{'龟', "gjq"},
		{'核', "h"},
		{'和', "h"},
		{'红', "hg"},
		{'华', "h"},
		{'稽', "jq"},
		{'贾', "jg"},
		{'劲', "j"},
		{'句', "jg"},
		{'扛', "kg"},
		{'落', "l"},
		{'绿', "l"},
		{'埋', "m"},
		{'脉', "m"},
		{'秘', "mb"},
		{'模', "m"},
		{'娜', "n"},
		{'难', "n"},
		{'炮', "pb"},
		{'胖', "p"},
		{'瀑', "pb"},
		{'栖', "qx"},
		{'齐', "qj"},
		{'区', "qo"},
		{'任', "r"},
		{'塞', "s"},
		{'杉', "s"},
		{'数', "s"},
		{'遂', "s"},
		{'拓', "t"},
		{'万', "wm"},
		{'尾', "wy"},
		{'蔚', "wy"},
		{'无', "w"},
		{'鲜', "x"},
		{'纤', "xq"},
		{'巷', "xh"},
		{'削', "x"},
		{'血', "x"},
		{'殷', "y"},
		{'佣', "y"},
		{'与', "y"},
		{'员', "y"},
		{'约', "y"},
		{'钥', "y"},
		{'择', "z"},
		{'粘', "zn"},
		{'着', "z"},
		{'爪', "z"},
		{'综', "z"},
		// Common Level-2 / modern tech & office Hanzi
		{'帧', "z"},
		{'栈', "z"},
		{'栅', "sz"},
		{'硅', "g"},
		{'钛', "t"},
		{'锂', "l"},
		{'炫', "x"},
		{'飙', "b"},
		{'睿', "r"},
		{'昊', "h"},
		{'鑫', "x"},
		{'淼', "m"},
		{'岚', "l"},
		{'琪', "q"},
		{'瑾', "j"},
		{'瑜', "y"},
		{'璇', "x"},
		{'璐', "l"},
		{'婷', "t"},
		{'妍', "y"},
		{'怡', "y"},
		{'昕', "x"},
		{'曦', "x"},
		{'皓', "h"},
		{'祺', "q"},
		{'筱', "x"},
		{'茜', "qx"},
		{'莹', "y"},
		{'菲', "f"},
		{'萱', "x"},
		{'薇', "w"},
		{'轩', "x"},
		{'辰', "c"},
		{'铭', "m"},
		{'锐', "r"},
		{'雯', "w"},
		{'霖', "l"},
		{'韵', "y"},
		{'鸿', "h"},
		{'麟', "l"},
		{'啰', "l"},
		{'嗯', "e"},
		{'哒', "d"},
		{'喵', "m"},
		{'熵', "s"},
		{'阈', "y"},
		{'幂', "m"},
		{'锚', "m"},
		{'契', "q"},
		{'嵌', "q"},
		{'溯', "s"},
		{'汰', "t"},
		{'渲', "x"},
		{'曳', "y"},
		{'弈', "y"},
		{'驿', "y"},
		{'溢', "y"},
		{'翊', "y"},
		{'耦', "o"},
		{'栈', "z"},
		{'缀', "z"},
		{'粽', "z"},
	}
	for _, ex := range extras {
		if ex.r >= CJKUnifiedStart && ex.r <= CJKUnifiedEnd {
			var mask uint32
			for i := 0; i < len(ex.initials); i++ {
				mask |= PinyinBit(ex.initials[i])
			}
			pinyinInitialMask[ex.r-CJKUnifiedStart] |= mask
		}
	}
}

// PinyinInitialsMask returns the a..z bitmask for a CJK rune, or 0 if not mapped.
func PinyinInitialsMask(r rune) uint32 {
	if r < CJKUnifiedStart || r > CJKUnifiedEnd {
		return 0
	}
	return pinyinInitialMask[r-CJKUnifiedStart]
}
