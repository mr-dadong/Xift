package lsm

import (
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/mr-dadong/Xift/internal/textkey"
)

type invCorpus struct {
	names []string
}

// buildInverted builds a trigram inverted index for the corpus and returns
// the file path plus per-name gram sets for verification.
func buildInverted(t *testing.T, dir string, names []string, maxDF int) (string, map[int][]uint64) {
	t.Helper()
	b := NewInvertedBuilder()
	gramSets := make(map[int][]uint64, len(names))
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
	if _, err := b.Finish(path, maxDF); err != nil {
		t.Fatal(err)
	}
	return path, gramSets
}

func postingSet(t *testing.T, raw []byte) map[uint32]bool {
	t.Helper()
	v, err := ParseRoaring(raw)
	if err != nil {
		t.Fatalf("ParseRoaring: %v", err)
	}
	set := make(map[uint32]bool)
	v.ForEach(func(x uint32) bool {
		set[x] = true
		return true
	})
	return set
}

func TestInvertedBuildAndLookup(t *testing.T) {
	dir := t.TempDir()
	names := []string{
		"report_2026.md",
		"kernel32.dll",
		"设计文档最终版.docx",
		"重设计文档.txt",
		"img_0001.png",
		"a",
		"ab",
	}
	path, gramSets := buildInverted(t, dir, names, math.MaxInt32)

	ix, err := OpenInvertedIndex(path, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ix.Close()

	if ix.GramCount() == 0 {
		t.Fatal("expected nonzero gram count")
	}

	// Every gram of every name must resolve to a posting containing localIdx.
	for i := range names {
		for _, g := range gramSets[i] {
			raw, ok, err := ix.LookupPosting(1, 0, g)
			if err != nil {
				t.Fatalf("LookupPosting(%#x): %v", g, err)
			}
			if !ok {
				t.Fatalf("gram %#x of name %q not found", g, names[i])
			}
			if !postingSet(t, raw)[uint32(i)] {
				t.Fatalf("gram %#x posting missing localIdx %d", g, i)
			}
		}
	}

	// A gram that appears in no name must miss.
	missing := textkey.AppendTrigramKeys([]byte("zzzzzz"), nil)[0]
	if _, ok, _ := ix.LookupPosting(1, 0, missing); ok {
		t.Fatal("absent gram must not be found")
	}

	// Df must equal the number of names containing the gram.
	for i := range names {
		for _, g := range gramSets[i] {
			want := 0
			for j := range names {
				if slices.Contains(gramSets[j], g) {
					want++
				}
			}
			df, ok := ix.Df(1, 0, g)
			if !ok || int(df) != want {
				t.Fatalf("Df(%#x) = %d ok=%v, want %d", g, df, ok, want)
			}
		}
	}
}

func TestInvertedManyGramsBlockedDict(t *testing.T) {
	dir := t.TempDir()
	// Force multiple 512-entry dictionary blocks (~3000 distinct grams).
	var names []string
	for i := 0; i < 400; i++ {
		names = append(names, "n"+itoa(i)+"_"+itoa(i*7%997)+"_"+itoa(i*13%991))
	}
	path, gramSets := buildInverted(t, dir, names, math.MaxInt32)

	cache := NewBlockCache(1 << 20)
	defer cache.Close()
	ix, err := OpenInvertedIndex(path, 7, cache)
	if err != nil {
		t.Fatal(err)
	}
	defer ix.Close()

	if ix.GramCount() <= invertedGramsPer {
		t.Fatalf("gram count %d did not span multiple blocks", ix.GramCount())
	}
	for i := range names {
		for _, g := range gramSets[i] {
			raw, ok, err := ix.LookupPosting(7, 0, g)
			if err != nil || !ok {
				t.Fatalf("LookupPosting(%#x): ok=%v err=%v", g, ok, err)
			}
			if !postingSet(t, raw)[uint32(i)] {
				t.Fatalf("gram %#x missing localIdx %d", g, i)
			}
		}
	}
	// Cached blocks must now hold dictionary data.
	if cache.Bytes() == 0 {
		t.Fatal("expected cached blocks after lookups")
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [12]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func TestInvertedHighDFCutoff(t *testing.T) {
	dir := t.TempDir()
	names := []string{
		"shared_aaa_1", // all contain trigram "she"/"har"/"are"...
		"shared_aaa_2",
		"shared_aaa_3",
		"unique_xyz_1",
	}
	path, gramSets := buildInverted(t, dir, names, 2)

	ix, err := OpenInvertedIndex(path, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ix.Close()

	// Grams shared by all 4 names exceed maxDF=2 and must be dropped, but their
	// dictionary sentinel (df == 0, empty posting) stays so readers can tell
	// "cut" from "absent".
	for _, g := range gramSets[0] {
		df := 0
		for j := range names {
			if slices.Contains(gramSets[j], g) {
				df++
			}
		}
		raw, ok, err := ix.LookupPosting(1, 0, g)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("gram %#x with df %d missing from the dictionary", g, df)
		}
		v, err := ParseRoaring(raw)
		if err != nil {
			t.Fatal(err)
		}
		storedDF, hit := ix.Df(1, 0, g)
		if !hit {
			t.Fatalf("gram %#x df lookup missed", g)
		}
		if df > 2 {
			if storedDF != 0 || v.Cardinality() != 0 {
				t.Fatalf("gram %#x with df %d must be a cut sentinel, got df=%d card=%d", g, df, storedDF, v.Cardinality())
			}
		} else if storedDF != uint32(df) || v.Cardinality() != uint64(df) {
			t.Fatalf("gram %#x with df %d kept incorrectly: df=%d card=%d", g, df, storedDF, v.Cardinality())
		}
	}
}

func TestInvertedCorruptionDetected(t *testing.T) {
	dir := t.TempDir()
	names := []string{"report_2026.md", "kernel32.dll", "设计文档.doc"}
	path, gramSets := buildInverted(t, dir, names, math.MaxInt32)

	// Corrupt one byte inside the postings region.
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	fi, _ := f.Stat()
	if _, err := f.WriteAt([]byte{0xa5}, fi.Size()-3); err != nil {
		t.Fatal(err)
	}
	f.Close()

	ix, err := OpenInvertedIndex(path, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ix.Close()

	sawErr := false
	for i := range names {
		for _, g := range gramSets[i] {
			if _, _, err := ix.LookupPosting(1, 0, g); err != nil {
				sawErr = true
			}
		}
	}
	if !sawErr {
		t.Fatal("posting corruption must surface via CRC")
	}
}

func TestInvertedHeaderTruncation(t *testing.T) {
	dir := t.TempDir()
	path, _ := buildInverted(t, dir, []string{"abcdef"}, math.MaxInt32)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{0, 4, 20, 35} {
		bad := filepath.Join(dir, "bad.idx")
		if err := os.WriteFile(bad, raw[:n], 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := OpenInvertedIndex(bad, 1, nil); err == nil {
			t.Fatalf("truncated header (%d bytes) must fail", n)
		}
	}
	// Magic corruption must fail.
	bad := filepath.Join(dir, "badmagic.idx")
	badRaw := slices.Clone(raw)
	badRaw[2] = 'Q'
	if err := os.WriteFile(bad, badRaw, 0o644); err != nil {
		t.Fatal(err)
	}
	if ix, err := OpenInvertedIndex(bad, 1, nil); err == nil {
		ix.Close()
		t.Fatal("bad magic must fail")
	}
}

func TestInvertedAddNameOutOfOrder(t *testing.T) {
	b := NewInvertedBuilder()
	if err := b.AddName([]uint64{1}, 5); err != nil {
		t.Fatal(err)
	}
	if err := b.AddName([]uint64{2}, 5); err == nil {
		t.Fatal("duplicate localIdx must fail")
	}
	if err := b.AddName([]uint64{2}, 4); err == nil {
		t.Fatal("out-of-order localIdx must fail")
	}
}
