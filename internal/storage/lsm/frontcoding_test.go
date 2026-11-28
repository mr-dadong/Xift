package lsm

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func sortedSyntheticNames(t *testing.T, n int) []string {
	t.Helper()
	rng := rand.New(rand.NewSource(11))
	prefixes := []string{"report", "img", "kernel", "设计文档", "notes", "data", "xift", "blob"}
	exts := []string{".go", ".md", ".txt", ".rs", ".png", ""}
	names := make([]string, 0, n)
	for len(names) < n {
		p := prefixes[rng.Intn(len(prefixes))]
		e := exts[rng.Intn(len(exts))]
		names = append(names, p+"_"+randSeq(rng, 2+rng.Intn(8))+e)
	}
	slices.Sort(names)
	return names
}

func randSeq(rng *rand.Rand, n int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = alphabet[rng.Intn(len(alphabet))]
	}
	return string(b)
}

func TestFrontCodingRoundtrip(t *testing.T) {
	names := sortedSyntheticNames(t, 500)
	enc := encodeNameBlockRaw(toByteSlices(names))
	dec, err := decodeNameBlock(enc)
	if err != nil {
		t.Fatalf("decodeNameBlock: %v", err)
	}
	count := decodedNameCount(dec)
	if count != len(names) {
		t.Fatalf("entry count = %d, want %d", count, len(names))
	}
	for i, want := range names {
		got := decodedNameAt(dec, i)
		if string(got) != want {
			t.Fatalf("entry %d = %q, want %q", i, got, want)
		}
	}
}

func toByteSlices(names []string) [][]byte {
	out := make([][]byte, len(names))
	for i, n := range names {
		out[i] = []byte(n)
	}
	return out
}

func TestFrontCodingCompression(t *testing.T) {
	// Highly prefix-shareable corpus must compress below raw size.
	names := make([]string, 0, 300)
	for i := 0; i < 300; i++ {
		names = append(names, "repository_snapshot_2026_part_"+padNum(i))
	}
	slices.Sort(names)

	raw := 0
	for _, n := range names {
		raw += len(n)
	}
	enc := encodeNameBlockRaw(toByteSlices(names))
	if len(enc) >= raw {
		t.Fatalf("front coding did not compress: enc=%d raw=%d", len(enc), raw)
	}
	if len(enc) > raw/2 {
		t.Fatalf("compression weaker than expected: enc=%d raw=%d", len(enc), raw)
	}
}

func padNum(i int) string {
	b := [4]byte{}
	b[0] = byte('0' + i/1000%10)
	b[1] = byte('0' + i/100%10)
	b[2] = byte('0' + i/10%10)
	b[3] = byte('0' + i%10)
	return string(b[:])
}

func TestFrontCodingEdgeNames(t *testing.T) {
	cases := [][]string{
		{""},
		{"", ""},
		{"a"},
		{"设计文档最终版.docx"},
		{"日本語のファイル名.txt", "日本語のファイル名v2.txt"},
		{"same", "same", "same"},
		{"", "a", "ab", "b"},
	}
	for _, names := range cases {
		enc := encodeNameBlockRaw(toByteSlices(names))
		dec, err := decodeNameBlock(enc)
		if err != nil {
			t.Fatalf("names %q: %v", names, err)
		}
		if got := decodedNameCount(dec); got != len(names) {
			t.Fatalf("names %q: count %d, want %d", names, got, len(names))
		}
		for i, want := range names {
			if got := string(decodedNameAt(dec, i)); got != want {
				t.Fatalf("names %q: entry %d = %q, want %q", names, i, got, want)
			}
		}
	}
}

func TestFrontCodingRejectsOverlongName(t *testing.T) {
	if _, err := encodeOneBlock([][]byte{bytes.Repeat([]byte{'x'}, nameBlockMaxBytes)}); err == nil {
		t.Fatal("over-long name must be rejected")
	}
}

func encodeOneBlock(names [][]byte) ([]byte, error) {
	var w nameBlocksScratch
	for _, n := range names {
		if _, err := w.append(n); err != nil {
			return nil, err
		}
	}
	return w.finish(), nil
}

func TestFrontCodingDecodeErrors(t *testing.T) {
	mk := func(names ...string) []byte {
		return encodeNameBlockRaw(toByteSlices(names))
	}

	cases := []struct {
		name string
		raw  []byte
	}{
		{"empty payload", nil},
		{"truncated count", mk("alpha")[:1]},
		{"truncated entry header", func() []byte {
			raw := mk("alpha", "alphabet")
			return raw[:4]
		}()},
		{"suffix beyond payload", func() []byte {
			raw := mk("alpha")
			raw[4] = 0xff // suffixLen lies
			return raw
		}()},
		{"shared beyond previous", func() []byte {
			raw := mk("alpha", "alphabet")
			// Layout: [count 2B][e0 hdr 4B]["alpha" 5B][e1 shared 2B]...
			// raw[11] is the LSB of entry 1's sharedLen.
			raw[11] = 0xff
			return raw
		}()},
		{"first entry nonzero shared", func() []byte {
			raw := mk("alpha")
			raw[2] = 1 // first entry sharedLen=1 without predecessor
			return raw
		}()},
	}
	for _, tc := range cases {
		if _, err := decodeNameBlock(tc.raw); err == nil {
			t.Fatalf("%s: decodeNameBlock must fail", tc.name)
		}
	}
}

// --- 2.2: name block file writer/reader ---

func TestNameBlocksWriterReader(t *testing.T) {
	dir := t.TempDir()
	names := sortedSyntheticNames(t, 2000)

	w, err := NewNameBlocksWriter(filepath.Join(dir, "names.blk"), filepath.Join(dir, "names.idx"))
	if err != nil {
		t.Fatal(err)
	}
	for i, n := range names {
		if _, _, err := w.Append([]byte(n), uint32(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}

	r, err := OpenNameBlocksReader(filepath.Join(dir, "names.blk"), filepath.Join(dir, "names.idx"), 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if r.blockCount() == 0 {
		t.Fatal("expected multiple blocks for 2000 names")
	}
	if int(r.entryCount()) != len(names) {
		t.Fatalf("idx entry count = %d, want %d", r.entryCount(), len(names))
	}
	for i, want := range names {
		got, err := r.ReadName(uint32(i))
		if err != nil {
			t.Fatalf("ReadName(%d): %v", i, err)
		}
		if got != want {
			t.Fatalf("ReadName(%d) = %q, want %q", i, got, want)
		}
	}

	// Out-of-range lookups must fail cleanly.
	if _, err := r.ReadName(uint32(len(names))); err == nil {
		t.Fatal("out-of-range localIdx must fail")
	}
	if _, err := r.readBlock(r.blockCount()); err == nil {
		t.Fatal("out-of-range blockID must fail")
	}
}

func TestNameBlocksCorruptionDetected(t *testing.T) {
	dir := t.TempDir()
	names := sortedSyntheticNames(t, 300)

	w, err := NewNameBlocksWriter(filepath.Join(dir, "names.blk"), filepath.Join(dir, "names.idx"))
	if err != nil {
		t.Fatal(err)
	}
	for i, n := range names {
		_, _, err := w.Append([]byte(n), uint32(i))
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}

	// Corrupt one payload byte inside the second block's slot.
	f, err := os.OpenFile(filepath.Join(dir, "names.blk"), os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	off := int64(nameBlockHdrSize) + nameBlockSlotBytes + 8 + 3 // block 1 payload
	if _, err := f.WriteAt([]byte{0x5a}, off); err != nil {
		t.Fatal(err)
	}
	f.Close()

	r, err := OpenNameBlocksReader(filepath.Join(dir, "names.blk"), filepath.Join(dir, "names.idx"), 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	corruptSeen := false
	for i := range names {
		_, err := r.ReadName(uint32(i))
		if err != nil {
			corruptSeen = true
			break
		}
	}
	if !corruptSeen {
		t.Fatal("corrupted block payload must be detected via CRC")
	}
}

func TestNameBlocksSingleOversizeEntry(t *testing.T) {
	dir := t.TempDir()
	w, err := NewNameBlocksWriter(filepath.Join(dir, "names.blk"), filepath.Join(dir, "names.idx"))
	if err != nil {
		t.Fatal(err)
	}
	long := bytes.Repeat([]byte{'z'}, 200)
	if _, _, err := w.Append(long, 0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.Append(bytes.Repeat([]byte{'x'}, nameBlockMaxBytes+1), 1); err == nil {
		t.Fatal("over-long name must be rejected at Append")
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	r, err := OpenNameBlocksReader(filepath.Join(dir, "names.blk"), filepath.Join(dir, "names.idx"), 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, err := r.ReadName(0)
	if err != nil || got != string(long) {
		t.Fatalf("ReadName(0) = %q err=%v", got, err)
	}
}

func TestNameBlocksFixedSlots(t *testing.T) {
	// Blocks must occupy fixed-size slots so offsets need no resident table.
	dir := t.TempDir()
	names := sortedSyntheticNames(t, 1000)
	w, err := NewNameBlocksWriter(filepath.Join(dir, "names.blk"), filepath.Join(dir, "names.idx"))
	if err != nil {
		t.Fatal(err)
	}
	for i, n := range names {
		_, _, _ = w.Append([]byte(n), uint32(i))
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(filepath.Join(dir, "names.blk"))
	if err != nil {
		t.Fatal(err)
	}
	r, err := OpenNameBlocksReader(filepath.Join(dir, "names.blk"), filepath.Join(dir, "names.idx"), 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	want := int64(nameBlockHdrSize + int(r.blockCount())*nameBlockSlotBytes)
	if fi.Size() != want {
		t.Fatalf("file size = %d, want %d (header + fixed slots)", fi.Size(), want)
	}
	var _ = binary.LittleEndian
}
