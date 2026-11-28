package lsm

import (
	"encoding/binary"
	"math/rand"
	"slices"
	"testing"
)

func buildAndParse(t *testing.T, vals []uint32) *RoaringView {
	t.Helper()
	var b RoaringBuilder
	for _, v := range vals {
		if err := b.Add(v); err != nil {
			t.Fatalf("Add(%d): %v", v, err)
		}
	}
	raw, _ := b.Finish()
	v, err := ParseRoaring(raw)
	if err != nil {
		t.Fatalf("ParseRoaring: %v", err)
	}
	return v
}

func viewValues(t *testing.T, v *RoaringView) []uint32 {
	t.Helper()
	var out []uint32
	v.ForEach(func(x uint32) bool {
		out = append(out, x)
		return true
	})
	return out
}

func TestRoaringEmpty(t *testing.T) {
	v := buildAndParse(t, nil)
	if got := v.Cardinality(); got != 0 {
		t.Fatalf("empty cardinality = %d, want 0", got)
	}
	if v.Contains(0) {
		t.Fatal("empty bitmap must not contain 0")
	}
}

func TestRoaringSingleValue(t *testing.T) {
	v := buildAndParse(t, []uint32{0x1234_5678})
	if got := v.Cardinality(); got != 1 {
		t.Fatalf("cardinality = %d, want 1", got)
	}
	if !v.Contains(0x1234_5678) {
		t.Fatal("missing 0x12345678")
	}
	if v.Contains(0x1234_5677) || v.Contains(0x1234_5679) {
		t.Fatal("false positives around single value")
	}
}

func TestRoaringBucketBoundaries(t *testing.T) {
	vals := []uint32{0, 65535, 65536, 65537, 4294967295}
	v := buildAndParse(t, vals)
	if got := v.Cardinality(); got != uint64(len(vals)) {
		t.Fatalf("cardinality = %d, want %d", got, len(vals))
	}
	for _, x := range vals {
		if !v.Contains(x) {
			t.Fatalf("missing %d", x)
		}
	}
	got := viewValues(t, v)
	if !slices.Equal(got, vals) {
		t.Fatalf("iteration = %v, want %v", got, vals)
	}
}

func TestRoaringArrayBitmapThreshold(t *testing.T) {
	seq := func(n int) []uint32 {
		out := make([]uint32, n)
		for i := range out {
			out[i] = uint32(i)
		}
		return out
	}

	// card 4096 in one bucket -> array container: header(4) + 4096*2 bytes.
	var b1 RoaringBuilder
	for _, x := range seq(4096) {
		if err := b1.Add(x); err != nil {
			t.Fatal(err)
		}
	}
	raw1, _ := b1.Finish()
	if len(raw1) != 4+4096*2 {
		t.Fatalf("array container size = %d, want %d", len(raw1), 4+4096*2)
	}

	// card 4097 in one bucket -> bitmap container: header(4) + 8192 bytes.
	var b2 RoaringBuilder
	for _, x := range seq(4097) {
		if err := b2.Add(x); err != nil {
			t.Fatal(err)
		}
	}
	raw2, _ := b2.Finish()
	if len(raw2) != 4+8192 {
		t.Fatalf("bitmap container size = %d, want %d", len(raw2), 4+8192)
	}

	// Full bucket: 65536 values, card-1 must encode 65535.
	var b3 RoaringBuilder
	for _, x := range seq(65536) {
		if err := b3.Add(x); err != nil {
			t.Fatal(err)
		}
	}
	raw3, _ := b3.Finish()
	if len(raw3) != 4+8192 {
		t.Fatalf("full-bucket bitmap size = %d, want %d", len(raw3), 4+8192)
	}
	v3, err := ParseRoaring(raw3)
	if err != nil {
		t.Fatal(err)
	}
	if got := v3.Cardinality(); got != 65536 {
		t.Fatalf("full-bucket cardinality = %d, want 65536", got)
	}
}

func TestRoaringRoundtripRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	vals := make([]uint32, 0, 3000)
	seen := make(map[uint32]struct{}, 3000)
	for len(vals) < 3000 {
		x := rng.Uint32()
		if _, dup := seen[x]; dup {
			continue
		}
		seen[x] = struct{}{}
		vals = append(vals, x)
	}
	slices.Sort(vals)

	v := buildAndParse(t, vals)
	if got := v.Cardinality(); got != uint64(len(vals)) {
		t.Fatalf("cardinality = %d, want %d", got, len(vals))
	}
	got := viewValues(t, v)
	if !slices.Equal(got, vals) {
		t.Fatal("roundtrip iteration mismatch")
	}
	for _, x := range vals {
		if !v.Contains(x) {
			t.Fatalf("missing %d", x)
		}
	}
}

func TestRoaringAddOutOfOrder(t *testing.T) {
	var b RoaringBuilder
	if err := b.Add(10); err != nil {
		t.Fatal(err)
	}
	if err := b.Add(10); err == nil {
		t.Fatal("duplicate add must fail")
	}
	if err := b.Add(9); err == nil {
		t.Fatal("out-of-order add must fail")
	}
	if b.Len() != 1 {
		t.Fatalf("len after rejected adds = %d, want 1", b.Len())
	}
}

func TestRoaringMultiBucketStreamOrder(t *testing.T) {
	// Values spanning several buckets must serialize containers in ascending key order.
	vals := []uint32{0, 1, 65536, 65537, 65538, 131072, 196608, 196609}
	raw := buildAndParse(t, vals).data
	prevKey := -1
	pos := 0
	for pos < len(raw) {
		key := int(binary.LittleEndian.Uint16(raw[pos:]))
		c1 := int(binary.LittleEndian.Uint16(raw[pos+2:]))
		card := c1 + 1
		if key <= prevKey {
			t.Fatalf("container keys not ascending: %d after %d", key, prevKey)
		}
		prevKey = key
		payload := 2 * card
		if card > roaringArrayMaxCard {
			payload = roaringBitmapBytes
		}
		pos += 4 + payload
	}
	if pos != len(raw) {
		t.Fatalf("stream length mismatch: consumed %d of %d", pos, len(raw))
	}
}

// --- 1.2: parse validation, intersect, benchmark ---

func TestRoaringParseErrors(t *testing.T) {
	mk := func(vals []uint32) []byte {
		var b RoaringBuilder
		for _, v := range vals {
			_ = b.Add(v)
		}
		raw, _ := b.Finish()
		return raw
	}

	cases := []struct {
		name string
		raw  []byte
	}{
		{"truncated header", mk([]uint32{1, 2, 3})[:2]},
		{"truncated array payload", mk([]uint32{1, 2, 3})[:6]},
		{"truncated bitmap payload", func() []byte {
			var b RoaringBuilder
			for i := 0; i < 4097; i++ {
				_ = b.Add(uint32(i))
			}
			raw, _ := b.Finish()
			return raw[:100]
		}()},
		{"array card lies about payload", func() []byte {
			raw := mk([]uint32{1, 2})
			raw[2] = 9 // card-1 -> claims 10 values, only 2 present
			return raw
		}()},
		{"bitmap card lies about payload", func() []byte {
			var b RoaringBuilder
			for i := 0; i < 4097; i++ {
				_ = b.Add(uint32(i))
			}
			raw, _ := b.Finish()
			raw[2], raw[3] = 0, 0 // card-1 -> 1 (array claim), but payload is a bitmap
			return raw
		}()},
		{"non-ascending keys", func() []byte {
			raw := mk([]uint32{65536, 131072}) // two containers: keys 1, 2
			raw[0] = 5                         // first key becomes 5 > second key 2
			return raw
		}()},
	}
	for _, tc := range cases {
		if _, err := ParseRoaring(tc.raw); err == nil {
			t.Fatalf("%s: ParseRoaring must fail", tc.name)
		}
	}

	// nil/empty input is a valid empty bitmap, not an error.
	if v, err := ParseRoaring(nil); err != nil || v.Cardinality() != 0 {
		t.Fatalf("nil input should parse as empty: v=%v err=%v", v, err)
	}
}

func intersectMaps(a, b []uint32) []uint32 {
	set := make(map[uint32]struct{}, len(a))
	for _, x := range a {
		set[x] = struct{}{}
	}
	var out []uint32
	for _, x := range b {
		if _, ok := set[x]; ok {
			out = append(out, x)
		}
	}
	return out
}

func TestRoaringIntersect(t *testing.T) {
	type testCase struct {
		name string
		a, b []uint32
	}

	var arrBmpA, arrBmpB []uint32
	for i := 0; i < 4000; i++ {
		arrBmpA = append(arrBmpA, uint32(i*2))
	}
	for i := 0; i < 5000; i++ {
		arrBmpB = append(arrBmpB, uint32(i))
	}
	var bmpBmpA, bmpBmpB []uint32
	for i := 0; i < 6000; i++ {
		bmpBmpA = append(bmpBmpA, uint32(i*3))
		bmpBmpB = append(bmpBmpB, uint32(i*2))
	}
	gen := func(n int, seed int64) []uint32 {
		r := rand.New(rand.NewSource(seed))
		seen := make(map[uint32]struct{})
		var out []uint32
		for len(out) < n {
			x := r.Uint32() % (1 << 20)
			if _, dup := seen[x]; !dup {
				seen[x] = struct{}{}
				out = append(out, x)
			}
		}
		slices.Sort(out)
		return out
	}

	cases := []testCase{
		{"disjoint buckets", []uint32{1, 2, 3}, []uint32{65536, 65537}},
		{"identical", []uint32{1, 2, 3, 65537}, []uint32{1, 2, 3, 65537}},
		{"subset", []uint32{1, 2, 3, 4, 5}, []uint32{2, 4}},
		{"empty base", nil, []uint32{1, 2}},
		{"empty other", []uint32{1, 2}, nil},
		{"array x bitmap", arrBmpA, arrBmpB},
		{"bitmap x bitmap", bmpBmpA, bmpBmpB},
		{"random multi-bucket", gen(900, 1), gen(900, 2)},
	}

	for _, tc := range cases {
		va := buildAndParse(t, tc.a)
		vb := buildAndParse(t, tc.b)
		want := intersectMaps(tc.a, tc.b)
		slices.Sort(want)

		var got []uint32
		if intersect2(va, vb, func(x uint32) bool {
			got = append(got, x)
			return true
		}) {
			t.Fatalf("%s: intersect2 returned stop", tc.name)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("%s: intersect = %v, want %v", tc.name, got, want)
		}

		// Symmetry: swapping arguments yields the same intersection.
		var got2 []uint32
		intersect2(vb, va, func(x uint32) bool {
			got2 = append(got2, x)
			return true
		})
		if !slices.Equal(got2, want) {
			t.Fatalf("%s: swapped intersect = %v, want %v", tc.name, got2, want)
		}
	}
}

func TestRoaringIntersectMultiView(t *testing.T) {
	// Intersection across three views must equal the pairwise-folded result.
	a := []uint32{1, 2, 3, 4, 65540, 131075}
	b := []uint32{2, 4, 65540, 131075, 131076}
	c := []uint32{2, 65540, 131075, 131076}
	views := []*RoaringView{buildAndParse(t, a), buildAndParse(t, b), buildAndParse(t, c)}

	want := intersectMaps(intersectMaps(a, b), c)

	var got []uint32
	if IntersectAll(views, func(x uint32) bool {
		got = append(got, x)
		return true
	}) {
		t.Fatal("IntersectAll returned stop")
	}
	if !slices.Equal(got, want) {
		t.Fatalf("multi-view intersect = %v, want %v", got, want)
	}

	// Early stop: callback returning false halts iteration and reports stop.
	n := 0
	stopped := IntersectAll(views, func(uint32) bool {
		n++
		return n < 2
	})
	if !stopped || n != 2 {
		t.Fatalf("early stop: stopped=%v n=%d, want stopped=true n=2", stopped, n)
	}
}

func TestRoaringIntersectBucketSkip(t *testing.T) {
	// Buckets present in base but missing in other must be skipped wholesale.
	var base, other []uint32
	for bucket := uint32(0); bucket < 8; bucket++ {
		for i := 0; i < 5000; i++ { // force bitmap containers
			base = append(base, bucket*65536+uint32(i))
		}
	}
	other = []uint32{1, 2, 3, 65536, 65537} // buckets 0 and 1 only

	vb := buildAndParse(t, base)
	vo := buildAndParse(t, other)

	var got []uint32
	intersect2(vb, vo, func(x uint32) bool {
		got = append(got, x)
		return true
	})
	want := intersectMaps(base, other)
	if !slices.Equal(got, want) {
		t.Fatalf("bucket skip = %v, want %v", got, want)
	}
}

func BenchmarkRoaringAnd(b *testing.B) {
	var x, y RoaringBuilder
	for i := 0; i < 50000; i++ {
		_ = x.Add(uint32(i * 2))
		_ = y.Add(uint32(i))
	}
	vx, err := func() (*RoaringView, error) { raw, _ := x.Finish(); return ParseRoaring(raw) }()
	if err != nil {
		b.Fatal(err)
	}
	vy, err := func() (*RoaringView, error) { raw, _ := y.Finish(); return ParseRoaring(raw) }()
	if err != nil {
		b.Fatal(err)
	}
	sink := 0
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		intersect2(vx, vy, func(uint32) bool {
			sink++
			return true
		})
	}
	if sink == 0 {
		b.Fatal("no intersection")
	}
}
