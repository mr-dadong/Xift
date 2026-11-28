package l2search

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestExtractText(t *testing.T) {
	nulAfterSniff := append(bytes.Repeat([]byte("a"), sniffBytes+8), 0)
	cases := []struct {
		name   string
		data   []byte
		max    int64
		want   string
		reason SkipReason
	}{
		{
			name: "ascii lowercased",
			data: []byte("Hello World\nMUTEX guard\n"),
			max:  1 << 20,
			want: "hello world\nmutex guard\n",
		},
		{
			name: "cjk preserved",
			data: []byte("设计文档 Mutex"),
			max:  1 << 20,
			want: "设计文档 mutex",
		},
		{
			name:   "nul in sniff window is binary",
			data:   append([]byte("text"), 0, 'a'),
			max:    1 << 20,
			reason: SkipBinary,
		},
		{
			name:   "nul after sniff window is text",
			data:   nulAfterSniff,
			max:    1 << 20,
			want:   string(nulAfterSniff),
			reason: SkipNone,
		},
		{
			name:   "oversized",
			data:   make([]byte, 11),
			max:    10,
			reason: SkipOversized,
		},
		{
			name:   "invalid utf8",
			data:   []byte{0xff, 0xfe, 'a'},
			max:    1 << 20,
			reason: SkipInvalidUTF8,
		},
		{
			name: "empty file",
			data: nil,
			max:  1 << 20,
			want: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, reason := ExtractText(tc.data, tc.max)
			if reason != tc.reason {
				t.Fatalf("reason = %v, want %v", reason, tc.reason)
			}
			if tc.reason != SkipNone {
				if got != nil {
					t.Fatalf("skipped extract returned %q, want nil", got)
				}
				return
			}
			if string(got) != tc.want {
				t.Fatalf("text = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestReadTextFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a.txt")
	if err := os.WriteFile(path, []byte("Mutex\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	text, reason, err := ReadTextFile(path, 1<<20)
	if err != nil || reason != SkipNone || string(text) != "mutex\n" {
		t.Fatalf("ReadTextFile = (%q,%v,%v)", text, reason, err)
	}

	big := filepath.Join(dir, "big.txt")
	if err := os.WriteFile(big, make([]byte, 32), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, reason, err := ReadTextFile(big, 16); err != nil || reason != SkipOversized {
		t.Fatalf("oversized file = (reason=%v, err=%v), want SkipOversized", reason, err)
	}

	if _, reason, err := ReadTextFile(filepath.Join(dir, "missing.txt"), 16); reason != SkipReadError || err == nil {
		t.Fatalf("missing file = (reason=%v, err=%v), want SkipReadError with error", reason, err)
	}
}
