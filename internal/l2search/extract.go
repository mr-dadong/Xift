// Package l2search implements the optional L2 full-text index over user
// whitelisted directories. It stays isolated from the L1 metadata engine: its
// own segments, block cache and disk budget.
package l2search

import (
	"bytes"
	"io"
	"os"
	"unicode/utf8"
)

const (
	// DefaultMaxFileBytes bounds how much of one file enters the content index.
	DefaultMaxFileBytes int64 = 4 << 20
	// sniffBytes is how much of a file is inspected for NUL bytes to classify
	// it as binary.
	sniffBytes = 8 << 10
)

// SkipReason explains why a file produced no content text.
type SkipReason uint8

const (
	// SkipNone means the file was extracted successfully.
	SkipNone SkipReason = iota
	// SkipBinary means a NUL byte appeared inside the sniff window.
	SkipBinary
	// SkipOversized means the file exceeded the configured size cap.
	SkipOversized
	// SkipInvalidUTF8 means the bytes were not valid UTF-8.
	SkipInvalidUTF8
	// SkipReadError means the file could not be read (missing, permission, I/O).
	SkipReadError
	// SkipNoPath means an event could not be mapped to a whitelisted file path.
	SkipNoPath
)

// String renders a stable label for status reporting.
func (r SkipReason) String() string {
	switch r {
	case SkipNone:
		return "indexed"
	case SkipBinary:
		return "binary"
	case SkipOversized:
		return "oversized"
	case SkipInvalidUTF8:
		return "invalid_utf8"
	case SkipReadError:
		return "read_error"
	case SkipNoPath:
		return "no_path"
	default:
		return "unknown"
	}
}

// ExtractText normalizes file bytes into the UTF-8 text that enters the content
// index. ASCII letters are lower-folded; everything else is preserved. A NUL
// byte in the first sniffBytes classifies the file as binary.
func ExtractText(data []byte, maxBytes int64) ([]byte, SkipReason) {
	if maxBytes > 0 && int64(len(data)) > maxBytes {
		return nil, SkipOversized
	}
	sniff := data
	if len(sniff) > sniffBytes {
		sniff = sniff[:sniffBytes]
	}
	if bytes.IndexByte(sniff, 0) >= 0 {
		return nil, SkipBinary
	}
	if !utf8.Valid(data) {
		return nil, SkipInvalidUTF8
	}
	out := make([]byte, len(data))
	for i, b := range data {
		if b >= 'A' && b <= 'Z' {
			b += 'a' - 'A'
		}
		out[i] = b
	}
	return out, SkipNone
}

// ReadTextFile reads at most maxBytes from path and extracts its indexable text.
// Oversized files are detected without reading the whole file.
func ReadTextFile(path string, maxBytes int64) ([]byte, SkipReason, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, SkipReadError, err
	}
	defer f.Close()
	if maxBytes <= 0 {
		maxBytes = DefaultMaxFileBytes
	}
	if info, statErr := f.Stat(); statErr == nil && info.Mode().IsRegular() && info.Size() > maxBytes {
		return nil, SkipOversized, nil
	}
	buf, err := io.ReadAll(io.LimitReader(f, maxBytes+1))
	if err != nil {
		return nil, SkipReadError, err
	}
	if int64(len(buf)) > maxBytes {
		return nil, SkipOversized, nil
	}
	text, reason := ExtractText(buf, maxBytes)
	return text, reason, nil
}
