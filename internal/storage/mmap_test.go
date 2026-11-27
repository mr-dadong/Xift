package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestMmapReadOnlyAndLifecycle(t *testing.T) {
	dir := t.TempDir()

	// Empty file should map cleanly with empty slice.
	emptyPath := filepath.Join(dir, "empty.bin")
	if err := os.WriteFile(emptyPath, nil, 0o644); err != nil {
		t.Fatalf("write empty file: %v", err)
	}
	emptyRegion, err := openReadOnlyMmap(emptyPath, mmapAdviceSequential)
	if err != nil {
		t.Fatalf("openReadOnlyMmap empty: %v", err)
	}
	if len(emptyRegion.Bytes()) != 0 {
		t.Fatalf("empty mmap len = %d, want 0", len(emptyRegion.Bytes()))
	}
	if err := emptyRegion.Close(); err != nil {
		t.Fatalf("close empty mmap: %v", err)
	}
	// Second Close must be idempotent.
	if err := emptyRegion.Close(); err != nil {
		t.Fatalf("second close empty mmap: %v", err)
	}

	// Non-empty file content must match byte-for-byte.
	payload := bytes.Repeat([]byte("XiftColumnarData0123456789\n"), 256)
	dataPath := filepath.Join(dir, "hot.col")
	if err := os.WriteFile(dataPath, payload, 0o644); err != nil {
		t.Fatalf("write data file: %v", err)
	}
	region, err := openReadOnlyMmap(dataPath, mmapAdviceRandom)
	if err != nil {
		t.Fatalf("openReadOnlyMmap data: %v", err)
	}
	if !bytes.Equal(region.Bytes(), payload) {
		t.Fatalf("mmap bytes mismatch: got %d bytes", len(region.Bytes()))
	}
	if err := region.Close(); err != nil {
		t.Fatalf("close data mmap: %v", err)
	}
	if err := region.Close(); err != nil {
		t.Fatalf("second close data mmap: %v", err)
	}
	// After Close, file must be removable on Windows as well as Unix.
	if err := os.Remove(dataPath); err != nil {
		t.Fatalf("remove file after Close failed: %v", err)
	}

	// Missing file returns error.
	if _, err := openReadOnlyMmap(filepath.Join(dir, "missing.col"), mmapAdviceNormal); err == nil {
		t.Fatalf("expected error for missing file")
	}
}
