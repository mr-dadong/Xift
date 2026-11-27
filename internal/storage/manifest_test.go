package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestManifestRoundTripAndCorruption(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "MANIFEST")

	// Missing manifest returns clean default.
	m, exists, err := LoadManifest(path)
	if err != nil || exists {
		t.Fatalf("LoadManifest missing = (%+v, %v, %v)", m, exists, err)
	}
	if m.NextSegmentID != 1 || m.NextNodeID != 0 {
		t.Fatalf("unexpected default manifest: %+v", m)
	}

	m.NextSegmentID = 3
	m.NextNodeID = 1500
	m.WALGeneration = 7
	m.Segments = []SegmentMeta{
		{ID: 1, BaseNodeID: 0, NodeCount: 1000, NamesBytes: 24000, Dense: true},
		{ID: 2, BaseNodeID: 1000, NodeCount: 500, NamesBytes: 12000, Dense: false},
	}
	m.Volumes = []VolumeCursor{
		{VolID: 1, Cursor: 987654321, RootPath: `C:\`},
		{VolID: 2, Cursor: 123456, RootPath: `D:\`},
	}

	if err := m.Save(path); err != nil {
		t.Fatalf("Save manifest: %v", err)
	}
	// Overwrite atomically again to verify Windows replace-existing works cleanly.
	m.WALGeneration = 8
	if err := m.Save(path); err != nil {
		t.Fatalf("second Save manifest: %v", err)
	}

	loaded, exists, err := LoadManifest(path)
	if err != nil || !exists {
		t.Fatalf("LoadManifest reload = (%+v, %v, %v)", loaded, exists, err)
	}
	if loaded.NextSegmentID != 3 || loaded.NextNodeID != 1500 || loaded.WALGeneration != 8 {
		t.Fatalf("loaded header mismatch: %+v", loaded)
	}
	if len(loaded.Segments) != 2 || loaded.Segments[0] != m.Segments[0] || loaded.Segments[1] != m.Segments[1] {
		t.Fatalf("loaded segments mismatch: %+v", loaded.Segments)
	}
	if len(loaded.Volumes) != 2 || loaded.Volumes[0] != m.Volumes[0] || loaded.Volumes[1] != m.Volumes[1] {
		t.Fatalf("loaded volumes mismatch: %+v", loaded.Volumes)
	}

	// Tamper a byte and verify ErrCorruptManifest.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	raw[len(raw)-2] ^= 0xAA
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("tamper manifest: %v", err)
	}
	if _, _, err := LoadManifest(path); !errors.Is(err, ErrCorruptManifest) {
		t.Fatalf("expected ErrCorruptManifest, got %v", err)
	}
}
