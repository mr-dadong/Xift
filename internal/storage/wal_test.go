package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestWALReplayTornTailAndCorruption(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal.log")

	w, replayed, truncated, err := OpenWAL(walPath, 1, true)
	if err != nil {
		t.Fatalf("OpenWAL initial: %v", err)
	}
	if len(replayed) != 0 || truncated != 0 {
		t.Fatalf("initial replay=%d truncated=%d, want 0/0", len(replayed), truncated)
	}

	batch := []Mutation{
		{Op: OpCreate, VolID: 1, NodeKey: 100, ParentKey: 1, Name: "设计文档.md", Flags: FlagNonASCII, Size: 1024, ModTime: 1700000001},
		{Op: OpModifyMeta, VolID: 1, NodeKey: 100, Size: 2048, ModTime: 1700000002},
		{Op: OpRename, VolID: 1, NodeKey: 100, ParentKey: 1, Name: "架构设计.md", Flags: FlagNonASCII, ReplaceFlags: true},
		{Op: OpMoveDir, VolID: 1, NodeKey: 200, NewParentKey: 300},
		{Op: OpDelete, VolID: 1, NodeKey: 100},
		{Op: OpCursorUpdate, VolID: 1, Cursor: 555555, RootPath: `C:\`},
	}
	if err := w.AppendBatch(batch); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close WAL: %v", err)
	}

	// Clean reopen replays all 6 mutations identically.
	w2, replayed, truncated, err := OpenWAL(walPath, 1, true)
	if err != nil {
		t.Fatalf("OpenWAL replay: %v", err)
	}
	if truncated != 0 || len(replayed) != len(batch) {
		t.Fatalf("replayed=%d truncated=%d, want %d/0", len(replayed), truncated, len(batch))
	}
	for i := range batch {
		if replayed[i] != batch[i] {
			t.Fatalf("mutation[%d] mismatch:\ngot:  %+v\nwant: %+v", i, replayed[i], batch[i])
		}
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("Close w2: %v", err)
	}

	// Simulate power-loss zero-filled preallocated tail (NTFS/ext4 behavior).
	f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open wal for zero append: %v", err)
	}
	zeros := make([]byte, 4096)
	if _, err := f.Write(zeros); err != nil {
		t.Fatalf("append zero tail: %v", err)
	}
	_ = f.Close()

	wZero, replayed, truncated, err := OpenWAL(walPath, 1, true)
	if err != nil {
		t.Fatalf("OpenWAL zero-filled tail should recover cleanly: %v", err)
	}
	if len(replayed) != len(batch) || truncated != len(zeros) {
		t.Fatalf("zero-tail recovery got replayed=%d truncated=%d, want %d/%d", len(replayed), truncated, len(batch), len(zeros))
	}
	_ = wZero.Close()

	// Simulate truncated partial frame appended at EOF.
	f, err = os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open wal for torn append: %v", err)
	}
	tornBytes := []byte{0x40, 0x00, 0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03}
	if _, err := f.Write(tornBytes); err != nil {
		t.Fatalf("append torn bytes: %v", err)
	}
	_ = f.Close()

	w3, replayed, truncated, err := OpenWAL(walPath, 1, true)
	if err != nil {
		t.Fatalf("OpenWAL torn tail should recover cleanly: %v", err)
	}
	if len(replayed) != len(batch) || truncated != len(tornBytes) {
		t.Fatalf("torn recovery got replayed=%d truncated=%d, want %d/%d", len(replayed), truncated, len(batch), len(tornBytes))
	}
	_ = w3.Close()

	// Generation fencing: if MANIFEST already advanced to gen=2 (crash before WAL.Reset),
	// OpenWAL(expectedGen=2) MUST skip stale gen=1 frames and reset cleanly to gen=2.
	wFenced, replayed, _, err := OpenWAL(walPath, 2, true)
	if err != nil {
		t.Fatalf("OpenWAL generation fencing failed: %v", err)
	}
	if len(replayed) != 0 || wFenced.Generation() != 2 {
		t.Fatalf("fenced WAL should replay 0 stale records at gen 2, got %d / gen %d", len(replayed), wFenced.Generation())
	}

	// Reset clears WAL atomically to gen=3.
	if err := wFenced.Reset(3); err != nil {
		t.Fatalf("Reset WAL: %v", err)
	}
	if err := wFenced.AppendBatch(batch[:2]); err != nil {
		t.Fatalf("append 2 frames: %v", err)
	}
	_ = wFenced.Close()

	// Corrupt the FIRST frame's payload while second frame remains valid -> mid-log ErrCorruptWAL.
	raw, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("read wal: %v", err)
	}
	// File header is 16B + frame header 8B = offset 24 for first payload byte.
	raw[24] ^= 0xFF
	if err := os.WriteFile(walPath, raw, 0o644); err != nil {
		t.Fatalf("write mid-corrupt wal: %v", err)
	}
	if _, _, _, err := OpenWAL(walPath, 3, true); !errors.Is(err, ErrCorruptWAL) {
		t.Fatalf("expected ErrCorruptWAL on mid-file corruption, got %v", err)
	}

	// Invalid file header magic -> ErrCorruptWAL.
	badPath := filepath.Join(dir, "bad_magic.log")
	if err := os.WriteFile(badPath, []byte("NOTAWAL!12345678"), 0o644); err != nil {
		t.Fatalf("write bad magic: %v", err)
	}
	if _, _, _, err := OpenWAL(badPath, 1, true); !errors.Is(err, ErrCorruptWAL) {
		t.Fatalf("expected ErrCorruptWAL on bad magic, got %v", err)
	}
}
