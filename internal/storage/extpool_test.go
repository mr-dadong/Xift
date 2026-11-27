package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestExtPoolInternPersistAndCorruption(t *testing.T) {
	dir := t.TempDir()
	poolPath := filepath.Join(dir, "ext.pool")

	pool, err := LoadExtPool(poolPath)
	if err != nil {
		t.Fatalf("LoadExtPool missing should return empty pool: %v", err)
	}
	if pool.Count() != 0 {
		t.Fatalf("initial count = %d, want 0", pool.Count())
	}

	idGo := pool.InternFromName("engine.go", false)
	idMD1 := pool.InternFromName("README.MD", false)
	idMD2 := pool.InternFromName("design.md", false)
	idPDF := pool.InternFromName("Report.Pdf", false)
	idDir := pool.InternFromName("folder.go", true)
	idNoExt := pool.InternFromName("Makefile", false)
	idDotOnly := pool.InternFromName(".gitignore", false)

	if idGo == 0 || idMD1 == 0 || idPDF == 0 {
		t.Fatalf("expected non-zero ext IDs: go=%d md=%d pdf=%d", idGo, idMD1, idPDF)
	}
	if idMD1 != idMD2 {
		t.Fatalf("case-insensitive ext mismatch: .MD=%d vs .md=%d", idMD1, idMD2)
	}
	if idDir != 0 || idNoExt != 0 || idDotOnly != 0 {
		t.Fatalf("expected 0 for dir/no-ext/dotfile: dir=%d noext=%d dot=%d", idDir, idNoExt, idDotOnly)
	}

	if err := pool.Save(poolPath); err != nil {
		t.Fatalf("Save ext.pool: %v", err)
	}

	// Reload and verify stable IDs + new ext allocation.
	reloaded, err := LoadExtPool(poolPath)
	if err != nil {
		t.Fatalf("LoadExtPool reload: %v", err)
	}
	if reloaded.Count() != 3 {
		t.Fatalf("reloaded count = %d, want 3", reloaded.Count())
	}
	if got := reloaded.Intern("GO"); got != idGo {
		t.Fatalf("reloaded GO id = %d, want %d", got, idGo)
	}
	if got := reloaded.Intern(".md"); got != idMD1 {
		t.Fatalf("reloaded .md id = %d, want %d", got, idMD1)
	}
	if got := reloaded.Ext(idPDF); got != "pdf" {
		t.Fatalf("Ext(%d) = %q, want %q", idPDF, got, "pdf")
	}
	idRS := reloaded.InternFromName("lib.rs", false)
	if idRS == 0 || idRS == idGo || idRS == idMD1 || idRS == idPDF {
		t.Fatalf("unexpected new ext ID for .rs: %d", idRS)
	}

	// Corrupt checksum must be rejected.
	raw, err := os.ReadFile(poolPath)
	if err != nil {
		t.Fatalf("read ext.pool: %v", err)
	}
	raw[len(raw)-1] ^= 0xFF
	if err := os.WriteFile(poolPath, raw, 0o644); err != nil {
		t.Fatalf("corrupt ext.pool: %v", err)
	}
	if _, err := LoadExtPool(poolPath); !errors.Is(err, ErrCorruptExtPool) {
		t.Fatalf("expected ErrCorruptExtPool, got %v", err)
	}
}
