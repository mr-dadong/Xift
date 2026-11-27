package storage

import (
	"os"
	"path/filepath"
	"strings"
)

// DefaultDataDir returns the default Tier 1 index directory shared by xift CLI and xift-daemon.
func DefaultDataDir() string {
	if env := strings.TrimSpace(os.Getenv("XIFT_DATA_DIR")); env != "" {
		return env
	}
	if cacheDir, err := os.UserCacheDir(); err == nil && cacheDir != "" {
		return filepath.Join(cacheDir, "xift")
	}
	return filepath.Join(os.TempDir(), "xift")
}
