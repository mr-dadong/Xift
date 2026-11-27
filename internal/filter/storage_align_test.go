package filter_test

import (
	"testing"

	"github.com/mr-dadong/Xift/internal/filter"
	"github.com/mr-dadong/Xift/internal/storage"
)

func TestFilterStorageFlagAlignment(t *testing.T) {
	if filter.FlagDir != storage.FlagDir {
		t.Errorf("FlagDir mismatch: filter=%#x, storage=%#x", filter.FlagDir, storage.FlagDir)
	}
	if filter.FlagFolded != storage.FlagFolded {
		t.Errorf("FlagFolded mismatch: filter=%#x, storage=%#x", filter.FlagFolded, storage.FlagFolded)
	}
	if filter.FlagSystem != storage.FlagSystem {
		t.Errorf("FlagSystem mismatch: filter=%#x, storage=%#x", filter.FlagSystem, storage.FlagSystem)
	}
}
