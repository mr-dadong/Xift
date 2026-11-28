package l2search

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const statsFileName = "stats.json"

// PersistedStats is the on-disk form of Stats, written after each flush so that
// `l2 status` can report indexer counters without a running daemon. Skip
// reasons use their stable string labels.
type PersistedStats struct {
	Indexed  int            `json:"indexed"`
	Deleted  int            `json:"deleted"`
	Skipped  map[string]int `json:"skipped,omitempty"`
	Batches  int            `json:"batches"`
	Roots    []string       `json:"roots,omitempty"`
	Paused   bool           `json:"paused"`
	DiskUsed int64          `json:"disk_used"`
	DiskCap  int64          `json:"disk_cap"`
}

// LoadStats reads the persisted indexer counters; a missing file is not an error.
func LoadStats(dir string) (PersistedStats, error) {
	raw, err := os.ReadFile(filepath.Join(dir, statsFileName))
	if errors.Is(err, os.ErrNotExist) {
		return PersistedStats{}, nil
	}
	if err != nil {
		return PersistedStats{}, fmt.Errorf("l2search: read stats: %w", err)
	}
	var st PersistedStats
	if err := json.Unmarshal(raw, &st); err != nil {
		return PersistedStats{}, fmt.Errorf("l2search: parse stats: %w", err)
	}
	return st, nil
}

// SaveStats atomically persists indexer counters.
func SaveStats(dir string, st PersistedStats) error {
	raw, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, statsFileName+".tmp")
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return fmt.Errorf("l2search: write stats: %w", err)
	}
	return os.Rename(tmp, filepath.Join(dir, statsFileName))
}

func statsToDisk(st Stats) PersistedStats {
	out := PersistedStats{
		Indexed:  st.Indexed,
		Deleted:  st.Deleted,
		Batches:  st.Batches,
		Roots:    st.Roots,
		Paused:   st.Paused,
		DiskUsed: st.DiskUsed,
		DiskCap:  st.DiskCap,
	}
	if len(st.Skipped) > 0 {
		out.Skipped = make(map[string]int, len(st.Skipped))
		for reason, n := range st.Skipped {
			if n == 0 {
				continue
			}
			out.Skipped[reason.String()] = n
		}
	}
	return out
}
