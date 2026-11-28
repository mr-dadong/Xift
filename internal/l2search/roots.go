package l2search

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const rootsFileName = "roots.json"

type rootsFile struct {
	Roots []string `json:"roots"`
}

// LoadRoots reads the persisted L2 whitelist from the index directory.
func LoadRoots(dir string) ([]string, error) {
	raw, err := os.ReadFile(filepath.Join(dir, rootsFileName))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("l2search: read roots: %w", err)
	}
	var rf rootsFile
	if err := json.Unmarshal(raw, &rf); err != nil {
		return nil, fmt.Errorf("l2search: parse roots: %w", err)
	}
	return rf.Roots, nil
}

// SaveRoots atomically persists the L2 whitelist in the index directory.
func SaveRoots(dir string, roots []string) error {
	raw, err := json.MarshalIndent(rootsFile{Roots: roots}, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("l2search: create index dir: %w", err)
	}
	tmp := filepath.Join(dir, rootsFileName+".tmp")
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return fmt.Errorf("l2search: write roots: %w", err)
	}
	return os.Rename(tmp, filepath.Join(dir, rootsFileName))
}
