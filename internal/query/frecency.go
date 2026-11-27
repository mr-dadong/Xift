package query

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// DefaultFrecencyMaxEntries caps the number of tracked paths in frecency.db.
	DefaultFrecencyMaxEntries = 8192

	// frecencyHalfLifeSeconds is 7 days in seconds (lambda = ln(2) / halfLife).
	frecencyHalfLifeSeconds = 7.0 * 24.0 * 3600.0
	frecencyDecayLambda     = math.Ln2 / frecencyHalfLifeSeconds

	// maxFrecencyBoost caps the relevance score boost so Frecency prioritizes
	// frequently opened peers without overriding an exact match with an unrelated file.
	maxFrecencyBoost = 220.0
	frecencyScale    = 80.0
)

// FrecencyOptions configures FrecencyStore capacity.
type FrecencyOptions struct {
	MaxEntries int
}

type frecencyEntry struct {
	Hash           uint64  `json:"h"`
	Score          float64 `json:"s"`
	LastAccessUnix int64   `json:"t"`
	Count          uint32  `json:"c"`
}

type frecencyDiskFormat struct {
	Version int             `json:"version"`
	Entries []frecencyEntry `json:"entries"`
}

// FrecencyStore maintains a bounded, persistent, exponential-decay Frecency table
// backed by <DataDir>/frecency.db and implements FrecencyProvider.
type FrecencyStore struct {
	filePath   string
	maxEntries int

	saveMu  sync.Mutex
	mu      sync.RWMutex
	entries map[uint64]frecencyEntry
	rev     uint64
	dirty   bool
	count   atomic.Int64
}

// OpenFrecencyStore loads an existing frecency.db (if present) or initializes an empty store.
func OpenFrecencyStore(filePath string, opts FrecencyOptions) (*FrecencyStore, error) {
	maxEntries := opts.MaxEntries
	if maxEntries <= 0 {
		maxEntries = DefaultFrecencyMaxEntries
	}
	fs := &FrecencyStore{
		filePath:   filePath,
		maxEntries: maxEntries,
		entries:    make(map[uint64]frecencyEntry),
	}
	if filePath != "" {
		data, err := os.ReadFile(filePath)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("query: read frecency db %q: %w", filePath, err)
		}
		if len(data) > 0 {
			var disk frecencyDiskFormat
			if err := json.Unmarshal(data, &disk); err == nil {
				for _, e := range disk.Entries {
					if e.Hash != 0 && e.Score > 0 {
						fs.entries[e.Hash] = e
					}
				}
				fs.evictOverflowLocked(time.Now())
				fs.count.Store(int64(len(fs.entries)))
			}
		}
	}
	return fs, nil
}

// HasEntries reports whether the store has at least one recorded path (lock-free fast path).
func (f *FrecencyStore) HasEntries() bool {
	if f == nil {
		return false
	}
	return f.count.Load() > 0
}

// Len returns the number of entries currently tracked.
func (f *FrecencyStore) Len() int {
	if f == nil {
		return 0
	}
	return int(f.count.Load())
}

// Record records a file open event at time.Now().
func (f *FrecencyStore) Record(path string) {
	f.RecordAt(path, time.Now())
}

// RecordAt records a file open event at the given timestamp, applying exponential decay
// to any prior score and evicting the lowest-decayed entry if capacity is exceeded.
func (f *FrecencyStore) RecordAt(path string, now time.Time) {
	if f == nil {
		return
	}
	h := normalizeFrecencyPathHash(path)
	if h == 0 {
		return
	}
	nowUnix := now.Unix()

	f.mu.Lock()
	defer f.mu.Unlock()

	cur := f.entries[h]
	decayed := decayedRawScore(cur, nowUnix)
	cur.Hash = h
	cur.Score = decayed + 1.0
	cur.LastAccessUnix = nowUnix
	cur.Count++
	f.entries[h] = cur
	f.rev++
	f.dirty = true

	f.evictOverflowLocked(now)
	f.count.Store(int64(len(f.entries)))
}

// Score implements FrecencyProvider using time.Now().
func (f *FrecencyStore) Score(_ uint32, path string) float64 {
	return f.ScoreAt(path, time.Now())
}

// ScoreAt evaluates the bounded exponential-decay relevance boost for path at now.
func (f *FrecencyStore) ScoreAt(path string, now time.Time) float64 {
	if f == nil || f.count.Load() == 0 {
		return 0
	}
	h := normalizeFrecencyPathHash(path)
	if h == 0 {
		return 0
	}

	f.mu.RLock()
	entry, ok := f.entries[h]
	f.mu.RUnlock()
	if !ok {
		return 0
	}

	raw := decayedRawScore(entry, now.Unix())
	if raw <= 0 {
		return 0
	}
	boost := frecencyScale * math.Log1p(raw)
	if boost > maxFrecencyBoost {
		boost = maxFrecencyBoost
	}
	return boost
}

// Save atomically persists the Frecency table to disk (temp file + Sync + Rename).
// Serialized with saveMu and revision-tracked so concurrent saves never overwrite newer
// snapshots or drop pending dirty state.
func (f *FrecencyStore) Save() error {
	if f == nil || f.filePath == "" {
		return nil
	}
	f.saveMu.Lock()
	defer f.saveMu.Unlock()

	f.mu.RLock()
	snapRev := f.rev
	entries := make([]frecencyEntry, 0, len(f.entries))
	for _, e := range f.entries {
		entries = append(entries, e)
	}
	f.mu.RUnlock()

	payload, err := json.Marshal(frecencyDiskFormat{
		Version: 1,
		Entries: entries,
	})
	if err != nil {
		return fmt.Errorf("query: marshal frecency db: %w", err)
	}

	dir := filepath.Dir(f.filePath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("query: mkdir frecency dir: %w", err)
	}
	tmp, err := os.CreateTemp(dir, "frecency-*.tmp")
	if err != nil {
		return fmt.Errorf("query: create temp frecency db: %w", err)
	}
	tmpPath := tmp.Name()
	if _, err := tmp.Write(payload); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("query: write temp frecency db: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("query: sync temp frecency db: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("query: close temp frecency db: %w", err)
	}
	if err := os.Rename(tmpPath, f.filePath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("query: rename frecency db: %w", err)
	}

	f.mu.Lock()
	if f.rev == snapRev {
		f.dirty = false
	}
	f.mu.Unlock()
	return nil
}

// Close flushes pending dirty updates to disk.
func (f *FrecencyStore) Close() error {
	if f == nil {
		return nil
	}
	f.mu.RLock()
	dirty := f.dirty
	f.mu.RUnlock()
	if !dirty {
		return nil
	}
	return f.Save()
}

func (f *FrecencyStore) evictOverflowLocked(now time.Time) {
	nowUnix := now.Unix()
	for len(f.entries) > f.maxEntries {
		var victimHash uint64
		victimScore := math.MaxFloat64
		for h, e := range f.entries {
			sc := decayedRawScore(e, nowUnix)
			if sc < victimScore || (sc == victimScore && h < victimHash) {
				victimScore = sc
				victimHash = h
			}
		}
		if victimHash == 0 {
			break
		}
		delete(f.entries, victimHash)
	}
}

func decayedRawScore(e frecencyEntry, nowUnix int64) float64 {
	if e.Score <= 0 {
		return 0
	}
	dt := float64(nowUnix - e.LastAccessUnix)
	if dt <= 0 {
		return e.Score
	}
	return e.Score * math.Exp(-frecencyDecayLambda*dt)
}

func normalizeFrecencyPathHash(path string) uint64 {
	p := strings.TrimSpace(path)
	if p == "" {
		return 0
	}
	p = strings.ReplaceAll(p, "\\", "/")
	for strings.HasSuffix(p, "/") && len(p) > 1 {
		p = strings.TrimSuffix(p, "/")
	}
	h := fnv.New64a()
	var buf [256]byte
	for i := 0; i < len(p); {
		n := len(p) - i
		if n > len(buf) {
			n = len(buf)
		}
		for j := 0; j < n; j++ {
			buf[j] = asciiLower(p[i+j])
		}
		_, _ = h.Write(buf[:n])
		i += n
	}
	sum := h.Sum64()
	if sum == 0 {
		return 1
	}
	return sum
}
