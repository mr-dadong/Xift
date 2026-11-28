// Command xift-bench builds a synthetic Tier 2 index and reports build time,
// disk usage, cold-start time, query latency percentiles, and memory samples.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"time"

	"github.com/mr-dadong/Xift/internal/query"
	"github.com/mr-dadong/Xift/internal/storage"
)

const queryTerms = "report|annual|design_000|invoice|设计文档|note|archive"

type report struct {
	Nodes       int     `json:"nodes"`
	Dirs        int     `json:"dirs"`
	Tier        string  `json:"tier"`
	BuildMs     float64 `json:"build_ms"`
	DiskBytes   int64   `json:"disk_bytes"`
	BuildRSS    uint64  `json:"build_rss_bytes"`
	ColdStartMs float64 `json:"cold_start_ms"`
	QueryCount  int     `json:"query_count"`
	QueryP50Ms  float64 `json:"query_p50_ms"`
	QueryP95Ms  float64 `json:"query_p95_ms"`
	QueryP99Ms  float64 `json:"query_p99_ms"`
	QueryRSS    uint64  `json:"query_rss_bytes"`
	HeapAlloc   uint64  `json:"heap_alloc_bytes"`
	CacheBytes  int64   `json:"cache_bytes"`
	CacheLimit  int64   `json:"cache_limit_bytes"`
}

func main() {
	nodes := flag.Int("nodes", 1_000_000, "file node count")
	dirs := flag.Int("dirs", 100_000, "directory node count")
	dataDir := flag.String("data-dir", "", "index directory (defaults to a temp dir)")
	keep := flag.Bool("keep", false, "keep the generated index directory")
	tier2 := flag.Bool("tier2", true, "force Tier 2 (low-memory) builds")
	cacheBytes := flag.Int64("cache-bytes", 0, "shared block cache budget in bytes (0 = 128 MB)")
	queries := flag.Int("queries", 50, "query latency samples")
	terms := flag.String("terms", queryTerms, "pipe-separated query terms")
	asJSON := flag.Bool("json", false, "emit a JSON report")
	flag.Parse()

	rep, err := run(*nodes, *dirs, *dataDir, *keep, *tier2, *cacheBytes, *queries, *terms)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] %v\n", err)
		os.Exit(1)
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(rep)
		return
	}
	fmt.Printf("Synthetic nodes:   %d files + %d dirs\n", rep.Nodes, rep.Dirs)
	fmt.Printf("Tier:              %s\n", rep.Tier)
	fmt.Printf("Build time:        %.2f ms\n", rep.BuildMs)
	fmt.Printf("Disk usage:        %.2f MiB\n", float64(rep.DiskBytes)/(1<<20))
	fmt.Printf("Build peak RSS:    %.2f MiB\n", float64(rep.BuildRSS)/(1<<20))
	fmt.Printf("Cold start:        %.2f ms\n", rep.ColdStartMs)
	fmt.Printf("Query p50/p95/p99: %.2f / %.2f / %.2f ms (%d samples)\n", rep.QueryP50Ms, rep.QueryP95Ms, rep.QueryP99Ms, rep.QueryCount)
	fmt.Printf("Query RSS:         %.2f MiB (HeapAlloc %.2f MiB)\n", float64(rep.QueryRSS)/(1<<20), float64(rep.HeapAlloc)/(1<<20))
	fmt.Printf("Block cache:       %.2f / %.2f MiB\n", float64(rep.CacheBytes)/(1<<20), float64(rep.CacheLimit)/(1<<20))
}

func run(nodes, dirs int, dataDir string, keep, tier2 bool, cacheBytes int64, querySamples int, termList string) (report, error) {
	var rep report
	if nodes <= 0 {
		return rep, fmt.Errorf("-nodes must be positive")
	}
	if dataDir == "" {
		dir, err := os.MkdirTemp("", "xift-bench-*")
		if err != nil {
			return rep, fmt.Errorf("create temp dir: %w", err)
		}
		dataDir = dir
		if !keep {
			defer os.RemoveAll(dataDir)
		}
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return rep, err
	}

	opts := storage.Options{
		DisableWALSync:  true,
		LowMemoryMode:   tier2,
		BlockCacheBytes: cacheBytes,
	}

	buildStart := time.Now()
	store, err := storage.Open(dataDir, opts)
	if err != nil {
		return rep, fmt.Errorf("open index: %w", err)
	}
	builder, err := store.BeginDenseSegment()
	if err != nil {
		_ = store.Close()
		return rep, fmt.Errorf("begin dense segment: %w", err)
	}

	counts := syntheticCounts{Dirs: dirs, Files: nodes}
	total := counts.totalNodes()
	startMTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	var appendErr error
	seen := 0
	streamSynthetic(counts, startMTime, func(row syntheticRow) {
		if appendErr != nil {
			return
		}
		parentID := uint32(0)
		if row.ParentKey > 0 {
			parentID = uint32(row.ParentKey - 1)
		}
		var flags uint16
		if row.IsDir {
			flags = storage.FlagDir
		}
		if _, appendErr = builder.AppendDense(1, row.NodeKey, parentID, row.Name, flags, 0, row.Size, row.ModTime); appendErr != nil {
			return
		}
		seen++
		if seen%1_000_000 == 0 {
			fmt.Fprintf(os.Stderr, "[bench] indexed %d/%d nodes\n", seen, total)
		}
	})
	if appendErr != nil {
		builder.Abort()
		_ = store.Close()
		return rep, fmt.Errorf("append: %w", appendErr)
	}
	meta, err := builder.Finish()
	if err != nil {
		builder.Abort()
		_ = store.Close()
		return rep, fmt.Errorf("finish segment: %w", err)
	}
	if err := store.CommitSegment(meta, nil); err != nil {
		_ = store.Close()
		return rep, fmt.Errorf("commit segment: %w", err)
	}
	rep.BuildMs = float64(time.Since(buildStart).Microseconds()) / 1000.0
	rep.BuildRSS = residentBytes()
	rep.DiskBytes = dirSize(dataDir)
	rep.Tier = store.ActiveTier().String()
	if err := store.Close(); err != nil {
		return rep, fmt.Errorf("close after build: %w", err)
	}

	coldStart := time.Now()
	store, err = storage.Open(dataDir, opts)
	if err != nil {
		return rep, fmt.Errorf("cold open: %w", err)
	}
	defer store.Close()
	rep.ColdStartMs = float64(time.Since(coldStart).Microseconds()) / 1000.0

	qe := query.NewEngine(store)
	ctx := context.Background()
	terms := splitTerms(termList)
	if len(terms) == 0 {
		terms = splitTerms(queryTerms)
	}
	searchOpts := query.SearchOptions{Limit: 100}

	// Warm up the cache with one pass, then sample.
	for _, term := range terms {
		if _, err := qe.Search(ctx, term, searchOpts); err != nil {
			return rep, fmt.Errorf("warmup query %q: %w", term, err)
		}
	}

	latencies := make([]time.Duration, 0, querySamples)
	for i := 0; i < querySamples; i++ {
		term := terms[i%len(terms)]
		begin := time.Now()
		if _, err := qe.Search(ctx, term, searchOpts); err != nil {
			return rep, fmt.Errorf("query %q: %w", term, err)
		}
		latencies = append(latencies, time.Since(begin))
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	rep.QueryCount = len(latencies)
	rep.QueryP50Ms = msAt(latencies, 0.50)
	rep.QueryP95Ms = msAt(latencies, 0.95)
	rep.QueryP99Ms = msAt(latencies, 0.99)

	debug.FreeOSMemory()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	rep.HeapAlloc = ms.HeapAlloc
	rep.QueryRSS = residentBytes()
	rep.CacheBytes, rep.CacheLimit = store.BlockCacheStats()
	rep.Nodes = nodes
	rep.Dirs = dirs
	return rep, nil
}

func splitTerms(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == '|' {
			if i > start {
				out = append(out, s[start:i])
			}
			start = i + 1
		}
	}
	return out
}

func msAt(d []time.Duration, q float64) float64 {
	if len(d) == 0 {
		return 0
	}
	idx := int(float64(len(d)-1) * q)
	return float64(d[idx].Microseconds()) / 1000.0
}

func dirSize(root string) int64 {
	var total int64
	_ = filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if info, statErr := d.Info(); statErr == nil {
			total += info.Size()
		}
		return nil
	})
	return total
}
