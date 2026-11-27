package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/mr-dadong/Xift/internal/query"
	"github.com/mr-dadong/Xift/internal/storage"
)

// StatusOutput is the structured output of `xift status --json`.
type StatusOutput struct {
	DataDir         string  `json:"data_dir"`
	LiveCount       uint64  `json:"live_count"`
	MemTableEntries int     `json:"memtable_entries"`
	RecommendedTier string  `json:"recommended_tier"`
	OpenMs          float64 `json:"open_ms"`
}

// Run executes the xift CLI with the given arguments and IO streams, returning a process exit code.
func Run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) > 0 {
		switch args[0] {
		case "index":
			return runIndex(ctx, args[1:], stdout, stderr)
		case "status":
			return runStatus(ctx, args[1:], stdout, stderr)
		case "search":
			return runSearch(ctx, args[1:], stdout, stderr)
		case "-h", "--help", "help":
			printUsage(stdout)
			return 0
		}
	}
	return runSearch(ctx, args, stdout, stderr)
}

func defaultDataDir() string {
	if env := strings.TrimSpace(os.Getenv("XIFT_DATA_DIR")); env != "" {
		return env
	}
	if cacheDir, err := os.UserCacheDir(); err == nil && cacheDir != "" {
		return filepath.Join(cacheDir, "xift")
	}
	return filepath.Join(os.TempDir(), "xift")
}

func printUsage(w io.Writer) {
	fmt.Fprintln(w, `xift - Cross-platform blazing-fast file search CLI

Usage:
  xift [search flags] <query...>      Search indexed files (default command)
  xift index [flags] [dir...]         Build/update read-only local Tier 1 index
  xift status [flags]                 Display index statistics

Query DSL examples:
  xift report 2026 ext:pdf,docx !draft
  xift "*.go | *.rs" path:src
  xift "size:>50mb dm:7d" --json
  xift "in:node_modules lodash"
  xift "all:kernel32" --limit 20

Search flags:
  --data-dir <dir>    Index data directory (default: $XIFT_DATA_DIR or user cache dir)
  --json              Output structured JSON result
  --jsonl             Output newline-delimited JSON (one item per line)
  -0, --zero          Separate paths with NUL byte instead of newline (for xargs -0)
  -c, --count         Print total match count only
  --limit <n>         Maximum results to return (default: 100)
  --offset <n>        Result pagination offset (default: 0)
  --sort <field>      Sort order: relevance | name | size | mtime (default: relevance)
  --desc              Sort descending (for name/size/mtime)
  --sep <char>        Path separator (default: OS native separator)`)
}

func runSearch(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("xift", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var (
		dataDir   string
		asJSON    bool
		asJSONL   bool
		zeroDelim bool
		countOnly bool
		limit     int
		offset    int
		sortStr   string
		desc      bool
		sepStr    string
	)

	fs.StringVar(&dataDir, "data-dir", defaultDataDir(), "index data directory")
	fs.BoolVar(&asJSON, "json", false, "output structured JSON")
	fs.BoolVar(&asJSONL, "jsonl", false, "output newline-delimited JSON")
	fs.BoolVar(&zeroDelim, "zero", false, "terminate output paths with NUL byte")
	fs.BoolVar(&zeroDelim, "0", false, "terminate output paths with NUL byte")
	fs.BoolVar(&countOnly, "count", false, "output total matched count only")
	fs.BoolVar(&countOnly, "c", false, "output total matched count only")
	fs.IntVar(&limit, "limit", 100, "maximum results to return")
	fs.IntVar(&offset, "offset", 0, "pagination offset")
	fs.StringVar(&sortStr, "sort", "relevance", "sort order: relevance|name|size|mtime")
	fs.BoolVar(&desc, "desc", false, "descending sort order")
	fs.StringVar(&sepStr, "sep", string(os.PathSeparator), "path separator")

	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			printUsage(stdout)
			return 0
		}
		return 2
	}

	sortOrder, err := query.ParseSortOrder(sortStr)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}

	pathSep := byte(os.PathSeparator)
	if len(sepStr) > 0 {
		pathSep = sepStr[0]
	}

	rawQuery := strings.Join(fs.Args(), " ")

	store, err := storage.Open(dataDir, storage.Options{})
	if err != nil {
		fmt.Fprintf(stderr, "xift: open index %q: %v\n", dataDir, err)
		return 1
	}
	defer store.Close()

	qe := query.NewEngine(store)
	res, err := qe.Search(ctx, rawQuery, query.SearchOptions{
		Offset:  offset,
		Limit:   limit,
		Sort:    sortOrder,
		Desc:    desc,
		PathSep: pathSep,
	})
	if err != nil {
		fmt.Fprintf(stderr, "xift: search failed: %v\n", err)
		return 1
	}

	if asJSON {
		enc := json.NewEncoder(stdout)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(res); err != nil {
			fmt.Fprintf(stderr, "xift: encode json: %v\n", err)
			return 1
		}
		return 0
	}

	if countOnly {
		fmt.Fprintf(stdout, "%d\n", res.Total)
		return 0
	}

	bw := bufio.NewWriterSize(stdout, 64*1024)
	defer bw.Flush()

	if asJSONL {
		enc := json.NewEncoder(bw)
		enc.SetEscapeHTML(false)
		for i := range res.Items {
			if err := enc.Encode(&res.Items[i]); err != nil {
				fmt.Fprintf(stderr, "xift: encode jsonl: %v\n", err)
				return 1
			}
		}
		return 0
	}

	term := byte('\n')
	if zeroDelim {
		term = 0
	}
	for i := range res.Items {
		_, _ = bw.WriteString(res.Items[i].Path)
		_ = bw.WriteByte(term)
	}
	return 0
}

func runStatus(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	_ = ctx
	fs := flag.NewFlagSet("xift status", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var (
		dataDir string
		asJSON  bool
	)
	fs.StringVar(&dataDir, "data-dir", defaultDataDir(), "index data directory")
	fs.BoolVar(&asJSON, "json", false, "output JSON")

	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}

	store, err := storage.Open(dataDir, storage.Options{})
	if err != nil {
		fmt.Fprintf(stderr, "xift status: open index %q: %v\n", dataDir, err)
		return 1
	}
	defer store.Close()

	st := store.OpenStats()
	out := StatusOutput{
		DataDir:         dataDir,
		LiveCount:       store.LiveCount(),
		MemTableEntries: store.MemTableEntryCount(),
		RecommendedTier: store.RecommendedTier().String(),
		OpenMs:          float64(st.OpenDuration.Microseconds()) / 1000.0,
	}

	if asJSON {
		enc := json.NewEncoder(stdout)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(out); err != nil {
			fmt.Fprintf(stderr, "xift status: encode json: %v\n", err)
			return 1
		}
		return 0
	}

	fmt.Fprintf(stdout, "DataDir:          %s\n", out.DataDir)
	fmt.Fprintf(stdout, "LiveCount:        %d\n", out.LiveCount)
	fmt.Fprintf(stdout, "MemTableEntries:  %d\n", out.MemTableEntries)
	fmt.Fprintf(stdout, "RecommendedTier:  %s\n", out.RecommendedTier)
	fmt.Fprintf(stdout, "ColdStartMs:      %.2f\n", out.OpenMs)
	return 0
}
