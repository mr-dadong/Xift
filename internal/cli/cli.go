package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/mr-dadong/Xift/internal/ipc"
	"github.com/mr-dadong/Xift/internal/query"
	"github.com/mr-dadong/Xift/internal/storage"
)

// StatusOutput is the structured output of `xift status --json`.
type StatusOutput struct {
	DataDir         string  `json:"data_dir"`
	LiveCount       uint64  `json:"live_count"`
	MemTableEntries int     `json:"memtable_entries"`
	ActiveTier      string  `json:"active_tier"`
	RecommendedTier string  `json:"recommended_tier"`
	BlockCacheBytes int64   `json:"block_cache_bytes"`
	BlockCacheLimit int64   `json:"block_cache_limit_bytes"`
	OpenMs          float64 `json:"open_ms"`
}

// DaemonStatusOutput is the structured output of `xift status --daemon --json`.
type DaemonStatusOutput struct {
	Endpoint        string `json:"endpoint"`
	ProtocolVersion uint16 `json:"protocol_version"`
	LiveCount       uint64 `json:"live_count"`
	ActiveSessions  uint32 `json:"active_sessions"`
	RejectedConns   uint64 `json:"rejected_conns"`
}

// Run executes the xift CLI with the given arguments and IO streams, returning a process exit code.
func Run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) > 0 {
		switch args[0] {
		case "index":
			return runIndex(ctx, args[1:], stdout, stderr)
		case "status":
			return runStatus(ctx, args[1:], stdout, stderr)
		case "snapshot":
			return runSnapshot(ctx, args[1:], stdout, stderr)
		case "l2":
			return runL2(ctx, args[1:], stdout, stderr)
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
	return storage.DefaultDataDir()
}

func printUsage(w io.Writer) {
	fmt.Fprintln(w, `xift - Cross-platform blazing-fast file search CLI

Usage:
  xift [search flags] <query...>      Search indexed files (default command)
  xift index [flags] [dir...]         Build/update read-only local Tier 1 index
                                      (Note: do not run xift index while xift-daemon is running)
  xift status [flags]                 Display index or Daemon statistics
  xift snapshot export|import|list|remove
                                      Export/import/mount read-only volume snapshots
  xift l2 add|remove|status           Manage the whitelisted L2 content index

Query DSL examples:
  xift report 2026 ext:pdf,docx !draft
  xift "*.go | *.rs" path:src
  xift "size:>50mb dm:7d" --json
  xift "in:node_modules lodash"
  xift "all:kernel32" --limit 20
  xift --daemon ext:go --limit 50

Search flags:
  --data-dir <dir>    Index data directory (default: $XIFT_DATA_DIR or user cache dir)
  --daemon            Query running xift-daemon over secure local IPC (no local fallback)
  --endpoint <addr>   Override Daemon IPC endpoint (used with --daemon)
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

func resolveEndpointDisplay(endpoint string) string {
	if endpoint != "" {
		return endpoint
	}
	if ep, err := ipc.DefaultEndpoint(); err == nil {
		return ep
	}
	return "<default>"
}

func runSearch(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("xift", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var (
		dataDir   string
		useDaemon bool
		endpoint  string
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
	fs.BoolVar(&useDaemon, "daemon", false, "query running xift-daemon over IPC")
	fs.StringVar(&endpoint, "endpoint", "", "override Daemon IPC endpoint")
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

	var res *query.SearchResult
	if useDaemon {
		res, err = searchViaDaemon(ctx, endpoint, rawQuery, offset, limit, sortOrder, desc, pathSep, countOnly && !asJSON)
		if err != nil {
			fmt.Fprintf(stderr, "xift: %v\n", err)
			return 1
		}
	} else {
		store, oerr := storage.Open(dataDir, storage.Options{})
		if oerr != nil {
			fmt.Fprintf(stderr, "xift: open index %q: %v\n", dataDir, oerr)
			return 1
		}
		defer store.Close()

		qe := query.NewEngine(store)
		res, err = qe.Search(ctx, rawQuery, query.SearchOptions{
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

func searchViaDaemon(
	ctx context.Context,
	endpoint string,
	rawQuery string,
	offset int,
	limit int,
	sortOrder query.SortOrder,
	desc bool,
	pathSep byte,
	countOnly bool,
) (*query.SearchResult, error) {
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = 100
	}
	epDisplay := resolveEndpointDisplay(endpoint)
	cli, err := ipc.Dial(ctx, endpoint)
	if err != nil {
		return nil, fmt.Errorf("connect daemon %s: %w", epDisplay, err)
	}
	defer cli.Close()

	meta, err := cli.QueryStart(ctx, rawQuery, uint8(sortOrder), desc, pathSep)
	if err != nil {
		return nil, fmt.Errorf("daemon query %s: %w", epDisplay, err)
	}
	defer func() { _ = cli.Cancel(meta.SessionID) }()

	if countOnly {
		return &query.SearchResult{
			Query:       rawQuery,
			Total:       int(meta.Total),
			Returned:    0,
			Offset:      offset,
			Limit:       limit,
			CostMs:      float64(meta.CostMicros) / 1000.0,
			Incremental: meta.Incremental,
			Warnings:    meta.Warnings,
			Items:       []query.ResultItem{},
		}, nil
	}

	capHint := int(meta.Retained) - offset
	if capHint < 0 {
		capHint = 0
	}
	if capHint > limit {
		capHint = limit
	}
	items := make([]query.ResultItem, 0, capHint)
	curOff := offset
	remaining := limit
	for remaining > 0 && uint64(curOff) < uint64(meta.Retained) {
		batch := remaining
		if batch > ipc.MaxViewportLimit {
			batch = ipc.MaxViewportLimit
		}
		vp, err := cli.FetchViewport(ctx, meta.SessionID, uint32(curOff), uint16(batch))
		if err != nil {
			return nil, fmt.Errorf("daemon fetch viewport %s: %w", epDisplay, err)
		}
		for i := range vp.Rows {
			r := &vp.Rows[i]
			fromFolded := (r.Flags & ipc.RowFlagFromFoldedScan) != 0
			cleanFlags := r.Flags &^ ipc.RowFlagFromFoldedScan
			var hls []query.HighlightRange
			if len(r.Highlights) > 0 {
				hls = make([]query.HighlightRange, len(r.Highlights))
				for j, h := range r.Highlights {
					hls[j] = query.HighlightRange{Start: int(h.Start), End: int(h.End)}
				}
			}
			items = append(items, query.ResultItem{
				NodeID:         r.NodeID,
				Path:           r.Path,
				Name:           r.Name,
				Flags:          cleanFlags,
				ExtID:          r.ExtID,
				VolID:          r.VolID,
				IsDir:          (cleanFlags & storage.FlagDir) != 0,
				Folded:         (cleanFlags & storage.FlagFolded) != 0,
				System:         (cleanFlags & storage.FlagSystem) != 0,
				FromFoldedScan: fromFolded,
				Size:           r.Size,
				ModTime:        r.ModTime,
				Score:          r.Score,
				Highlights:     hls,
			})
		}
		if len(vp.Rows) < batch {
			break
		}
		curOff += len(vp.Rows)
		remaining -= len(vp.Rows)
	}

	return &query.SearchResult{
		Query:       rawQuery,
		Total:       int(meta.Total),
		Returned:    len(items),
		Offset:      offset,
		Limit:       limit,
		CostMs:      float64(meta.CostMicros) / 1000.0,
		Incremental: meta.Incremental,
		Warnings:    meta.Warnings,
		Items:       items,
	}, nil
}

func runStatus(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("xift status", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var (
		dataDir   string
		useDaemon bool
		endpoint  string
		asJSON    bool
	)
	fs.StringVar(&dataDir, "data-dir", defaultDataDir(), "index data directory")
	fs.BoolVar(&useDaemon, "daemon", false, "query running xift-daemon status over IPC")
	fs.StringVar(&endpoint, "endpoint", "", "override Daemon IPC endpoint")
	fs.BoolVar(&asJSON, "json", false, "output JSON")

	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}

	if useDaemon {
		epDisplay := resolveEndpointDisplay(endpoint)
		cli, err := ipc.Dial(ctx, endpoint)
		if err != nil {
			fmt.Fprintf(stderr, "xift status: connect daemon %s: %v\n", epDisplay, err)
			return 1
		}
		defer cli.Close()

		st, err := cli.Status(ctx)
		if err != nil {
			fmt.Fprintf(stderr, "xift status: daemon %s: %v\n", epDisplay, err)
			return 1
		}
		out := DaemonStatusOutput{
			Endpoint:        epDisplay,
			ProtocolVersion: st.ProtocolVersion,
			LiveCount:       st.LiveCount,
			ActiveSessions:  st.ActiveSessions,
			RejectedConns:   st.RejectedConns,
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
		fmt.Fprintf(stdout, "Endpoint:         %s\n", out.Endpoint)
		fmt.Fprintf(stdout, "ProtocolVersion:  %d\n", out.ProtocolVersion)
		fmt.Fprintf(stdout, "LiveCount:        %d\n", out.LiveCount)
		fmt.Fprintf(stdout, "ActiveSessions:   %d\n", out.ActiveSessions)
		fmt.Fprintf(stdout, "RejectedConns:    %d\n", out.RejectedConns)
		return 0
	}

	store, err := storage.Open(dataDir, storage.Options{})
	if err != nil {
		fmt.Fprintf(stderr, "xift status: open index %q: %v\n", dataDir, err)
		return 1
	}
	defer store.Close()

	st := store.OpenStats()
	cacheUsed, cacheLimit := store.BlockCacheStats()
	out := StatusOutput{
		DataDir:         dataDir,
		LiveCount:       store.LiveCount(),
		MemTableEntries: store.MemTableEntryCount(),
		ActiveTier:      store.ActiveTier().String(),
		RecommendedTier: store.RecommendedTier().String(),
		BlockCacheBytes: cacheUsed,
		BlockCacheLimit: cacheLimit,
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
	fmt.Fprintf(stdout, "ActiveTier:       %s\n", out.ActiveTier)
	fmt.Fprintf(stdout, "RecommendedTier:  %s\n", out.RecommendedTier)
	fmt.Fprintf(stdout, "BlockCacheBytes:  %d / %d\n", out.BlockCacheBytes, out.BlockCacheLimit)
	fmt.Fprintf(stdout, "ColdStartMs:      %.2f\n", out.OpenMs)
	return 0
}
