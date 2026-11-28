package cli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/mr-dadong/Xift/internal/ipc"
	"github.com/mr-dadong/Xift/internal/l2search"
)

// L2StatusOutput is the structured output of `xift l2 status --json`.
type L2StatusOutput struct {
	Enabled  bool           `json:"enabled"`
	Roots    []string       `json:"roots"`
	Indexed  int            `json:"indexed"`
	Deleted  int            `json:"deleted"`
	Batches  int            `json:"batches"`
	Skipped  map[string]int `json:"skipped,omitempty"`
	Paused   bool           `json:"paused"`
	DiskUsed int64          `json:"disk_used"`
	DiskCap  int64          `json:"disk_cap"`
	Docs     int            `json:"docs"`
	Segments int            `json:"segments"`
}

func l2Dir(dataDir string) string {
	return filepath.Join(dataDir, "l2")
}

func runL2(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "xift l2: expected add, remove or status")
		return 2
	}
	sub := args[0]
	switch sub {
	case "add", "remove":
		return runL2Whitelist(ctx, sub, args[1:], stdout, stderr)
	case "status":
		return runL2StatusCmd(ctx, args[1:], stdout, stderr)
	case "-h", "--help", "help":
		fmt.Fprintln(stdout, "Usage: xift l2 add <dir> | remove <dir> | status [--data-dir <dir>] [--json]")
		fmt.Fprintln(stdout, "add whitelists a directory; a running daemon scans and indexes it immediately.")
		return 0
	default:
		fmt.Fprintf(stderr, "xift l2: unknown subcommand %q\n", sub)
		return 2
	}
}

func runL2Whitelist(ctx context.Context, sub string, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("xift l2 "+sub, flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		dataDir   string
		useDaemon bool
		endpoint  string
		asJSON    bool
	)
	fs.StringVar(&dataDir, "data-dir", defaultDataDir(), "index data directory")
	fs.BoolVar(&useDaemon, "daemon", false, "apply through the running xift-daemon")
	fs.StringVar(&endpoint, "endpoint", "", "override Daemon IPC endpoint")
	fs.BoolVar(&asJSON, "json", false, "output JSON")
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	if fs.NArg() != 1 {
		fmt.Fprintf(stderr, "xift l2 %s: exactly one directory is required\n", sub)
		return 2
	}
	dir := fs.Arg(0)

	if useDaemon {
		cli, err := ipc.Dial(ctx, endpoint)
		if err != nil {
			fmt.Fprintf(stderr, "xift l2 %s: connect daemon: %v\n", sub, err)
			return 1
		}
		defer cli.Close()
		payload, _ := json.Marshal(struct {
			Dir string `json:"dir"`
		}{Dir: dir})
		out, err := cli.Admin(ctx, "l2."+sub, payload)
		if err != nil {
			fmt.Fprintf(stderr, "xift l2 %s: %v\n", sub, err)
			return 1
		}
		var resp struct {
			Roots []string `json:"roots"`
		}
		_ = json.Unmarshal(out, &resp)
		return writeL2Roots(stdout, stderr, asJSON, resp.Roots)
	}

	if sub == "add" {
		abs, err := filepath.Abs(dir)
		if err != nil {
			fmt.Fprintf(stderr, "xift l2 add: %v\n", err)
			return 1
		}
		info, err := os.Stat(abs)
		if err != nil || !info.IsDir() {
			fmt.Fprintf(stderr, "xift l2 add: %q is not a directory\n", dir)
			return 1
		}
		dir = abs
	}
	roots, err := l2search.LoadRoots(l2Dir(dataDir))
	if err != nil {
		fmt.Fprintf(stderr, "xift l2 %s: %v\n", sub, err)
		return 1
	}
	if sub == "add" {
		roots = appendRootPath(roots, dir)
	} else {
		out := roots[:0]
		for _, r := range roots {
			if r != dir {
				out = append(out, r)
			}
		}
		roots = out
	}
	if err := l2search.SaveRoots(l2Dir(dataDir), roots); err != nil {
		fmt.Fprintf(stderr, "xift l2 %s: %v\n", sub, err)
		return 1
	}
	return writeL2Roots(stdout, stderr, asJSON, roots)
}

func writeL2Roots(stdout, stderr io.Writer, asJSON bool, roots []string) int {
	if roots == nil {
		roots = []string{}
	}
	if asJSON {
		enc := json.NewEncoder(stdout)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(struct {
			Roots []string `json:"roots"`
		}{Roots: roots}); err != nil {
			fmt.Fprintf(stderr, "xift l2: encode json: %v\n", err)
			return 1
		}
		return 0
	}
	for _, r := range roots {
		fmt.Fprintln(stdout, r)
	}
	fmt.Fprintf(stderr, "note: %d whitelisted directories; a daemon started with L2 enabled builds the content index\n", len(roots))
	return 0
}

func appendRootPath(roots []string, dir string) []string {
	for _, r := range roots {
		if r == dir {
			return roots
		}
	}
	return append(roots, dir)
}

func runL2StatusCmd(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("xift l2 status", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		dataDir   string
		useDaemon bool
		endpoint  string
		asJSON    bool
	)
	fs.StringVar(&dataDir, "data-dir", defaultDataDir(), "index data directory")
	fs.BoolVar(&useDaemon, "daemon", false, "query the running xift-daemon")
	fs.StringVar(&endpoint, "endpoint", "", "override Daemon IPC endpoint")
	fs.BoolVar(&asJSON, "json", false, "output JSON")
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}

	dir := l2Dir(dataDir)
	roots, err := l2search.LoadRoots(dir)
	if err != nil {
		fmt.Fprintf(stderr, "xift l2 status: %v\n", err)
		return 1
	}
	if roots == nil {
		roots = []string{}
	}
	st, err := l2search.LoadStats(dir)
	if err != nil {
		fmt.Fprintf(stderr, "xift l2 status: %v\n", err)
		return 1
	}
	out := L2StatusOutput{
		Enabled:  len(roots) > 0,
		Roots:    roots,
		Indexed:  st.Indexed,
		Deleted:  st.Deleted,
		Batches:  st.Batches,
		Skipped:  st.Skipped,
		Paused:   st.Paused,
		DiskUsed: st.DiskUsed,
		DiskCap:  st.DiskCap,
	}

	store, err := l2search.OpenStore(dir, nil)
	if err == nil {
		out.Docs = store.DocCount()
		out.Segments = store.SegmentCount()
		out.DiskUsed = store.DiskBytes()
		out.Paused = out.Paused || store.Paused()
		if st.DiskCap > 0 {
			out.DiskCap = st.DiskCap
		} else {
			out.DiskCap = l2search.DefaultDiskCapBytes
		}
		_ = store.Close()
	}
	if out.Skipped == nil {
		out.Skipped = map[string]int{}
	}

	if useDaemon {
		cli, derr := ipc.Dial(ctx, endpoint)
		if derr != nil {
			fmt.Fprintf(stderr, "xift l2 status: connect daemon: %v\n", derr)
			return 1
		}
		defer cli.Close()
		if payload, derr := cli.Admin(ctx, "l2.status", nil); derr == nil {
			var live ipc.L2Status
			if json.Unmarshal(payload, &live) == nil {
				out.Roots = live.Roots
				out.Enabled = len(live.Roots) > 0
				out.Indexed = live.Indexed
				out.Deleted = live.Deleted
				out.Batches = live.Batches
				out.Skipped = live.Skipped
				out.Paused = live.Paused
				out.DiskUsed = live.DiskUsed
				out.DiskCap = live.DiskCap
			}
		}
	}

	if asJSON {
		enc := json.NewEncoder(stdout)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(out); err != nil {
			fmt.Fprintf(stderr, "xift l2 status: encode json: %v\n", err)
			return 1
		}
		return 0
	}
	fmt.Fprintf(stdout, "Enabled:     %v\n", out.Enabled)
	fmt.Fprintf(stdout, "Roots:       %d\n", len(out.Roots))
	for _, r := range out.Roots {
		fmt.Fprintf(stdout, "  - %s\n", r)
	}
	fmt.Fprintf(stdout, "Indexed:     %d\n", out.Indexed)
	fmt.Fprintf(stdout, "Docs:        %d\n", out.Docs)
	fmt.Fprintf(stdout, "Deleted:     %d\n", out.Deleted)
	fmt.Fprintf(stdout, "Batches:     %d\n", out.Batches)
	for _, reason := range []string{"binary", "oversized", "invalid_utf8", "read_error", "no_path"} {
		if n := out.Skipped[reason]; n > 0 {
			fmt.Fprintf(stdout, "Skipped:     %s=%d\n", reason, n)
		}
	}
	fmt.Fprintf(stdout, "DiskUsed:    %d / %d\n", out.DiskUsed, out.DiskCap)
	fmt.Fprintf(stdout, "Paused:      %v\n", out.Paused)
	return 0
}
