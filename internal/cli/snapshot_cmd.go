package cli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"strconv"

	"github.com/mr-dadong/Xift/internal/storage"
)

// SnapshotListOutput is the structured output of `xift snapshot list --json`.
type SnapshotListOutput struct {
	Snapshots []storage.SnapshotInfo `json:"snapshots"`
}

func runSnapshot(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "xift snapshot: expected export, import, list or remove")
		return 2
	}
	switch args[0] {
	case "export":
		return runSnapshotExport(args[1:], stdout, stderr)
	case "import":
		return runSnapshotImport(args[1:], stdout, stderr)
	case "list":
		return runSnapshotList(args[1:], stdout, stderr)
	case "remove":
		return runSnapshotRemove(args[1:], stdout, stderr)
	case "-h", "--help", "help":
		fmt.Fprintln(stdout, `Usage:
  xift snapshot export --volume <id> --out <dir> [--label <name>] [--root <path>] [--json]
  xift snapshot import <package-dir> [--json]
  xift snapshot list [--json]
  xift snapshot remove <id>
Common flags: --data-dir <dir>`)
		return 0
	default:
		fmt.Fprintf(stderr, "xift snapshot: unknown subcommand %q\n", args[0])
		return 2
	}
}

func encodeJSON(stdout, stderr io.Writer, v any, prefix string) int {
	enc := json.NewEncoder(stdout)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		fmt.Fprintf(stderr, "%s: encode json: %v\n", prefix, err)
		return 1
	}
	return 0
}

func runSnapshotExport(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("xift snapshot export", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		dataDir  string
		volume   int
		outDir   string
		label    string
		rootPath string
		asJSON   bool
	)
	fs.StringVar(&dataDir, "data-dir", defaultDataDir(), "index data directory")
	fs.IntVar(&volume, "volume", -1, "volume ID to export")
	fs.StringVar(&outDir, "out", "", "output directory for the snapshot package")
	fs.StringVar(&label, "label", "", "volume label recorded in the package")
	fs.StringVar(&rootPath, "root", "", "volume root path recorded in the package")
	fs.BoolVar(&asJSON, "json", false, "output JSON")
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	if volume < 0 || volume > 65535 || outDir == "" {
		fmt.Fprintln(stderr, "xift snapshot export: --volume and --out are required")
		return 2
	}

	store, err := storage.Open(dataDir, storage.Options{})
	if err != nil {
		fmt.Fprintf(stderr, "xift snapshot export: open index: %v\n", err)
		return 1
	}
	defer store.Close()

	pkg, err := store.ExportVolumeSnapshot(uint16(volume), outDir, storage.ExportOptions{
		VolumeLabel: label,
		RootPath:    rootPath,
	})
	if err != nil {
		fmt.Fprintf(stderr, "xift snapshot export: %v\n", err)
		return 1
	}
	if asJSON {
		return encodeJSON(stdout, stderr, pkg, "xift snapshot export")
	}
	fmt.Fprintf(stdout, "exported volume %d: %d nodes, %d files -> %s\n", pkg.VolumeID, pkg.Nodes, len(pkg.Files), outDir)
	return 0
}

func runSnapshotImport(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("xift snapshot import", flag.ContinueOnError)
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
	if fs.NArg() != 1 {
		fmt.Fprintln(stderr, "xift snapshot import: exactly one package directory is required")
		return 2
	}

	store, err := storage.Open(dataDir, storage.Options{})
	if err != nil {
		fmt.Fprintf(stderr, "xift snapshot import: open index: %v\n", err)
		return 1
	}
	defer store.Close()

	info, err := store.ImportVolumeSnapshot(fs.Arg(0))
	if err != nil {
		fmt.Fprintf(stderr, "xift snapshot import: %v\n", err)
		return 1
	}
	if asJSON {
		return encodeJSON(stdout, stderr, info, "xift snapshot import")
	}
	fmt.Fprintf(stdout, "imported snapshot %d: volume %d (%s), %d nodes\n", info.ID, info.VolumeID, info.VolumeLabel, info.Nodes)
	return 0
}

func runSnapshotList(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("xift snapshot list", flag.ContinueOnError)
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
		fmt.Fprintf(stderr, "xift snapshot list: open index: %v\n", err)
		return 1
	}
	defer store.Close()

	snaps := store.Snapshots()
	if snaps == nil {
		snaps = []storage.SnapshotInfo{}
	}
	if asJSON {
		return encodeJSON(stdout, stderr, SnapshotListOutput{Snapshots: snaps}, "xift snapshot list")
	}
	if len(snaps) == 0 {
		fmt.Fprintln(stdout, "no mounted snapshots")
		return 0
	}
	for _, s := range snaps {
		fmt.Fprintf(stdout, "%d\tvol=%d\t%s\t%d nodes\t%s\n", s.ID, s.VolumeID, s.VolumeLabel, s.Nodes, s.RootPath)
	}
	return 0
}

func runSnapshotRemove(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("xift snapshot remove", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var dataDir string
	fs.StringVar(&dataDir, "data-dir", defaultDataDir(), "index data directory")
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	if fs.NArg() != 1 {
		fmt.Fprintln(stderr, "xift snapshot remove: exactly one snapshot ID is required")
		return 2
	}
	id, err := strconv.ParseUint(fs.Arg(0), 10, 32)
	if err != nil {
		fmt.Fprintf(stderr, "xift snapshot remove: invalid snapshot ID %q\n", fs.Arg(0))
		return 2
	}

	store, err := storage.Open(dataDir, storage.Options{})
	if err != nil {
		fmt.Fprintf(stderr, "xift snapshot remove: open index: %v\n", err)
		return 1
	}
	defer store.Close()

	if err := store.RemoveSnapshot(uint32(id)); err != nil {
		fmt.Fprintf(stderr, "xift snapshot remove: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "removed snapshot %d\n", id)
	return 0
}
