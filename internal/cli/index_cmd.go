package cli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mr-dadong/Xift/internal/filter"
	"github.com/mr-dadong/Xift/internal/provider"
	"github.com/mr-dadong/Xift/internal/provider/platform"
	"github.com/mr-dadong/Xift/internal/storage"
)

// IndexStats is the structured summary returned by `xift index --json`.
type IndexStats struct {
	DataDir      string  `json:"data_dir"`
	IndexedNodes uint64  `json:"indexed_nodes"`
	Volumes      int     `json:"volumes"`
	DurationMs   float64 `json:"duration_ms"`
}

func runIndex(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fsFlags := flag.NewFlagSet("xift index", flag.ContinueOnError)
	fsFlags.SetOutput(stderr)

	var (
		dataDir string
		rebuild bool
		noPrune bool
		asJSON  bool
	)
	fsFlags.StringVar(&dataDir, "data-dir", defaultDataDir(), "index data directory")
	fsFlags.BoolVar(&rebuild, "rebuild", false, "clear existing index before scanning")
	fsFlags.BoolVar(&noPrune, "no-prune", false, "disable default noise pruning presets")
	fsFlags.BoolVar(&asJSON, "json", false, "output JSON stats")

	if err := fsFlags.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}

	if rebuild {
		if err := os.RemoveAll(filepath.Join(dataDir, "index")); err != nil {
			fmt.Fprintf(stderr, "xift index: clear old index: %v\n", err)
			return 1
		}
	}

	start := time.Now()
	store, err := storage.Open(dataDir, storage.Options{})
	if err != nil {
		fmt.Fprintf(stderr, "xift index: open storage %q: %v\n", dataDir, err)
		return 1
	}
	defer store.Close()

	var pruner *filter.Pruner
	if noPrune {
		disabled := make(map[filter.ProfileID]bool)
		for _, prof := range filter.BuiltInProfiles() {
			disabled[prof.ID] = false
		}
		pruner = filter.NewPruner(filter.NewRuleEngine(filter.RuleEngineConfig{EnabledProfiles: disabled}), nil)
	} else {
		pruner = filter.NewPruner(nil, nil)
	}

	targets := fsFlags.Args()
	var totalNodes uint64
	volumes := 0

	if len(targets) > 0 {
		for idx, rawTarget := range targets {
			absPath, err := filepath.Abs(rawTarget)
			if err != nil {
				fmt.Fprintf(stderr, "xift index: resolve %q: %v\n", rawTarget, err)
				return 1
			}
			n, err := indexDirectoryTree(ctx, store, pruner, uint16(idx+1), absPath)
			if err != nil {
				fmt.Fprintf(stderr, "xift index: scan %q: %v\n", absPath, err)
				return 1
			}
			totalNodes += uint64(n)
			volumes++
		}
	} else {
		prov, err := platform.NewDefaultProvider(pruner, provider.Options{})
		if err != nil {
			fmt.Fprintf(stderr, "xift index: init platform provider: %v\n", err)
			return 1
		}
		vols, err := prov.EnumerateVolumes(ctx)
		if err != nil {
			fmt.Fprintf(stderr, "xift index: enumerate volumes: %v\n", err)
			return 1
		}
		for _, vol := range vols {
			sw, err := provider.NewStorageSegmentWriter(store, vol)
			if err != nil {
				fmt.Fprintf(stderr, "xift index: begin segment for %s: %v\n", vol.RootPath, err)
				return 1
			}
			cursor, err := prov.FullScan(ctx, vol, sw)
			if err != nil {
				sw.Abort()
				fmt.Fprintf(stderr, "xift index: scan volume %s: %v\n", vol.RootPath, err)
				return 1
			}
			meta, err := sw.Commit(cursor)
			if err != nil {
				fmt.Fprintf(stderr, "xift index: commit segment for %s: %v\n", vol.RootPath, err)
				return 1
			}
			totalNodes += uint64(meta.NodeCount)
			volumes++
		}
	}

	dur := time.Since(start)
	stats := IndexStats{
		DataDir:      dataDir,
		IndexedNodes: totalNodes,
		Volumes:      volumes,
		DurationMs:   float64(dur.Microseconds()) / 1000.0,
	}

	if asJSON {
		enc := json.NewEncoder(stdout)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(stats); err != nil {
			fmt.Fprintf(stderr, "xift index: encode json: %v\n", err)
			return 1
		}
		return 0
	}

	fmt.Fprintf(stdout, "Indexed %d nodes across %d target(s) into %s in %.2fms\n",
		stats.IndexedNodes, stats.Volumes, stats.DataDir, stats.DurationMs)
	return 0
}

// indexDirectoryTree performs a strictly read-only top-down scan of rootDir applying filter.Pruner
// (including folded dependency shells and project ignore boundaries) and commits a Dense Segment.
func indexDirectoryTree(ctx context.Context, store *storage.Engine, pruner *filter.Pruner, volID uint16, rootDir string) (uint32, error) {
	info, err := os.Stat(rootDir)
	if err != nil {
		return 0, err
	}
	if !info.IsDir() {
		return 0, fmt.Errorf("%q is not a directory", rootDir)
	}

	vol := provider.VolumeInfo{
		VolID:    volID,
		RootPath: rootDir,
	}
	sw, err := provider.NewStorageSegmentWriter(store, vol)
	if err != nil {
		return 0, err
	}
	committed := false
	defer func() {
		if !committed {
			sw.Abort()
		}
	}()

	var nextKey uint64 = 1
	rootKey := nextKey
	nextKey++

	// Emit volume/directory root node.
	if err := sw.WriteRecord(provider.ScanRecord{
		VolID:     volID,
		NodeKey:   rootKey,
		ParentKey: 0,
		Name:      rootDir,
		Flags:     storage.FlagDir,
		ModTime:   info.ModTime().UnixNano(),
	}); err != nil {
		return 0, err
	}

	ignoreStack := filter.NewIgnoreStack(pruner.Rules().CaseInsensitive(), false)
	var walk func(diskDir, relDir string, parentKey uint64, depth int, parentSystem bool) error
	walk = func(diskDir, relDir string, parentKey uint64, depth int, parentSystem bool) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		entries, err := os.ReadDir(diskDir)
		if err != nil {
			return nil // Skip unreadable directories gracefully without aborting indexing.
		}

		names := make([]string, 0, len(entries))
		for _, ent := range entries {
			names = append(names, ent.Name())
		}
		scope := pruner.EnterDirIgnoreScope(ignoreStack, diskDir, relDir, names)
		defer ignoreStack.RestoreScope(scope)

		for _, ent := range entries {
			if err := ctx.Err(); err != nil {
				return err
			}
			name := ent.Name()
			entType := ent.Type()
			isSymlink := (entType & fs.ModeSymlink) != 0
			isDir := ent.IsDir() && !isSymlink

			rel := name
			if relDir != "" {
				rel = relDir + "/" + name
			}
			key := nextKey
			nextKey++

			if isDir {
				dec := pruner.EvalTopDownDir(volID, key, name, rel, depth, parentSystem, ignoreStack)
				if !dec.Keep {
					continue
				}
				var mt int64
				if fi, err := ent.Info(); err == nil {
					mt = fi.ModTime().UnixNano()
				}
				if err := sw.WriteRecord(provider.ScanRecord{
					VolID:     volID,
					NodeKey:   key,
					ParentKey: parentKey,
					Name:      name,
					Flags:     dec.Flags | storage.FlagDir,
					ModTime:   mt,
				}); err != nil {
					return err
				}
				if dec.Descend {
					childSystem := parentSystem || (dec.Flags&storage.FlagSystem) != 0
					if err := walk(filepath.Join(diskDir, name), rel, key, depth+1, childSystem); err != nil {
						return err
					}
				}
				continue
			}

			dec := pruner.EvalTopDownFile(name, rel, parentSystem, ignoreStack)
			if !dec.Keep {
				continue
			}
			var sz, mt int64
			if fi, err := ent.Info(); err == nil {
				sz = fi.Size()
				mt = fi.ModTime().UnixNano()
			}
			flags := dec.Flags
			if isSymlink {
				flags |= storage.FlagSymlink
			}
			if strings.HasPrefix(name, ".") {
				flags |= storage.FlagHidden
			}
			if err := sw.WriteRecord(provider.ScanRecord{
				VolID:     volID,
				NodeKey:   key,
				ParentKey: parentKey,
				Name:      name,
				Flags:     flags,
				Size:      sz,
				ModTime:   mt,
			}); err != nil {
				return err
			}
		}
		return nil
	}

	if err := walk(rootDir, "", rootKey, 1, false); err != nil {
		return 0, err
	}

	meta, err := sw.Commit(0)
	if err != nil {
		return 0, err
	}
	committed = true
	return meta.NodeCount, nil
}
