package query

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/mr-dadong/Xift/internal/storage"
)

// executeFoldedSearch implements on-demand read-only local directory scanning for `in:<dir>`
// queries (V2.1 Section 4.1 & 7.1). Results are never written to storage.Engine, MemTable, or WAL.
func (e *Engine) executeFoldedSearch(
	ctx context.Context,
	snap *storage.Snapshot,
	q *Query,
	plan *compiledPlan,
	opts SearchOptions,
	start time.Time,
) (*SearchResult, error) {
	targetDirs := findInTargetDirs(snap, q.InDirs)
	if len(targetDirs) == 0 {
		cost := time.Since(start)
		return &SearchResult{
			Query:    q.Raw,
			Total:    0,
			Returned: 0,
			Offset:   opts.Offset,
			Limit:    opts.Limit,
			Cost:     cost,
			CostMs:   float64(cost.Microseconds()) / 1000.0,
			Warnings: q.Warnings,
			Items:    []ResultItem{},
		}, nil
	}

	var matched []ResultItem
	visitedEntries := 0

	type queueItem struct {
		diskPath string
		relPath  string
		depth    int
	}

	for _, dirPath := range targetDirs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		queue := []queueItem{{diskPath: dirPath, relPath: "", depth: 0}}
		for len(queue) > 0 && visitedEntries < opts.FoldedMaxEntries {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			cur := queue[0]
			queue = queue[1:]

			entries, err := os.ReadDir(cur.diskPath)
			if err != nil {
				continue
			}
			for _, ent := range entries {
				if visitedEntries >= opts.FoldedMaxEntries {
					break
				}
				if (visitedEntries & 1023) == 0 {
					if err := ctx.Err(); err != nil {
						return nil, err
					}
				}
				visitedEntries++

				name := ent.Name()
				entType := ent.Type()
				isSymlink := (entType & fs.ModeSymlink) != 0
				isDir := ent.IsDir() && !isSymlink

				fullDisk := filepath.Join(cur.diskPath, name)
				var rel string
				if cur.relPath == "" {
					rel = name
				} else {
					rel = cur.relPath + "/" + name
				}

				if isDir && cur.depth+1 < opts.FoldedMaxDepth {
					queue = append(queue, queueItem{
						diskPath: fullDisk,
						relPath:  rel,
						depth:    cur.depth + 1,
					})
				}

				fullSlash := filepath.ToSlash(fullDisk)
				var infoSize int64
				var infoMod int64
				infoLoaded := false
				loadInfo := func() (int64, int64) {
					if !infoLoaded {
						infoLoaded = true
						if fi, err := ent.Info(); err == nil {
							infoSize = fi.Size()
							infoMod = fi.ModTime().UnixNano()
						}
					}
					return infoSize, infoMod
				}

				ok, tier := evalDiskEntry(plan.root, name, rel, fullSlash, isDir, loadInfo)
				if !ok {
					continue
				}
				sz, mt := loadInfo()

				outPath := fullSlash
				if opts.PathSep == '\\' {
					outPath = strings.ReplaceAll(fullSlash, "/", "\\")
				}
				score := tier.Score() + depthWeight/float64(cur.depth+2)
				nonASCII := storage.HasNonASCII(name)
				matched = append(matched, ResultItem{
					Path:           outPath,
					Name:           name,
					IsDir:          isDir,
					FromFoldedScan: true,
					Size:           sz,
					ModTime:        mt,
					Score:          score,
					Highlights:     computeHighlights([]byte(name), nonASCII, plan.positiveNames),
				})
			}
		}
	}

	sortResultItems(matched, opts.Sort, opts.Desc)
	total := len(matched)
	var items []ResultItem
	if opts.Offset < total {
		end := opts.Offset + opts.Limit
		if end > total {
			end = total
		}
		items = matched[opts.Offset:end]
	} else {
		items = []ResultItem{}
	}

	cost := time.Since(start)
	return &SearchResult{
		Query:    q.Raw,
		Total:    total,
		Returned: len(items),
		Offset:   opts.Offset,
		Limit:    opts.Limit,
		Cost:     cost,
		CostMs:   float64(cost.Microseconds()) / 1000.0,
		Warnings: q.Warnings,
		Items:    items,
	}, nil
}

func findInTargetDirs(snap *storage.Snapshot, inDirs []string) []string {
	var foldedHits []string
	var regularHits []string
	seen := make(map[string]struct{})

	matchTarget := func(nameLower, fullPathLower string) bool {
		for _, target := range inDirs {
			if strings.ContainsRune(target, '/') {
				if fullPathLower == target || strings.HasSuffix(fullPathLower, "/"+target) {
					return true
				}
			} else if nameLower == target {
				return true
			}
		}
		return false
	}

	snap.ForEachLive(func(nodeID uint32, node storage.HotNode, name []byte) bool {
		if (node.Flags & storage.FlagDir) == 0 {
			return true
		}
		nameLower := strings.ToLower(string(name))
		// Quick pre-check before resolving path.
		possible := false
		for _, t := range inDirs {
			if strings.ContainsRune(t, '/') {
				if strings.HasSuffix(t, "/"+nameLower) || t == nameLower {
					possible = true
					break
				}
			} else if nameLower == t {
				possible = true
				break
			}
		}
		if !possible {
			return true
		}

		diskPath, err := snap.ResolvePathWithSep(nodeID, byte(os.PathSeparator))
		if err != nil {
			return true
		}
		diskPath = filepath.Clean(diskPath)
		slashLower := strings.ToLower(filepath.ToSlash(diskPath))
		if !matchTarget(nameLower, slashLower) {
			return true
		}
		if _, dup := seen[diskPath]; dup {
			return true
		}
		seen[diskPath] = struct{}{}
		if (node.Flags & storage.FlagFolded) != 0 {
			foldedHits = append(foldedHits, diskPath)
		} else {
			regularHits = append(regularHits, diskPath)
		}
		return true
	})

	if len(foldedHits) > 0 {
		return foldedHits
	}
	return regularHits
}

func evalDiskEntry(
	ce *compiledExpr,
	name string,
	relPath string,
	fullSlash string,
	isDir bool,
	loadInfo func() (int64, int64),
) (bool, MatchTier) {
	if ce == nil {
		return true, TierNone
	}
	switch ce.kind {
	case ExprAtom:
		ca := &ce.atom
		switch ca.atom.Kind {
		case AtomType:
			return isDir == ca.atom.WantDir, TierNone
		case AtomExt:
			if isDir {
				return false, TierNone
			}
			ext := storage.ExtractLowerExt(name)
			if ext == "" {
				return false, TierNone
			}
			for _, want := range ca.atom.Exts {
				if ext == want {
					return true, TierNone
				}
			}
			return false, TierNone
		case AtomWildcard:
			if ca.atom.PureExt != "" {
				if isDir {
					return false, TierNone
				}
				return storage.ExtractLowerExt(name) == ca.atom.PureExt, TierWildcard
			}
			nonASCII := storage.HasNonASCII(name)
			return MatchWildcard([]byte(name), nonASCII, ca.atom.Value), TierWildcard
		case AtomName:
			nonASCII := storage.HasNonASCII(name)
			if tier, _, _ := MatchName([]byte(name), nonASCII, ca.needleBytes, ca.atom.Value); tier != TierNone {
				return true, tier
			}
			// Inside `in:<dir>`, also match relative subpaths (e.g. `in:node_modules lodash` -> `lodash/index.js`).
			if strings.Contains(strings.ToLower(relPath), ca.atom.Value) {
				return true, TierSubstring
			}
			return false, TierNone
		case AtomPath:
			fl := strings.ToLower(fullSlash)
			if ca.atom.HasWildcard {
				return MatchWildcard([]byte(fl), storage.HasNonASCII(fl), ca.atom.Value), TierNone
			}
			return strings.Contains(fl, ca.atom.Value), TierNone
		case AtomSize:
			sz, _ := loadInfo()
			return ca.atom.Size.Matches(sz), TierNone
		case AtomModTime:
			_, mt := loadInfo()
			return ca.atom.ModTime.Matches(mt), TierNone
		default:
			return false, TierNone
		}
	case ExprNot:
		ok, _ := evalDiskEntry(ce.child, name, relPath, fullSlash, isDir, loadInfo)
		return !ok, TierNone
	case ExprAnd:
		best := TierNone
		for _, ch := range ce.children {
			ok, tier := evalDiskEntry(ch, name, relPath, fullSlash, isDir, loadInfo)
			if !ok {
				return false, TierNone
			}
			if tier > best {
				best = tier
			}
		}
		return true, best
	case ExprOr:
		matched := false
		best := TierNone
		for _, ch := range ce.children {
			ok, tier := evalDiskEntry(ch, name, relPath, fullSlash, isDir, loadInfo)
			if ok {
				matched = true
				if tier > best {
					best = tier
				}
			}
		}
		return matched, best
	default:
		return false, TierNone
	}
}

func sortResultItems(items []ResultItem, order SortOrder, desc bool) {
	slices.SortFunc(items, func(a, b ResultItem) int {
		var cmp int
		switch order {
		case SortByRelevance:
			if a.Score > b.Score {
				return -1
			}
			if a.Score < b.Score {
				return 1
			}
			return strings.Compare(a.Path, b.Path)
		case SortByName:
			cmp = strings.Compare(strings.ToLower(a.Name), strings.ToLower(b.Name))
		case SortBySize:
			if a.Size < b.Size {
				cmp = -1
			} else if a.Size > b.Size {
				cmp = 1
			}
		case SortByModTime:
			if a.ModTime < b.ModTime {
				cmp = -1
			} else if a.ModTime > b.ModTime {
				cmp = 1
			}
		}
		if desc {
			cmp = -cmp
		}
		if cmp != 0 {
			return cmp
		}
		return strings.Compare(a.Path, b.Path)
	})
}
