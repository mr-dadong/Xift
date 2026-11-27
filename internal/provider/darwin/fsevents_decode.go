package darwin

import (
	"fmt"
	"hash/fnv"
	"path"
	"strings"

	"github.com/mr-dadong/Xift/internal/provider"
	"github.com/mr-dadong/Xift/internal/storage"
)

// macOS CoreServices FSEventStreamEventFlags bitmasks (pure Go constants).
const (
	FSEventFlagNone            uint32 = 0x00000000
	FSEventFlagMustScanSubDirs uint32 = 0x00000001
	FSEventFlagUserDropped     uint32 = 0x00000002
	FSEventFlagKernelDropped   uint32 = 0x00000004
	FSEventFlagEventIdsWrapped uint32 = 0x00000008
	FSEventFlagHistoryDone     uint32 = 0x00000010
	FSEventFlagRootChanged     uint32 = 0x00000020
	FSEventFlagMount           uint32 = 0x00000040
	FSEventFlagUnmount         uint32 = 0x00000080

	FSEventFlagItemCreated      uint32 = 0x00000100
	FSEventFlagItemRemoved      uint32 = 0x00000200
	FSEventFlagItemInodeMetaMod uint32 = 0x00000400
	FSEventFlagItemRenamed      uint32 = 0x00000800
	FSEventFlagItemModified     uint32 = 0x00001000
	FSEventFlagItemFinderInfo   uint32 = 0x00002000
	FSEventFlagItemChangeOwner  uint32 = 0x00004000
	FSEventFlagItemXattrMod     uint32 = 0x00008000
	FSEventFlagItemIsFile       uint32 = 0x00010000
	FSEventFlagItemIsDir        uint32 = 0x00020000
	FSEventFlagItemIsSymlink    uint32 = 0x00040000
)

// RawFSEvent represents a single raw callback item delivered by CoreServices FSEventStreamCallback.
type RawFSEvent struct {
	EventID uint64
	Path    string
	Flags   uint32
}

// StatLookupFunc resolves live metadata for an FSEvents path (read-only lstat).
// Even when ok=false (the path was deleted or moved away), implementations SHOULD still
// populate parentKey by lstat-ing path.Dir(fullPath) so deletes/renames carry real directory inodes.
type StatLookupFunc func(fullPath string) (nodeKey, parentKey uint64, size, modTime int64, isDir, ok bool)

// NormalizeFSEvents translates a raw batch of macOS FSEvents into canonical provider.FileEvents.
func NormalizeFSEvents(
	volID uint16,
	volRoot string,
	raw []RawFSEvent,
	deduper *FirmlinkDeduper,
	statFn StatLookupFunc,
) ([]provider.FileEvent, error) {
	return NormalizeFSEventsWithCache(volID, volRoot, raw, deduper, nil, statFn)
}

// NormalizeFSEventsWithCache translates a raw batch of macOS FSEvents using cache to preserve real
// APFS inodes across deletes and renames (preventing FNV hash mismatches against FullScan inodes).
func NormalizeFSEventsWithCache(
	volID uint16,
	volRoot string,
	raw []RawFSEvent,
	deduper *FirmlinkDeduper,
	cache *PathKeyCache,
	statFn StatLookupFunc,
) ([]provider.FileEvent, error) {
	if len(raw) == 0 {
		return nil, nil
	}

	// Pre-scan for EventIdsWrapped so look-ahead rename pairing can never skip a wrap signal.
	for i := range raw {
		if raw[i].Flags&FSEventFlagEventIdsWrapped != 0 {
			return nil, fmt.Errorf("fsevents id wrapped at %d: %w", raw[i].EventID, provider.ErrCursorExpired)
		}
	}

	normRoot := normalizeUnixPath(volRoot)
	out := make([]provider.FileEvent, 0, len(raw))

	resolveParentKey := func(fullPath string, statParent uint64) uint64 {
		if statParent != 0 {
			return statParent
		}
		parentPath := path.Dir(fullPath)
		if ent, ok := cache.Get(parentPath); ok && ent.NodeKey != 0 {
			return ent.NodeKey
		}
		if statFn != nil {
			if k, _, _, _, _, ok := statFn(parentPath); ok && k != 0 {
				cache.Put(parentPath, CachedNode{NodeKey: k, IsDir: true})
				return k
			}
		}
		return hashPathKey(parentPath)
	}

	for i := 0; i < len(raw); i++ {
		r := raw[i]

		if r.Flags&FSEventFlagHistoryDone != 0 {
			continue
		}

		fullPath := r.Path
		if deduper != nil {
			fullPath = deduper.Canonicalize(fullPath)
		} else {
			fullPath = normalizeUnixPath(fullPath)
		}
		if fullPath == "" || IsTimeMachineSnapshotPath(fullPath) {
			continue
		}

		// Queue overflow or coalesce hint -> emit directory-level EventRescanDir.
		if r.Flags&(FSEventFlagMustScanSubDirs|FSEventFlagKernelDropped|FSEventFlagUserDropped|FSEventFlagRootChanged) != 0 {
			var nodeKey uint64
			if statFn != nil {
				if k, _, _, _, _, ok := statFn(fullPath); ok {
					nodeKey = k
				}
			}
			if nodeKey == 0 {
				if ent, ok := cache.Get(fullPath); ok {
					nodeKey = ent.NodeKey
				}
			}
			if nodeKey == 0 {
				nodeKey = hashPathKey(fullPath)
			}
			out = append(out, provider.FileEvent{
				VolID:   volID,
				Type:    provider.EventRescanDir,
				NodeKey: nodeKey,
				OldPath: fullPath,
				NewName: path.Base(fullPath),
				RelPath: trimVolumeRel(normRoot, fullPath),
				IsDir:   true,
				Cursor:  r.EventID,
			})
			continue
		}

		// Handle ItemRenamed (FSEvents delivers source and target as consecutive ItemRenamed events).
		if r.Flags&FSEventFlagItemRenamed != 0 {
			isDirFlag := (r.Flags & FSEventFlagItemIsDir) != 0
			existsCur := false
			var k1, pk1 uint64
			var sz1, mt1 int64
			if statFn != nil {
				var d1 bool
				k1, pk1, sz1, mt1, d1, existsCur = statFn(fullPath)
				if existsCur {
					isDirFlag = d1
				}
			}

			// Look ahead for paired rename target when the first event's path no longer exists.
			if !existsCur && i+1 < len(raw) && raw[i+1].Flags&FSEventFlagItemRenamed != 0 {
				nextPath := raw[i+1].Path
				if deduper != nil {
					nextPath = deduper.Canonicalize(nextPath)
				} else {
					nextPath = normalizeUnixPath(nextPath)
				}
				if nextPath != "" && !IsTimeMachineSnapshotPath(nextPath) {
					var k2, pk2 uint64
					var sz2, mt2 int64
					isDir2 := (raw[i+1].Flags & FSEventFlagItemIsDir) != 0
					existsNext := false
					if statFn != nil {
						var d2 bool
						k2, pk2, sz2, mt2, d2, existsNext = statFn(nextPath)
						if existsNext {
							isDir2 = d2
						}
					}
					if existsNext {
						oldEnt, hadOld := cache.Get(fullPath)
						if k2 == 0 && hadOld {
							k2 = oldEnt.NodeKey
						}
						if k2 == 0 {
							k2 = hashPathKey(nextPath)
						}
						srcParent := pk1
						if srcParent == 0 && hadOld {
							srcParent = oldEnt.ParentKey
						}
						srcParent = resolveParentKey(fullPath, srcParent)
						dstParent := resolveParentKey(nextPath, pk2)

						if isDir2 {
							cache.RenamePrefix(fullPath, nextPath)
						}
						cache.Delete(fullPath)
						cache.Put(nextPath, CachedNode{NodeKey: k2, ParentKey: dstParent, IsDir: isDir2})

						rel := trimVolumeRel(normRoot, nextPath)
						out = append(out, provider.FileEvent{
							VolID:        volID,
							Type:         provider.EventRename,
							NodeKey:      k2,
							ParentKey:    srcParent,
							NewParentKey: dstParent,
							OldPath:      fullPath,
							NewName:      path.Base(nextPath),
							RelPath:      rel,
							Depth:        pathDepth(rel),
							Flags:        fsEventBaseFlags(raw[i+1].Flags, isDir2),
							Size:         sz2,
							ModTime:      mt2,
							Cursor:       raw[i+1].EventID,
							IsDir:        isDir2,
						})
						i++ // Consume paired target event
						continue
					}
				}
			}

			// Single unpaired rename: exists -> Create, absent -> Delete.
			rel := trimVolumeRel(normRoot, fullPath)
			if existsCur {
				if k1 == 0 {
					k1 = hashPathKey(fullPath)
				}
				pk1 = resolveParentKey(fullPath, pk1)
				cache.Put(fullPath, CachedNode{NodeKey: k1, ParentKey: pk1, IsDir: isDirFlag})
				out = append(out, provider.FileEvent{
					VolID:     volID,
					Type:      provider.EventCreate,
					NodeKey:   k1,
					ParentKey: pk1,
					NewName:   path.Base(fullPath),
					RelPath:   rel,
					Depth:     pathDepth(rel),
					Flags:     fsEventBaseFlags(r.Flags, isDirFlag),
					Size:      sz1,
					ModTime:   mt1,
					Cursor:    r.EventID,
					IsDir:     isDirFlag,
				})
			} else {
				oldEnt, hadOld := cache.Delete(fullPath)
				delKey := oldEnt.NodeKey
				if !hadOld || delKey == 0 {
					delKey = hashPathKey(fullPath)
				}
				delParent := pk1
				if delParent == 0 {
					delParent = oldEnt.ParentKey
				}
				delParent = resolveParentKey(fullPath, delParent)
				if hadOld && !isDirFlag {
					isDirFlag = oldEnt.IsDir
				}
				out = append(out, provider.FileEvent{
					VolID:     volID,
					Type:      provider.EventDelete,
					NodeKey:   delKey,
					ParentKey: delParent,
					NewName:   path.Base(fullPath),
					RelPath:   rel,
					Depth:     pathDepth(rel),
					Flags:     fsEventBaseFlags(r.Flags, isDirFlag),
					Cursor:    r.EventID,
					IsDir:     isDirFlag,
				})
			}
			continue
		}

		// Standard Create / Delete / Modify classification.
		isDir := (r.Flags & FSEventFlagItemIsDir) != 0
		rel := trimVolumeRel(normRoot, fullPath)
		baseName := path.Base(fullPath)

		var nodeKey, parentKey uint64
		var size, modTime int64
		exists := false
		if statFn != nil {
			var d bool
			nodeKey, parentKey, size, modTime, d, exists = statFn(fullPath)
			if exists {
				isDir = d
			}
		}

		var evType provider.EventType
		switch {
		case r.Flags&FSEventFlagItemRemoved != 0 && !exists:
			evType = provider.EventDelete
		case r.Flags&FSEventFlagItemCreated != 0:
			if exists {
				evType = provider.EventCreate
			} else {
				evType = provider.EventDelete
			}
		case r.Flags&(FSEventFlagItemModified|FSEventFlagItemInodeMetaMod|FSEventFlagItemChangeOwner|FSEventFlagItemXattrMod) != 0:
			if !exists && statFn != nil {
				evType = provider.EventDelete
			} else {
				evType = provider.EventModifyMeta
			}
		default:
			continue
		}

		if evType == provider.EventDelete {
			oldEnt, hadOld := cache.Delete(fullPath)
			if nodeKey == 0 && hadOld {
				nodeKey = oldEnt.NodeKey
			}
			if parentKey == 0 && hadOld {
				parentKey = oldEnt.ParentKey
			}
			if hadOld && !isDir {
				isDir = oldEnt.IsDir
			}
		}

		if nodeKey == 0 {
			nodeKey = hashPathKey(fullPath)
		}
		parentKey = resolveParentKey(fullPath, parentKey)

		if evType != provider.EventDelete {
			cache.Put(fullPath, CachedNode{NodeKey: nodeKey, ParentKey: parentKey, IsDir: isDir})
		}

		out = append(out, provider.FileEvent{
			VolID:     volID,
			Type:      evType,
			NodeKey:   nodeKey,
			ParentKey: parentKey,
			NewName:   baseName,
			RelPath:   rel,
			Depth:     pathDepth(rel),
			Flags:     fsEventBaseFlags(r.Flags, isDir),
			Size:      size,
			ModTime:   modTime,
			Cursor:    r.EventID,
			IsDir:     isDir,
		})
	}

	return out, nil
}

func fsEventBaseFlags(fsFlags uint32, isDir bool) uint16 {
	var f uint16
	if isDir {
		f |= storage.FlagDir
	}
	if fsFlags&FSEventFlagItemIsSymlink != 0 {
		f |= storage.FlagSymlink
	}
	return f
}

func trimVolumeRel(volRoot, fullPath string) string {
	if volRoot == "" || volRoot == "/" {
		return strings.TrimPrefix(fullPath, "/")
	}
	if fullPath == volRoot {
		return ""
	}
	if strings.HasPrefix(fullPath, volRoot+"/") {
		return fullPath[len(volRoot)+1:]
	}
	return strings.TrimPrefix(fullPath, "/")
}

func pathDepth(rel string) int {
	if rel == "" {
		return 0
	}
	return strings.Count(rel, "/") + 1
}

func hashPathKey(p string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(p))
	v := h.Sum64()
	if v == 0 {
		v = 1
	}
	return v
}
