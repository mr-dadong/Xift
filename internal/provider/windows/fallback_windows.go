//go:build windows

package windows

import (
	"context"
	"errors"
	"hash/fnv"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf16"
	"unsafe"

	"golang.org/x/sys/windows"

	"github.com/mr-dadong/Xift/internal/filter"
	"github.com/mr-dadong/Xift/internal/provider"
	"github.com/mr-dadong/Xift/internal/storage"
)

const (
	readDirChangesBufSize = 64 * 1024
	maxPathCacheEntries   = 200_000
	watchNotifyFilter     = windows.FILE_NOTIFY_CHANGE_FILE_NAME |
		windows.FILE_NOTIFY_CHANGE_DIR_NAME |
		windows.FILE_NOTIFY_CHANGE_SIZE |
		windows.FILE_NOTIFY_CHANGE_LAST_WRITE
)

type cachedPathEntry struct {
	nodeKey   uint64
	parentKey uint64
	isDir     bool
}

type pathKeyCache struct {
	mu      sync.RWMutex
	entries map[string]cachedPathEntry
}

func newPathKeyCache() *pathKeyCache {
	return &pathKeyCache{
		entries: make(map[string]cachedPathEntry, 1024),
	}
}

func (c *pathKeyCache) put(relPath string, ent cachedPathEntry) {
	key := strings.ToLower(filepath.ToSlash(relPath))
	c.mu.Lock()
	if len(c.entries) >= maxPathCacheEntries {
		if _, exists := c.entries[key]; !exists {
			// Bounded cap: avoid unbounded growth over daemon lifetime.
			c.mu.Unlock()
			return
		}
	}
	c.entries[key] = ent
	c.mu.Unlock()
}

func (c *pathKeyCache) get(relPath string) (cachedPathEntry, bool) {
	key := strings.ToLower(filepath.ToSlash(relPath))
	c.mu.RLock()
	ent, ok := c.entries[key]
	c.mu.RUnlock()
	return ent, ok
}

func (c *pathKeyCache) delete(relPath string) (cachedPathEntry, bool) {
	key := strings.ToLower(filepath.ToSlash(relPath))
	c.mu.Lock()
	ent, ok := c.entries[key]
	if ok {
		delete(c.entries, key)
		if ent.isDir && key != "" {
			prefix := key + "/"
			for k := range c.entries {
				if strings.HasPrefix(k, prefix) {
					delete(c.entries, k)
				}
			}
		}
	}
	c.mu.Unlock()
	return ent, ok
}

// renamePrefix re-keys all cached descendants when a directory is renamed from oldRel to newRel.
func (c *pathKeyCache) renamePrefix(oldRel, newRel string) {
	oldKey := strings.ToLower(filepath.ToSlash(oldRel))
	newKey := strings.ToLower(filepath.ToSlash(newRel))
	if oldKey == "" || oldKey == newKey {
		return
	}
	oldPrefix := oldKey + "/"
	newPrefix := newKey + "/"

	c.mu.Lock()
	var updates []struct {
		newK string
		ent  cachedPathEntry
	}
	for k, ent := range c.entries {
		if strings.HasPrefix(k, oldPrefix) {
			updates = append(updates, struct {
				newK string
				ent  cachedPathEntry
			}{
				newK: newPrefix + k[len(oldPrefix):],
				ent:  ent,
			})
			delete(c.entries, k)
		}
	}
	for _, u := range updates {
		c.entries[u.newK] = u.ent
	}
	c.mu.Unlock()
}

// getFileKeyAndMetaReadOnly opens a file or directory with 0 access mask (metadata query only,
// strictly read-only — never requests write access) to obtain its NTFS/exFAT 64-bit FileIndex.
func getFileKeyAndMetaReadOnly(fullPath string) (nodeKey uint64, size int64, modTime int64, attrs uint32, ok bool) {
	p16, err := windows.UTF16PtrFromString(fullPath)
	if err != nil {
		return 0, 0, 0, 0, false
	}
	h, err := windows.CreateFile(
		p16,
		0, // Metadata query only (read-only)
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		return 0, 0, 0, 0, false
	}
	defer windows.CloseHandle(h)

	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(h, &info); err != nil {
		return 0, 0, 0, 0, false
	}
	nodeKey = (uint64(info.FileIndexHigh) << 32) | uint64(info.FileIndexLow)
	size = (int64(info.FileSizeHigh) << 32) | int64(info.FileSizeLow)
	modTime = info.LastWriteTime.Nanoseconds()
	return nodeKey, size, modTime, info.FileAttributes, true
}

func fallbackPathHash(relPath string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(strings.ToLower(filepath.ToSlash(relPath))))
	v := h.Sum64()
	if v == 0 {
		v = 1
	}
	return v
}

func isReparsePointPath(fullPath string) bool {
	p16, err := windows.UTF16PtrFromString(fullPath)
	if err != nil {
		return false
	}
	attrs, err := windows.GetFileAttributes(p16)
	if err != nil {
		return false
	}
	return attrs&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0
}

func winAttrsToFlags(attrs uint32, isDir bool) uint16 {
	var fl uint16
	if isDir {
		fl |= storage.FlagDir
	}
	if attrs&windows.FILE_ATTRIBUTE_HIDDEN != 0 {
		fl |= storage.FlagHidden
	}
	if attrs&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		fl |= storage.FlagSymlink
	}
	return fl
}

// scanDirectoryTopDown performs strictly read-only top-down early-stop scanning with Pruner,
// skipping directory junctions/reparse points to prevent infinite cycles.
func (p *Provider) scanDirectoryTopDown(
	ctx context.Context,
	vol provider.VolumeInfo,
	rootPath string,
	rootKey uint64,
	emitRoot bool,
	writer provider.SegmentWriter,
	cache *pathKeyCache,
) error {
	cleanRoot := filepath.Clean(rootPath)
	if rootKey == 0 {
		if k, _, _, _, ok := getFileKeyAndMetaReadOnly(cleanRoot); ok && k != 0 {
			rootKey = k
		} else {
			rootKey = fallbackPathHash(cleanRoot)
		}
	}

	rootName := filepath.Base(cleanRoot)
	if rootName == "" || rootName == "." || rootName == `\` {
		rootName = cleanRoot
	}
	if emitRoot {
		if err := writer.WriteRecord(provider.ScanRecord{
			VolID:     vol.VolID,
			NodeKey:   rootKey,
			ParentKey: rootKey,
			Name:      rootName,
			Flags:     storage.FlagDir,
		}); err != nil {
			return err
		}
	}
	if cache != nil {
		cache.put("", cachedPathEntry{nodeKey: rootKey, parentKey: rootKey, isDir: true})
	}

	rules := p.pruner.Rules()
	ignoreStack := filter.NewIgnoreStack(rules.CaseInsensitive(), rules.GitignoreIgnoreFiles())

	var walk func(diskDir, relDir string, parentKey uint64, depth int, parentSystem bool) error
	walk = func(diskDir, relDir string, parentKey uint64, depth int, parentSystem bool) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		entries, err := os.ReadDir(diskDir)
		if err != nil {
			if depth == 1 && diskDir == cleanRoot {
				if errors.Is(err, fs.ErrPermission) {
					return provider.ErrPermissionDenied
				}
				return err
			}
			// Gracefully skip unreadable subdirectories without aborting scan.
			return nil
		}

		names := make([]string, len(entries))
		for i, ent := range entries {
			names[i] = ent.Name()
		}
		scope := p.pruner.EnterDirIgnoreScope(ignoreStack, diskDir, relDir, names)
		defer ignoreStack.RestoreScope(scope)

		for _, ent := range entries {
			if err := ctx.Err(); err != nil {
				return err
			}
			name := ent.Name()
			fullPath := filepath.Join(diskDir, name)
			relPath := name
			if relDir != "" {
				relPath = relDir + "/" + name
			}

			key, size, modTime, attrs, okMeta := getFileKeyAndMetaReadOnly(fullPath)
			isReparse := (attrs&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0) ||
				ent.Type()&(fs.ModeSymlink|fs.ModeIrregular) != 0 ||
				(!okMeta && isReparsePointPath(fullPath))
			isDir := ent.IsDir()
			if okMeta {
				isDir = (attrs & windows.FILE_ATTRIBUTE_DIRECTORY) != 0
			}
			if !okMeta || key == 0 {
				key = fallbackPathHash(relPath)
				if info, iErr := ent.Info(); iErr == nil {
					size = info.Size()
					modTime = info.ModTime().UnixNano()
				}
			}

			var extraFlags uint16
			if attrs&windows.FILE_ATTRIBUTE_HIDDEN != 0 {
				extraFlags |= storage.FlagHidden
			}
			if isReparse {
				extraFlags |= storage.FlagSymlink
			}

			if isDir {
				dec := p.pruner.EvalTopDownDir(vol.VolID, key, name, relPath, depth, parentSystem, ignoreStack)
				if !dec.Keep {
					continue
				}
				if err := writer.WriteRecord(provider.ScanRecord{
					VolID:     vol.VolID,
					NodeKey:   key,
					ParentKey: parentKey,
					Name:      name,
					Flags:     dec.Flags | extraFlags,
					ModTime:   modTime,
				}); err != nil {
					return err
				}
				if cache != nil {
					cache.put(relPath, cachedPathEntry{nodeKey: key, parentKey: parentKey, isDir: true})
				}
				// Never descend into junctions / symlinks (reparse points) to prevent cycles.
				if dec.Descend && !isReparse {
					childSys := (dec.Flags & storage.FlagSystem) != 0
					if err := walk(fullPath, relPath, key, depth+1, childSys); err != nil {
						return err
					}
				}
				continue
			}

			dec := p.pruner.EvalTopDownFile(name, relPath, parentSystem, ignoreStack)
			if !dec.Keep {
				continue
			}
			if err := writer.WriteRecord(provider.ScanRecord{
				VolID:     vol.VolID,
				NodeKey:   key,
				ParentKey: parentKey,
				Name:      name,
				Flags:     dec.Flags | extraFlags,
				Size:      size,
				ModTime:   modTime,
			}); err != nil {
				return err
			}
			if cache != nil {
				cache.put(relPath, cachedPathEntry{nodeKey: key, parentKey: parentKey, isDir: false})
			}
		}
		return nil
	}

	return walk(cleanRoot, "", rootKey, 1, false)
}

// watchDirectoryChanges monitors a non-NTFS volume or unprivileged directory strictly read-only
// via ReadDirectoryChangesW + Overlapped + CancelIoEx (joined safely before close).
func (p *Provider) watchDirectoryChanges(
	ctx context.Context,
	vol provider.VolumeInfo,
	sinceCursor uint64,
	out chan<- provider.FileEvent,
) error {
	// Pin to one OS thread for the lifetime of non-IOCP overlapped ReadDirectoryChangesW IRPs:
	// on Windows, if the issuing OS thread exits while an IRP is pending, the kernel aborts it with ERROR_OPERATION_ABORTED.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	cleanRoot := filepath.Clean(vol.RootPath)
	p16, err := windows.UTF16PtrFromString(cleanRoot)
	if err != nil {
		return err
	}

	handle, err := windows.CreateFile(
		p16,
		windows.FILE_LIST_DIRECTORY, // Strictly read-only directory listing right
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OVERLAPPED,
		0,
	)
	if err != nil {
		if errors.Is(err, windows.ERROR_ACCESS_DENIED) {
			return provider.ErrPermissionDenied
		}
		return err
	}
	defer windows.CloseHandle(handle)

	waiter, err := newOverlappedWaiter(ctx, readDirChangesBufSize)
	if err != nil {
		return err
	}
	defer waiter.close()

	rootKey := vol.RootKey
	if rootKey == 0 {
		if k, _, _, _, ok := getFileKeyAndMetaReadOnly(cleanRoot); ok && k != 0 {
			rootKey = k
		} else {
			rootKey = fallbackPathHash(cleanRoot)
		}
	}

	cache := p.getOrCreatePathCache(vol.VolID)
	cache.put("", cachedPathEntry{nodeKey: rootKey, parentKey: rootKey, isDir: true})

	buf := waiter.buf
	var cursorSeq atomic.Uint64
	cursorSeq.Store(sinceCursor)

	var pendingOldRel string
	var pendingOldEnt cachedPathEntry
	var hasPendingOld bool

	flushOrphanOld := func() bool {
		if !hasPendingOld {
			return true
		}
		hasPendingOld = false
		c := cursorSeq.Add(1)
		select {
		case out <- provider.FileEvent{
			VolID:     vol.VolID,
			Type:      provider.EventDelete,
			NodeKey:   pendingOldEnt.nodeKey,
			ParentKey: pendingOldEnt.parentKey,
			OldPath:   pendingOldRel,
			NewName:   filepath.Base(filepath.FromSlash(pendingOldRel)),
			RelPath:   pendingOldRel,
			Cursor:    c,
			IsDir:     pendingOldEnt.isDir,
		}:
			return true
		case <-ctx.Done():
			return false
		}
	}

	resolveParentKey := func(relSlash string) uint64 {
		idx := strings.LastIndexByte(relSlash, '/')
		if idx < 0 {
			return rootKey
		}
		parentRel := relSlash[:idx]
		if ent, ok := cache.get(parentRel); ok && ent.nodeKey != 0 {
			return ent.nodeKey
		}
		parentDisk := filepath.Join(cleanRoot, filepath.FromSlash(parentRel))
		if k, _, _, _, ok := getFileKeyAndMetaReadOnly(parentDisk); ok && k != 0 {
			cache.put(parentRel, cachedPathEntry{nodeKey: k, isDir: true})
			return k
		}
		return fallbackPathHash(parentRel)
	}

	armedSignaled := false
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		if err := waiter.reset(); err != nil {
			return err
		}

		var bytesReturned uint32
		err := windows.ReadDirectoryChanges(
			handle,
			&buf[0],
			uint32(len(buf)),
			true, // watchSubtree
			watchNotifyFilter,
			&bytesReturned,
			&waiter.ov,
			0,
		)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, windows.ERROR_OPERATION_ABORTED) {
				return nil
			}
			return err
		}
		if !armedSignaled {
			armedSignaled = true
			if p.onWatchArmed != nil {
				p.onWatchArmed()
			}
		}

		bytesReturned, err = waiter.wait(handle)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if bytesReturned == 0 {
			// Kernel buffer overflow -> clear stale rename state and emit root EventRescanDir fallback.
			hasPendingOld = false
			c := cursorSeq.Add(1)
			select {
			case out <- provider.FileEvent{
				VolID:   vol.VolID,
				Type:    provider.EventRescanDir,
				NodeKey: rootKey,
				OldPath: cleanRoot,
				IsDir:   true,
				Cursor:  c,
			}:
			case <-ctx.Done():
				return nil
			}
			continue
		}

		offset := uint32(0)
		for {
			if int(offset)+int(unsafe.Sizeof(windows.FileNotifyInformation{})) > int(bytesReturned) {
				break
			}
			fni := (*windows.FileNotifyInformation)(unsafe.Pointer(&buf[offset]))
			nameByteLen := fni.FileNameLength
			nameStart := offset + uint32(unsafe.Offsetof(fni.FileName))
			if int(nameStart+nameByteLen) > int(bytesReturned) {
				break
			}

			// If a previous RENAMED_OLD_NAME wasn't immediately followed by RENAMED_NEW_NAME, flush it as Delete.
			if hasPendingOld && fni.Action != windows.FILE_ACTION_RENAMED_NEW_NAME {
				if !flushOrphanOld() {
					return nil
				}
			}

			u16Slice := unsafe.Slice((*uint16)(unsafe.Pointer(&buf[nameStart])), nameByteLen/2)
			relOS := string(utf16.Decode(u16Slice))
			relSlash := filepath.ToSlash(relOS)
			fullPath := filepath.Join(cleanRoot, relOS)
			baseName := filepath.Base(relOS)
			depth := strings.Count(relSlash, "/") + 1
			parentKey := resolveParentKey(relSlash)

			c := cursorSeq.Add(1)
			var emitEv *provider.FileEvent

			switch fni.Action {
			case windows.FILE_ACTION_ADDED:
				key, size, modTime, attrs, okMeta := getFileKeyAndMetaReadOnly(fullPath)
				isDir := okMeta && (attrs&windows.FILE_ATTRIBUTE_DIRECTORY != 0)
				if !okMeta || key == 0 {
					key = fallbackPathHash(relSlash)
				}
				cache.put(relSlash, cachedPathEntry{nodeKey: key, parentKey: parentKey, isDir: isDir})
				emitEv = &provider.FileEvent{
					VolID:     vol.VolID,
					Type:      provider.EventCreate,
					NodeKey:   key,
					ParentKey: parentKey,
					NewName:   baseName,
					RelPath:   relSlash,
					Depth:     depth,
					Flags:     winAttrsToFlags(attrs, isDir),
					Size:      size,
					ModTime:   modTime,
					Cursor:    c,
					IsDir:     isDir,
				}

			case windows.FILE_ACTION_REMOVED:
				ent, ok := cache.delete(relSlash)
				key := ent.nodeKey
				if !ok || key == 0 {
					key = fallbackPathHash(relSlash)
				}
				pKey := ent.parentKey
				if pKey == 0 {
					pKey = parentKey
				}
				emitEv = &provider.FileEvent{
					VolID:     vol.VolID,
					Type:      provider.EventDelete,
					NodeKey:   key,
					ParentKey: pKey,
					NewName:   baseName,
					RelPath:   relSlash,
					Depth:     depth,
					Cursor:    c,
					IsDir:     ent.isDir,
				}

			case windows.FILE_ACTION_MODIFIED:
				key, size, modTime, attrs, okMeta := getFileKeyAndMetaReadOnly(fullPath)
				if !okMeta {
					break
				}
				isDir := (attrs & windows.FILE_ATTRIBUTE_DIRECTORY) != 0
				if key == 0 {
					if ent, ok := cache.get(relSlash); ok && ent.nodeKey != 0 {
						key = ent.nodeKey
					} else {
						key = fallbackPathHash(relSlash)
					}
				}
				cache.put(relSlash, cachedPathEntry{nodeKey: key, parentKey: parentKey, isDir: isDir})
				emitEv = &provider.FileEvent{
					VolID:     vol.VolID,
					Type:      provider.EventModifyMeta,
					NodeKey:   key,
					ParentKey: parentKey,
					NewName:   baseName,
					RelPath:   relSlash,
					Depth:     depth,
					Flags:     winAttrsToFlags(attrs, isDir),
					Size:      size,
					ModTime:   modTime,
					Cursor:    c,
					IsDir:     isDir,
				}

			case windows.FILE_ACTION_RENAMED_OLD_NAME:
				ent, ok := cache.get(relSlash)
				if !ok {
					ent = cachedPathEntry{nodeKey: fallbackPathHash(relSlash), parentKey: parentKey}
				}
				pendingOldRel = relSlash
				pendingOldEnt = ent
				hasPendingOld = true

			case windows.FILE_ACTION_RENAMED_NEW_NAME:
				key, size, modTime, attrs, okMeta := getFileKeyAndMetaReadOnly(fullPath)
				isDir := okMeta && (attrs&windows.FILE_ATTRIBUTE_DIRECTORY != 0)
				if (!okMeta || key == 0) && hasPendingOld && pendingOldEnt.nodeKey != 0 {
					key = pendingOldEnt.nodeKey
					isDir = pendingOldEnt.isDir
				}
				if key == 0 {
					key = fallbackPathHash(relSlash)
				}
				srcParent := parentKey
				oldPath := ""
				if hasPendingOld {
					if pendingOldEnt.parentKey != 0 {
						srcParent = pendingOldEnt.parentKey
					}
					oldPath = pendingOldRel
					hasPendingOld = false
					// Re-key directory children on directory rename before removing old entry.
					if isDir {
						cache.renamePrefix(oldPath, relSlash)
					}
					cache.delete(oldPath)
				}
				cache.put(relSlash, cachedPathEntry{nodeKey: key, parentKey: parentKey, isDir: isDir})
				emitEv = &provider.FileEvent{
					VolID:        vol.VolID,
					Type:         provider.EventRename,
					NodeKey:      key,
					ParentKey:    srcParent,
					NewParentKey: parentKey,
					OldPath:      oldPath,
					NewName:      baseName,
					RelPath:      relSlash,
					Depth:        depth,
					Flags:        winAttrsToFlags(attrs, isDir),
					Size:         size,
					ModTime:      modTime,
					Cursor:       c,
					IsDir:        isDir,
				}
			}

			if emitEv != nil {
				select {
				case out <- *emitEv:
				case <-ctx.Done():
					return nil
				}
			}

			if fni.NextEntryOffset == 0 {
				break
			}
			offset += fni.NextEntryOffset
		}

		// Flush any trailing unpaired RENAMED_OLD_NAME at end of buffer.
		if hasPendingOld {
			if !flushOrphanOld() {
				return nil
			}
		}
	}
}
