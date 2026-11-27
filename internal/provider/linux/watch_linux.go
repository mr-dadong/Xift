//go:build linux

package linux

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path"
	"strings"
	"sync/atomic"

	"golang.org/x/sys/unix"

	"github.com/mr-dadong/Xift/internal/filter"
	"github.com/mr-dadong/Xift/internal/provider"
	"github.com/mr-dadong/Xift/internal/storage"
)

const (
	// Linux fanotify init flags for kernel >= 5.9 filesystem-wide monitoring.
	fanClassNotif     = 0x00000000
	fanCloexec        = 0x00000001
	fanNonblock       = 0x00000002
	fanReportDFIDName = 0x00000c00 // FAN_REPORT_DIR_FID | FAN_REPORT_NAME
	fanMarkAdd        = 0x00000001
	fanMarkFilesystem = 0x00000100

	inotifyWatchMask = unix.IN_CREATE |
		unix.IN_DELETE |
		unix.IN_MOVED_FROM |
		unix.IN_MOVED_TO |
		unix.IN_CLOSE_WRITE |
		unix.IN_ATTRIB |
		unix.IN_DELETE_SELF |
		unix.IN_DONT_FOLLOW
)

func (p *Provider) watchLinux(
	ctx context.Context,
	vol provider.VolumeInfo,
	sinceCursor uint64,
	out chan<- provider.FileEvent,
) error {
	cleanRoot := path.Clean(vol.RootPath)
	if cleanRoot == "" || cleanRoot == "." {
		return provider.ErrInvalidVolume
	}

	// Primary path: fanotify (Kernel >= 5.9 + CAP_SYS_ADMIN) ONLY for full mount roots where
	// FAN_MARK_FILESYSTEM is naturally scoped and OpenByHandleAt resolves real inodes (C8 fix).
	// For subtrees or unprivileged processes, fall back transparently to inotify.
	if isMountRoot(cleanRoot) {
		if err := p.tryWatchFanotify(ctx, vol, cleanRoot, sinceCursor, out); err == nil {
			return nil
		} else if !isFanotifyFallbackErr(err) {
			return err
		}
	}

	return p.watchInotify(ctx, vol, cleanRoot, sinceCursor, out)
}

func isFanotifyFallbackErr(err error) bool {
	return errors.Is(err, unix.EPERM) ||
		errors.Is(err, unix.EACCES) ||
		errors.Is(err, unix.EINVAL) ||
		errors.Is(err, unix.ENOSYS) ||
		errors.Is(err, unix.EOPNOTSUPP)
}

type fanChildKey struct {
	dirIno uint64
	name   string
}

type fanChildMeta struct {
	ino   uint64
	isDir bool
}

func (p *Provider) tryWatchFanotify(
	ctx context.Context,
	vol provider.VolumeInfo,
	cleanRoot string,
	sinceCursor uint64,
	out chan<- provider.FileEvent,
) error {
	// Open mount root read-only as anchor FD for OpenByHandleAt.
	mountFD, err := unix.Open(cleanRoot, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer unix.Close(mountFD)

	// Probe NameToHandleAt + OpenByHandleAt support before committing to fanotify.
	probeHandle, _, err := unix.NameToHandleAt(mountFD, ".", unix.AT_EMPTY_PATH)
	if err != nil {
		return err
	}
	testFD, err := unix.OpenByHandleAt(mountFD, probeHandle, unix.O_RDONLY|unix.O_PATH|unix.O_CLOEXEC)
	if err != nil {
		return err
	}
	_ = unix.Close(testFD)

	fanFD, err := unix.FanotifyInit(
		fanClassNotif|fanReportDFIDName|fanNonblock|fanCloexec,
		unix.O_RDONLY|unix.O_LARGEFILE,
	)
	if err != nil {
		return err
	}
	defer unix.Close(fanFD)

	mask := FanCreate | FanDelete | FanMovedFrom | FanMovedTo | FanCloseWrite | FanAttrib | FanOnDir | FanEventOnChild
	if err := unix.FanotifyMark(fanFD, fanMarkAdd|fanMarkFilesystem, mask, unix.AT_FDCWD, cleanRoot); err != nil {
		return err
	}

	var cursorSeq atomic.Uint64
	cursorSeq.Store(sinceCursor)
	buf := make([]byte, 64*1024)
	pollFds := []unix.PollFd{{Fd: int32(fanFD), Events: unix.POLLIN}}

	// Bounded cache mapping (parentIno, exact case-sensitive name) -> childIno for deletes & moves.
	childCache := make(map[fanChildKey]fanChildMeta, 4096)
	pendingMoved := make(map[uint64]PendingMove, 64) // keyed by childIno when MovedFrom precedes MovedTo

	resolveDirFD := func(handleType int32, raw []byte) (int, uint64, string, bool) {
		if len(raw) == 0 {
			return -1, 0, "", false
		}
		fh := unix.NewFileHandle(handleType, raw)
		dfd, err := unix.OpenByHandleAt(mountFD, fh, unix.O_RDONLY|unix.O_PATH|unix.O_CLOEXEC)
		if err != nil {
			return -1, 0, "", false
		}
		var dst unix.Stat_t
		if err := unix.Fstat(dfd, &dst); err != nil {
			_ = unix.Close(dfd)
			return -1, 0, "", false
		}
		dirPath, _ := os.Readlink(fmt.Sprintf("/proc/self/fd/%d", dfd))
		return dfd, dst.Ino, dirPath, true
	}

	for {
		if ctx.Err() != nil {
			return nil
		}
		nReady, err := unix.Poll(pollFds, 50)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return err
		}
		if nReady <= 0 {
			// Flush unpaired MovedFrom entries as EventDelete.
			for k, pm := range pendingMoved {
				delete(pendingMoved, k)
				sendEventCtx(ctx, out, provider.FileEvent{
					VolID:     vol.VolID,
					Type:      provider.EventDelete,
					NodeKey:   pm.NodeKey,
					ParentKey: pm.ParentKey,
					OldPath:   pm.OldPath,
					NewName:   pm.Name,
					Cursor:    cursorSeq.Add(1),
					IsDir:     pm.IsDir,
				})
			}
			continue
		}

		n, err := unix.Read(fanFD, buf)
		if err != nil {
			if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EINTR) {
				continue
			}
			return err
		}
		if n <= 0 {
			continue
		}

		parsed, err := ParseFanotifyBuffer(buf[:n])
		if err != nil {
			continue
		}

		for i := range parsed {
			fe := parsed[i]
			if fe.FD >= 0 {
				_ = unix.Close(int(fe.FD))
			}

			c := cursorSeq.Add(1)
			if fe.Mask&FanQOverflow != 0 {
				if !sendEventCtx(ctx, out, provider.FileEvent{
					VolID:   vol.VolID,
					Type:    provider.EventRescanDir,
					NodeKey: vol.RootKey,
					OldPath: cleanRoot,
					IsDir:   true,
					Cursor:  c,
				}) {
					return nil
				}
				continue
			}

			dfd, dirIno, dirPath, okDir := resolveDirFD(fe.DirHandleType, fe.DirHandle)
			if !okDir {
				continue
			}

			// Enforce scope to cleanRoot subtree.
			if dirPath != "" && cleanRoot != "/" && dirPath != cleanRoot && !strings.HasPrefix(dirPath, cleanRoot+"/") {
				_ = unix.Close(dfd)
				continue
			}

			relPath := ""
			if dirPath != "" {
				full := path.Join(dirPath, fe.Name)
				if cleanRoot == "/" {
					relPath = strings.TrimPrefix(full, "/")
				} else if strings.HasPrefix(full, cleanRoot+"/") {
					relPath = full[len(cleanRoot)+1:]
				}
			}
			depth := 0
			if relPath != "" {
				depth = strings.Count(relPath, "/") + 1
			}

			isDir := (fe.Mask & FanOnDir) != 0
			var stx unix.Statx_t
			haveStat := false
			if fe.Name != "" && fe.Mask&(FanDelete|FanDeleteSelf|FanMovedFrom) == 0 {
				if s, sErr := statxNoFollow(dfd, fe.Name); sErr == nil {
					stx = s
					haveStat = true
				}
			}
			_ = unix.Close(dfd)

			ck := fanChildKey{dirIno: dirIno, name: fe.Name}
			var flags uint16
			if len(fe.Name) > 0 && fe.Name[0] == '.' {
				flags |= storage.FlagHidden
			}

			switch {
			case fe.Mask&(FanDelete|FanDeleteSelf) != 0:
				cm, ok := childCache[ck]
				delete(childCache, ck)
				nodeKey := cm.ino
				if !ok || nodeKey == 0 {
					nodeKey = hashCaseSensitiveChild(dirIno, fe.Name)
				}
				if cm.isDir {
					isDir = true
				}
				if isDir {
					flags |= storage.FlagDir
				}
				if !sendEventCtx(ctx, out, provider.FileEvent{
					VolID:     vol.VolID,
					Type:      provider.EventDelete,
					NodeKey:   nodeKey,
					ParentKey: dirIno,
					NewName:   fe.Name,
					RelPath:   relPath,
					Depth:     depth,
					Flags:     flags,
					Cursor:    c,
					IsDir:     isDir,
				}) {
					return nil
				}

			case fe.Mask&FanMovedFrom != 0:
				cm := childCache[ck]
				delete(childCache, ck)
				nodeKey := cm.ino
				if nodeKey == 0 {
					nodeKey = hashCaseSensitiveChild(dirIno, fe.Name)
				}
				pendingMoved[nodeKey] = PendingMove{
					ParentKey: dirIno,
					NodeKey:   nodeKey,
					OldPath:   relPath,
					Name:      fe.Name,
					IsDir:     isDir || cm.isDir,
				}

			case fe.Mask&FanMovedTo != 0:
				var nodeKey uint64
				var size, modTime int64
				if haveStat {
					nodeKey = stx.Ino
					size = int64(stx.Size)
					modTime = statxModTimeNano(&stx)
					mode := stx.Mode & unix.S_IFMT
					isDir = mode == unix.S_IFDIR
					if mode == unix.S_IFLNK {
						flags |= storage.FlagSymlink
					}
				}
				if nodeKey == 0 {
					nodeKey = hashCaseSensitiveChild(dirIno, fe.Name)
				}
				if isDir {
					flags |= storage.FlagDir
				}
				if len(childCache) < 200_000 {
					childCache[ck] = fanChildMeta{ino: nodeKey, isDir: isDir}
				}
				if old, paired := pendingMoved[nodeKey]; paired {
					delete(pendingMoved, nodeKey)
					if !sendEventCtx(ctx, out, provider.FileEvent{
						VolID:        vol.VolID,
						Type:         provider.EventRename,
						NodeKey:      nodeKey,
						ParentKey:    old.ParentKey,
						NewParentKey: dirIno,
						OldPath:      old.OldPath,
						NewName:      fe.Name,
						RelPath:      relPath,
						Depth:        depth,
						Flags:        flags,
						Size:         size,
						ModTime:      modTime,
						Cursor:       c,
						IsDir:        isDir,
					}) {
						return nil
					}
				} else {
					if !sendEventCtx(ctx, out, provider.FileEvent{
						VolID:     vol.VolID,
						Type:      provider.EventCreate,
						NodeKey:   nodeKey,
						ParentKey: dirIno,
						NewName:   fe.Name,
						RelPath:   relPath,
						Depth:     depth,
						Flags:     flags,
						Size:      size,
						ModTime:   modTime,
						Cursor:    c,
						IsDir:     isDir,
					}) {
						return nil
					}
				}

			case fe.Mask&FanCreate != 0:
				var nodeKey uint64
				var size, modTime int64
				if haveStat {
					nodeKey = stx.Ino
					size = int64(stx.Size)
					modTime = statxModTimeNano(&stx)
					mode := stx.Mode & unix.S_IFMT
					isDir = mode == unix.S_IFDIR
					if mode == unix.S_IFLNK {
						flags |= storage.FlagSymlink
					}
				}
				if nodeKey == 0 {
					nodeKey = hashCaseSensitiveChild(dirIno, fe.Name)
				}
				if isDir {
					flags |= storage.FlagDir
				}
				if len(childCache) < 200_000 {
					childCache[ck] = fanChildMeta{ino: nodeKey, isDir: isDir}
				}
				if !sendEventCtx(ctx, out, provider.FileEvent{
					VolID:     vol.VolID,
					Type:      provider.EventCreate,
					NodeKey:   nodeKey,
					ParentKey: dirIno,
					NewName:   fe.Name,
					RelPath:   relPath,
					Depth:     depth,
					Flags:     flags,
					Size:      size,
					ModTime:   modTime,
					Cursor:    c,
					IsDir:     isDir,
				}) {
					return nil
				}

			case fe.Mask&(FanCloseWrite|FanAttrib) != 0:
				if !haveStat {
					continue
				}
				mode := stx.Mode & unix.S_IFMT
				isDir = mode == unix.S_IFDIR
				if isDir {
					flags |= storage.FlagDir
				}
				if mode == unix.S_IFLNK {
					flags |= storage.FlagSymlink
				}
				if len(childCache) < 200_000 {
					childCache[ck] = fanChildMeta{ino: stx.Ino, isDir: isDir}
				}
				if !sendEventCtx(ctx, out, provider.FileEvent{
					VolID:     vol.VolID,
					Type:      provider.EventModifyMeta,
					NodeKey:   stx.Ino,
					ParentKey: dirIno,
					NewName:   fe.Name,
					RelPath:   relPath,
					Depth:     depth,
					Flags:     flags,
					Size:      int64(stx.Size),
					ModTime:   statxModTimeNano(&stx),
					Cursor:    c,
					IsDir:     isDir,
				}) {
					return nil
				}
			}
		}
	}
}

type watchedDir struct {
	wd           int32
	diskDir      string
	relDir       string
	dirKey       uint64
	depth        int
	parentSystem bool
	children     map[string]cachedChild // exact case-sensitive name -> (ino, isDir)
}

type cachedChild struct {
	ino   uint64
	isDir bool
}

func (p *Provider) watchInotify(
	ctx context.Context,
	vol provider.VolumeInfo,
	cleanRoot string,
	sinceCursor uint64,
	out chan<- provider.FileEvent,
) error {
	var rootSt unix.Stat_t
	if err := unix.Lstat(cleanRoot, &rootSt); err != nil {
		if errors.Is(err, unix.EACCES) || errors.Is(err, unix.EPERM) {
			return provider.ErrPermissionDenied
		}
		return err
	}

	fd, err := unix.InotifyInit1(unix.IN_NONBLOCK | unix.IN_CLOEXEC)
	if err != nil {
		return err
	}
	defer unix.Close(fd)

	byWD := make(map[int32]*watchedDir, 256)
	byPath := make(map[string]int32, 256)
	skipMounts := buildSkipMountSet(cleanRoot)

	removeSubtreeWatches := func(targetDiskDir string) {
		cleanTarget := path.Clean(targetDiskDir)
		prefix := cleanTarget + "/"
		for pth, wd := range byPath {
			if pth == cleanTarget || strings.HasPrefix(pth, prefix) {
				_, _ = unix.InotifyRmWatch(fd, uint32(wd))
				delete(byPath, pth)
				delete(byWD, wd)
			}
		}
	}

	rules := p.pruner.Rules()
	rootStack := filter.NewIgnoreStack(rules.CaseInsensitive(), rules.GitignoreIgnoreFiles())

	var registerTree func(diskDir, relDir string, depth int, parentSystem bool, ignStack *filter.IgnoreStack) error
	registerTree = func(diskDir, relDir string, depth int, parentSystem bool, ignStack *filter.IgnoreStack) error {
		if ctx.Err() != nil {
			return nil
		}
		if _, skip := skipMounts[diskDir]; skip && diskDir != cleanRoot {
			return nil
		}
		var st unix.Stat_t
		if err := unix.Lstat(diskDir, &st); err != nil {
			if depth == 0 {
				return err
			}
			return nil
		}
		if (st.Mode & unix.S_IFMT) != unix.S_IFDIR {
			return nil
		}
		// Enforce single-filesystem boundary (C9 fix).
		if st.Dev != rootSt.Dev {
			return nil
		}

		if depth > 0 {
			dec := p.pruner.EvalTopDownDir(vol.VolID, st.Ino, path.Base(diskDir), relDir, depth, parentSystem, ignStack)
			if !dec.Keep || !dec.Descend {
				// If a previously watched directory just became folded/excluded, remove its subtree watches (C10 fix).
				removeSubtreeWatches(diskDir)
				return nil
			}
			parentSystem = (dec.Flags & storage.FlagSystem) != 0
		}

		wdInt, err := unix.InotifyAddWatch(fd, diskDir, inotifyWatchMask)
		if err != nil {
			if depth == 0 {
				if errors.Is(err, unix.EACCES) || errors.Is(err, unix.EPERM) {
					return provider.ErrPermissionDenied
				}
				return err
			}
			return nil
		}
		wd := int32(wdInt)
		// If this wd was previously mapped to an old path (directory renamed), remove the stale byPath key (C10 fix).
		if prev, exists := byWD[wd]; exists && prev.diskDir != diskDir {
			delete(byPath, prev.diskDir)
		}

		wdir := &watchedDir{
			wd:           wd,
			diskDir:      diskDir,
			relDir:       relDir,
			dirKey:       st.Ino,
			depth:        depth,
			parentSystem: parentSystem,
			children:     make(map[string]cachedChild, 16),
		}
		byWD[wd] = wdir
		byPath[diskDir] = wd

		entries, err := os.ReadDir(diskDir)
		if err != nil {
			if depth == 0 && (errors.Is(err, unix.EACCES) || errors.Is(err, unix.EPERM)) {
				return provider.ErrPermissionDenied
			}
			return nil
		}

		// Respect .gitignore / .xiftignore while registering inotify watches (I9 fix).
		names := make([]string, len(entries))
		for i, ent := range entries {
			names[i] = ent.Name()
		}
		scope := p.pruner.EnterDirIgnoreScope(ignStack, diskDir, relDir, names)
		defer ignStack.RestoreScope(scope)

		for _, ent := range entries {
			if ctx.Err() != nil {
				return nil
			}
			name := ent.Name()
			childDisk := path.Join(diskDir, name)
			var cst unix.Stat_t
			if err := unix.Lstat(childDisk, &cst); err != nil {
				continue
			}
			mode := cst.Mode & unix.S_IFMT
			isDir := mode == unix.S_IFDIR
			wdir.children[name] = cachedChild{ino: cst.Ino, isDir: isDir}
			if isDir {
				childRel := name
				if relDir != "" {
					childRel = relDir + "/" + name
				}
				if err := registerTree(childDisk, childRel, depth+1, parentSystem, ignStack); err != nil {
					return err
				}
			}
		}
		return nil
	}

	if err := registerTree(cleanRoot, "", 0, false, rootStack); err != nil {
		return err
	}
	if p.onWatchArmed != nil {
		p.onWatchArmed()
	}

	var cursorSeq atomic.Uint64
	cursorSeq.Store(sinceCursor)
	tracker := NewMoveCookieTracker()
	buf := make([]byte, 64*1024)
	pollFds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}

	for {
		if ctx.Err() != nil {
			return nil
		}
		nReady, err := unix.Poll(pollFds, 50)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return err
		}
		if nReady <= 0 {
			tracker.FlushStale(vol.VolID, func(ev provider.FileEvent) {
				// Directory moved outside watched tree: clean up its leaked inotify watches (C10 fix).
				if ev.IsDir && ev.OldPath != "" {
					removeSubtreeWatches(path.Join(cleanRoot, ev.OldPath))
				}
				ev.Cursor = cursorSeq.Add(1)
				sendEventCtx(ctx, out, ev)
			})
			continue
		}

		n, err := unix.Read(fd, buf)
		if err != nil {
			if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EINTR) {
				continue
			}
			return err
		}
		if n <= 0 {
			continue
		}

		parsed, err := ParseInotifyBuffer(buf[:n])
		if err != nil {
			continue
		}

		for i := range parsed {
			ie := parsed[i]
			c := cursorSeq.Add(1)

			if ie.Mask&INQOverflow != 0 {
				if !sendEventCtx(ctx, out, provider.FileEvent{
					VolID:   vol.VolID,
					Type:    provider.EventRescanDir,
					NodeKey: vol.RootKey,
					OldPath: cleanRoot,
					IsDir:   true,
					Cursor:  c,
				}) {
					return nil
				}
				continue
			}

			wdir, ok := byWD[ie.WD]
			if !ok {
				continue
			}
			if ie.Mask&INIgnored != 0 {
				delete(byWD, ie.WD)
				if curWD, okP := byPath[wdir.diskDir]; okP && curWD == ie.WD {
					delete(byPath, wdir.diskDir)
				}
				continue
			}
			if ie.Name == "" {
				continue
			}

			fullPath := path.Join(wdir.diskDir, ie.Name)
			relPath := ie.Name
			if wdir.relDir != "" {
				relPath = wdir.relDir + "/" + ie.Name
			}
			isDir := (ie.Mask & INIsDir) != 0
			depth := wdir.depth + 1

			switch {
			case ie.Mask&INMovedFrom != 0:
				ch := wdir.children[ie.Name]
				delete(wdir.children, ie.Name)
				nodeKey := ch.ino
				if nodeKey == 0 {
					nodeKey = hashCaseSensitiveChild(wdir.dirKey, ie.Name)
				}
				tracker.PushMovedFrom(ie.Cookie, PendingMove{
					WD:        ie.WD,
					ParentKey: wdir.dirKey,
					NodeKey:   nodeKey,
					OldPath:   relPath,
					Name:      ie.Name,
					IsDir:     isDir || ch.isDir,
				})

			case ie.Mask&INMovedTo != 0:
				var st unix.Stat_t
				var nodeKey uint64
				var size, modTime int64
				var flags uint16
				if err := unix.Lstat(fullPath, &st); err == nil {
					nodeKey = st.Ino
					size = st.Size
					modTime = st.Mtim.Sec*1_000_000_000 + st.Mtim.Nsec
					mode := st.Mode & unix.S_IFMT
					isDir = mode == unix.S_IFDIR
					if mode == unix.S_IFLNK {
						flags |= storage.FlagSymlink
					}
				}
				old, paired := tracker.TakeMovedFrom(ie.Cookie)
				if nodeKey == 0 && paired {
					nodeKey = old.NodeKey
				}
				if nodeKey == 0 {
					nodeKey = hashCaseSensitiveChild(wdir.dirKey, ie.Name)
				}
				wdir.children[ie.Name] = cachedChild{ino: nodeKey, isDir: isDir}

				if isDir {
					flags |= storage.FlagDir
					// Clean up old watch path entries before re-registering (so renaming to node_modules unregisters watches!).
					if paired && old.OldPath != "" {
						removeSubtreeWatches(path.Join(cleanRoot, old.OldPath))
					}
					_ = registerTree(fullPath, relPath, depth, wdir.parentSystem, filter.NewIgnoreStack(rules.CaseInsensitive(), rules.GitignoreIgnoreFiles()))
				}
				if len(ie.Name) > 0 && ie.Name[0] == '.' {
					flags |= storage.FlagHidden
				}

				if paired {
					if !sendEventCtx(ctx, out, provider.FileEvent{
						VolID:        vol.VolID,
						Type:         provider.EventRename,
						NodeKey:      nodeKey,
						ParentKey:    old.ParentKey,
						NewParentKey: wdir.dirKey,
						OldPath:      old.OldPath,
						NewName:      ie.Name,
						RelPath:      relPath,
						Depth:        depth,
						Flags:        flags,
						Size:         size,
						ModTime:      modTime,
						Cursor:       c,
						IsDir:        isDir,
					}) {
						return nil
					}
				} else {
					if !sendEventCtx(ctx, out, provider.FileEvent{
						VolID:     vol.VolID,
						Type:      provider.EventCreate,
						NodeKey:   nodeKey,
						ParentKey: wdir.dirKey,
						NewName:   ie.Name,
						RelPath:   relPath,
						Depth:     depth,
						Flags:     flags,
						Size:      size,
						ModTime:   modTime,
						Cursor:    c,
						IsDir:     isDir,
					}) {
						return nil
					}
				}

			case ie.Mask&INCreate != 0:
				var st unix.Stat_t
				var nodeKey uint64
				var size, modTime int64
				var flags uint16
				if err := unix.Lstat(fullPath, &st); err == nil {
					nodeKey = st.Ino
					size = st.Size
					modTime = st.Mtim.Sec*1_000_000_000 + st.Mtim.Nsec
					mode := st.Mode & unix.S_IFMT
					isDir = mode == unix.S_IFDIR
					if mode == unix.S_IFLNK {
						flags |= storage.FlagSymlink
					}
				}
				if nodeKey == 0 {
					nodeKey = hashCaseSensitiveChild(wdir.dirKey, ie.Name)
				}
				wdir.children[ie.Name] = cachedChild{ino: nodeKey, isDir: isDir}
				if isDir {
					flags |= storage.FlagDir
					if !p.pruner.PrunedDirSet().IsPruned(vol.VolID, wdir.dirKey) {
						_ = registerTree(fullPath, relPath, depth, wdir.parentSystem, filter.NewIgnoreStack(rules.CaseInsensitive(), rules.GitignoreIgnoreFiles()))
					}
				}
				if len(ie.Name) > 0 && ie.Name[0] == '.' {
					flags |= storage.FlagHidden
				}
				if !sendEventCtx(ctx, out, provider.FileEvent{
					VolID:     vol.VolID,
					Type:      provider.EventCreate,
					NodeKey:   nodeKey,
					ParentKey: wdir.dirKey,
					NewName:   ie.Name,
					RelPath:   relPath,
					Depth:     depth,
					Flags:     flags,
					Size:      size,
					ModTime:   modTime,
					Cursor:    c,
					IsDir:     isDir,
				}) {
					return nil
				}

			case ie.Mask&INDelete != 0:
				ch := wdir.children[ie.Name]
				delete(wdir.children, ie.Name)
				nodeKey := ch.ino
				if nodeKey == 0 {
					nodeKey = hashCaseSensitiveChild(wdir.dirKey, ie.Name)
				}
				if isDir || ch.isDir {
					removeSubtreeWatches(fullPath)
				}
				if !sendEventCtx(ctx, out, provider.FileEvent{
					VolID:     vol.VolID,
					Type:      provider.EventDelete,
					NodeKey:   nodeKey,
					ParentKey: wdir.dirKey,
					NewName:   ie.Name,
					RelPath:   relPath,
					Depth:     depth,
					Cursor:    c,
					IsDir:     isDir || ch.isDir,
				}) {
					return nil
				}

			case ie.Mask&(INCloseWrite|INAttrib) != 0:
				var st unix.Stat_t
				if err := unix.Lstat(fullPath, &st); err != nil {
					continue
				}
				mode := st.Mode & unix.S_IFMT
				isDir = mode == unix.S_IFDIR
				var flags uint16
				if isDir {
					flags |= storage.FlagDir
				}
				if mode == unix.S_IFLNK {
					flags |= storage.FlagSymlink
				}
				if len(ie.Name) > 0 && ie.Name[0] == '.' {
					flags |= storage.FlagHidden
				}
				wdir.children[ie.Name] = cachedChild{ino: st.Ino, isDir: isDir}
				if !sendEventCtx(ctx, out, provider.FileEvent{
					VolID:     vol.VolID,
					Type:      provider.EventModifyMeta,
					NodeKey:   st.Ino,
					ParentKey: wdir.dirKey,
					NewName:   ie.Name,
					RelPath:   relPath,
					Depth:     depth,
					Flags:     flags,
					Size:      st.Size,
					ModTime:   st.Mtim.Sec*1_000_000_000 + st.Mtim.Nsec,
					Cursor:    c,
					IsDir:     isDir,
				}) {
					return nil
				}
			}
		}
	}
}

func sendEventCtx(ctx context.Context, out chan<- provider.FileEvent, ev provider.FileEvent) bool {
	select {
	case out <- ev:
		return true
	case <-ctx.Done():
		return false
	}
}

// hashCaseSensitiveChild produces a fallback key preserving Linux case sensitivity.
func hashCaseSensitiveChild(parentKey uint64, name string) uint64 {
	h := fnv.New64a()
	var b [8]byte
	for i := 0; i < 8; i++ {
		b[i] = byte(parentKey >> (i * 8))
	}
	_, _ = h.Write(b[:])
	_, _ = h.Write([]byte(name))
	v := h.Sum64()
	if v == 0 {
		v = 1
	}
	return v
}
