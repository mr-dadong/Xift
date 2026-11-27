//go:build linux

package linux

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"path"
	"runtime"
	"sync"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/mr-dadong/Xift/internal/filter"
	"github.com/mr-dadong/Xift/internal/provider"
	"github.com/mr-dadong/Xift/internal/storage"
)

type rawDirent struct {
	ino  uint64
	typ  uint8
	name string
}

type scanDirJob struct {
	diskDir      string
	relDir       string
	parentKey    uint64
	depth        int
	parentSystem bool
	ignoreStack  *filter.IgnoreStack
}

func readDirents64(fd int, buf []byte) ([]rawDirent, error) {
	var out []rawDirent
	for {
		n, err := unix.Getdents(fd, buf)
		if err != nil {
			return out, err
		}
		if n <= 0 {
			return out, nil
		}
		offset := 0
		for offset+19 <= n {
			ino := binary.LittleEndian.Uint64(buf[offset : offset+8])
			reclen := int(binary.LittleEndian.Uint16(buf[offset+16 : offset+18]))
			dtype := buf[offset+18]
			if reclen < 19 || offset+reclen > n {
				break
			}
			rawName := buf[offset+19 : offset+reclen]
			if nul := bytes.IndexByte(rawName, 0); nul >= 0 {
				rawName = rawName[:nul]
			}
			name := string(rawName)
			if name != "" && name != "." && name != ".." {
				out = append(out, rawDirent{ino: ino, typ: dtype, name: name})
			}
			offset += reclen
		}
	}
}

func statxNoFollow(dirFD int, name string) (unix.Statx_t, error) {
	var stx unix.Statx_t
	mask := unix.STATX_TYPE | unix.STATX_INO | unix.STATX_SIZE | unix.STATX_MTIME
	err := unix.Statx(dirFD, name, unix.AT_SYMLINK_NOFOLLOW|unix.AT_STATX_DONT_SYNC, mask, &stx)
	return stx, err
}

func statxModTimeNano(stx *unix.Statx_t) int64 {
	return stx.Mtime.Sec*1_000_000_000 + int64(stx.Mtime.Nsec)
}

// buildSkipMountSet records ALL mountpoints other than cleanRoot (virtual, remote, AND other local volumes)
// so single-volume scans never cross mount boundaries or collide cross-FS inodes (C9 fix).
func buildSkipMountSet(cleanRoot string) map[string]struct{} {
	entries, err := ReadSelfMountInfo()
	if err != nil {
		return nil
	}
	skip := make(map[string]struct{}, len(entries))
	for _, e := range entries {
		mp := path.Clean(e.MountPoint)
		if mp == "" || mp == cleanRoot {
			continue
		}
		skip[mp] = struct{}{}
	}
	return skip
}

// isMountRoot reports whether cleanRoot is itself a mount point in /proc/self/mountinfo.
func isMountRoot(cleanRoot string) bool {
	entries, err := ReadSelfMountInfo()
	if err != nil {
		return cleanRoot == "/"
	}
	for _, e := range entries {
		if path.Clean(e.MountPoint) == cleanRoot {
			return true
		}
	}
	return false
}

// scanTopDownLinux performs multi-threaded strictly read-only scanning via Openat + Getdents + Statx,
// strictly enforcing single-filesystem device boundaries, skipping virtual/remote/other mountpoints
// and symbolic links while applying Pruner early stop.
func (p *Provider) scanTopDownLinux(
	ctx context.Context,
	vol provider.VolumeInfo,
	rootPath string,
	rootKey uint64,
	emitRoot bool,
	writer provider.SegmentWriter,
) error {
	cleanRoot := path.Clean(rootPath)
	if cleanRoot == "" || cleanRoot == "." {
		return provider.ErrInvalidVolume
	}

	var rootSt unix.Statx_t
	if err := unix.Statx(unix.AT_FDCWD, cleanRoot, unix.AT_SYMLINK_NOFOLLOW, unix.STATX_INO|unix.STATX_TYPE, &rootSt); err != nil {
		if errors.Is(err, unix.EACCES) || errors.Is(err, unix.EPERM) {
			return provider.ErrPermissionDenied
		}
		return err
	}
	if rootKey == 0 {
		rootKey = rootSt.Ino
	}

	// Verify root directory itself is readable before emitting root or spawning workers (I4 fix).
	probeFD, err := unix.Openat(unix.AT_FDCWD, cleanRoot, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		if errors.Is(err, unix.EACCES) || errors.Is(err, unix.EPERM) {
			return provider.ErrPermissionDenied
		}
		return err
	}
	_ = unix.Close(probeFD)

	if emitRoot {
		rootName := path.Base(cleanRoot)
		if cleanRoot == "/" {
			rootName = "/"
		}
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

	skipMounts := buildSkipMountSet(cleanRoot)
	rules := p.pruner.Rules()
	rootStack := filter.NewIgnoreStack(rules.CaseInsensitive(), rules.GitignoreIgnoreFiles())

	workers := p.opts.Workers
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
		if workers > 8 {
			workers = 8
		}
		if workers < 1 {
			workers = 1
		}
	}

	scanCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		qMu      sync.Mutex
		queue    = []scanDirJob{{diskDir: cleanRoot, relDir: "", parentKey: rootKey, depth: 1, ignoreStack: rootStack}}
		inFlight = 1
		qCond    = sync.NewCond(&qMu)

		errOnce  sync.Once
		firstErr error
	)

	setErr := func(err error) {
		if err == nil {
			return
		}
		errOnce.Do(func() {
			firstErr = err
			cancel()
		})
	}

	enqueue := func(job scanDirJob) {
		qMu.Lock()
		queue = append(queue, job)
		inFlight++
		qCond.Signal()
		qMu.Unlock()
	}

	finishOne := func() {
		qMu.Lock()
		inFlight--
		if inFlight == 0 {
			qCond.Broadcast()
		}
		qMu.Unlock()
	}

	// Ensure context cancellation wakes idle workers waiting on qCond.
	stopWake := make(chan struct{})
	go func() {
		select {
		case <-scanCtx.Done():
			qMu.Lock()
			qCond.Broadcast()
			qMu.Unlock()
		case <-stopWake:
		}
	}()
	defer close(stopWake)

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if !p.opts.DisableLowPriority {
				restore := provider.SetLowPriorityThread()
				defer restore()
			}
			buf := make([]byte, 128*1024)

			for {
				qMu.Lock()
				for len(queue) == 0 && inFlight > 0 && scanCtx.Err() == nil {
					qCond.Wait()
				}
				if scanCtx.Err() != nil || (len(queue) == 0 && inFlight == 0) {
					qMu.Unlock()
					return
				}
				job := queue[len(queue)-1]
				queue = queue[:len(queue)-1]
				qMu.Unlock()

				if err := p.processLinuxDir(scanCtx, vol, cleanRoot, rootSt.Dev_major, rootSt.Dev_minor, job, skipMounts, buf, writer, enqueue); err != nil {
					setErr(err)
				}
				finishOne()
			}
		}()
	}

	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	return ctx.Err()
}

func (p *Provider) processLinuxDir(
	ctx context.Context,
	vol provider.VolumeInfo,
	cleanRoot string,
	rootDevMajor, rootDevMinor uint32,
	job scanDirJob,
	skipMounts map[string]struct{},
	buf []byte,
	writer provider.SegmentWriter,
	enqueue func(scanDirJob),
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	// Strictly read-only open without following symlinks.
	fd, err := unix.Openat(unix.AT_FDCWD, job.diskDir, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		if job.depth == 1 && job.diskDir == cleanRoot {
			if errors.Is(err, unix.EACCES) || errors.Is(err, unix.EPERM) {
				return provider.ErrPermissionDenied
			}
			return err
		}
		return nil // Skip unreadable/vanished subdirectories gracefully
	}
	defer unix.Close(fd)

	entries, err := readDirents64(fd, buf)
	if err != nil {
		return nil
	}

	names := make([]string, len(entries))
	for i := range entries {
		names[i] = entries[i].name
	}
	scope := p.pruner.EnterDirIgnoreScope(job.ignoreStack, job.diskDir, job.relDir, names)
	defer job.ignoreStack.RestoreScope(scope)

	for i := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		ent := entries[i]
		fullPath := path.Join(job.diskDir, ent.name)
		if _, skip := skipMounts[fullPath]; skip {
			continue
		}

		relPath := ent.name
		if job.relDir != "" {
			relPath = job.relDir + "/" + ent.name
		}

		isDir := ent.typ == unix.DT_DIR
		isSymlink := ent.typ == unix.DT_LNK
		var stx unix.Statx_t
		haveStat := false

		if ent.typ == unix.DT_UNKNOWN {
			if s, sErr := statxNoFollow(fd, ent.name); sErr == nil {
				stx = s
				haveStat = true
				mode := stx.Mode & unix.S_IFMT
				isDir = mode == unix.S_IFDIR
				isSymlink = mode == unix.S_IFLNK
			}
		}

		var extraFlags uint16
		if len(ent.name) > 0 && ent.name[0] == '.' {
			extraFlags |= storage.FlagHidden
		}
		if isSymlink {
			extraFlags |= storage.FlagSymlink
		}

		if isDir {
			nodeKey := ent.ino
			var modTime int64
			if !haveStat {
				if s, sErr := statxNoFollow(fd, ent.name); sErr == nil {
					stx = s
					haveStat = true
				}
			}
			if haveStat {
				// Enforce same-filesystem device boundary (prevents cross-mount double-indexing & inode collisions).
				if stx.Dev_major != rootDevMajor || stx.Dev_minor != rootDevMinor {
					continue
				}
				if stx.Ino != 0 {
					nodeKey = stx.Ino
				}
				modTime = statxModTimeNano(&stx)
			}

			dec := p.pruner.EvalTopDownDir(vol.VolID, nodeKey, ent.name, relPath, job.depth, job.parentSystem, job.ignoreStack)
			if !dec.Keep {
				continue
			}
			if err := writer.WriteRecord(provider.ScanRecord{
				VolID:     vol.VolID,
				NodeKey:   nodeKey,
				ParentKey: job.parentKey,
				Name:      ent.name,
				Flags:     dec.Flags | extraFlags,
				ModTime:   modTime,
			}); err != nil {
				return err
			}
			if dec.Descend && !isSymlink {
				enqueue(scanDirJob{
					diskDir:      fullPath,
					relDir:       relPath,
					parentKey:    nodeKey,
					depth:        job.depth + 1,
					parentSystem: (dec.Flags & storage.FlagSystem) != 0,
					ignoreStack:  job.ignoreStack.Clone(),
				})
			}
			continue
		}

		dec := p.pruner.EvalTopDownFile(ent.name, relPath, job.parentSystem, job.ignoreStack)
		if !dec.Keep {
			continue
		}

		nodeKey := ent.ino
		var size, modTime int64
		if !haveStat {
			if s, sErr := statxNoFollow(fd, ent.name); sErr == nil {
				stx = s
				haveStat = true
			}
		}
		if haveStat {
			if stx.Ino != 0 {
				nodeKey = stx.Ino
			}
			size = int64(stx.Size)
			modTime = statxModTimeNano(&stx)
		}

		if err := writer.WriteRecord(provider.ScanRecord{
			VolID:     vol.VolID,
			NodeKey:   nodeKey,
			ParentKey: job.parentKey,
			Name:      ent.name,
			Flags:     dec.Flags | extraFlags,
			Size:      size,
			ModTime:   modTime,
		}); err != nil {
			return err
		}
	}

	return nil
}

// Compile-time ABI guard for unix.Statx_t.
var _ = unsafe.Sizeof(unix.Statx_t{})
