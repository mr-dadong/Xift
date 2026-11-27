//go:build darwin

package darwin

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"path"
	"sync"
	"unsafe"

	"github.com/ebitengine/purego"
	"golang.org/x/sys/unix"

	"github.com/mr-dadong/Xift/internal/filter"
	"github.com/mr-dadong/Xift/internal/provider"
	"github.com/mr-dadong/Xift/internal/storage"
)

// macOS <sys/attr.h> constants for getattrlistbulk (pure Go, zero CGO).
const (
	attrBitMapCount uint16 = 5

	attrCmnName          uint32 = 0x00000001
	attrCmnObjType       uint32 = 0x00000008
	attrCmnModTime       uint32 = 0x00000400
	attrCmnFlags         uint32 = 0x00080000
	attrCmnFileID        uint32 = 0x02000000
	attrCmnReturnedAttrs uint32 = 0x80000000

	attrFileDataLength uint32 = 0x00000200

	fsOptNoFollow uint64 = 0x00000001

	vnodeReg uint32 = 1
	vnodeDir uint32 = 2
	vnodeLnk uint32 = 5

	ufHidden uint32 = 0x00008000

	getattrBulkBufSize = 256 * 1024
)

type attrList struct {
	bitmapCount uint16
	reserved    uint16
	commonAttr  uint32
	volAttr     uint32
	dirAttr     uint32
	fileAttr    uint32
	forkAttr    uint32
}

type bulkDirEntry struct {
	name    string
	objType uint32
	fileID  uint64
	modTime int64
	flags   uint32
	size    int64
}

var (
	libcLoadOnce     sync.Once
	libcLoadErr      error
	fnGetAttrListBlk func(dirfd int32, attrListPtr, attrBuf unsafe.Pointer, bufSize uintptr, options uint64) int32
	fnDarwinErrnoPtr func() *int32
)

func ensureGetAttrListBulk() error {
	libcLoadOnce.Do(func() {
		libc, err := purego.Dlopen("/usr/lib/libSystem.B.dylib", purego.RTLD_LAZY|purego.RTLD_GLOBAL)
		if err != nil {
			libcLoadErr = fmt.Errorf("dlopen libSystem.B.dylib: %w", err)
			return
		}
		purego.RegisterLibFunc(&fnGetAttrListBlk, libc, "getattrlistbulk")
		purego.RegisterLibFunc(&fnDarwinErrnoPtr, libc, "__error")
	})
	return libcLoadErr
}

func readDirBulkEntries(dirFD int, buf []byte) ([]bulkDirEntry, error) {
	if err := ensureGetAttrListBulk(); err != nil {
		return nil, err
	}

	req := attrList{
		bitmapCount: attrBitMapCount,
		commonAttr:  attrCmnReturnedAttrs | attrCmnName | attrCmnObjType | attrCmnModTime | attrCmnFlags | attrCmnFileID,
		fileAttr:    attrFileDataLength,
	}

	var entries []bulkDirEntry
	for {
		n := fnGetAttrListBlk(
			int32(dirFD),
			unsafe.Pointer(&req),
			unsafe.Pointer(&buf[0]),
			uintptr(len(buf)),
			fsOptNoFollow,
		)
		if n < 0 {
			if fnDarwinErrnoPtr != nil {
				if p := fnDarwinErrnoPtr(); p != nil && *p != 0 {
					return entries, unix.Errno(*p)
				}
			}
			return entries, unix.EIO
		}
		if n == 0 {
			return entries, nil
		}

		offset := 0
		for i := int32(0); i < n && offset+24 <= len(buf); i++ {
			recLen := int(binary.LittleEndian.Uint32(buf[offset : offset+4]))
			if recLen < 24 || offset+recLen > len(buf) {
				break
			}
			rec := buf[offset : offset+recLen]
			retCommon := binary.LittleEndian.Uint32(rec[4:8])
			retFile := binary.LittleEndian.Uint32(rec[16:20])

			cur := 24
			var ent bulkDirEntry

			// Attributes appear in strict bit order defined by <sys/attr.h>.
			if retCommon&attrCmnName != 0 && cur+8 <= len(rec) {
				nameOff := int(int32(binary.LittleEndian.Uint32(rec[cur : cur+4])))
				nameLen := int(binary.LittleEndian.Uint32(rec[cur+4 : cur+8]))
				absStart := cur + nameOff
				if absStart >= 0 && nameLen > 0 && absStart+nameLen <= len(rec) {
					raw := rec[absStart : absStart+nameLen]
					if nul := bytes.IndexByte(raw, 0); nul >= 0 {
						raw = raw[:nul]
					}
					ent.name = string(raw)
				}
				cur += 8
			}
			if retCommon&attrCmnObjType != 0 && cur+4 <= len(rec) {
				ent.objType = binary.LittleEndian.Uint32(rec[cur : cur+4])
				cur += 4
			}
			if retCommon&attrCmnModTime != 0 && cur+16 <= len(rec) {
				sec := int64(binary.LittleEndian.Uint64(rec[cur : cur+8]))
				nsec := int64(binary.LittleEndian.Uint64(rec[cur+8 : cur+16]))
				ent.modTime = sec*1_000_000_000 + nsec
				cur += 16
			}
			if retCommon&attrCmnFlags != 0 && cur+4 <= len(rec) {
				ent.flags = binary.LittleEndian.Uint32(rec[cur : cur+4])
				cur += 4
			}
			if retCommon&attrCmnFileID != 0 && cur+8 <= len(rec) {
				ent.fileID = binary.LittleEndian.Uint64(rec[cur : cur+8])
				cur += 8
			}
			if retFile&attrFileDataLength != 0 && cur+8 <= len(rec) {
				ent.size = int64(binary.LittleEndian.Uint64(rec[cur : cur+8]))
			}

			if ent.name != "" && ent.name != "." && ent.name != ".." {
				entries = append(entries, ent)
			}
			offset += recLen
		}
	}
}

// scanTopDownBulk performs strictly read-only top-down early-stop directory scanning using
// macOS getattrlistbulk via purego, skipping APFS firmlink duplicates, Time Machine snapshots,
// auxiliary mounts, and symbolic links while populating the path->inode cache for FSEvents.
func (p *Provider) scanTopDownBulk(
	ctx context.Context,
	vol provider.VolumeInfo,
	rootPath string,
	rootKey uint64,
	emitRoot bool,
	writer provider.SegmentWriter,
) error {
	cleanRoot := normalizeUnixPath(rootPath)
	if cleanRoot == "" {
		return provider.ErrInvalidVolume
	}

	var rootSt unix.Stat_t
	if err := unix.Lstat(cleanRoot, &rootSt); err != nil {
		if errors.Is(err, unix.EACCES) || errors.Is(err, unix.EPERM) {
			return fmt.Errorf("macOS Full Disk Access required for %s: %w", cleanRoot, provider.ErrPermissionDenied)
		}
		return err
	}
	if rootKey == 0 {
		rootKey = rootSt.Ino
	}

	cache := p.getOrCreatePathCache(vol.VolID)
	cache.Put(cleanRoot, CachedNode{NodeKey: rootKey, ParentKey: rootKey, IsDir: true})

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

	rules := p.pruner.Rules()
	ignoreStack := filter.NewIgnoreStack(rules.CaseInsensitive(), rules.GitignoreIgnoreFiles())
	buf := make([]byte, getattrBulkBufSize)

	var walk func(diskDir, relDir string, parentKey uint64, depth int, parentSystem bool) error
	walk = func(diskDir, relDir string, parentKey uint64, depth int, parentSystem bool) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		// Iron Law #1: Strictly read-only directory open, never following symlinks.
		fd, err := unix.Open(diskDir, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			if depth == 1 && diskDir == cleanRoot {
				if errors.Is(err, unix.EACCES) || errors.Is(err, unix.EPERM) {
					return fmt.Errorf("macOS Full Disk Access required for %s: %w", diskDir, provider.ErrPermissionDenied)
				}
				return err
			}
			return nil // Gracefully skip protected subdirectories (e.g., TCC-protected dirs)
		}

		entries, readErr := readDirBulkEntries(fd, buf)
		_ = unix.Close(fd)
		if readErr != nil {
			if depth == 1 && diskDir == cleanRoot {
				if errors.Is(readErr, unix.EACCES) || errors.Is(readErr, unix.EPERM) {
					return fmt.Errorf("macOS Full Disk Access required for %s: %w", diskDir, provider.ErrPermissionDenied)
				}
				return readErr
			}
			return nil
		}

		names := make([]string, len(entries))
		for i := range entries {
			names[i] = entries[i].name
		}
		scope := p.pruner.EnterDirIgnoreScope(ignoreStack, diskDir, relDir, names)
		defer ignoreStack.RestoreScope(scope)

		for i := range entries {
			if err := ctx.Err(); err != nil {
				return err
			}
			ent := entries[i]
			fullPath := path.Join(diskDir, ent.name)
			if IsTimeMachineSnapshotPath(fullPath) || p.deduper.IsDuplicateDataPath(fullPath) {
				continue
			}
			if cleanRoot == "/" && IsAuxiliaryMacOSMount(fullPath) {
				continue
			}

			relPath := ent.name
			if relDir != "" {
				relPath = relDir + "/" + ent.name
			}

			nodeKey := ent.fileID
			if nodeKey == 0 {
				nodeKey = hashPathKey(fullPath)
			}

			var extraFlags uint16
			if ent.flags&ufHidden != 0 || (len(ent.name) > 0 && ent.name[0] == '.') {
				extraFlags |= storage.FlagHidden
			}
			if ent.objType == vnodeLnk {
				extraFlags |= storage.FlagSymlink
			}

			if ent.objType == vnodeDir {
				dec := p.pruner.EvalTopDownDir(vol.VolID, nodeKey, ent.name, relPath, depth, parentSystem, ignoreStack)
				if !dec.Keep {
					continue
				}
				if err := writer.WriteRecord(provider.ScanRecord{
					VolID:     vol.VolID,
					NodeKey:   nodeKey,
					ParentKey: parentKey,
					Name:      ent.name,
					Flags:     dec.Flags | extraFlags,
					ModTime:   ent.modTime,
				}); err != nil {
					return err
				}
				cache.Put(fullPath, CachedNode{NodeKey: nodeKey, ParentKey: parentKey, IsDir: true})
				if dec.Descend {
					childSys := (dec.Flags & storage.FlagSystem) != 0
					if err := walk(fullPath, relPath, nodeKey, depth+1, childSys); err != nil {
						return err
					}
				}
				continue
			}

			dec := p.pruner.EvalTopDownFile(ent.name, relPath, parentSystem, ignoreStack)
			if !dec.Keep {
				continue
			}
			if err := writer.WriteRecord(provider.ScanRecord{
				VolID:     vol.VolID,
				NodeKey:   nodeKey,
				ParentKey: parentKey,
				Name:      ent.name,
				Flags:     dec.Flags | extraFlags,
				Size:      ent.size,
				ModTime:   ent.modTime,
			}); err != nil {
				return err
			}
			cache.Put(fullPath, CachedNode{NodeKey: nodeKey, ParentKey: parentKey, IsDir: false})
		}
		return nil
	}

	return walk(cleanRoot, "", rootKey, 1, false)
}
