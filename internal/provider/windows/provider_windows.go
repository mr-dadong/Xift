//go:build windows

package windows

import (
	"context"
	"errors"
	"strings"
	"sync"
	"unicode/utf16"

	"golang.org/x/sys/windows"

	"github.com/mr-dadong/Xift/internal/filter"
	"github.com/mr-dadong/Xift/internal/provider"
)

// Provider is the Pure Go (CGO_ENABLED=0) Windows FSProvider supporting NTFS MFT+USN Journal
// with automatic strictly read-only fallback for non-NTFS volumes and unprivileged directories.
type Provider struct {
	pruner *filter.Pruner
	opts   provider.Options

	cacheMu      sync.Mutex
	pathCaches   map[uint16]*pathKeyCache
	onWatchArmed func() // optional internal test hook invoked once OS watch IRP is armed
}

// New creates a Windows FSProvider bound to pruner (allocating a default Pruner if nil).
func New(pruner *filter.Pruner, opts provider.Options) *Provider {
	if pruner == nil {
		pruner = filter.NewPruner(filter.DefaultRuleEngine(), nil)
	}
	return &Provider{
		pruner:     pruner,
		opts:       opts,
		pathCaches: make(map[uint16]*pathKeyCache, 4),
	}
}

func (p *Provider) getOrCreatePathCache(volID uint16) *pathKeyCache {
	p.cacheMu.Lock()
	defer p.cacheMu.Unlock()
	c, ok := p.pathCaches[volID]
	if !ok {
		c = newPathKeyCache()
		p.pathCaches[volID] = c
	}
	return c
}

// EnumerateVolumes discovers mounted local Windows drives (fixed, and optionally removable),
// identifying NTFS vs non-NTFS volumes strictly read-only.
func (p *Provider) EnumerateVolumes(ctx context.Context) ([]provider.VolumeInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	buf := make([]uint16, 256)
	n, err := windows.GetLogicalDriveStrings(uint32(len(buf)), &buf[0])
	if err != nil {
		return nil, err
	}
	if int(n) > len(buf) {
		buf = make([]uint16, n+1)
		n, err = windows.GetLogicalDriveStrings(uint32(len(buf)), &buf[0])
		if err != nil {
			return nil, err
		}
	}

	var vols []provider.VolumeInfo
	var nextVolID uint16 = 1

	start := 0
	for i := 0; i < int(n); i++ {
		if buf[i] != 0 {
			continue
		}
		if i > start {
			rootStr := string(utf16.Decode(buf[start:i]))
			if vol, ok := p.inspectDrive(rootStr, nextVolID); ok {
				vols = append(vols, vol)
				nextVolID++
			}
		}
		start = i + 1
	}
	return vols, nil
}

func (p *Provider) inspectDrive(rootStr string, volID uint16) (provider.VolumeInfo, bool) {
	rootPtr, err := windows.UTF16PtrFromString(rootStr)
	if err != nil {
		return provider.VolumeInfo{}, false
	}

	dtype := windows.GetDriveType(rootPtr)
	switch dtype {
	case windows.DRIVE_FIXED:
		// Always indexable.
	case windows.DRIVE_REMOVABLE:
		if !p.opts.IncludeRemovable {
			return provider.VolumeInfo{}, false
		}
	default:
		// Skip CD-ROM, RAMDISK, REMOTE, UNKNOWN.
		return provider.VolumeInfo{}, false
	}

	var serial uint32
	var maxCompLen uint32
	var fsFlags uint32
	fsNameBuf := make([]uint16, 64)
	err = windows.GetVolumeInformation(
		rootPtr,
		nil,
		0,
		&serial,
		&maxCompLen,
		&fsFlags,
		&fsNameBuf[0],
		uint32(len(fsNameBuf)),
	)
	if err != nil {
		return provider.VolumeInfo{}, false
	}

	fsType := windows.UTF16ToString(fsNameBuf)
	isNTFS := strings.EqualFold(fsType, "NTFS")
	rootKey := uint64(0)
	if k, _, _, _, ok := getFileKeyAndMetaReadOnly(rootStr); ok && k != 0 {
		rootKey = k
	} else if isNTFS {
		rootKey = defaultNTFSRootFRN
	}

	return provider.VolumeInfo{
		VolID:           volID,
		RootPath:        strings.TrimSuffix(rootStr, `\`),
		FSType:          fsType,
		DeviceID:        uint64(serial),
		RootKey:         rootKey,
		ReadOnly:        (fsFlags & windows.FILE_READ_ONLY_VOLUME) != 0,
		CaseInsensitive: true,
		SupportsJournal: isNTFS,
	}, true
}

// FullScan performs a read-only full scan of vol.
// For NTFS drive roots, it attempts two-pass MFT enumeration first and falls back smoothly to
// read-only top-down directory scanning when running without Administrator privileges.
func (p *Provider) FullScan(ctx context.Context, vol provider.VolumeInfo, writer provider.SegmentWriter) (uint64, error) {
	if vol.RootPath == "" || writer == nil {
		return 0, provider.ErrInvalidVolume
	}
	if !p.opts.DisableLowPriority {
		restore := provider.SetLowPriorityThread()
		defer restore()
	}

	if letter, isDrive := extractDriveLetterRoot(vol.RootPath); isDrive && strings.EqualFold(vol.FSType, "NTFS") {
		cursor, err := p.scanNTFSMFT(ctx, vol, letter, writer)
		if err == nil {
			return cursor, nil
		}
		if !errors.Is(err, provider.ErrPermissionDenied) {
			return 0, err
		}
		// Unprivileged process: fall back smoothly to read-only top-down traversal.
	}

	cache := p.getOrCreatePathCache(vol.VolID)
	scanPath := vol.RootPath
	if len(scanPath) == 2 && scanPath[1] == ':' {
		scanPath += `\`
	}
	if err := p.scanDirectoryTopDown(ctx, vol, scanPath, vol.RootKey, true, writer, cache); err != nil {
		return 0, err
	}
	return 0, nil
}

// Watch monitors vol for real-time filesystem changes.
// Uses NTFS USN Journal when privileged on an NTFS drive root, otherwise falls back smoothly
// to strictly read-only ReadDirectoryChangesW.
func (p *Provider) Watch(ctx context.Context, vol provider.VolumeInfo, sinceCursor uint64, out chan<- provider.FileEvent) error {
	if vol.RootPath == "" || out == nil {
		return provider.ErrInvalidVolume
	}
	if !p.opts.DisableLowPriority {
		restore := provider.SetLowPriorityThread()
		defer restore()
	}

	if letter, isDrive := extractDriveLetterRoot(vol.RootPath); isDrive && strings.EqualFold(vol.FSType, "NTFS") {
		err := p.watchUSNJournal(ctx, vol, letter, sinceCursor, out)
		if err == nil || !errors.Is(err, provider.ErrPermissionDenied) {
			return err
		}
		// Unprivileged process: fall back smoothly to ReadDirectoryChangesW.
	}

	watchVol := vol
	if len(watchVol.RootPath) == 2 && watchVol.RootPath[1] == ':' {
		watchVol.RootPath += `\`
	}
	return p.watchDirectoryChanges(ctx, watchVol, sinceCursor, out)
}

// ScanSubtree performs a bounded strictly read-only subtree scan for EventRescanDir recovery.
func (p *Provider) ScanSubtree(ctx context.Context, vol provider.VolumeInfo, dirPath string, dirKey uint64, writer provider.SegmentWriter) error {
	if dirPath == "" || writer == nil {
		return provider.ErrInvalidVolume
	}
	if !p.opts.DisableLowPriority {
		restore := provider.SetLowPriorityThread()
		defer restore()
	}
	cache := p.getOrCreatePathCache(vol.VolID)
	return p.scanDirectoryTopDown(ctx, vol, dirPath, dirKey, false, writer, cache)
}
