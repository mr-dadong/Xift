//go:build darwin

package darwin

import (
	"context"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/sys/unix"

	"github.com/mr-dadong/Xift/internal/filter"
	"github.com/mr-dadong/Xift/internal/provider"
)

// Provider is the Pure Go (CGO_ENABLED=0) macOS FSProvider built on purego getattrlistbulk
// and CoreServices FSEvents with APFS Firmlink deduplication and Time Machine exclusion.
type Provider struct {
	pruner  *filter.Pruner
	opts    provider.Options
	deduper *FirmlinkDeduper

	cacheMu    sync.Mutex
	pathCaches map[uint16]*PathKeyCache
}

// New creates a macOS FSProvider bound to pruner.
func New(pruner *filter.Pruner, opts provider.Options) *Provider {
	if pruner == nil {
		pruner = filter.NewPruner(filter.DefaultRuleEngine(), nil)
	}
	return &Provider{
		pruner:     pruner,
		opts:       opts,
		deduper:    NewFirmlinkDeduper(LoadDefaultFirmlinks()),
		pathCaches: make(map[uint16]*PathKeyCache, 4),
	}
}

func (p *Provider) getOrCreatePathCache(volID uint16) *PathKeyCache {
	p.cacheMu.Lock()
	defer p.cacheMu.Unlock()
	c, ok := p.pathCaches[volID]
	if !ok {
		c = NewPathKeyCache()
		p.pathCaches[volID] = c
	}
	return c
}

// EnumerateVolumes discovers "/" and mounted local volumes under "/Volumes" (skipping Time Machine & network shares).
func (p *Provider) EnumerateVolumes(ctx context.Context) ([]provider.VolumeInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var vols []provider.VolumeInfo
	var nextID uint16 = 1

	addVol := func(root string, requireLocal bool) {
		if IsTimeMachineSnapshotPath(root) {
			return
		}
		var st unix.Stat_t
		if err := unix.Stat(root, &st); err != nil {
			return
		}
		var sfs unix.Statfs_t
		fsType := "APFS"
		readOnly := false
		if err := unix.Statfs(root, &sfs); err == nil {
			if requireLocal && (sfs.Flags&unix.MNT_LOCAL) == 0 && !p.opts.IncludeRemoteMounts {
				return
			}
			fsType = unix.ByteSliceToString(sfs.Fstypename[:])
			readOnly = (sfs.Flags & unix.MNT_RDONLY) != 0
		}

		// Detect true volume case sensitivity via pathconf(_PC_CASE_SENSITIVE = 11 on macOS XNU).
		const pcCaseSensitive = 11
		caseInsensitive := true
		if cs, err := unix.Pathconf(root, pcCaseSensitive); err == nil && cs == 1 {
			caseInsensitive = false
		}

		vols = append(vols, provider.VolumeInfo{
			VolID:           nextID,
			RootPath:        root,
			FSType:          fsType,
			DeviceID:        uint64(st.Dev),
			RootKey:         st.Ino,
			ReadOnly:        readOnly,
			CaseInsensitive: caseInsensitive,
			SupportsJournal: true,
		})
		nextID++
	}

	addVol("/", false)
	if p.opts.IncludeRemovable {
		if entries, err := os.ReadDir("/Volumes"); err == nil {
			for _, ent := range entries {
				full := filepath.Join("/Volumes", ent.Name())
				info, err := os.Lstat(full)
				if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
					continue
				}
				addVol(full, true)
			}
		}
	}
	return vols, nil
}

// FullScan scans vol read-only via purego getattrlistbulk with top-down early stop and Firmlink dedup.
func (p *Provider) FullScan(ctx context.Context, vol provider.VolumeInfo, writer provider.SegmentWriter) (uint64, error) {
	if vol.RootPath == "" || writer == nil {
		return 0, provider.ErrInvalidVolume
	}
	if !p.opts.DisableLowPriority {
		restore := provider.SetLowPriorityThread()
		defer restore()
	}

	startCursor := currentFSEventID()
	if err := p.scanTopDownBulk(ctx, vol, vol.RootPath, vol.RootKey, true, writer); err != nil {
		return 0, err
	}
	return startCursor, nil
}

// Watch subscribes to CoreServices FSEvents starting from sinceCursor with 0.15s latency.
func (p *Provider) Watch(ctx context.Context, vol provider.VolumeInfo, sinceCursor uint64, out chan<- provider.FileEvent) error {
	if vol.RootPath == "" || out == nil {
		return provider.ErrInvalidVolume
	}
	if !p.opts.DisableLowPriority {
		restore := provider.SetLowPriorityThread()
		defer restore()
	}
	return p.watchFSEvents(ctx, vol, sinceCursor, out)
}

// ScanSubtree performs a bounded strictly read-only subtree scan using getattrlistbulk.
func (p *Provider) ScanSubtree(ctx context.Context, vol provider.VolumeInfo, dirPath string, dirKey uint64, writer provider.SegmentWriter) error {
	if dirPath == "" || writer == nil {
		return provider.ErrInvalidVolume
	}
	if !p.opts.DisableLowPriority {
		restore := provider.SetLowPriorityThread()
		defer restore()
	}
	return p.scanTopDownBulk(ctx, vol, dirPath, dirKey, false, writer)
}
