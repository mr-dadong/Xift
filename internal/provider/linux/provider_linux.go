//go:build linux

package linux

import (
	"context"
	"strings"

	"golang.org/x/sys/unix"

	"github.com/mr-dadong/Xift/internal/filter"
	"github.com/mr-dadong/Xift/internal/provider"
)

// Provider is the Pure Go (CGO_ENABLED=0) Linux FSProvider built on multi-threaded
// Getdents64+Statx, /proc/self/mountinfo filtering, and fanotify with seamless inotify fallback.
type Provider struct {
	pruner       *filter.Pruner
	opts         provider.Options
	onWatchArmed func() // optional internal test hook invoked once inotify/fanotify is armed
}

// New creates a Linux FSProvider bound to pruner.
func New(pruner *filter.Pruner, opts provider.Options) *Provider {
	if pruner == nil {
		pruner = filter.NewPruner(filter.DefaultRuleEngine(), nil)
	}
	return &Provider{
		pruner: pruner,
		opts:   opts,
	}
}

// EnumerateVolumes parses /proc/self/mountinfo and returns locally indexable filesystems,
// skipping virtual pseudo-filesystems and (by default) remote/FUSE mounts.
func (p *Provider) EnumerateVolumes(ctx context.Context) ([]provider.VolumeInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries, err := ReadSelfMountInfo()
	if err != nil {
		return nil, err
	}
	filtered := FilterIndexableMounts(entries, p.opts.IncludeRemoteMounts)

	vols := make([]provider.VolumeInfo, 0, len(filtered))
	var nextID uint16 = 1
	for _, m := range filtered {
		var st unix.Stat_t
		if err := unix.Stat(m.MountPoint, &st); err != nil {
			continue
		}
		ro := false
		for _, opt := range strings.Split(m.Options, ",") {
			if opt == "ro" {
				ro = true
				break
			}
		}
		vols = append(vols, provider.VolumeInfo{
			VolID:           nextID,
			RootPath:        m.MountPoint,
			FSType:          m.FSType,
			DeviceID:        uint64(st.Dev),
			RootKey:         st.Ino,
			ReadOnly:        ro,
			CaseInsensitive: false,
			SupportsJournal: false,
		})
		nextID++
	}
	return vols, nil
}

// FullScan performs multi-threaded read-only Getdents64+Statx scanning with Pruner early stop.
func (p *Provider) FullScan(ctx context.Context, vol provider.VolumeInfo, writer provider.SegmentWriter) (uint64, error) {
	if vol.RootPath == "" || writer == nil {
		return 0, provider.ErrInvalidVolume
	}
	if err := p.scanTopDownLinux(ctx, vol, vol.RootPath, vol.RootKey, true, writer); err != nil {
		return 0, err
	}
	return 0, nil
}

// Watch monitors vol via fanotify, falling back seamlessly to inotify without CAP_SYS_ADMIN.
func (p *Provider) Watch(ctx context.Context, vol provider.VolumeInfo, sinceCursor uint64, out chan<- provider.FileEvent) error {
	if vol.RootPath == "" || out == nil {
		return provider.ErrInvalidVolume
	}
	if !p.opts.DisableLowPriority {
		restore := provider.SetLowPriorityThread()
		defer restore()
	}
	return p.watchLinux(ctx, vol, sinceCursor, out)
}

// ScanSubtree performs a bounded strictly read-only subtree scan for EventRescanDir recovery.
func (p *Provider) ScanSubtree(ctx context.Context, vol provider.VolumeInfo, dirPath string, dirKey uint64, writer provider.SegmentWriter) error {
	if dirPath == "" || writer == nil {
		return provider.ErrInvalidVolume
	}
	return p.scanTopDownLinux(ctx, vol, dirPath, dirKey, false, writer)
}
