package daemon

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/mr-dadong/Xift/internal/l2search"
	"github.com/mr-dadong/Xift/internal/provider"
	"github.com/mr-dadong/Xift/internal/storage"
)

// subtreeProvider is the narrow provider surface the L2 scanner needs. The
// platform FSProvider satisfies it; tests can supply a small fake.
type subtreeProvider interface {
	ScanSubtreeWithRoot(ctx context.Context, vol provider.VolumeInfo, dirPath string, writer provider.SegmentWriter) error
}

// BuildL2Config enumerates volumes from prov and assembles the content-index
// configuration used by the daemon. It performs no user-file reads; indexing
// starts only after the pipeline opens and a whitelisted root is scheduled.
func BuildL2Config(ctx context.Context, prov provider.FSProvider) (*L2Config, error) {
	if prov == nil {
		return nil, fmt.Errorf("daemon: L2 wiring requires a provider: %w", provider.ErrUnsupportedOS)
	}
	vols, err := prov.EnumerateVolumes(ctx)
	if err != nil {
		return nil, fmt.Errorf("daemon: enumerate volumes for L2: %w", err)
	}
	volRoots := make(map[uint16]string, len(vols))
	for _, vol := range vols {
		volRoots[vol.VolID] = vol.RootPath
	}
	return &L2Config{
		VolRoots: volRoots,
		Scanner:  providerScanner{prov: prov, vols: vols},
	}, nil
}

// providerScanner adapts an FSProvider subtree scan to the L2 workspace
// scanner, resolving each whitelisted root to its owning volume.
type providerScanner struct {
	prov subtreeProvider
	vols []provider.VolumeInfo
}

// ScanSubtreeWithRoot streams one whitelisted subtree in L1 key space.
func (s providerScanner) ScanSubtreeWithRoot(ctx context.Context, root string, fn func(l2search.ScanRecord) error) error {
	abs, err := filepath.Abs(root)
	if err != nil {
		return fmt.Errorf("daemon: resolve L2 root %q: %w", root, err)
	}
	vol, ok := volumeForPath(s.vols, abs)
	if !ok {
		return fmt.Errorf("daemon: L2 root %q is not under any enumerated volume", abs)
	}
	return s.prov.ScanSubtreeWithRoot(ctx, vol, abs, provider.SegmentWriterFunc(func(rec provider.ScanRecord) error {
		volID := rec.VolID
		if volID == 0 {
			volID = vol.VolID
		}
		return fn(l2search.ScanRecord{
			VolID:     volID,
			NodeKey:   rec.NodeKey,
			ParentKey: rec.ParentKey,
			Name:      rec.Name,
			IsDir:     rec.Flags&storage.FlagDir != 0,
			ModTime:   rec.ModTime,
		})
	}))
}

// volumeForPath resolves abs to the enumerated volume whose root contains it,
// preferring the longest root so nested mount points win over their parents.
// Path matching is component-boundary aware: a sibling whose name merely shares
// a prefix with a root does not match.
func volumeForPath(vols []provider.VolumeInfo, abs string) (provider.VolumeInfo, bool) {
	if !filepath.IsAbs(abs) {
		return provider.VolumeInfo{}, false
	}
	target := filepath.Clean(abs)
	best := provider.VolumeInfo{}
	bestLen := -1
	for _, vol := range vols {
		root := normalizedVolumeRoot(vol.RootPath)
		if root == "" || !filepath.IsAbs(root) {
			continue
		}
		rel, err := filepath.Rel(root, target)
		if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			continue
		}
		if len(root) > bestLen {
			best, bestLen = vol, len(root)
		}
	}
	return best, bestLen >= 0
}

// normalizedVolumeRoot cleans a volume root and restores the separator Windows
// drive roots lose during enumeration (C: -> C:\).
func normalizedVolumeRoot(root string) string {
	clean := filepath.Clean(root)
	if filepath.IsAbs(clean) {
		return clean
	}
	clean = filepath.Clean(root + string(filepath.Separator))
	if filepath.IsAbs(clean) {
		return clean
	}
	return ""
}
