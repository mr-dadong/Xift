package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"time"
)

const (
	// SnapshotPackageVersion is the on-disk snapshot package format version.
	SnapshotPackageVersion = 1
	// SnapshotManifestName is the package manifest file name.
	SnapshotManifestName = "snapshot.json"
	// snapshotSegDirName is the single clean segment directory inside a package.
	snapshotSegDirName = "seg_000001"
	// snapshotSegID is the segment ID carried by exported packages; import
	// assigns its own node-ID base so package node IDs never collide with L1.
	snapshotSegID uint32 = 1
)

// SnapshotFile records one package file and its integrity checksum.
type SnapshotFile struct {
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	CRC32C uint32 `json:"crc32c"`
}

// SnapshotPackage is the portable manifest written next to an exported segment.
type SnapshotPackage struct {
	Version     int            `json:"version"`
	VolumeID    uint16         `json:"volume_id"`
	VolumeLabel string         `json:"volume_label"`
	RootPath    string         `json:"root_path"`
	ExportedAt  int64          `json:"exported_at"`
	Nodes       uint32         `json:"nodes"`
	Segment     string         `json:"segment"`
	Files       []SnapshotFile `json:"files"`
	// Exts maps the exporting engine's extension IDs to extension strings so
	// the importing engine can remap them into its own pool.
	Exts map[string]string `json:"exts,omitempty"`
}

// ExportOptions labels the exported volume snapshot.
type ExportOptions struct {
	VolumeLabel string
	RootPath    string
}

// ExportVolumeSnapshot streams every live node of volID into a clean, portable
// Tier 2 segment package under outDir and writes snapshot.json with per-file
// crc32c checksums. The source index is only read.
func (e *Engine) ExportVolumeSnapshot(volID uint16, outDir string, opts ExportOptions) (SnapshotPackage, error) {
	if outDir == "" {
		return SnapshotPackage{}, errors.New("storage: snapshot export requires an output directory")
	}
	if _, err := os.Stat(filepath.Join(outDir, SnapshotManifestName)); err == nil {
		return SnapshotPackage{}, fmt.Errorf("storage: %s already contains a snapshot package", outDir)
	}
	segDir := filepath.Join(outDir, snapshotSegDirName)
	if _, err := os.Stat(segDir); err == nil {
		return SnapshotPackage{}, fmt.Errorf("storage: %s already exists", segDir)
	}

	snap := e.AcquireSnapshot()
	defer snap.Release()
	if snap.closed {
		return SnapshotPackage{}, ErrClosed
	}

	ids := make([]uint32, 0, 1024)
	snap.ForEachLive(func(id uint32, node HotNode, name []byte) bool {
		if node.VolID != volID || node.Flags&FlagDeleted != 0 || len(name) == 0 {
			return true
		}
		if _, _, ok := snap.SnapshotSource(id); ok {
			return true // already part of a mounted package
		}
		ids = append(ids, id)
		return true
	})
	slices.Sort(ids)
	ids = slices.Compact(ids)
	if len(ids) == 0 {
		return SnapshotPackage{}, fmt.Errorf("storage: volume %d has no live nodes to export", volID)
	}

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return SnapshotPackage{}, fmt.Errorf("storage: create snapshot dir: %w", err)
	}
	b, err := NewTier2SegmentBuilder(segDir, snapshotSegID, 0, true, e.extPool)
	if err != nil {
		return SnapshotPackage{}, err
	}
	finished := false
	defer func() {
		if !finished {
			b.Abort()
		}
	}()

	exts := make(map[string]string)
	cold := make([]ColdMeta, 0, 4096)
	const coldChunk = 4096
	for start := 0; start < len(ids); start += coldChunk {
		end := start + coldChunk
		if end > len(ids) {
			end = len(ids)
		}
		batch := ids[start:end]
		if cap(cold) < len(batch) {
			cold = make([]ColdMeta, len(batch))
		}
		cold = cold[:len(batch)]
		if err := snap.ReadColdMetaBatch(batch, cold); err != nil {
			return SnapshotPackage{}, fmt.Errorf("storage: read cold meta: %w", err)
		}
		for j, id := range batch {
			node, name, ok := snap.LookupNode(id)
			if !ok {
				return SnapshotPackage{}, fmt.Errorf("storage: live node %d vanished during export", id)
			}
			parent := RootParentIdx
			if node.ParentIdx != RootParentIdx && node.ParentIdx != OrphanParentIdx {
				if pos := sort.Search(len(ids), func(k int) bool { return ids[k] >= node.ParentIdx }); pos < len(ids) && ids[pos] == node.ParentIdx {
					parent = uint32(pos)
				}
			}
			flags := node.Flags &^ FlagDeleted
			if _, err := b.AppendDense(volID, uint64(id), parent, string(name), flags, node.ExtID, cold[j].Size, cold[j].ModTime); err != nil {
				return SnapshotPackage{}, err
			}
			if node.ExtID != 0 {
				if _, seen := exts[fmt.Sprint(node.ExtID)]; !seen {
					exts[fmt.Sprint(node.ExtID)] = e.extPool.Ext(node.ExtID)
				}
			}
		}
	}

	meta, err := b.Finish()
	if err != nil {
		return SnapshotPackage{}, err
	}
	finished = true

	rootPath := opts.RootPath
	if rootPath == "" {
		e.mu.RLock()
		for _, v := range e.manifest.Volumes {
			if v.VolID == volID {
				rootPath = v.RootPath
				break
			}
		}
		e.mu.RUnlock()
	}

	pkg := SnapshotPackage{
		Version:     SnapshotPackageVersion,
		VolumeID:    volID,
		VolumeLabel: opts.VolumeLabel,
		RootPath:    rootPath,
		ExportedAt:  time.Now().UnixNano(),
		Nodes:       meta.NodeCount,
		Segment:     snapshotSegDirName,
		Exts:        exts,
	}
	files, err := collectPackageFiles(outDir)
	if err != nil {
		return SnapshotPackage{}, err
	}
	pkg.Files = files

	raw, err := json.MarshalIndent(pkg, "", "  ")
	if err != nil {
		return SnapshotPackage{}, err
	}
	if err := atomicWriteFile(filepath.Join(outDir, SnapshotManifestName), raw); err != nil {
		return SnapshotPackage{}, fmt.Errorf("storage: write snapshot.json: %w", err)
	}
	return pkg, nil
}

// collectPackageFiles lists every regular file under dir with its crc32c.
func collectPackageFiles(dir string) ([]SnapshotFile, error) {
	var files []SnapshotFile
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !d.Type().IsRegular() {
			return nil
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		sum, err := crc32cFile(path)
		if err != nil {
			return err
		}
		files = append(files, SnapshotFile{
			Name:   filepath.ToSlash(rel),
			Size:   info.Size(),
			CRC32C: sum,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Name < files[j].Name })
	return files, nil
}

// readSnapshotPackage loads and validates snapshot.json plus every listed file.
func readSnapshotPackage(dir string) (SnapshotPackage, error) {
	raw, err := os.ReadFile(filepath.Join(dir, SnapshotManifestName))
	if err != nil {
		return SnapshotPackage{}, fmt.Errorf("storage: read snapshot package: %w", err)
	}
	var pkg SnapshotPackage
	if err := json.Unmarshal(raw, &pkg); err != nil {
		return SnapshotPackage{}, fmt.Errorf("storage: parse snapshot package: %w", err)
	}
	if pkg.Version != SnapshotPackageVersion {
		return SnapshotPackage{}, fmt.Errorf("storage: unsupported snapshot package version %d", pkg.Version)
	}
	if pkg.Nodes == 0 {
		return SnapshotPackage{}, errors.New("storage: snapshot package contains no nodes")
	}
	if pkg.Segment == "" || pkg.Segment != filepath.Base(pkg.Segment) {
		return SnapshotPackage{}, fmt.Errorf("storage: invalid snapshot segment name %q", pkg.Segment)
	}
	if len(pkg.Files) == 0 {
		return SnapshotPackage{}, errors.New("storage: snapshot package has no file manifest")
	}
	for _, f := range pkg.Files {
		rel := filepath.FromSlash(f.Name)
		if rel == "" || filepath.IsAbs(rel) || rel == ".." || len(rel) >= 3 && rel[:3] == ".."+string(filepath.Separator) {
			return SnapshotPackage{}, fmt.Errorf("storage: snapshot file %q escapes the package", f.Name)
		}
		path := filepath.Join(dir, rel)
		info, err := os.Stat(path)
		if err != nil {
			return SnapshotPackage{}, fmt.Errorf("storage: snapshot file %q: %w", f.Name, err)
		}
		if !info.Mode().IsRegular() || info.Size() != f.Size {
			return SnapshotPackage{}, fmt.Errorf("storage: snapshot file %q size mismatch", f.Name)
		}
		sum, err := crc32cFile(path)
		if err != nil {
			return SnapshotPackage{}, fmt.Errorf("storage: snapshot file %q: %w", f.Name, err)
		}
		if sum != f.CRC32C {
			return SnapshotPackage{}, fmt.Errorf("storage: snapshot file %q failed crc32c validation", f.Name)
		}
	}
	return pkg, nil
}

func crc32cFile(path string) (uint32, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	h := crc32.New(crc32cTable)
	if _, err := io.Copy(h, f); err != nil {
		return 0, err
	}
	return h.Sum32(), nil
}
