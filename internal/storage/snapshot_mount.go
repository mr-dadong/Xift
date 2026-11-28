package storage

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
)

// SnapshotInfo describes one imported, read-only snapshot package.
type SnapshotInfo struct {
	ID          uint32 `json:"id"`
	VolumeID    uint16 `json:"volume_id"`
	VolumeLabel string `json:"volume_label"`
	RootPath    string `json:"root_path"`
	ExportedAt  int64  `json:"exported_at"`
	Nodes       uint32 `json:"nodes"`
	BaseNodeID  uint32 `json:"base_node_id"`
	// Package is the snapshot directory relative to the index directory.
	Package string `json:"package"`
	// Segment is the segment directory name inside Package.
	Segment string `json:"segment"`
}

type snapshotMount struct {
	info SnapshotInfo
	seg  *Segment
}

// snapshotRange maps a mounted package's node-ID window into query results.
type snapshotRange struct {
	base   uint32
	count  uint32
	label  string
	volID  uint16
	root   string
	export int64
}

type snapshotListFile struct {
	Snapshots []SnapshotInfo `json:"snapshots"`
}

func snapshotListPath(indexDir string) string {
	return filepath.Join(indexDir, "snapshots.json")
}

// ImportVolumeSnapshot validates a portable package (manifest plus every file's
// crc32c), copies it into the index, remaps its node-ID window and extension
// IDs, and mounts it read-only. Corrupt packages are rejected before mounting.
func (e *Engine) ImportVolumeSnapshot(pkgDir string) (SnapshotInfo, error) {
	pkg, err := readSnapshotPackage(pkgDir)
	if err != nil {
		return SnapshotInfo{}, err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return SnapshotInfo{}, ErrClosed
	}

	var maxID uint32
	for _, m := range e.snapshots {
		if m.info.ID > maxID {
			maxID = m.info.ID
		}
	}
	id := maxID + 1
	base := e.manifest.NextNodeID
	if base < 1 {
		base = 1
	}

	rel := filepath.ToSlash(filepath.Join("snapshots", fmt.Sprintf("snapshot_%06d", id)))
	dst := filepath.Join(e.indexDir, filepath.FromSlash(rel))
	if err := os.RemoveAll(dst); err != nil {
		return SnapshotInfo{}, err
	}
	if err := copySnapshotPackage(pkgDir, dst); err != nil {
		_ = os.RemoveAll(dst)
		return SnapshotInfo{}, err
	}
	rollback := func() { _ = os.RemoveAll(dst) }

	if err := remapSnapshotPackage(dst, pkg, base, e.extPool); err != nil {
		rollback()
		return SnapshotInfo{}, err
	}
	seg, err := OpenSegmentWithCache(filepath.Join(dst, pkg.Segment), 0, e.blockCache)
	if err != nil {
		rollback()
		return SnapshotInfo{}, err
	}
	if seg.meta.NodeCount != pkg.Nodes {
		seg.Release()
		rollback()
		return SnapshotInfo{}, fmt.Errorf("storage: snapshot segment holds %d nodes, manifest says %d", seg.meta.NodeCount, pkg.Nodes)
	}

	info := SnapshotInfo{
		ID:          id,
		VolumeID:    pkg.VolumeID,
		VolumeLabel: pkg.VolumeLabel,
		RootPath:    pkg.RootPath,
		ExportedAt:  pkg.ExportedAt,
		Nodes:       pkg.Nodes,
		BaseNodeID:  base,
		Package:     rel,
		Segment:     pkg.Segment,
	}

	next := e.manifest.Clone()
	if end := base + pkg.Nodes; end > next.NextNodeID {
		next.NextNodeID = end
	}
	if err := next.Save(filepath.Join(e.indexDir, "MANIFEST")); err != nil {
		seg.Release()
		rollback()
		return SnapshotInfo{}, fmt.Errorf("storage: persist snapshot node range: %w", err)
	}
	if err := e.extPool.Save(filepath.Join(e.indexDir, "ext.pool")); err != nil {
		seg.Release()
		rollback()
		return SnapshotInfo{}, fmt.Errorf("storage: persist extension pool: %w", err)
	}

	e.manifest = next
	e.snapshots = append(e.snapshots, &snapshotMount{info: info, seg: seg})
	e.writeGen++
	if err := e.saveSnapshotListLocked(); err != nil {
		e.snapshots = e.snapshots[:len(e.snapshots)-1]
		seg.Release()
		rollback()
		return SnapshotInfo{}, err
	}
	return info, nil
}

// Snapshots returns the mounted read-only snapshot packages.
func (e *Engine) Snapshots() []SnapshotInfo {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]SnapshotInfo, len(e.snapshots))
	for i, m := range e.snapshots {
		out[i] = m.info
	}
	return out
}

// RemoveSnapshot unmounts a package and deletes its copied directory. The
// online L1 index is never touched.
func (e *Engine) RemoveSnapshot(id uint32) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	idx := slices.IndexFunc(e.snapshots, func(m *snapshotMount) bool { return m.info.ID == id })
	if idx < 0 {
		return fmt.Errorf("storage: snapshot %d: %w", id, ErrNotFound)
	}
	mount := e.snapshots[idx]
	removed := slices.Delete(slices.Clone(e.snapshots), idx, idx+1)
	old := e.snapshots
	e.snapshots = removed
	if err := e.saveSnapshotListLocked(); err != nil {
		e.snapshots = old
		return err
	}
	mount.seg.Release()
	e.writeGen++
	dir := filepath.Join(e.indexDir, filepath.FromSlash(mount.info.Package))
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("storage: remove snapshot directory: %w", err)
	}
	return nil
}

// loadSnapshotMounts opens every registered package. A package whose files
// disappeared is skipped so a missing snapshot never prevents the index from
// opening; RemoveSnapshot still cleans up the registry entry.
func (e *Engine) loadSnapshotMounts() {
	raw, err := os.ReadFile(snapshotListPath(e.indexDir))
	if err != nil {
		return
	}
	var list snapshotListFile
	if err := json.Unmarshal(raw, &list); err != nil {
		return
	}
	for _, info := range list.Snapshots {
		dir := filepath.Join(e.indexDir, filepath.FromSlash(info.Package), info.Segment)
		seg, err := OpenSegmentWithCache(dir, 0, e.blockCache)
		if err != nil {
			continue
		}
		e.snapshots = append(e.snapshots, &snapshotMount{info: info, seg: seg})
	}
}

func (e *Engine) saveSnapshotListLocked() error {
	list := snapshotListFile{Snapshots: make([]SnapshotInfo, len(e.snapshots))}
	for i, m := range e.snapshots {
		list.Snapshots[i] = m.info
	}
	raw, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		return err
	}
	if err := atomicWriteFile(snapshotListPath(e.indexDir), raw); err != nil {
		return fmt.Errorf("storage: write snapshots.json: %w", err)
	}
	return nil
}

func (e *Engine) releaseSnapshotMountsLocked() {
	for _, m := range e.snapshots {
		m.seg.Release()
	}
	e.snapshots = nil
}

// SnapshotSource reports whether nodeID belongs to a mounted read-only package.
func (s *Snapshot) SnapshotSource(nodeID uint32) (label string, volID uint16, ok bool) {
	for i := range s.snapshots {
		r := &s.snapshots[i]
		if nodeID >= r.base && nodeID < r.base+r.count {
			return r.label, r.volID, true
		}
	}
	return "", 0, false
}

// SnapshotVolume returns the mounted package metadata for volID.
func (s *Snapshot) SnapshotVolume(volID uint16) (SnapshotInfo, bool) {
	for i := range s.snapshots {
		r := &s.snapshots[i]
		if r.volID == volID {
			return SnapshotInfo{VolumeID: volID, VolumeLabel: r.label, RootPath: r.root, ExportedAt: r.export}, true
		}
	}
	return SnapshotInfo{}, false
}

// copySnapshotPackage copies every regular package file into dst.
func copySnapshotPackage(src, dst string) error {
	err := filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		if !d.Type().IsRegular() {
			return nil
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			return err
		}
		if _, err := io.Copy(out, in); err != nil {
			_ = out.Close()
			return err
		}
		return out.Close()
	})
	if err != nil {
		return fmt.Errorf("storage: copy snapshot package: %w", err)
	}
	return nil
}

// remapSnapshotPackage moves the package node IDs to base and re-interns its
// extension IDs into pool, so a mounted package never collides with L1 IDs.
func remapSnapshotPackage(dir string, pkg SnapshotPackage, base uint32, pool *ExtPool) error {
	segDir := filepath.Join(dir, pkg.Segment)
	metaPath := filepath.Join(segDir, "meta.hdr")
	meta, err := readSegmentMeta(metaPath)
	if err != nil {
		return err
	}
	if !meta.Tier2 {
		return errors.New("storage: snapshot segment is not a Tier 2 segment")
	}
	if meta.NodeCount != pkg.Nodes {
		return fmt.Errorf("storage: snapshot segment holds %d nodes, manifest says %d", meta.NodeCount, pkg.Nodes)
	}
	if meta.Dense {
		meta.BaseNodeID = base
		if err := writeSegmentMeta(metaPath, meta); err != nil {
			return err
		}
	} else if err := shiftNodeIDs(filepath.Join(segDir, "nodeids.col"), base); err != nil {
		return err
	}

	newExt := make(map[uint16]uint16, len(pkg.Exts))
	for idStr, ext := range pkg.Exts {
		oldID, err := strconv.ParseUint(idStr, 10, 16)
		if err != nil {
			continue
		}
		newExt[uint16(oldID)] = pool.Intern(ext)
	}
	return shiftHotColumns(filepath.Join(segDir, "hot.col"), base, newExt)
}

// shiftHotColumns adds base to every parent reference (except sentinels) and
// rewrites extension IDs through ext.
func shiftHotColumns(path string, base uint32, ext map[uint16]uint16) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()

	const recSize = HotNodeSize
	buf := make([]byte, recSize*4096)
	var off int64
	for {
		n, rerr := f.ReadAt(buf, off)
		if n == 0 {
			if rerr == io.EOF {
				return nil
			}
			return rerr
		}
		if n%recSize != 0 {
			return fmt.Errorf("storage: hot.col has a partial record at %d", off)
		}
		for i := 0; i < n; i += recSize {
			if p := binary.LittleEndian.Uint32(buf[i+4 : i+8]); p != RootParentIdx && p != OrphanParentIdx {
				binary.LittleEndian.PutUint32(buf[i+4:i+8], p+base)
			}
			if len(ext) > 0 {
				if id := binary.LittleEndian.Uint16(buf[i+12 : i+14]); id != 0 {
					if next, ok := ext[id]; ok {
						binary.LittleEndian.PutUint16(buf[i+12:i+14], next)
					}
				}
			}
		}
		if _, werr := f.WriteAt(buf[:n], off); werr != nil {
			return werr
		}
		off += int64(n)
		if rerr == io.EOF {
			return nil
		}
		if rerr != nil {
			return rerr
		}
	}
}

func shiftNodeIDs(path string, base uint32) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	buf := make([]byte, 4*8192)
	var off int64
	for {
		n, rerr := f.ReadAt(buf, off)
		if n == 0 {
			if rerr == io.EOF {
				return nil
			}
			return rerr
		}
		if n%4 != 0 {
			return fmt.Errorf("storage: nodeids.col has a partial record at %d", off)
		}
		for i := 0; i < n; i += 4 {
			binary.LittleEndian.PutUint32(buf[i:i+4], binary.LittleEndian.Uint32(buf[i:i+4])+base)
		}
		if _, werr := f.WriteAt(buf[:n], off); werr != nil {
			return werr
		}
		off += int64(n)
		if rerr == io.EOF {
			return nil
		}
		if rerr != nil {
			return rerr
		}
	}
}
