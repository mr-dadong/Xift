package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestEngineDelayedUnmapAcrossCompactAndConcurrency(t *testing.T) {
	dataDir := t.TempDir()
	eng, err := Open(dataDir, Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer eng.Close()

	// Seed base segment.
	b, err := eng.BeginDenseSegment()
	if err != nil {
		t.Fatalf("BeginDenseSegment: %v", err)
	}
	rootID, _ := b.AppendDense(1, 1, RootParentIdx, "D:", FlagDir, 0, 0, 100)
	dirID, _ := b.AppendDense(1, 2, rootID, "work", FlagDir, 0, 0, 101)
	for i := 0; i < 500; i++ {
		if _, err := b.AppendDense(1, uint64(100+i), dirID, fmt.Sprintf("doc_%04d.md", i), 0, 0, int64(i+1), int64(1700000000+i)); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	segMeta, err := b.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if err := eng.CommitSegment(segMeta, &VolumeCursor{VolID: 1, Cursor: 1, RootPath: `D:\`}); err != nil {
		t.Fatalf("CommitSegment: %v", err)
	}

	// 1. Verify delayed Unmap & directory cleanup when a Snapshot spans Compact().
	oldSegDir := filepath.Join(dataDir, "index", "segments", SegmentDirName(segMeta.ID))
	pinnedSnap := eng.AcquireSnapshot()

	if err := eng.ApplyBatch([]Mutation{
		{Op: OpDelete, VolID: 1, NodeKey: 100},
		{Op: OpCreate, VolID: 1, NodeKey: 9000, ParentKey: 2, Name: "pinned_test.go", Size: 512, ModTime: 1700009000},
	}); err != nil {
		t.Fatalf("ApplyBatch before compact: %v", err)
	}
	if err := eng.Compact(); err != nil {
		t.Fatalf("Compact while snapshot pinned: %v", err)
	}

	// Old segment MUST remain mapped and readable while pinnedSnap is held!
	if _, err := os.Stat(oldSegDir); err != nil {
		t.Fatalf("old segment dir removed prematurely while snapshot still pinned: %v", err)
	}
	visited := 0
	pinnedSnap.ForEachLive(func(nodeID uint32, node HotNode, name []byte) bool {
		if len(name) == 0 {
			t.Errorf("empty name on pinned snapshot node %d", nodeID)
		}
		visited++
		return true
	})
	if visited == 0 {
		t.Fatalf("pinned snapshot read 0 nodes")
	}
	if cm, err := pinnedSnap.ReadColdMeta(3); err != nil || cm.Size != 2 {
		t.Fatalf("pinned snapshot ReadColdMeta(3) = (%+v, %v)", cm, err)
	}

	// Releasing pinnedSnap MUST unmap and remove oldSegDir immediately (Windows-safe).
	pinnedSnap.Release()
	if _, err := os.Stat(oldSegDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old segment dir should be removed after pinnedSnap.Release(), stat err=%v", err)
	}

	// 2. Mixed concurrent readers + writer (ApplyBatch + Flush + Compact) stress test.
	var stop atomic.Bool
	var wg sync.WaitGroup
	var errCount atomic.Int32

	// 4 concurrent readers.
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var coldBuf [32]ColdMeta
			var idBuf [32]uint32
			for !stop.Load() {
				snap := eng.AcquireSnapshot()
				n := 0
				snap.ForEachLive(func(nodeID uint32, node HotNode, name []byte) bool {
					if len(name) == 0 {
						errCount.Add(1)
						return false
					}
					if n < len(idBuf) {
						idBuf[n] = nodeID
						n++
					}
					return true
				})
				if n > 0 {
					if err := snap.ReadColdMetaBatch(idBuf[:n], coldBuf[:n]); err != nil {
						errCount.Add(1)
					}
					if _, err := snap.ResolvePath(idBuf[n-1]); err != nil {
						errCount.Add(1)
					}
				}
				snap.Release()
			}
		}()
	}

	// 1 writer performing rapid mutations, flushes, and compactions.
	for step := 0; step < 120; step++ {
		k := uint64(20000 + step)
		batch := []Mutation{
			{Op: OpCreate, VolID: 1, NodeKey: k, ParentKey: 2, Name: fmt.Sprintf("live_%d.go", step), Size: int64(step * 100), ModTime: int64(1700010000 + step)},
			{Op: OpModifyMeta, VolID: 1, NodeKey: k, Size: int64(step*100 + 50), ModTime: int64(1700020000 + step)},
		}
		if step > 5 {
			batch = append(batch, Mutation{Op: OpDelete, VolID: 1, NodeKey: uint64(20000 + step - 5)})
		}
		if err := eng.ApplyBatch(batch); err != nil {
			t.Fatalf("concurrent ApplyBatch step %d: %v", step, err)
		}
		if step%30 == 15 {
			if err := eng.Flush(); err != nil {
				t.Fatalf("concurrent Flush step %d: %v", step, err)
			}
		}
		if step%60 == 59 {
			if err := eng.Compact(); err != nil {
				t.Fatalf("concurrent Compact step %d: %v", step, err)
			}
		}
	}

	stop.Store(true)
	wg.Wait()

	if errCount.Load() != 0 {
		t.Fatalf("concurrent readers encountered %d errors", errCount.Load())
	}
}

func TestColdStart1MNodesSLA(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 1M cold-start SLA test in -short mode")
	}

	const (
		totalNodes = 1_000_000
		dirCount   = 100_000
	)
	dataDir := t.TempDir()

	buildStart := time.Now()
	eng, err := Open(dataDir, Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("Open initial: %v", err)
	}
	b, err := eng.BeginDenseSegment()
	if err != nil {
		t.Fatalf("BeginDenseSegment: %v", err)
	}

	// Write root + 99,999 directories + 900,000 files.
	if _, err := b.AppendDense(1, 1, RootParentIdx, "C:", FlagDir, 0, 0, 1700000000); err != nil {
		t.Fatalf("append root: %v", err)
	}
	for d := uint32(1); d < dirCount; d++ {
		parent := (d - 1) / 8
		name := fmt.Sprintf("dir_%06d", d)
		if _, err := b.AppendDense(1, uint64(d+1), parent, name, FlagDir, 0, 0, 1700000000+int64(d)); err != nil {
			t.Fatalf("append dir %d: %v", d, err)
		}
	}

	exts := []string{"go", "md", "pdf", "json", "txt", "rs", "ts", "png"}
	extIDs := make([]uint16, len(exts))
	for i, ext := range exts {
		extIDs[i] = eng.ExtPool().Intern(ext)
	}
	for f := uint32(dirCount); f < totalNodes; f++ {
		parent := f % dirCount
		extIdx := f & 7
		name := fmt.Sprintf("report_%07d.%s", f, exts[extIdx])
		if _, err := b.AppendDense(1, uint64(f+1), parent, name, 0, extIDs[extIdx], int64(f)*16, 1700000000+int64(f)); err != nil {
			t.Fatalf("append file %d: %v", f, err)
		}
	}

	meta, err := b.Finish()
	if err != nil {
		t.Fatalf("Finish 1M segment: %v", err)
	}
	if err := eng.CommitSegment(meta, &VolumeCursor{VolID: 1, Cursor: 424242, RootPath: `C:\`}); err != nil {
		t.Fatalf("CommitSegment 1M: %v", err)
	}
	if err := eng.Close(); err != nil {
		t.Fatalf("Close after build: %v", err)
	}
	buildDur := time.Since(buildStart)

	runtime.GC()
	var mBefore runtime.MemStats
	runtime.ReadMemStats(&mBefore)

	// Cold start open of 1,000,000-node Tier 1 columnar index!
	reopened, err := Open(dataDir, Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("Cold start Open 1M: %v", err)
	}
	defer reopened.Close()

	openDur := reopened.OpenStats().OpenDuration
	runtime.GC()
	var mAfter runtime.MemStats
	runtime.ReadMemStats(&mAfter)

	if got := reopened.LiveCount(); got != totalNodes {
		t.Fatalf("reopened LiveCount = %d, want %d", got, totalNodes)
	}

	// Verify viewport batch cold-column ReadAt lookup (60 rows).
	viewportIDs := make([]uint32, 60)
	viewportMeta := make([]ColdMeta, 60)
	for i := 0; i < 60; i++ {
		viewportIDs[i] = uint32(dirCount + i*10000)
	}
	coldStart := time.Now()
	if err := reopened.ReadColdMetaBatch(viewportIDs, viewportMeta); err != nil {
		t.Fatalf("ReadColdMetaBatch 60 rows: %v", err)
	}
	coldDur := time.Since(coldStart)
	for i, id := range viewportIDs {
		if viewportMeta[i].Size != int64(id)*16 {
			t.Fatalf("viewport[%d] (id=%d) size=%d, want %d", i, id, viewportMeta[i].Size, int64(id)*16)
		}
	}

	samplePath, err := reopened.ResolvePath(totalNodes - 1)
	if err != nil || samplePath == "" {
		t.Fatalf("ResolvePath(last) = (%q, %v)", samplePath, err)
	}

	var heapDeltaMB float64
	if mAfter.HeapAlloc > mBefore.HeapAlloc {
		heapDeltaMB = float64(mAfter.HeapAlloc-mBefore.HeapAlloc) / (1024 * 1024)
	}
	t.Logf("1M Tier1 SLA: build=%v | cold_open=%v (SLA <500ms) | viewport_60_cold_read=%v | heap_delta=%.2f MB | hot.col=%.2f MB | names.col=%.2f MB | sample_path=%s",
		buildDur.Round(time.Millisecond),
		openDur.Round(time.Microsecond),
		coldDur.Round(time.Microsecond),
		heapDeltaMB,
		float64(meta.NodeCount*HotNodeSize)/(1024*1024),
		float64(meta.NamesBytes)/(1024*1024),
		samplePath,
	)

	if openDur >= 500*time.Millisecond {
		t.Fatalf("cold start 1M open took %v, exceeds < 500ms SLA", openDur)
	}
	// Go heap delta for DirByKey (100k dirs * 16B ~ 2-4 MB) + del.bit (125 KB) must stay tiny (well under 16 MB, proving hot.col/names.col are NOT on Go heap).
	if heapDeltaMB > 16.0 {
		t.Fatalf("cold start Go heap delta %.2f MB exceeds 16 MB zero-GC budget", heapDeltaMB)
	}
}

func BenchmarkColdStart1M(b *testing.B) {
	const (
		totalNodes = 1_000_000
		dirCount   = 100_000
	)
	dataDir := b.TempDir()
	eng, err := Open(dataDir, Options{DisableWALSync: true})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	builder, err := eng.BeginDenseSegment()
	if err != nil {
		b.Fatalf("BeginDenseSegment: %v", err)
	}
	_, _ = builder.AppendDense(1, 1, RootParentIdx, "C:", FlagDir, 0, 0, 1700000000)
	for d := uint32(1); d < dirCount; d++ {
		_, _ = builder.AppendDense(1, uint64(d+1), (d-1)/8, fmt.Sprintf("dir_%06d", d), FlagDir, 0, 0, 1700000000)
	}
	extGo := eng.ExtPool().Intern("go")
	for f := uint32(dirCount); f < totalNodes; f++ {
		_, _ = builder.AppendDense(1, uint64(f+1), f%dirCount, fmt.Sprintf("file_%07d.go", f), 0, extGo, int64(f), 1700000000)
	}
	meta, err := builder.Finish()
	if err != nil {
		b.Fatalf("Finish: %v", err)
	}
	if err := eng.CommitSegment(meta, nil); err != nil {
		b.Fatalf("CommitSegment: %v", err)
	}
	_ = eng.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e, err := Open(dataDir, Options{DisableWALSync: true})
		if err != nil {
			b.Fatalf("Open: %v", err)
		}
		_ = e.Close()
	}
}

