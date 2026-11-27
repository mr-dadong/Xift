package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"runtime"
	"time"

	"github.com/mr-dadong/Xift/internal/provider/mftspike"
)

func main() {
	drive := flag.String("drive", "C", "Windows NTFS drive letter to scan read-only (e.g. C)")
	synthetic := flag.Bool("synthetic", false, "Run offline synthetic 3.38M record benchmark without admin rights")
	syntheticCount := flag.Int("count", 3_382_748, "Record count when -synthetic is enabled")
	outDir := flag.String("out", "", "Directory for hot.col and names.col (defaults to temporary directory)")
	keepOut := flag.Bool("keep", false, "Keep generated hot.col and names.col files after benchmark")
	query := flag.String("query", "kernel32", "Sample substring query to benchmark over mmap hot columns")
	flag.Parse()

	if err := run(*drive, *synthetic, *syntheticCount, *outDir, *keepOut, *query); err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] %v\n", err)
		os.Exit(1)
	}
}

func run(drive string, synthetic bool, syntheticCount int, outDir string, keepOut bool, query string) error {
	var err error
	tempCreated := false
	if outDir == "" {
		outDir, err = os.MkdirTemp("", "xift-mft-spike-*")
		if err != nil {
			return fmt.Errorf("create temp output dir: %w", err)
		}
		tempCreated = true
	}
	if tempCreated && !keepOut {
		defer os.RemoveAll(outDir)
	}

	var enumerate func(shouldDecode func(frn, parentFRN uint64, isDir bool) bool, emit func(mftspike.RawRecord)) (int, error)
	sourceLabel := fmt.Sprintf("Windows NTFS MFT (%s: READ-ONLY)", drive)
	if synthetic {
		sourceLabel = fmt.Sprintf("Synthetic Stream (%d records)", syntheticCount)
		enumerate = func(shouldDecode func(frn, parentFRN uint64, isDir bool) bool, emit func(mftspike.RawRecord)) (int, error) {
			seen := 0
			emitSyntheticMFT(syntheticCount, func(rec mftspike.RawRecord) {
				seen++
				if shouldDecode != nil && !shouldDecode(rec.FRN, rec.ParentFRN, rec.IsDir) {
					return
				}
				emit(rec)
			})
			return seen, nil
		}
	} else {
		enumerate = func(shouldDecode func(frn, parentFRN uint64, isDir bool) bool, emit func(mftspike.RawRecord)) (int, error) {
			return mftspike.EnumerateNTFSMFTFiltered(drive, shouldDecode, emit)
		}
	}

	fmt.Printf("=== Xift P0-02 Spike: Pure Go MFT + Two-Pass Pruner + Hot Column mmap ===\n")
	fmt.Printf("Source : %s\n", sourceLabel)
	fmt.Printf("OutDir : %s\n\n", outDir)

	totalStart := time.Now()

	// Pass 1: Collect directory skeleton ONLY (skip UTF-16 filename decoding for all non-dir files!)
	pass1Start := time.Now()
	sb := mftspike.NewSkeletonBuilder(mftspike.DefaultRules())
	rawTotal, err := enumerate(
		func(frn, parentFRN uint64, isDir bool) bool { return isDir },
		func(rec mftspike.RawRecord) {
			sb.AddDir(rec.FRN, rec.ParentFRN, rec.Name)
		},
	)
	if err != nil {
		return err
	}
	pass1ScanDur := time.Since(pass1Start)

	bfsStart := time.Now()
	pruner := sb.Build()
	dirCount := sb.DirCount()
	bfsDur := time.Since(bfsStart)
	// Release Pass 1 raw directory slice before Pass 2
	sb = nil

	// Pass 2: Skip UTF-16 decoding for records under PrunedDirSet BEFORE allocation, then flush 16B HotNode + names.col
	pass2Start := time.Now()
	writer, err := mftspike.NewHotColumnWriter(outDir)
	if err != nil {
		return err
	}

	dirIndexMap := make(map[uint64]uint32, dirCount/3+1024)
	var nextDirIdx uint32
	var keptCount, foldedCount, systemCount, droppedCount int
	var writeErr error

	pass2Total, err := enumerate(
		func(frn, parentFRN uint64, isDir bool) bool {
			if parentFRN != frn && pruner.IsPrunedDir(parentFRN) {
				droppedCount++
				return false
			}
			return true
		},
		func(rec mftspike.RawRecord) {
			if writeErr != nil {
				return
			}
			dec := pruner.Evaluate(rec)
			if !dec.Keep {
				droppedCount++
				return
			}

			var flags uint16
			if rec.IsDir {
				flags |= mftspike.FlagDir
				if _, exists := dirIndexMap[rec.FRN]; !exists {
					dirIndexMap[rec.FRN] = nextDirIdx
					nextDirIdx++
				}
			}
			if rec.Hidden {
				flags |= mftspike.FlagHidden
			}
			if mftspike.HasNonASCII(rec.Name) {
				flags |= mftspike.FlagNonASCII
			}
			if dec.Folded {
				flags |= mftspike.FlagFolded
				foldedCount++
			}
			if dec.System {
				flags |= mftspike.FlagSystem
				systemCount++
			}

			parentIdx := uint32(math.MaxUint32)
			if pid, ok := dirIndexMap[rec.ParentFRN]; ok {
				parentIdx = pid
			}

			if err := writer.Append(rec.Name, parentIdx, flags, 1); err != nil {
				writeErr = err
				return
			}
			keptCount++
		},
	)
	if err != nil {
		_, _ = writer.Close()
		return err
	}
	if writeErr != nil {
		_, _ = writer.Close()
		return writeErr
	}
	if pass2Total > 0 {
		rawTotal = pass2Total
	}

	stats, err := writer.Close()
	if err != nil {
		return err
	}
	pass2Dur := time.Since(pass2Start)
	totalScanDur := time.Since(totalStart)

	// Drop Pass 1/2 temporary maps and GC before measuring steady-state mmap footprint.
	prunedDirCount := pruner.PrunedDirCount()
	systemDirCount := pruner.SystemDirCount()
	pruner = nil
	dirIndexMap = nil
	runtime.GC()

	// Mount hot.col + names.col read-only via mmap and run a sample query.
	mmapStart := time.Now()
	mm, err := mftspike.OpenMmapHotColumns(outDir)
	if err != nil {
		return err
	}
	defer mm.Close()
	mmapDur := time.Since(mmapStart)

	queryStart := time.Now()
	hits := mm.SearchSubstring(query, 20)
	queryDur := time.Since(queryStart)

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	wsBytes, peakWSBytes, _ := mftspike.ProcessWorkingSetBytes()

	pruneRatio := 0.0
	if rawTotal > 0 {
		pruneRatio = float64(droppedCount) * 100.0 / float64(rawTotal)
	}

	fmt.Printf("Total Raw Records : %d (Dirs: %d)\n", rawTotal, dirCount)
	fmt.Printf("PrunedDirSet      : %d dirs | SystemDirSet: %d dirs\n", prunedDirCount, systemDirCount)
	fmt.Printf("Kept Records      : %d (Folded shells: %d, System soft-demoted: %d)\n", keptCount, foldedCount, systemCount)
	fmt.Printf("Dropped Records   : %d (Pruning Ratio: %.1f%%)\n\n", droppedCount, pruneRatio)

	fmt.Printf("Timing Breakdown:\n")
	fmt.Printf("  Pass 1 (Dir Scan) : %v\n", pass1ScanDur.Round(time.Millisecond))
	fmt.Printf("  Pass 1 (BFS Prop) : %v\n", bfsDur.Round(time.Microsecond))
	fmt.Printf("  Pass 2 (Filter+IO): %v\n", pass2Dur.Round(time.Millisecond))
	fmt.Printf("  Total Scan+Flush  : %v\n", totalScanDur.Round(time.Millisecond))
	fmt.Printf("  Read-Only mmap    : %v\n", mmapDur.Round(time.Microsecond))
	fmt.Printf("  Sample Query %q : %v (%d hits shown, cap 20)\n\n", query, queryDur.Round(time.Microsecond), len(hits))

	fmt.Printf("Column Sizes & Memory:\n")
	fmt.Printf("  hot.col           : %.2f MB (%d nodes x 16B)\n", float64(stats.HotColBytes)/(1<<20), stats.NodeCount)
	fmt.Printf("  names.col         : %.2f MB (unique exts: %d)\n", float64(stats.NamesColBytes)/(1<<20), stats.ExtCount)
	fmt.Printf("  Hot Columns Total : %.2f MB\n", float64(stats.HotColBytes+stats.NamesColBytes)/(1<<20))
	fmt.Printf("  Go HeapAlloc / Sys: %.2f MB / %.2f MB\n", float64(ms.HeapAlloc)/(1<<20), float64(ms.Sys)/(1<<20))
	if wsBytes > 0 {
		fmt.Printf("  Win WorkingSet    : %.2f MB (Peak: %.2f MB)\n", float64(wsBytes)/(1<<20), float64(peakWSBytes)/(1<<20))
	}

	if len(hits) > 0 {
		fmt.Printf("\nSample Matches for %q:\n", query)
		for i := 0; i < min(len(hits), 5); i++ {
			idx := hits[i]
			fmt.Printf("  [%d] %s (flags=0x%02x)\n", idx, mm.Name(idx), mm.Node(idx).Flags)
		}
	}

	return nil
}

// emitSyntheticMFT generates a realistic developer-machine MFT stream (~70% noise in
// node_modules/.git/GPUCache/target + ~30% user & Windows system files) in out-of-order FRN layout.
func emitSyntheticMFT(total int, emit func(mftspike.RawRecord)) {
	if total <= 0 {
		return
	}
	const (
		rootFRN        uint64 = 5
		workDirFRN     uint64 = 10
		nodeModFRN     uint64 = 11
		gitDirFRN      uint64 = 12
		gpuCacheFRN    uint64 = 13
		targetDirFRN   uint64 = 14
		winDirFRN      uint64 = 15
		sys32DirFRN    uint64 = 16
		docsDirFRN     uint64 = 17
		subPkgBaseFRN  uint64 = 100
		subPkgCount           = 400
	)

	emitted := 0
	send := func(r mftspike.RawRecord) bool {
		if emitted >= total {
			return false
		}
		emit(r)
		emitted++
		return true
	}

	// Emit a few sentinel files first (out of order: BEFORE their parent dirs!)
	if !send(mftspike.RawRecord{FRN: 900_000_001, ParentFRN: sys32DirFRN, Name: "kernel32.dll"}) {
		return
	}
	if !send(mftspike.RawRecord{FRN: 900_000_002, ParentFRN: docsDirFRN, Name: "系统设计文档_v2.1.md"}) {
		return
	}

	// Sub-directories inside node_modules / .git / GPUCache / target
	for i := 0; i < subPkgCount; i++ {
		var parent uint64
		switch i % 4 {
		case 0:
			parent = nodeModFRN
		case 1:
			parent = gitDirFRN
		case 2:
			parent = gpuCacheFRN
		default:
			parent = targetDirFRN
		}
		if !send(mftspike.RawRecord{
			FRN:       subPkgBaseFRN + uint64(i),
			ParentFRN: parent,
			Name:      fmt.Sprintf("pkg_%03d", i),
			IsDir:     true,
		}) {
			return
		}
	}

	// Top-level skeleton directories emitted AFTER sub-directories to stress out-of-order Pass 1
	topDirs := []mftspike.RawRecord{
		{FRN: rootFRN, ParentFRN: rootFRN, Name: "", IsDir: true},
		{FRN: workDirFRN, ParentFRN: rootFRN, Name: "Workspace", IsDir: true},
		{FRN: docsDirFRN, ParentFRN: workDirFRN, Name: "Documents", IsDir: true},
		{FRN: nodeModFRN, ParentFRN: workDirFRN, Name: "node_modules", IsDir: true},
		{FRN: gitDirFRN, ParentFRN: workDirFRN, Name: ".git", IsDir: true},
		{FRN: gpuCacheFRN, ParentFRN: workDirFRN, Name: "GPUCache", IsDir: true},
		{FRN: targetDirFRN, ParentFRN: workDirFRN, Name: "target", IsDir: true},
		{FRN: winDirFRN, ParentFRN: rootFRN, Name: "Windows", IsDir: true},
		{FRN: sys32DirFRN, ParentFRN: winDirFRN, Name: "System32", IsDir: true},
	}
	for _, d := range topDirs {
		if !send(d) {
			return
		}
	}

	// Remaining records: 70% inside pruned sub-packages, 15% Windows system files, 15% clean user files
	nextFRN := uint64(10_000)
	for emitted < total {
		slot := emitted % 20
		var rec mftspike.RawRecord
		rec.FRN = nextFRN
		nextFRN++

		switch {
		case slot < 14: // 70% noise inside node_modules / .git / GPUCache / target
			rec.ParentFRN = subPkgBaseFRN + uint64(emitted%subPkgCount)
			rec.Name = fmt.Sprintf("chunk_%07d.js", emitted)
		case slot < 17: // 15% Windows system files (SoftDemote)
			rec.ParentFRN = sys32DirFRN
			rec.Name = fmt.Sprintf("sysmod_%07d.dll", emitted)
		default: // 15% clean user files
			rec.ParentFRN = docsDirFRN
			if slot == 17 {
				rec.Name = fmt.Sprintf("项目文档_%07d.md", emitted)
			} else {
				rec.Name = fmt.Sprintf("module_%07d.go", emitted)
			}
		}
		if !send(rec) {
			return
		}
	}
}
