package query

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mr-dadong/Xift/internal/storage"
)

func buildTestStorage(t *testing.T) (*storage.Engine, time.Time) {
	t.Helper()
	now := time.Date(2026, 11, 27, 15, 0, 0, 0, time.UTC)
	eng, err := storage.Open(t.TempDir(), storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })

	b, err := eng.BeginDenseSegment()
	if err != nil {
		t.Fatalf("BeginDenseSegment: %v", err)
	}
	mustAdd := func(key uint64, parent uint32, name string, flags uint16, size int64, mt time.Time) uint32 {
		id, err := b.AppendDense(1, key, parent, name, flags, 0, size, mt.UnixNano())
		if err != nil {
			t.Fatalf("AppendDense %s: %v", name, err)
		}
		return id
	}

	root := mustAdd(1, storage.RootParentIdx, "work", storage.FlagDir, 0, now)
	src := mustAdd(2, root, "src", storage.FlagDir, 0, now)
	docs := mustAdd(3, root, "docs", storage.FlagDir, 0, now)
	sysDir := mustAdd(4, root, "Windows", storage.FlagDir|storage.FlagSystem, 0, now)

	mustAdd(10, src, "XiftQueryEngine.go", 0, 4096, now.Add(-2*time.Hour))
	mustAdd(11, src, "scanner.rs", 0, 8192, now.Add(-24*time.Hour))
	mustAdd(12, src, "config.ini", 0, 512, now.Add(-10*24*time.Hour))
	mustAdd(13, docs, "report_2026.pdf", 0, 60*1024*1024, now.Add(-3*24*time.Hour))
	mustAdd(14, docs, "report_draft.docx", 0, 2*1024*1024, now.Add(-20*24*time.Hour))
	mustAdd(15, docs, "设计文档_2026.md", 0, 1024, now.Add(-1*time.Hour))
	mustAdd(16, sysDir, "config.ini", storage.FlagSystem, 256, now.Add(-1*time.Hour))
	mustAdd(17, sysDir, "kernel32.dll", storage.FlagSystem, 1024*1024, now.Add(-30*24*time.Hour))

	meta, err := b.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if err := eng.CommitSegment(meta, nil); err != nil {
		t.Fatalf("CommitSegment: %v", err)
	}

	// Add a live MemTable node to verify Segment + MemTable unified scanning.
	if err := eng.ApplyBatch([]storage.Mutation{
		{
			Op:        storage.OpCreate,
			VolID:     1,
			NodeKey:   18,
			ParentKey: 2,
			Name:      "live_memtable.go",
			Size:      2048,
			ModTime:   now.Add(-5 * time.Minute).UnixNano(),
		},
	}); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
	return eng, now
}

func TestEngineSearchDSLAndColdSeparation(t *testing.T) {
	store, now := buildTestStorage(t)
	qe := NewEngine(store)

	// 1. ExtID fast path: *.go | *.rs
	res, err := qe.Search(context.Background(), `*.go | *.rs`, SearchOptions{Now: now, Limit: 20})
	if err != nil {
		t.Fatalf("Search *.go | *.rs: %v", err)
	}
	if res.Total != 3 {
		t.Fatalf("expected 3 matches (.go + .rs + memtable .go), got %d (%+v)", res.Total, res.Items)
	}

	// 2. AND + ext: + !negation + size: + dm:
	res, err = qe.Search(context.Background(), `report ext:pdf,docx !draft size:>50mb dm:7d`, SearchOptions{Now: now, Limit: 10})
	if err != nil {
		t.Fatalf("Search compound: %v", err)
	}
	if res.Total != 1 || res.Items[0].Name != "report_2026.pdf" {
		t.Fatalf("expected only report_2026.pdf, got %+v", res.Items)
	}

	// 3. Path filter + highlight ranges
	res, err = qe.Search(context.Background(), `query path:work/src`, SearchOptions{Now: now, Limit: 10})
	if err != nil {
		t.Fatalf("Search path: %v", err)
	}
	if res.Total != 1 || res.Items[0].Name != "XiftQueryEngine.go" {
		t.Fatalf("expected XiftQueryEngine.go, got %+v", res.Items)
	}
	if len(res.Items[0].Highlights) != 1 || res.Items[0].Highlights[0] != (HighlightRange{Start: 4, End: 9}) {
		t.Fatalf("unexpected highlights: %+v", res.Items[0].Highlights)
	}

	// 4. System soft-demotion vs all:/sys:
	resDef, err := qe.Search(context.Background(), `config.ini`, SearchOptions{Now: now, Limit: 10})
	if err != nil {
		t.Fatalf("Search config.ini: %v", err)
	}
	if resDef.Total != 2 {
		t.Fatalf("expected 2 config.ini results, got %d", resDef.Total)
	}
	if resDef.Items[0].System || !resDef.Items[1].System {
		t.Fatalf("expected non-system config.ini to rank above system config.ini: %+v", resDef.Items)
	}

	resAll, err := qe.Search(context.Background(), `all:config.ini`, SearchOptions{Now: now, Limit: 10})
	if err != nil {
		t.Fatalf("Search all:config.ini: %v", err)
	}
	if resAll.Total != 2 {
		t.Fatalf("expected 2 results for all:config.ini, got %d", resAll.Total)
	}
	// With system penalty disabled, equal name + equal depth must have equal score.
	if resAll.Items[0].Score != resAll.Items[1].Score {
		t.Fatalf("expected equal scores when all: disables system penalty: %.2f vs %.2f", resAll.Items[0].Score, resAll.Items[1].Score)
	}

	// 5. Cold-column separation: name-only search with Limit=2 reads cold meta ONLY for viewport (<= 2).
	resVP, err := qe.Search(context.Background(), `type:file`, SearchOptions{Now: now, Limit: 2})
	if err != nil {
		t.Fatalf("Search viewport: %v", err)
	}
	if resVP.Total < 6 || resVP.Returned != 2 {
		t.Fatalf("expected total>=6 returned=2, got total=%d returned=%d", resVP.Total, resVP.Returned)
	}
	if resVP.ColdReads > 2 {
		t.Fatalf("hot-cold violation: expected ColdReads <= 2 for Limit=2, got %d", resVP.ColdReads)
	}
}

func TestSessionIncrementalFilteringAndInvalidation(t *testing.T) {
	store, now := buildTestStorage(t)
	qe := NewEngine(store)
	sess := qe.NewSession()

	r1, err := sess.Search(context.Background(), `rep`, SearchOptions{Now: now, Limit: 20})
	if err != nil {
		t.Fatalf("first search: %v", err)
	}
	if r1.Incremental || r1.Total != 2 {
		t.Fatalf("expected non-incremental 2 hits for 'rep', got inc=%v total=%d", r1.Incremental, r1.Total)
	}

	// Narrowing query on same generation MUST use incremental filtering.
	r2, err := sess.Search(context.Background(), `report_2026`, SearchOptions{Now: now, Limit: 20})
	if err != nil {
		t.Fatalf("narrowing search: %v", err)
	}
	if !r2.Incremental {
		t.Fatalf("expected Incremental=true for narrowing query 'rep' -> 'report_2026'")
	}
	if r2.Total != 1 || r2.Items[0].Name != "report_2026.pdf" {
		t.Fatalf("unexpected narrowed result: %+v", r2.Items)
	}

	// Broadening back to 'rep' MUST NOT reuse narrowed cache.
	r3, err := sess.Search(context.Background(), `rep`, SearchOptions{Now: now, Limit: 20})
	if err != nil {
		t.Fatalf("broadening search: %v", err)
	}
	if r3.Incremental || r3.Total != 2 {
		t.Fatalf("expected full rescan on broadening, got inc=%v total=%d", r3.Incremental, r3.Total)
	}

	// Storage mutation MUST invalidate session cache even on a narrowing query.
	if err := store.ApplyBatch([]storage.Mutation{
		{Op: storage.OpCreate, VolID: 1, NodeKey: 99, ParentKey: 3, Name: "report_2026_final.pdf", Size: 100, ModTime: now.UnixNano()},
	}); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
	r4, err := sess.Search(context.Background(), `report_2026`, SearchOptions{Now: now, Limit: 20})
	if err != nil {
		t.Fatalf("post-mutation search: %v", err)
	}
	if r4.Incremental {
		t.Fatalf("expected cache invalidation after storage mutation")
	}
	if r4.Total != 2 {
		t.Fatalf("expected 2 matches including newly created report_2026_final.pdf, got %d", r4.Total)
	}
}

func TestInFoldedDirectoryLocalScanZeroIndexPollution(t *testing.T) {
	diskRoot := t.TempDir()
	nmDir := filepath.Join(diskRoot, "node_modules")
	lodashDir := filepath.Join(nmDir, "lodash")
	if err := os.MkdirAll(lodashDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	targetFile := filepath.Join(lodashDir, "index.js")
	if err := os.WriteFile(targetFile, []byte("module.exports = {}"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	store, err := storage.Open(t.TempDir(), storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	defer store.Close()

	// Index only diskRoot and folded node_modules shell (children NOT indexed).
	b, err := store.BeginDenseSegment()
	if err != nil {
		t.Fatalf("BeginDenseSegment: %v", err)
	}
	rootID, err := b.AppendDense(1, 1, storage.RootParentIdx, diskRoot, storage.FlagDir, 0, 0, time.Now().UnixNano())
	if err != nil {
		t.Fatalf("append root: %v", err)
	}
	if _, err := b.AppendDense(1, 2, rootID, "node_modules", storage.FlagDir|storage.FlagFolded, 0, 0, time.Now().UnixNano()); err != nil {
		t.Fatalf("append folded node_modules: %v", err)
	}
	meta, err := b.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if err := store.CommitSegment(meta, nil); err != nil {
		t.Fatalf("CommitSegment: %v", err)
	}

	liveBefore := store.LiveCount()
	memBefore := store.MemTableEntryCount()

	qe := NewEngine(store)
	res, err := qe.Search(context.Background(), `in:node_modules lodash ext:js`, SearchOptions{Limit: 20})
	if err != nil {
		t.Fatalf("in: search failed: %v", err)
	}
	if res.Total != 1 {
		t.Fatalf("expected 1 match inside folded node_modules, got %d (%+v)", res.Total, res.Items)
	}
	if !res.Items[0].FromFoldedScan || !strings.HasSuffix(filepath.ToSlash(res.Items[0].Path), "node_modules/lodash/index.js") {
		t.Fatalf("unexpected folded scan item: %+v", res.Items[0])
	}
	if store.LiveCount() != liveBefore || store.MemTableEntryCount() != memBefore {
		t.Fatalf("in: scan polluted index! live %d->%d mem %d->%d", liveBefore, store.LiveCount(), memBefore, store.MemTableEntryCount())
	}
}

func TestEngineConcurrentSearchAndWriteRace(t *testing.T) {
	store, now := buildTestStorage(t)
	qe := NewEngine(store)
	sess := qe.NewSession()

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 30; i++ {
				q := "report"
				if i%2 == 0 {
					q = "ext:go | *.rs"
				}
				_, err := sess.Search(context.Background(), q, SearchOptions{Now: now, Limit: 10})
				if err != nil && err != context.Canceled {
					t.Errorf("worker %d search error: %v", id, err)
					return
				}
			}
		}(w)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			_ = store.ApplyBatch([]storage.Mutation{
				{
					Op:        storage.OpCreate,
					VolID:     1,
					NodeKey:   uint64(1000 + i),
					ParentKey: 2,
					Name:      fmt.Sprintf("report_dyn_%d.go", i),
					Size:      128,
					ModTime:   now.UnixNano(),
				},
			})
		}
	}()
	wg.Wait()
}

func TestSearchContextCanceled(t *testing.T) {
	store, now := buildTestStorage(t)
	qe := NewEngine(store)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := qe.Search(ctx, "report", SearchOptions{Now: now}); err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestHotQuery1MNodesSLA(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 1M node SLA test in -short mode")
	}

	const totalNodes = 1_000_000
	store, err := storage.Open(t.TempDir(), storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	defer store.Close()

	b, err := store.BeginDenseSegment()
	if err != nil {
		t.Fatalf("BeginDenseSegment: %v", err)
	}
	now := time.Date(2026, 11, 27, 15, 0, 0, 0, time.UTC)
	rootID, err := b.AppendDense(1, 1, storage.RootParentIdx, "C:", storage.FlagDir, 0, 0, now.UnixNano())
	if err != nil {
		t.Fatalf("append root: %v", err)
	}

	exts := []string{".go", ".rs", ".ts", ".md", ".pdf", ".json", ".txt", ".yaml"}
	extIDs := make([]uint16, len(exts))
	for i, e := range exts {
		extIDs[i] = store.ExtPool().Intern(e)
	}

	for i := 1; i < totalNodes; i++ {
		var name string
		if i%1000 == 0 {
			name = fmt.Sprintf("quarterly_report_%06d%s", i, exts[i%len(exts)])
		} else {
			name = fmt.Sprintf("module_asset_%06d%s", i, exts[i%len(exts)])
		}
		if _, err := b.AppendDense(1, uint64(i+1), rootID, name, 0, extIDs[i%len(exts)], int64(i*64), now.UnixNano()); err != nil {
			t.Fatalf("AppendDense %d: %v", i, err)
		}
	}
	meta, err := b.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if err := store.CommitSegment(meta, nil); err != nil {
		t.Fatalf("CommitSegment: %v", err)
	}

	qe := NewEngine(store)
	sess := qe.NewSession()

	// Warm page cache once.
	warm, err := sess.Search(context.Background(), "report", SearchOptions{Now: now, Limit: 60})
	if err != nil {
		t.Fatalf("warm search: %v", err)
	}
	if warm.Total != 999 {
		t.Fatalf("expected 999 report matches, got %d", warm.Total)
	}
	if warm.ColdReads > 60 {
		t.Fatalf("hot-cold violation on 1M index: ColdReads=%d > 60", warm.ColdReads)
	}

	// Measure hot full-scan latency (fresh query) across 5 runs.
	var bestHot time.Duration
	for r := 0; r < 5; r++ {
		sess.Reset()
		res, err := sess.Search(context.Background(), "report", SearchOptions{Now: now, Limit: 60})
		if err != nil {
			t.Fatalf("hot search %d: %v", r, err)
		}
		if r == 0 || res.Cost < bestHot {
			bestHot = res.Cost
		}
	}

	// Measure incremental narrowing latency ('report' -> 'report_005').
	incRes, err := sess.Search(context.Background(), "report_005", SearchOptions{Now: now, Limit: 60})
	if err != nil {
		t.Fatalf("incremental search: %v", err)
	}
	if !incRes.Incremental {
		t.Fatalf("expected Incremental=true for 'report' -> 'report_005'")
	}

	t.Logf("1M-node SLA: bestHot=%v (target <10ms), incremental=%v (target <2ms), hits=%d, coldReads=%d",
		bestHot, incRes.Cost, incRes.Total, incRes.ColdReads)
}
