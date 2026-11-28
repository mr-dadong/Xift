package daemon

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/mr-dadong/Xift/internal/l2search"
	"github.com/mr-dadong/Xift/internal/provider"
	"github.com/mr-dadong/Xift/internal/storage"
)

type fakeSubtreeProvider struct {
	calls  int
	gotVol provider.VolumeInfo
	gotDir string
	recs   []provider.ScanRecord
	err    error
}

func (f *fakeSubtreeProvider) ScanSubtreeWithRoot(ctx context.Context, vol provider.VolumeInfo, dirPath string, writer provider.SegmentWriter) error {
	f.calls++
	f.gotVol = vol
	f.gotDir = dirPath
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, rec := range f.recs {
		if err := writer.WriteRecord(rec); err != nil {
			return err
		}
	}
	return f.err
}

// TestProviderScanner verifies provider scan records are adapted to the L2
// record shape with the owning volume resolved by longest prefix.
func TestProviderScanner(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "docs")
	nested := filepath.Join(root, "sub")
	vols := []provider.VolumeInfo{
		{VolID: 2, RootPath: base},
		{VolID: 1, RootPath: root},
	}
	fake := &fakeSubtreeProvider{recs: []provider.ScanRecord{
		{VolID: 1, NodeKey: 10, ParentKey: 10, Name: "docs", Flags: storage.FlagDir, ModTime: 1_700_000_000_000_000_001},
		{VolID: 1, NodeKey: 11, ParentKey: 10, Name: "a.txt", ModTime: 1_700_000_000_000_000_002},
	}}
	scanner := providerScanner{prov: fake, vols: vols}

	var got []l2search.ScanRecord
	err := scanner.ScanSubtreeWithRoot(context.Background(), nested, func(rec l2search.ScanRecord) error {
		got = append(got, rec)
		return nil
	})
	if err != nil {
		t.Fatalf("ScanSubtreeWithRoot: %v", err)
	}
	if fake.calls != 1 || fake.gotVol.VolID != 1 {
		t.Fatalf("provider call = %d vol %d, want one call on volume 1", fake.calls, fake.gotVol.VolID)
	}
	if fake.gotDir != nested {
		t.Fatalf("provider dir = %q, want %q", fake.gotDir, nested)
	}
	if len(got) != 2 {
		t.Fatalf("records = %d, want 2", len(got))
	}
	if !got[0].IsDir {
		t.Fatalf("root record = %+v, want directory", got[0])
	}
	if got[0].NodeKey != 10 || got[0].ParentKey != 10 || got[0].Name != "docs" {
		t.Fatalf("root record = %+v, want self-parented docs", got[0])
	}
	if got[0].ModTime != 1_700_000_000_000_000_001 {
		t.Fatalf("ModTime = %d, want nanoseconds preserved", got[0].ModTime)
	}
	if got[1].IsDir || got[1].NodeKey != 11 || got[1].ParentKey != 10 {
		t.Fatalf("file record = %+v, want node 11 under 10", got[1])
	}
}

// TestProviderScannerUnknownRoot verifies roots outside every enumerated volume
// are reported instead of scanning with a zero volume.
func TestProviderScannerUnknownRoot(t *testing.T) {
	fake := &fakeSubtreeProvider{}
	scanner := providerScanner{prov: fake, vols: []provider.VolumeInfo{{VolID: 1, RootPath: t.TempDir()}}}
	err := scanner.ScanSubtreeWithRoot(context.Background(), t.TempDir(), func(l2search.ScanRecord) error {
		return nil
	})
	if err == nil {
		t.Fatal("unknown root must return an error")
	}
	if fake.calls != 0 {
		t.Fatalf("provider called %d times for an unknown root", fake.calls)
	}
}

type fakeL2Provider struct {
	provider.FSProvider
	vols    []provider.VolumeInfo
	err     error
	recs    []provider.ScanRecord
	scanErr error
}

func (f *fakeL2Provider) EnumerateVolumes(context.Context) ([]provider.VolumeInfo, error) {
	return f.vols, f.err
}

func (f *fakeL2Provider) ScanSubtreeWithRoot(ctx context.Context, _ provider.VolumeInfo, _ string, writer provider.SegmentWriter) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, rec := range f.recs {
		if err := writer.WriteRecord(rec); err != nil {
			return err
		}
	}
	return f.scanErr
}

// TestDaemonL2Wiring verifies daemon start-up either fails fast when no
// platform provider exists or builds a scanner plus volume-root map when it
// does, and never reads user files while doing so.
func TestDaemonL2Wiring(t *testing.T) {
	if _, err := BuildL2Config(context.Background(), nil); err == nil {
		t.Fatal("nil provider must fail L2 wiring")
	}

	_, err := BuildL2Config(context.Background(), &fakeL2Provider{err: provider.ErrUnsupportedOS})
	if !errors.Is(err, provider.ErrUnsupportedOS) {
		t.Fatalf("unsupported platform err = %v, want ErrUnsupportedOS", err)
	}

	root := t.TempDir()
	prov := &fakeL2Provider{vols: []provider.VolumeInfo{{VolID: 4, RootPath: root}}}
	cfg, err := BuildL2Config(context.Background(), prov)
	if err != nil {
		t.Fatalf("BuildL2Config: %v", err)
	}
	if cfg.Scanner == nil {
		t.Fatal("Scanner must be wired")
	}
	if cfg.VolRoots[4] != root {
		t.Fatalf("VolRoots = %v, want volume 4 -> %q", cfg.VolRoots, root)
	}
}

// TestL2PipelineWithProviderScanner verifies the production-shaped wiring:
// BuildL2Config + providerScanner open the pipeline, index an added root, map
// hits back to L1 NodeIDs, and keep serving when another root has no volume.
func TestL2PipelineWithProviderScanner(t *testing.T) {
	dataDir := t.TempDir()
	store, err := storage.Open(dataDir, storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	defer store.Close()

	root := t.TempDir()
	writeL2File(t, filepath.Join(root, "a.txt"), "provider wiring needle")

	b, err := store.BeginDenseSegment()
	if err != nil {
		t.Fatal(err)
	}
	rootID, err := b.AppendDense(1, 100, storage.RootParentIdx, filepath.Base(root), storage.FlagDir, 0, 0, 1_700_000_000)
	if err != nil {
		t.Fatal(err)
	}
	fileID, err := b.AppendDense(1, 101, rootID, "a.txt", 0, 0, 8, 1_700_000_000)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := b.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CommitSegment(meta, nil); err != nil {
		t.Fatal(err)
	}

	prov := &fakeL2Provider{
		vols: []provider.VolumeInfo{{VolID: 1, RootPath: root}},
		recs: []provider.ScanRecord{
			{VolID: 1, NodeKey: 100, ParentKey: 100, Name: filepath.Base(root), Flags: storage.FlagDir, ModTime: 1_700_000_000_000_000_000},
			{VolID: 1, NodeKey: 101, ParentKey: 100, Name: "a.txt", ModTime: 1_700_000_000_000_000_000},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg, err := BuildL2Config(ctx, prov)
	if err != nil {
		t.Fatalf("BuildL2Config: %v", err)
	}
	cfg.Debounce = 20 * time.Millisecond
	cfg.BatchDocs = 10
	p, err := OpenL2Pipeline(ctx, dataDir, store, *cfg)
	if err != nil {
		t.Fatalf("OpenL2Pipeline: %v", err)
	}
	defer p.Close()
	if _, err := p.AddRoot(root); err != nil {
		t.Fatalf("AddRoot: %v", err)
	}
	waitForL2(t, 20*time.Second, func() bool {
		ids, _, serr := p.Search([]byte("wiring needle"), 0)
		return serr == nil && len(ids) == 1 && ids[0] == fileID
	})

	// A root outside every enumerated volume is skipped, not fatal.
	outside := t.TempDir()
	if _, err := p.AddRoot(outside); err != nil {
		t.Fatalf("AddRoot outside volume: %v", err)
	}
	waitForL2(t, 20*time.Second, func() bool {
		return p.Status().Skipped[l2search.SkipReadError] >= 1
	})
	if ids, _, err := p.Search([]byte("wiring needle"), 0); err != nil || len(ids) != 1 || ids[0] != fileID {
		t.Fatalf("search after skipped root = %v, %v; want [%d]", ids, err, fileID)
	}
}

// TestProviderScannerPropagatesCancellation verifies context cancellation and
// writer errors surface unchanged.
func TestProviderScannerPropagatesCancellation(t *testing.T) {
	root := t.TempDir()
	scanner := providerScanner{
		prov: &fakeSubtreeProvider{},
		vols: []provider.VolumeInfo{{VolID: 1, RootPath: root}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := scanner.ScanSubtreeWithRoot(ctx, root, func(l2search.ScanRecord) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled scan err = %v, want context.Canceled", err)
	}

	sentinel := errors.New("stop")
	fake := &fakeSubtreeProvider{recs: []provider.ScanRecord{{VolID: 1, NodeKey: 1, ParentKey: 1, Name: "x", Flags: storage.FlagDir}}}
	scanner = providerScanner{prov: fake, vols: []provider.VolumeInfo{{VolID: 1, RootPath: root}}}
	if err := scanner.ScanSubtreeWithRoot(context.Background(), root, func(l2search.ScanRecord) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatalf("writer error = %v, want sentinel", err)
	}
}
