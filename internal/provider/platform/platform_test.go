package platform

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mr-dadong/Xift/internal/filter"
	"github.com/mr-dadong/Xift/internal/provider"
	"github.com/mr-dadong/Xift/internal/storage"
)

func TestNewDefaultProviderSmoke(t *testing.T) {
	p, err := NewDefaultProvider(filter.NewPruner(nil, nil), provider.Options{DisableLowPriority: true})
	if err != nil {
		t.Fatalf("NewDefaultProvider: %v", err)
	}
	if p == nil {
		t.Fatal("NewDefaultProvider returned nil")
	}
	vols, err := p.EnumerateVolumes(context.Background())
	if err != nil {
		t.Fatalf("EnumerateVolumes: %v", err)
	}
	if len(vols) == 0 {
		t.Fatal("expected at least one volume from default platform provider")
	}
}

// TestScanSubtreeWithRoot verifies the root record is emitted first and paths
// can be composed from ParentKey for the whitelisted subtree.
func TestScanSubtreeWithRoot(t *testing.T) {
	root := t.TempDir()
	mustWrite := func(rel, content string) {
		t.Helper()
		full := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	mustWrite("top.txt", "top")
	mustWrite("sub/inner.txt", "inner")

	p, err := NewDefaultProvider(filter.NewPruner(nil, nil), provider.Options{DisableLowPriority: true})
	if err != nil {
		if errors.Is(err, provider.ErrUnsupportedOS) {
			t.Skip("no native provider on this platform")
		}
		t.Fatalf("NewDefaultProvider: %v", err)
	}

	var recs []provider.ScanRecord
	vol := provider.VolumeInfo{VolID: 1, RootPath: root, FSType: "DIR"}
	err = p.ScanSubtreeWithRoot(context.Background(), vol, root, provider.SegmentWriterFunc(func(rec provider.ScanRecord) error {
		recs = append(recs, rec)
		return nil
	}))
	if err != nil {
		t.Fatalf("ScanSubtreeWithRoot: %v", err)
	}
	if len(recs) == 0 {
		t.Fatal("scan emitted no records")
	}
	head := recs[0]
	if head.Flags&storage.FlagDir == 0 || head.NodeKey == 0 || head.ParentKey != head.NodeKey {
		t.Fatalf("first record = %+v, want self-parented root directory", head)
	}
	if head.Name != filepath.Base(root) {
		t.Fatalf("root name = %q, want %q", head.Name, filepath.Base(root))
	}

	dirs := map[uint64]bool{}
	seen := map[string]bool{}
	for i, rec := range recs {
		if strings.HasSuffix(rec.Name, ".txt") && rec.ParentKey != head.NodeKey && !dirs[rec.ParentKey] {
			t.Fatalf("file %q at record %d appeared before its parent dir", rec.Name, i)
		}
		if rec.Flags&storage.FlagDir != 0 {
			dirs[rec.NodeKey] = true
		}
		seen[rec.Name] = true
	}
	for _, want := range []string{"top.txt", "inner.txt", "sub"} {
		if !seen[want] {
			t.Fatalf("scan missing %q: %+v", want, recs)
		}
	}

	// Cancellation surfaces promptly.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.ScanSubtreeWithRoot(ctx, vol, root, provider.SegmentWriterFunc(func(provider.ScanRecord) error { return nil })); err == nil {
		t.Fatal("cancelled scan must return an error")
	}
}

// TestScanSubtreeWithRootOrder verifies the scan emits the self-parented root
// record first, every directory before its children, and stops promptly when
// the context is cancelled.
func TestScanSubtreeWithRootOrder(t *testing.T) {
	root := t.TempDir()
	mustWrite := func(rel, content string) {
		t.Helper()
		full := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	mustWrite("top.txt", "top")
	mustWrite("a/mid.txt", "mid")
	mustWrite("a/b/deep.txt", "deep")
	mustWrite("a/b/c/deepest.txt", "deepest")

	p, err := NewDefaultProvider(filter.NewPruner(nil, nil), provider.Options{DisableLowPriority: true})
	if err != nil {
		if errors.Is(err, provider.ErrUnsupportedOS) {
			t.Skip("no native provider on this platform")
		}
		t.Fatalf("NewDefaultProvider: %v", err)
	}

	var recs []provider.ScanRecord
	vol := provider.VolumeInfo{VolID: 1, RootPath: root, FSType: "DIR"}
	if err := p.ScanSubtreeWithRoot(context.Background(), vol, root, provider.SegmentWriterFunc(func(rec provider.ScanRecord) error {
		recs = append(recs, rec)
		return nil
	})); err != nil {
		t.Fatalf("ScanSubtreeWithRoot: %v", err)
	}
	if len(recs) < 8 {
		t.Fatalf("scan emitted %d records, want the full tree", len(recs))
	}
	head := recs[0]
	if head.ParentKey != head.NodeKey || head.Flags&storage.FlagDir == 0 {
		t.Fatalf("first record = %+v, want self-parented root directory", head)
	}

	// Every non-root record's parent must already have been emitted: this holds
	// exactly when every directory precedes its children.
	emitted := map[uint64]bool{head.NodeKey: true}
	for i, rec := range recs[1:] {
		if !emitted[rec.ParentKey] {
			t.Fatalf("record %d %q has parent key %d not emitted yet", i+1, rec.Name, rec.ParentKey)
		}
		emitted[rec.NodeKey] = true
	}
	for _, want := range []string{"top.txt", "mid.txt", "deep.txt", "deepest.txt", "a", "b", "c"} {
		found := false
		for _, rec := range recs {
			if rec.Name == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("scan missing %q", want)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.ScanSubtreeWithRoot(ctx, vol, root, provider.SegmentWriterFunc(func(provider.ScanRecord) error { return nil })); err == nil {
		t.Fatal("cancelled scan must return an error")
	}
}
