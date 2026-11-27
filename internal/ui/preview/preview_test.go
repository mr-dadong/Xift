package preview

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestTextPreview64KiBCapAndBinarySniff(t *testing.T) {
	dir := t.TempDir()

	// Oversized 128 KiB text file -> capped at 64 KiB with Truncated=true.
	bigPath := filepath.Join(dir, "huge.log")
	if err := os.WriteFile(bigPath, bytes.Repeat([]byte("A"), 128*1024), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := Config{}
	cfg.normalize()
	res := defaultLoadPreview(context.Background(), bigPath, false, cfg)
	if res.Kind != KindText || !res.Truncated || len(res.Text) != 64*1024 {
		t.Fatalf("unexpected text preview: kind=%v truncated=%v len=%d", res.Kind, res.Truncated, len(res.Text))
	}

	// Non-UTF-8 (Latin-1 / GBK-like) >64 KiB file must NOT be wiped to empty.
	gbkPath := filepath.Join(dir, "legacy_gbk.txt")
	if err := os.WriteFile(gbkPath, bytes.Repeat([]byte{0xC4, 0xE3, 0xBA, 0xC3}, 20*1024), 0o644); err != nil {
		t.Fatal(err)
	}
	resGBK := defaultLoadPreview(context.Background(), gbkPath, false, cfg)
	if resGBK.Kind != KindText || !resGBK.Truncated || len(resGBK.Text) < 60*1024 {
		t.Fatalf("legacy non-UTF-8 file must retain preview bytes: kind=%v len=%d", resGBK.Kind, len(resGBK.Text))
	}

	// Binary file -> KindBinary.
	binPath := filepath.Join(dir, "blob.dat")
	if err := os.WriteFile(binPath, []byte{0x7f, 'E', 'L', 'F', 0x00, 0x02}, 0o644); err != nil {
		t.Fatal(err)
	}
	resBin := defaultLoadPreview(context.Background(), binPath, false, cfg)
	if resBin.Kind != KindBinary {
		t.Fatalf("expected KindBinary, got %v", resBin.Kind)
	}
}

func TestArchiveImageAndPDFPreviews(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{}
	cfg.normalize()

	// 1. ZIP with 520 entries -> capped at 500 entries, header-only.
	zipPath := filepath.Join(dir, "pkg.zip")
	zf, err := os.Create(zipPath)
	if err != nil {
		t.Fatal(err)
	}
	zw := zip.NewWriter(zf)
	for i := 0; i < 520; i++ {
		w, err := zw.Create(fmt.Sprintf("dir/file_%03d.txt", i))
		if err != nil {
			t.Fatal(err)
		}
		_, _ = w.Write([]byte("hi"))
	}
	_ = zw.Close()
	_ = zf.Close()

	zRes := defaultLoadPreview(context.Background(), zipPath, false, cfg)
	if zRes.Kind != KindArchive || !zRes.Truncated || len(zRes.Entries) != 500 {
		t.Fatalf("zip preview: kind=%v truncated=%v entries=%d", zRes.Kind, zRes.Truncated, len(zRes.Entries))
	}

	// 2. TAR.GZ with 3 entries.
	tgzPath := filepath.Join(dir, "bundle.tar.gz")
	tf, err := os.Create(tgzPath)
	if err != nil {
		t.Fatal(err)
	}
	gz := gzip.NewWriter(tf)
	tw := tar.NewWriter(gz)
	for _, name := range []string{"a.go", "b.go", "c.md"} {
		_ = tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: 2})
		_, _ = tw.Write([]byte("ok"))
	}
	_ = tw.Close()
	_ = gz.Close()
	_ = tf.Close()

	tRes := defaultLoadPreview(context.Background(), tgzPath, false, cfg)
	if tRes.Kind != KindArchive || len(tRes.Entries) != 3 {
		t.Fatalf("tar.gz preview: kind=%v entries=%d", tRes.Kind, len(tRes.Entries))
	}

	// 3. PNG 800x600 -> downscaled to <= 512px.
	pngPath := filepath.Join(dir, "photo.png")
	img := image.NewRGBA(image.Rect(0, 0, 800, 600))
	img.SetRGBA(0, 0, color.RGBA{R: 10, G: 20, B: 30, A: 255})
	pf, err := os.Create(pngPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := png.Encode(pf, img); err != nil {
		t.Fatal(err)
	}
	_ = pf.Close()

	iRes := defaultLoadPreview(context.Background(), pngPath, false, cfg)
	if iRes.Kind != KindImage || iRes.Image == nil || iRes.ImageW > 512 || iRes.ImageH > 512 {
		t.Fatalf("image preview: kind=%v w=%d h=%d bmp=%v", iRes.Kind, iRes.ImageW, iRes.ImageH, iRes.Image != nil)
	}

	// 4. PDF metadata parsing.
	pdfPath := filepath.Join(dir, "spec.pdf")
	pdfBytes := []byte("%PDF-1.7\n1 0 obj\n<< /Type /Pages /Count 12 >>\nendobj\n2 0 obj\n<< /Title (Xift Spec) /Author (Xift Team) >>\nendobj\n%%EOF\n")
	if err := os.WriteFile(pdfPath, pdfBytes, 0o644); err != nil {
		t.Fatal(err)
	}
	pRes := defaultLoadPreview(context.Background(), pdfPath, false, cfg)
	if pRes.Kind != KindPDF || !strings.Contains(pRes.Text, "1.7") || !strings.Contains(pRes.Text, "12") || !strings.Contains(pRes.Text, "Xift Spec") {
		t.Fatalf("pdf preview unexpected: %+v", pRes)
	}
}

func TestPreviewManagerCancellationAndStaleSuppression(t *testing.T) {
	slowCanceled := make(chan struct{}, 1)
	ready := make(chan struct{}, 8)

	mgr := NewManager(Config{
		Timeout: 500 * time.Millisecond,
		OnReady: func() {
			select {
			case ready <- struct{}{}:
			default:
			}
		},
		Loader: func(ctx context.Context, absPath string, _ bool, _ Config) Result {
			if strings.HasSuffix(absPath, "slow.log") {
				<-ctx.Done()
				slowCanceled <- struct{}{}
				return Result{Kind: KindText, Text: "STALE"}
			}
			return Result{Kind: KindText, Text: "FAST_OK"}
		},
	})
	defer mgr.Close()

	mgr.Select("/tmp/slow.log", false)
	mgr.Select("/tmp/fast.go", false)

	select {
	case <-slowCanceled:
	case <-time.After(1 * time.Second):
		t.Fatalf("expected slow preview load to be canceled on selection change")
	}

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		snap := mgr.Snapshot()
		if snap.Kind == KindText && snap.Text == "FAST_OK" {
			mgr.Clear()
			if mgr.Snapshot().Kind != KindNone {
				t.Fatalf("expected KindNone after Clear")
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("preview did not settle on FAST_OK: %+v", mgr.Snapshot())
}
