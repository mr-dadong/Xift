package cli

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/mr-dadong/Xift/internal/daemon"
	"github.com/mr-dadong/Xift/internal/query"
	"github.com/mr-dadong/Xift/internal/storage"
	"github.com/mr-dadong/Xift/internal/storage/lsm"
)

func TestCLIEndToEndIndexSearchJSONPipeAndFoldedIn(t *testing.T) {
	workspace := t.TempDir()
	dataDir := t.TempDir()

	// Create sample workspace with source files and a folded node_modules tree.
	files := map[string]string{
		"src/main.go":                      "package main\nfunc main() {}\n",
		"src/engine.rs":                    "fn main() {}\n",
		"docs/my report 2026.pdf":          strings.Repeat("x", 2048),
		"node_modules/lodash/index.js":     "module.exports = {};\n",
		"node_modules/lodash/package.json": `{"name":"lodash"}`,
	}
	for rel, content := range files {
		full := filepath.Join(workspace, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", rel, err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", rel, err)
		}
	}

	// 1. xift index --data-dir <dataDir> --json <workspace>
	var out, errBuf bytes.Buffer
	code := Run(context.Background(), []string{"index", "--data-dir", dataDir, "--json", workspace}, &out, &errBuf)
	if code != 0 {
		t.Fatalf("xift index exited %d: stderr=%s stdout=%s", code, errBuf.String(), out.String())
	}
	var idxStats IndexStats
	if err := json.Unmarshal(out.Bytes(), &idxStats); err != nil {
		t.Fatalf("unmarshal index json: %v (raw=%s)", err, out.String())
	}
	if idxStats.IndexedNodes == 0 {
		t.Fatalf("expected non-zero indexed nodes")
	}

	// 2. xift status --data-dir <dataDir> --json
	out.Reset()
	errBuf.Reset()
	code = Run(context.Background(), []string{"status", "--data-dir", dataDir, "--json"}, &out, &errBuf)
	if code != 0 {
		t.Fatalf("xift status exited %d: %s", code, errBuf.String())
	}
	var status StatusOutput
	if err := json.Unmarshal(out.Bytes(), &status); err != nil {
		t.Fatalf("unmarshal status json: %v", err)
	}
	if status.LiveCount == 0 || status.RecommendedTier != "tier1-mmap" {
		t.Fatalf("unexpected status: %+v", status)
	}
	if status.ActiveTier != "tier1-mmap" {
		t.Fatalf("active tier = %q, want tier1-mmap", status.ActiveTier)
	}
	if want := int64(lsm.DefaultBlockCacheBytes); status.BlockCacheLimit != want {
		t.Fatalf("block cache limit = %d, want default %d", status.BlockCacheLimit, want)
	}

	// 3. Default pipe mode: one path per line, no decoration.
	out.Reset()
	errBuf.Reset()
	code = Run(context.Background(), []string{"--data-dir", dataDir, "ext:go"}, &out, &errBuf)
	if code != 0 {
		t.Fatalf("xift search exited %d: %s", code, errBuf.String())
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 1 || !strings.HasSuffix(filepath.ToSlash(lines[0]), "src/main.go") {
		t.Fatalf("expected 1 line ending with src/main.go, got %q", out.String())
	}

	// 4. --json mode with compound query.
	out.Reset()
	errBuf.Reset()
	code = Run(context.Background(), []string{"--data-dir", dataDir, "--json", "--limit", "10", "*.go | *.rs"}, &out, &errBuf)
	if code != 0 {
		t.Fatalf("xift --json exited %d: %s", code, errBuf.String())
	}
	var sr query.SearchResult
	if err := json.Unmarshal(out.Bytes(), &sr); err != nil {
		t.Fatalf("unmarshal search json: %v (raw=%s)", err, out.String())
	}
	if sr.Total != 2 || sr.Returned != 2 {
		t.Fatalf("expected 2 results for '*.go | *.rs', got total=%d returned=%d", sr.Total, sr.Returned)
	}

	// 5. -0 NUL-delimited output for filenames with spaces.
	out.Reset()
	errBuf.Reset()
	code = Run(context.Background(), []string{"--data-dir", dataDir, "-0", "my report"}, &out, &errBuf)
	if code != 0 {
		t.Fatalf("xift -0 exited %d: %s", code, errBuf.String())
	}
	rawBytes := out.Bytes()
	if len(rawBytes) == 0 || rawBytes[len(rawBytes)-1] != 0 {
		t.Fatalf("expected NUL-terminated output, got %q", string(rawBytes))
	}
	if !strings.Contains(string(rawBytes), "my report 2026.pdf") {
		t.Fatalf("expected my report 2026.pdf in NUL output, got %q", string(rawBytes))
	}

	// 6. --jsonl mode.
	out.Reset()
	errBuf.Reset()
	code = Run(context.Background(), []string{"--data-dir", dataDir, "--jsonl", "main.go"}, &out, &errBuf)
	if code != 0 {
		t.Fatalf("xift --jsonl exited %d: %s", code, errBuf.String())
	}
	var item query.ResultItem
	if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &item); err != nil {
		t.Fatalf("unmarshal jsonl line: %v", err)
	}
	if item.Name != "main.go" {
		t.Fatalf("expected main.go, got %+v", item)
	}

	// 7. --count mode.
	out.Reset()
	errBuf.Reset()
	code = Run(context.Background(), []string{"--data-dir", dataDir, "--count", "*.go | *.rs"}, &out, &errBuf)
	if code != 0 || strings.TrimSpace(out.String()) != "2" {
		t.Fatalf("expected count=2, got code=%d out=%q", code, out.String())
	}

	// 8. Verify node_modules was folded (children NOT in index) AND `in:node_modules lodash` finds it!
	out.Reset()
	errBuf.Reset()
	code = Run(context.Background(), []string{"--data-dir", dataDir, "--count", "lodash"}, &out, &errBuf)
	if code != 0 || strings.TrimSpace(out.String()) != "0" {
		t.Fatalf("expected lodash NOT in default index (folded), got code=%d out=%q", code, out.String())
	}

	out.Reset()
	errBuf.Reset()
	code = Run(context.Background(), []string{"--data-dir", dataDir, "--json", "in:node_modules lodash ext:js"}, &out, &errBuf)
	if code != 0 {
		t.Fatalf("xift in:node_modules exited %d: %s", code, errBuf.String())
	}
	var inRes query.SearchResult
	if err := json.Unmarshal(out.Bytes(), &inRes); err != nil {
		t.Fatalf("unmarshal in: json: %v", err)
	}
	if inRes.Total != 1 || !inRes.Items[0].FromFoldedScan {
		t.Fatalf("expected 1 folded scan hit for lodash/index.js, got %+v", inRes)
	}
}

func uniqueCLIEndpoint(t *testing.T) string {
	t.Helper()
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS == "windows" {
		return `\\.\pipe\xift-cli-test-` + hex.EncodeToString(b[:])
	}
	dir := filepath.Join(t.TempDir(), "run")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(dir, "daemon.sock")
}

func TestCLIDaemonModeMatchesLocalAndUnreachableFailsLoudly(t *testing.T) {
	dataDir := t.TempDir()
	// Seed 1,300 files so we can test --limit 1200 auto-batching across the 500-row RPC cap.
	store, err := storage.Open(dataDir, storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	b, err := store.BeginDenseSegment()
	if err != nil {
		t.Fatal(err)
	}
	rootID, err := b.AppendDense(1, 1, storage.RootParentIdx, "repo", storage.FlagDir, 0, 0, 1_700_000_000)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1300; i++ {
		name := fmt.Sprintf("item_%04d.go", i)
		if i == 0 {
			name = "my item 0000.go"
		}
		if _, err := b.AppendDense(1, uint64(i+2), rootID, name, 0, 0, int64((i+1)*17), int64(1_700_000_000+i)); err != nil {
			t.Fatal(err)
		}
	}
	metaSeg, err := b.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CommitSegment(metaSeg, nil); err != nil {
		t.Fatal(err)
	}
	_ = store.Close()

	// Start in-process Daemon serving dataDir.
	ep := uniqueCLIEndpoint(t)
	dCtx, dCancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	dDone := make(chan error, 1)
	go func() {
		dDone <- daemon.Run(dCtx, daemon.Config{
			DataDir:  dataDir,
			Endpoint: ep,
			OnReady:  func(string) { close(ready) },
		})
	}()
	select {
	case <-ready:
	case err := <-dDone:
		t.Fatalf("daemon exited early: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatalf("daemon not ready")
	}
	t.Cleanup(func() {
		dCancel()
		<-dDone
	})

	runCLI := func(args ...string) (int, string, string) {
		var out, errBuf bytes.Buffer
		code := Run(context.Background(), args, &out, &errBuf)
		return code, out.String(), errBuf.String()
	}

	// Compare local vs daemon byte-for-byte across plain, -0, -c, --jsonl, --limit 1200, --offset, --sort size --desc.
	byteExactCases := [][]string{
		{"--limit", "1200", "ext:go"},
		{"--offset", "15", "--limit", "25", "--sort", "size", "--desc", "item"},
		{"-0", "my item"},
		{"-c", "ext:go"},
		{"--jsonl", "--offset", "5", "--limit", "10", "--sort", "name", "item"},
	}
	for _, extra := range byteExactCases {
		localArgs := append([]string{"--data-dir", dataDir}, extra...)
		daemonArgs := append([]string{"--daemon", "--endpoint", ep}, extra...)

		lc, lOut, lErr := runCLI(localArgs...)
		dc, dOut, dErr := runCLI(daemonArgs...)
		if lc != 0 || dc != 0 {
			t.Fatalf("args=%v: local(%d, %s) daemon(%d, %s)", extra, lc, lErr, dc, dErr)
		}
		if lOut != dOut {
			t.Fatalf("args=%v output mismatch:\nlocal  len=%d\ndaemon len=%d", extra, len(lOut), len(dOut))
		}
	}

	// Compare --json (ignoring CostMs timing field).
	lc, lOut, lErr := runCLI("--data-dir", dataDir, "--json", "--limit", "20", "--sort", "size", "--desc", "ext:go")
	dc, dOut, dErr := runCLI("--daemon", "--endpoint", ep, "--json", "--limit", "20", "--sort", "size", "--desc", "ext:go")
	if lc != 0 || dc != 0 {
		t.Fatalf("--json exit codes: local(%d, %s) daemon(%d, %s)", lc, lErr, dc, dErr)
	}
	var lRes, dRes query.SearchResult
	if err := json.Unmarshal([]byte(lOut), &lRes); err != nil {
		t.Fatalf("unmarshal local json: %v", err)
	}
	if err := json.Unmarshal([]byte(dOut), &dRes); err != nil {
		t.Fatalf("unmarshal daemon json: %v", err)
	}
	lRes.CostMs, dRes.CostMs = 0, 0
	if !reflect.DeepEqual(lRes, dRes) {
		t.Fatalf("--json SearchResult mismatch:\nlocal:  %+v\ndaemon: %+v", lRes, dRes)
	}

	// xift status --daemon --endpoint E --json
	sc, sOut, sErr := runCLI("status", "--daemon", "--endpoint", ep, "--json")
	if sc != 0 {
		t.Fatalf("status --daemon exited %d: %s", sc, sErr)
	}
	var dst DaemonStatusOutput
	if err := json.Unmarshal([]byte(sOut), &dst); err != nil {
		t.Fatalf("unmarshal daemon status: %v (raw=%s)", err, sOut)
	}
	if dst.LiveCount != 1301 || dst.ProtocolVersion != 1 {
		t.Fatalf("unexpected daemon status: %+v", dst)
	}

	// Unreachable daemon must exit 1, mention endpoint in stderr, and NOT fall back to local mode.
	deadEP := uniqueCLIEndpoint(t)
	uc, _, uErr := runCLI("--daemon", "--endpoint", deadEP, "--data-dir", dataDir, "ext:go")
	if uc != 1 {
		t.Fatalf("unreachable daemon exited %d, want 1", uc)
	}
	if !strings.Contains(uErr, deadEP) {
		t.Fatalf("unreachable daemon stderr %q does not mention endpoint %q", uErr, deadEP)
	}
}
