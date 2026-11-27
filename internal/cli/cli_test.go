package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mr-dadong/Xift/internal/query"
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
