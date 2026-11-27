package filter

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestProjectMarkerDetection(t *testing.T) {
	for _, marker := range []string{"package.json", "Cargo.toml", "go.mod", "pyproject.toml", ".git", "pom.xml"} {
		if !IsProjectMarker(marker, true) {
			t.Errorf("IsProjectMarker(%q, ci=true) = false, want true", marker)
		}
	}
	if !IsProjectMarker("PACKAGE.JSON", true) {
		t.Errorf("case-insensitive PACKAGE.JSON should be detected")
	}
	if IsProjectMarker("PACKAGE.JSON", false) {
		t.Errorf("case-sensitive PACKAGE.JSON should not be detected")
	}
	if IsProjectMarker("README.md", true) {
		t.Errorf("README.md should not be a project marker")
	}
	if !DetectProjectRoot([]string{"README.md", "src", "go.mod"}, false) {
		t.Errorf("DetectProjectRoot with go.mod = false, want true")
	}
	if DetectProjectRoot([]string{"README.md", "docs"}, true) {
		t.Errorf("DetectProjectRoot without markers = true, want false")
	}
}

func TestIgnoreGitSyntaxAndSemantics(t *testing.T) {
	content := strings.Join([]string{
		"# comment line",
		"",
		`\#literal_hash`,
		"trailing_space   ",
		`escaped_space\ `,
		"coverage/",
		"/root_only_dir/",
		"sub/anchored_dir/",
		"**/deep_cache/",
		"logs/**/archive/",
		"*.log",
		".env",
		"!important.log",
		"gen_[0-9]/",
		"out/",
		"!out/",
	}, "\n")

	m, err := ParseIgnore(strings.NewReader(content), "proj", IgnoreKindGit, false)
	if err != nil {
		t.Fatalf("ParseIgnore: %v", err)
	}

	cases := []struct {
		relPath string
		isDir   bool
		want    IgnoreMatch
	}{
		{"proj/#literal_hash", false, IgnoreMatched},
		{"proj/trailing_space", false, IgnoreMatched},
		{"proj/escaped_space ", false, IgnoreMatched},
		{"proj/coverage", true, IgnoreMatched},
		{"proj/sub/coverage", true, IgnoreMatched},
		{"proj/coverage", false, IgnoreUnmatched}, // trailing '/' is dir-only
		{"proj/root_only_dir", true, IgnoreMatched},
		{"proj/sub/root_only_dir", true, IgnoreUnmatched}, // leading '/' anchored to proj
		{"proj/sub/anchored_dir", true, IgnoreMatched},
		{"proj/other/sub/anchored_dir", true, IgnoreUnmatched}, // middle '/' anchored to proj
		{"proj/a/b/deep_cache", true, IgnoreMatched},
		{"proj/deep_cache", true, IgnoreMatched},
		{"proj/logs/archive", true, IgnoreMatched},
		{"proj/logs/2026/11/archive", true, IgnoreMatched},
		{"proj/app.log", false, IgnoreMatched},
		{"proj/important.log", false, IgnoreNegated},
		{"proj/.env", false, IgnoreMatched},
		{"proj/gen_7", true, IgnoreMatched},
		{"proj/gen_a", true, IgnoreUnmatched},
		{"proj/out", true, IgnoreNegated},
		{"otherproj/coverage", true, IgnoreUnmatched}, // outside baseRelDir
	}

	for _, tc := range cases {
		if got := m.Match(tc.relPath, tc.isDir); got != tc.want {
			t.Errorf("Match(%q, isDir=%v) = %v, want %v", tc.relPath, tc.isDir, got, tc.want)
		}
	}
}

func TestIgnoreDefaultGitignoreFoldsDirsAndKeepsFiles(t *testing.T) {
	gitMatcher, err := ParseIgnore(strings.NewReader("coverage/\n*.log\n.env\n"), "proj", IgnoreKindGit, true)
	if err != nil {
		t.Fatalf("ParseIgnore: %v", err)
	}

	// Default mode (gitignoreIgnoreFiles = false): fold ignored dirs, keep regular files
	stack := NewIgnoreStack(true, false)
	stack.Push(gitMatcher)

	if act, decided, _ := stack.Evaluate("proj/coverage", true); !decided || act != ActionFold {
		t.Errorf("coverage dir = (%v, %v), want (ActionFold, true)", act, decided)
	}
	if act, decided, _ := stack.Evaluate("proj/.env", false); decided || act != ActionKeep {
		t.Errorf(".env file in default mode = (%v, %v), want (ActionKeep, false)", act, decided)
	}
	if act, decided, _ := stack.Evaluate("proj/debug.log", false); decided || act != ActionKeep {
		t.Errorf("debug.log in default mode = (%v, %v), want (ActionKeep, false)", act, decided)
	}

	// Opt-in file ignore mode (gitignoreIgnoreFiles = true)
	strictStack := NewIgnoreStack(true, true)
	strictStack.Push(gitMatcher)
	if act, decided, _ := strictStack.Evaluate("proj/.env", false); !decided || act != ActionHardExclude {
		t.Errorf(".env file in strict mode = (%v, %v), want (ActionHardExclude, true)", act, decided)
	}
}

func TestIgnoreXiftignoreOverridesGitignoreAndSupportsNegation(t *testing.T) {
	gitM, err := ParseIgnore(strings.NewReader("out/\ngenerated/\n"), "proj", IgnoreKindGit, true)
	if err != nil {
		t.Fatalf("ParseIgnore git: %v", err)
	}
	xiftM, err := ParseIgnore(strings.NewReader("!generated/\n*.gen.tmp\n"), "proj", IgnoreKindXift, true)
	if err != nil {
		t.Fatalf("ParseIgnore xift: %v", err)
	}

	stack := NewIgnoreStack(true, false)
	stack.Push(gitM)
	stack.Push(xiftM)

	if act, decided, fromXift := stack.Evaluate("proj/out", true); !decided || act != ActionFold || fromXift {
		t.Errorf("proj/out = (%v, %v, %v), want (ActionFold, true, false)", act, decided, fromXift)
	}
	if act, decided, fromXift := stack.Evaluate("proj/generated", true); !decided || act != ActionKeep || !fromXift {
		t.Errorf("proj/generated = (%v, %v, %v), want (ActionKeep, true, true)", act, decided, fromXift)
	}
	if act, decided, fromXift := stack.Evaluate("proj/cache.gen.tmp", false); !decided || act != ActionHardExclude || !fromXift {
		t.Errorf("proj/cache.gen.tmp = (%v, %v, %v), want (ActionHardExclude, true, true)", act, decided, fromXift)
	}
}

func TestIgnoreNestedInheritanceAndSiblingIsolation(t *testing.T) {
	rootGit, err := ParseIgnore(strings.NewReader("artifacts/\n"), "repo", IgnoreKindGit, false)
	if err != nil {
		t.Fatalf("root ParseIgnore: %v", err)
	}
	subGit, err := ParseIgnore(strings.NewReader("!artifacts/\nlocal_build/\n"), "repo/packages/app", IgnoreKindGit, false)
	if err != nil {
		t.Fatalf("sub ParseIgnore: %v", err)
	}

	stack := NewIgnoreStack(false, false)
	stack.Push(rootGit)
	stack.Push(subGit)

	// Subpackage overrides parent rule
	if act, decided, _ := stack.Evaluate("repo/packages/app/artifacts", true); !decided || act != ActionKeep {
		t.Errorf("subpackage artifacts = (%v, %v), want (ActionKeep, true)", act, decided)
	}
	if act, decided, _ := stack.Evaluate("repo/packages/app/local_build", true); !decided || act != ActionFold {
		t.Errorf("subpackage local_build = (%v, %v), want (ActionFold, true)", act, decided)
	}

	// Sibling package still inherits root rule and is unaffected by app's .gitignore
	if act, decided, _ := stack.Evaluate("repo/packages/lib/artifacts", true); !decided || act != ActionFold {
		t.Errorf("sibling artifacts = (%v, %v), want (ActionFold, true)", act, decided)
	}
	if act, decided, _ := stack.Evaluate("repo/packages/lib/local_build", true); decided || act != ActionKeep {
		t.Errorf("sibling local_build = (%v, %v), want (ActionKeep, false)", act, decided)
	}
}

func TestIgnoreFileSizeLimitAndPathologicalGlobProtection(t *testing.T) {
	dir := t.TempDir()
	bigPath := filepath.Join(dir, ".gitignore")
	bigData := bytes.Repeat([]byte("a"), MaxIgnoreFileBytes+64)
	if err := os.WriteFile(bigPath, bigData, 0o644); err != nil {
		t.Fatalf("write oversized ignore file: %v", err)
	}

	if _, err := LoadIgnoreFile(bigPath, "proj", IgnoreKindGit, true); !errors.Is(err, ErrIgnoreFileTooLarge) {
		t.Fatalf("LoadIgnoreFile oversized = %v, want ErrIgnoreFileTooLarge", err)
	}

	// Pathological glob pattern (anti-ReDoS) must complete well within milliseconds
	evilPattern := strings.Repeat("**/", 20) + strings.Repeat("*a", 20) + "b"
	m, err := ParseIgnore(strings.NewReader(evilPattern+"\n"), "proj", IgnoreKindGit, false)
	if err != nil {
		t.Fatalf("ParseIgnore evil pattern: %v", err)
	}
	target := "proj/" + strings.Repeat("dir/", 20) + strings.Repeat("a", 60) + "c"

	start := time.Now()
	if got := m.Match(target, false); got != IgnoreUnmatched {
		t.Errorf("evil pattern unexpectedly matched: %v", got)
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Errorf("pathological glob took %v (possible exponential backtracking)", elapsed)
	}
}
