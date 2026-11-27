package filter

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

const (
	// MaxIgnoreFileBytes caps the maximum size of a single .gitignore or .xiftignore file (1 MiB)
	// to protect the daemon against malformed or adversarial files.
	MaxIgnoreFileBytes = 1 << 20
)

var (
	// ErrIgnoreFileTooLarge is returned when an ignore file exceeds MaxIgnoreFileBytes.
	ErrIgnoreFileTooLarge = errors.New("filter: ignore file exceeds size limit")
	// ErrNotRegularIgnoreFile is returned when an ignore path points to a non-regular file (e.g. FIFO/device).
	ErrNotRegularIgnoreFile = errors.New("filter: ignore path is not a regular file")
)

// IgnoreKind distinguishes .gitignore from .xiftignore files.
type IgnoreKind uint8

const (
	// IgnoreKindGit represents a standard .gitignore file.
	IgnoreKindGit IgnoreKind = iota
	// IgnoreKindXift represents a Xift-specific .xiftignore file (higher priority than .gitignore).
	IgnoreKindXift
)

// IgnoreMatch is the tri-state result of matching a path against an IgnoreMatcher.
type IgnoreMatch uint8

const (
	IgnoreUnmatched IgnoreMatch = iota
	IgnoreMatched
	IgnoreNegated
)

func (m IgnoreMatch) String() string {
	switch m {
	case IgnoreUnmatched:
		return "unmatched"
	case IgnoreMatched:
		return "matched"
	case IgnoreNegated:
		return "negated"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(m))
	}
}

var (
	defaultProjectMarkers = []string{
		"package.json",
		"Cargo.toml",
		"go.mod",
		"pyproject.toml",
		".git",
		"pom.xml",
		"build.gradle",
		"build.gradle.kts",
		"CMakeLists.txt",
		"Makefile",
		"composer.json",
		"Gemfile",
		"mix.exs",
		"setup.py",
		"deno.json",
	}
	projectMarkerSetExact = func() map[string]struct{} {
		m := make(map[string]struct{}, len(defaultProjectMarkers))
		for _, name := range defaultProjectMarkers {
			m[name] = struct{}{}
		}
		return m
	}()
	projectMarkerSetLower = func() map[string]struct{} {
		m := make(map[string]struct{}, len(defaultProjectMarkers))
		for _, name := range defaultProjectMarkers {
			m[strings.ToLower(name)] = struct{}{}
		}
		return m
	}()
)

// DefaultProjectMarkers returns a copy of built-in project root marker file/directory names.
func DefaultProjectMarkers() []string {
	out := make([]string, len(defaultProjectMarkers))
	copy(out, defaultProjectMarkers)
	return out
}

// IsProjectMarker reports whether name is a recognized project root marker (e.g. package.json, go.mod, .git).
func IsProjectMarker(name string, caseInsensitive bool) bool {
	if name == "" {
		return false
	}
	if !caseInsensitive {
		_, ok := projectMarkerSetExact[name]
		return ok
	}
	// Stack-allocated ASCII lowercase lookup for zero heap allocations.
	needsLower := false
	nonASCII := false
	for i := 0; i < len(name); i++ {
		c := name[i]
		if c >= 0x80 {
			nonASCII = true
			break
		}
		if c >= 'A' && c <= 'Z' {
			needsLower = true
		}
	}
	if !needsLower && !nonASCII {
		_, ok := projectMarkerSetLower[name]
		return ok
	}
	if !nonASCII && len(name) <= 64 {
		var buf [64]byte
		for i := 0; i < len(name); i++ {
			c := name[i]
			if c >= 'A' && c <= 'Z' {
				c += 'a' - 'A'
			}
			buf[i] = c
		}
		_, ok := projectMarkerSetLower[string(buf[:len(name)])]
		return ok
	}
	_, ok := projectMarkerSetLower[strings.ToLower(name)]
	return ok
}

// DetectProjectRoot reports whether any entry name in a directory indicates a project boundary.
func DetectProjectRoot(entries []string, caseInsensitive bool) bool {
	for _, name := range entries {
		if IsProjectMarker(name, caseInsensitive) {
			return true
		}
	}
	return false
}

type ignoreRule struct {
	pattern  string
	negated  bool
	dirOnly  bool
	anchored bool
	literal  bool // true when pattern contains no glob metacharacters (*, ?, [, \)
}

// IgnoreMatcher holds parsed rules from a single .gitignore or .xiftignore file scoped to baseRelDir.
type IgnoreMatcher struct {
	kind            IgnoreKind
	baseRelDir      string // normalized slash path without leading/trailing slash (empty = volume root)
	caseInsensitive bool
	rules           []ignoreRule
}

// Kind returns whether this matcher originated from .gitignore or .xiftignore.
func (m *IgnoreMatcher) Kind() IgnoreKind {
	return m.kind
}

// BaseRelDir returns the normalized directory path this matcher applies to.
func (m *IgnoreMatcher) BaseRelDir() string {
	return m.baseRelDir
}

// RuleCount returns the number of active rules parsed from the file.
func (m *IgnoreMatcher) RuleCount() int {
	return len(m.rules)
}

// LoadIgnoreFile opens filePath read-only and parses it with a strict regular-file check and MaxIgnoreFileBytes guard.
func LoadIgnoreFile(filePath, baseRelDir string, kind IgnoreKind, caseInsensitive bool) (*IgnoreMatcher, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("filter: stat ignore file %s: %w", filePath, err)
	}
	if !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: %s", ErrNotRegularIgnoreFile, filePath)
	}
	if fi.Size() > MaxIgnoreFileBytes {
		return nil, fmt.Errorf("%w: %s (%d > %d bytes)", ErrIgnoreFileTooLarge, filePath, fi.Size(), MaxIgnoreFileBytes)
	}

	raw, err := io.ReadAll(io.LimitReader(f, MaxIgnoreFileBytes+1))
	if err != nil {
		return nil, fmt.Errorf("filter: read ignore file %s: %w", filePath, err)
	}
	if len(raw) > MaxIgnoreFileBytes {
		return nil, fmt.Errorf("%w: %s (> %d bytes)", ErrIgnoreFileTooLarge, filePath, MaxIgnoreFileBytes)
	}
	return ParseIgnore(bytes.NewReader(raw), baseRelDir, kind, caseInsensitive)
}

// ParseIgnore parses .gitignore / .xiftignore syntax from r scoped to baseRelDir.
func ParseIgnore(r io.Reader, baseRelDir string, kind IgnoreKind, caseInsensitive bool) (*IgnoreMatcher, error) {
	m := &IgnoreMatcher{
		kind:            kind,
		baseRelDir:      normalizeRelPath(baseRelDir, caseInsensitive),
		caseInsensitive: caseInsensitive,
	}

	scanner := bufio.NewScanner(r)
	// Allow lines up to 64 KiB within the 1 MiB file cap.
	scanner.Buffer(make([]byte, 4096), 64*1024)
	firstLine := true
	for scanner.Scan() {
		line := scanner.Text()
		if firstLine {
			line = strings.TrimPrefix(line, "\xef\xbb\xbf") // Strip UTF-8 BOM (common on Windows)
			firstLine = false
		}
		if rule, ok := parseIgnoreLine(line, caseInsensitive); ok {
			m.rules = append(m.rules, rule)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("filter: scan ignore rules: %w", err)
	}
	return m, nil
}

func parseIgnoreLine(line string, caseInsensitive bool) (ignoreRule, bool) {
	line = strings.TrimSuffix(line, "\r")
	if line == "" {
		return ignoreRule{}, false
	}

	// Trim unescaped trailing spaces/tabs per gitignore specification.
	line = trimUnescapedTrailingSpaces(line)
	if line == "" {
		return ignoreRule{}, false
	}

	// Comment line (unescaped leading '#')
	if line[0] == '#' {
		return ignoreRule{}, false
	}

	var rule ignoreRule
	if line[0] == '!' {
		rule.negated = true
		line = line[1:]
		if line == "" {
			return ignoreRule{}, false
		}
	} else if len(line) >= 2 && line[0] == '\\' && (line[1] == '#' || line[1] == '!') {
		line = line[1:]
	}

	// Unescape "\ " while preserving escaped backslashes ("\\ ").
	line = unescapeEscapedSpaces(line)

	// Trailing slash marks directory-only rule.
	if strings.HasSuffix(line, "/") {
		rule.dirOnly = true
		line = strings.TrimRight(line, "/")
		if line == "" {
			return ignoreRule{}, false
		}
	}

	// Leading or intermediate '/' anchors the pattern relative to baseRelDir.
	if strings.HasPrefix(line, "/") {
		rule.anchored = true
		line = strings.TrimLeft(line, "/")
	} else if strings.IndexByte(line, '/') >= 0 {
		rule.anchored = true
	}

	line = collapseSlashes(line)
	line = collapseDoubleStarSegments(line)
	if line == "" {
		return ignoreRule{}, false
	}

	if caseInsensitive {
		line = strings.ToLower(line)
	}
	rule.pattern = line
	rule.literal = !hasGlobMeta(line)
	return rule, true
}

func trimUnescapedTrailingSpaces(s string) string {
	end := len(s)
	for end > 0 && (s[end-1] == ' ' || s[end-1] == '\t') {
		bs := 0
		for j := end - 2; j >= 0 && s[j] == '\\'; j-- {
			bs++
		}
		if bs%2 == 1 {
			break // Escaped space, keep it
		}
		end--
	}
	return s[:end]
}

// unescapeEscapedSpaces converts "\ " (escaped space) to " " while keeping "\\ " intact as "\\ ".
func unescapeEscapedSpaces(s string) string {
	if !strings.Contains(s, `\ `) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			if s[i+1] == '\\' {
				b.WriteString(`\\`)
				i++
				continue
			}
			if s[i+1] == ' ' {
				b.WriteByte(' ')
				i++
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

func hasGlobMeta(p string) bool {
	for i := 0; i < len(p); i++ {
		switch p[i] {
		case '*', '?', '[', '\\':
			return true
		}
	}
	return false
}

// Match evaluates relPath itself against this matcher's rules (later rules override earlier ones).
// Note: Match tests relPath directly rather than walking ancestor prefixes; ancestor pruning is
// enforced by top-down early stop, Pass 1 BFS propagation, and PrunedDirSet.
func (m *IgnoreMatcher) Match(relPath string, isDir bool) IgnoreMatch {
	if m == nil || len(m.rules) == 0 {
		return IgnoreUnmatched
	}
	norm := normalizeRelPath(relPath, m.caseInsensitive)
	if norm == "" {
		return IgnoreUnmatched
	}

	var subPath string
	if m.baseRelDir == "" {
		subPath = norm
	} else {
		if !pathHasPrefix(norm, m.baseRelDir) || len(norm) == len(m.baseRelDir) {
			return IgnoreUnmatched
		}
		subPath = norm[len(m.baseRelDir)+1:]
	}

	baseName := subPath
	if idx := strings.LastIndexByte(subPath, '/'); idx >= 0 {
		baseName = subPath[idx+1:]
	}

	result := IgnoreUnmatched
	for i := range m.rules {
		r := &m.rules[i]
		if r.dirOnly && !isDir {
			continue
		}
		var matched bool
		if !r.anchored {
			if r.literal {
				matched = baseName == r.pattern
			} else {
				matched = matchSegmentGlob(r.pattern, baseName, false)
			}
		} else {
			if r.literal {
				matched = subPath == r.pattern
			} else {
				matched = matchPathGlob(r.pattern, subPath, false)
			}
		}
		if matched {
			if r.negated {
				result = IgnoreNegated
			} else {
				result = IgnoreMatched
			}
		}
	}
	return result
}

// IgnoreScope captures IgnoreStack depths upon entering a directory so callers can restore them cleanly on exit.
type IgnoreScope struct {
	xiftDepth    int
	gitDepth     int
	projectDepth int
}

// IgnoreStack manages nested .gitignore and .xiftignore matchers and project boundary depth during traversal.
type IgnoreStack struct {
	caseInsensitive      bool
	gitignoreIgnoreFiles bool
	projectDepth         int
	xiftMatchers         []*IgnoreMatcher
	gitMatchers          []*IgnoreMatcher
}

// NewIgnoreStack creates an empty hierarchical ignore stack.
func NewIgnoreStack(caseInsensitive, gitignoreIgnoreFiles bool) *IgnoreStack {
	return &IgnoreStack{
		caseInsensitive:      caseInsensitive,
		gitignoreIgnoreFiles: gitignoreIgnoreFiles,
	}
}

// InProject reports whether the current traversal scope is inside a detected project root.
func (s *IgnoreStack) InProject() bool {
	return s != nil && s.projectDepth > 0
}

// EnterProject increments the active project boundary depth.
func (s *IgnoreStack) EnterProject() {
	if s != nil {
		s.projectDepth++
	}
}

// Push appends a matcher to the stack in shallow-to-deep order.
func (s *IgnoreStack) Push(m *IgnoreMatcher) {
	if m == nil || len(m.rules) == 0 {
		return
	}
	if m.kind == IgnoreKindXift {
		s.xiftMatchers = append(s.xiftMatchers, m)
	} else {
		s.gitMatchers = append(s.gitMatchers, m)
	}
}

// SaveScope captures the current stack and project boundary state.
func (s *IgnoreStack) SaveScope() IgnoreScope {
	if s == nil {
		return IgnoreScope{}
	}
	return IgnoreScope{
		xiftDepth:    len(s.xiftMatchers),
		gitDepth:     len(s.gitMatchers),
		projectDepth: s.projectDepth,
	}
}

// RestoreScope restores the stack and project boundary state back to scope.
func (s *IgnoreStack) RestoreScope(scope IgnoreScope) {
	if s == nil {
		return
	}
	s.Truncate(scope.xiftDepth, scope.gitDepth)
	if scope.projectDepth < s.projectDepth {
		s.projectDepth = scope.projectDepth
	}
}

// Depth returns the current number of matchers in (xift, git) stacks for scoped Truncate restoring.
func (s *IgnoreStack) Depth() (xiftDepth, gitDepth int) {
	return len(s.xiftMatchers), len(s.gitMatchers)
}

// Truncate restores the matcher slices back to previously saved depths.
func (s *IgnoreStack) Truncate(xiftDepth, gitDepth int) {
	if xiftDepth < len(s.xiftMatchers) {
		for i := xiftDepth; i < len(s.xiftMatchers); i++ {
			s.xiftMatchers[i] = nil
		}
		s.xiftMatchers = s.xiftMatchers[:xiftDepth]
	}
	if gitDepth < len(s.gitMatchers) {
		for i := gitDepth; i < len(s.gitMatchers); i++ {
			s.gitMatchers[i] = nil
		}
		s.gitMatchers = s.gitMatchers[:gitDepth]
	}
}

// Clone returns a shallow copy of the stack slices for independent branch traversal.
func (s *IgnoreStack) Clone() *IgnoreStack {
	if s == nil {
		return nil
	}
	cp := &IgnoreStack{
		caseInsensitive:      s.caseInsensitive,
		gitignoreIgnoreFiles: s.gitignoreIgnoreFiles,
		projectDepth:         s.projectDepth,
	}
	if len(s.xiftMatchers) > 0 {
		cp.xiftMatchers = append([]*IgnoreMatcher(nil), s.xiftMatchers...)
	}
	if len(s.gitMatchers) > 0 {
		cp.gitMatchers = append([]*IgnoreMatcher(nil), s.gitMatchers...)
	}
	return cp
}

// Evaluate checks nested .xiftignore layers first (higher precedence), then .gitignore layers.
// Returns (action, decided, fromXiftIgnore).
func (s *IgnoreStack) Evaluate(relPath string, isDir bool) (Action, bool, bool) {
	if s == nil {
		return ActionKeep, false, false
	}

	// 1. Evaluate .xiftignore layers shallow -> deep (deeper overrides shallower).
	xiftState := IgnoreUnmatched
	for _, m := range s.xiftMatchers {
		if st := m.Match(relPath, isDir); st != IgnoreUnmatched {
			xiftState = st
		}
	}
	switch xiftState {
	case IgnoreNegated:
		return ActionKeep, true, true
	case IgnoreMatched:
		if isDir {
			return ActionFold, true, true
		}
		return ActionHardExclude, true, true
	}

	// 2. Evaluate .gitignore layers shallow -> deep (deeper overrides shallower).
	gitState := IgnoreUnmatched
	for _, m := range s.gitMatchers {
		if st := m.Match(relPath, isDir); st != IgnoreUnmatched {
			gitState = st
		}
	}
	switch gitState {
	case IgnoreNegated:
		return ActionKeep, true, false
	case IgnoreMatched:
		if isDir {
			return ActionFold, true, false
		}
		if s.gitignoreIgnoreFiles {
			return ActionHardExclude, true, false
		}
		return ActionKeep, false, false
	}

	return ActionKeep, false, false
}
