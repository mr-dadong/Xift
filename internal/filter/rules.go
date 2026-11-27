package filter

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"
)

// Action defines the 3-level noise governance decision for a directory or file.
type Action uint8

const (
	// ActionKeep keeps the node normally in the index.
	ActionKeep Action = iota
	// ActionHardExclude (Level 0) drops the node (and its entire subtree for directories) completely.
	ActionHardExclude
	// ActionFold (Level 1) retains only the directory shell with FlagFolded and drops all descendants.
	ActionFold
	// ActionSoftDemote (Level 2) retains the node and descendants with FlagSystem set.
	ActionSoftDemote
)

// Node flag bits aligned with storage.HotNode.Flags (verified by cross-package tests).
const (
	FlagDir    uint16 = 1 << 0
	FlagFolded uint16 = 1 << 5
	FlagSystem uint16 = 1 << 6
)

func (a Action) String() string {
	switch a {
	case ActionKeep:
		return "keep"
	case ActionHardExclude:
		return "hard_exclude"
	case ActionFold:
		return "fold"
	case ActionSoftDemote:
		return "soft_demote"
	default:
		return fmt.Sprintf("unknown_action(%d)", uint8(a))
	}
}

func (a Action) Valid() bool {
	return a <= ActionSoftDemote
}

func (a Action) MarshalJSON() ([]byte, error) {
	if !a.Valid() {
		return nil, fmt.Errorf("filter: invalid action %d", uint8(a))
	}
	return json.Marshal(a.String())
}

func (a *Action) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("filter: invalid action JSON: %w", err)
	}
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "keep":
		*a = ActionKeep
	case "hard_exclude":
		*a = ActionHardExclude
	case "fold":
		*a = ActionFold
	case "soft_demote":
		*a = ActionSoftDemote
	default:
		return fmt.Errorf("filter: unknown action %q", s)
	}
	return nil
}

// RuleKind specifies what a UserRule matches against.
type RuleKind string

const (
	// RuleKindPathPrefix matches an exact normalized volume-relative path or any descendant under it.
	RuleKindPathPrefix RuleKind = "path_prefix"
	// RuleKindGlob matches a glob pattern against the normalized volume-relative path (or basename if pattern has no '/').
	RuleKindGlob RuleKind = "glob"
	// RuleKindDirName matches a directory basename.
	RuleKindDirName RuleKind = "dir_name"
	// RuleKindExt matches a file extension (case-normalized, with or without leading '.').
	RuleKindExt RuleKind = "ext"
)

func (k RuleKind) Valid() bool {
	switch k {
	case RuleKindPathPrefix, RuleKindGlob, RuleKindDirName, RuleKindExt:
		return true
	default:
		return false
	}
}

// UserRule is a user-defined override rule with higher precedence than built-in presets and ignore files.
type UserRule struct {
	ID       string   `json:"id"`
	Kind     RuleKind `json:"kind"`
	Pattern  string   `json:"pattern"`
	Action   Action   `json:"action"`
	Enabled  bool     `json:"enabled"`
	Priority int      `json:"priority,omitempty"`
}

// RuleEngineConfig configures a RuleEngine instance.
type RuleEngineConfig struct {
	// CaseInsensitive overrides platform default case sensitivity when non-nil.
	CaseInsensitive *bool
	// EnabledProfiles overrides default profile states when non-nil. Unlisted profiles keep their DefaultEnabled state.
	EnabledProfiles map[ProfileID]bool
	// GitignoreIgnoreFiles controls whether .gitignore also excludes matched regular files.
	// Defaults to false so .gitignore folds ignored directories without hiding uncommitted user files.
	GitignoreIgnoreFiles bool
	// ExcludeNTFSMetaFiles controls whether files starting with '$' (e.g. $MFT, $LogFile) are hard-excluded.
	// Defaults to true when nil.
	ExcludeNTFSMetaFiles *bool
}

var (
	// ErrInvalidRule is returned when a UserRule has invalid kind, action, or degenerate pattern.
	ErrInvalidRule = errors.New("filter: invalid user rule")
	// ErrCorruptRulesFile is returned when a persisted user rules JSON file cannot be parsed or validated.
	ErrCorruptRulesFile = errors.New("filter: corrupt rules file")
)

const (
	userRulesSchemaVersion = 1
	autoRuleIDPrefix       = "user-rule-"
)

type persistedRulesFile struct {
	Version int        `json:"version"`
	Rules   []UserRule `json:"rules"`
}

type compiledPathRule struct {
	prefix   string // normalized slash path without leading/trailing slash (lowercased if CI)
	action   Action
	priority int
	order    int
}

type compiledGlobRule struct {
	pattern  string // normalized pattern (lowercased if CI)
	hasSlash bool
	action   Action
	priority int
	order    int
}

type compiledRules struct {
	caseInsensitive      bool
	excludeNTFSMetaFiles bool
	gitignoreIgnoreFiles bool

	// User rules (highest priority)
	userPathPrefixes []compiledPathRule // sorted by (priority desc, len(prefix) desc, order desc)
	userGlobs        []compiledGlobRule // sorted by (priority desc, order desc)
	userDirNames     map[string]Action
	userExts         map[string]Action // keys include leading '.', lowercased

	// Built-in preset rules
	presetDirs     map[string]Action // unanchored dir name -> strictest Action
	presetRootDirs map[string]Action // depth == 1 dir name -> strictest Action
	presetExts     map[string]Action // leading '.', lowercased -> Action
}

// RuleEngine evaluates 3-level noise governance rules with lock-free hot-path reads
// backed by an atomically swapped immutable compiled snapshot.
// Lock ordering invariant: persistMu (if acquired) is always acquired before mu.
type RuleEngine struct {
	persistMu            sync.Mutex
	mu                   sync.Mutex
	caseInsensitive      bool
	excludeNTFSMetaFiles bool
	gitignoreIgnoreFiles bool
	profiles             []Profile
	profileEnabled       map[ProfileID]bool
	userRules            []UserRule
	nextAutoID           uint64
	snap                 atomic.Pointer[compiledRules]
}

// NewRuleEngine creates a RuleEngine with the specified configuration.
func NewRuleEngine(cfg RuleEngineConfig) *RuleEngine {
	ci := DefaultCaseInsensitive()
	if cfg.CaseInsensitive != nil {
		ci = *cfg.CaseInsensitive
	}
	excludeMeta := true
	if cfg.ExcludeNTFSMetaFiles != nil {
		excludeMeta = *cfg.ExcludeNTFSMetaFiles
	}

	profiles := BuiltInProfiles()
	enabled := make(map[ProfileID]bool, len(profiles))
	for _, p := range profiles {
		enabled[p.ID] = p.DefaultEnabled
	}
	for id, on := range cfg.EnabledProfiles {
		enabled[id] = on
	}

	eng := &RuleEngine{
		caseInsensitive:      ci,
		excludeNTFSMetaFiles: excludeMeta,
		gitignoreIgnoreFiles: cfg.GitignoreIgnoreFiles,
		profiles:             profiles,
		profileEnabled:       enabled,
	}
	eng.recompileLocked()
	return eng
}

// DefaultRuleEngine returns a RuleEngine with default cross-platform settings.
func DefaultRuleEngine() *RuleEngine {
	return NewRuleEngine(RuleEngineConfig{})
}

// CaseInsensitive reports whether the engine uses case-insensitive matching.
func (e *RuleEngine) CaseInsensitive() bool {
	return e.caseInsensitive
}

// GitignoreIgnoreFiles reports whether .gitignore should exclude regular files in addition to folding directories.
func (e *RuleEngine) GitignoreIgnoreFiles() bool {
	return e.gitignoreIgnoreFiles
}

// SetProfileEnabled toggles a built-in profile and atomically publishes a new rule snapshot.
func (e *RuleEngine) SetProfileEnabled(id ProfileID, enabled bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.profileEnabled[id] = enabled
	e.recompileLocked()
}

// ProfileEnabled reports whether a built-in profile is currently enabled.
func (e *RuleEngine) ProfileEnabled(id ProfileID) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.profileEnabled[id]
}

// AddUserRule adds or replaces a user rule (matched by ID if non-empty) and atomically recompiles.
func (e *RuleEngine) AddUserRule(rule UserRule) error {
	if err := validateUserRule(rule); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	if rule.ID == "" {
		rule.ID = e.allocateAutoIDLocked()
	} else {
		e.advanceAutoIDPastLocked(rule.ID)
	}
	replaced := false
	for i := range e.userRules {
		if e.userRules[i].ID == rule.ID {
			e.userRules[i] = rule
			replaced = true
			break
		}
	}
	if !replaced {
		e.userRules = append(e.userRules, rule)
	}
	e.recompileLocked()
	return nil
}

// RemoveUserRule deletes the user rule with the given ID. Returns true if removed.
func (e *RuleEngine) RemoveUserRule(id string) bool {
	if id == "" {
		return false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for i := range e.userRules {
		if e.userRules[i].ID == id {
			e.userRules = append(e.userRules[:i], e.userRules[i+1:]...)
			e.recompileLocked()
			return true
		}
	}
	return false
}

// UserRules returns a snapshot copy of all configured user rules.
func (e *RuleEngine) UserRules() []UserRule {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]UserRule, len(e.userRules))
	copy(out, e.userRules)
	return out
}

// SaveUserRules atomically persists user rules to path as JSON.
// Serialized via persistMu so concurrent saves never overwrite newer snapshots with stale ones.
func (e *RuleEngine) SaveUserRules(path string) error {
	e.persistMu.Lock()
	defer e.persistMu.Unlock()

	e.mu.Lock()
	rulesCopy := make([]UserRule, len(e.userRules))
	copy(rulesCopy, e.userRules)
	e.mu.Unlock()

	payload := persistedRulesFile{
		Version: userRulesSchemaVersion,
		Rules:   rulesCopy,
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return fmt.Errorf("filter: marshal user rules: %w", err)
	}
	data = append(data, '\n')
	return atomicWriteFile(path, data)
}

// LoadUserRules loads and validates user rules from path, atomically replacing the active user rules.
// If the file is corrupt or invalid, existing in-memory rules remain untouched.
func (e *RuleEngine) LoadUserRules(path string) error {
	e.persistMu.Lock()
	defer e.persistMu.Unlock()

	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("filter: read rules file: %w", err)
	}
	var payload persistedRulesFile
	if err := json.Unmarshal(raw, &payload); err != nil {
		return fmt.Errorf("%w: %v", ErrCorruptRulesFile, err)
	}
	if payload.Version != userRulesSchemaVersion {
		return fmt.Errorf("%w: unsupported version %d", ErrCorruptRulesFile, payload.Version)
	}
	seenIDs := make(map[string]struct{}, len(payload.Rules))
	for i, r := range payload.Rules {
		if err := validateUserRule(r); err != nil {
			return fmt.Errorf("%w: rule[%d]: %v", ErrCorruptRulesFile, i, err)
		}
		if r.ID != "" {
			if _, dup := seenIDs[r.ID]; dup {
				return fmt.Errorf("%w: duplicate rule id %q", ErrCorruptRulesFile, r.ID)
			}
			seenIDs[r.ID] = struct{}{}
		}
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// Advance nextAutoID past every explicit auto-style ID in the loaded file before assigning missing IDs.
	for _, r := range payload.Rules {
		if r.ID != "" {
			e.advanceAutoIDPastLocked(r.ID)
		}
	}
	loaded := make([]UserRule, len(payload.Rules))
	for i, r := range payload.Rules {
		if r.ID == "" {
			for {
				e.nextAutoID++
				cand := fmt.Sprintf("%s%d", autoRuleIDPrefix, e.nextAutoID)
				if _, exists := seenIDs[cand]; !exists {
					r.ID = cand
					seenIDs[cand] = struct{}{}
					break
				}
			}
		}
		loaded[i] = r
	}
	e.userRules = loaded
	e.recompileLocked()
	return nil
}

func (e *RuleEngine) advanceAutoIDPastLocked(id string) {
	if !strings.HasPrefix(id, autoRuleIDPrefix) {
		return
	}
	numStr := id[len(autoRuleIDPrefix):]
	if n, err := strconv.ParseUint(numStr, 10, 64); err == nil && n > e.nextAutoID {
		e.nextAutoID = n
	}
}

func (e *RuleEngine) allocateAutoIDLocked() string {
	for {
		e.nextAutoID++
		cand := fmt.Sprintf("%s%d", autoRuleIDPrefix, e.nextAutoID)
		collision := false
		for i := range e.userRules {
			if e.userRules[i].ID == cand {
				collision = true
				break
			}
		}
		if !collision {
			return cand
		}
	}
}

// HasPathRules reports whether any active user path-prefix or glob rules require full relative paths.
func (e *RuleEngine) HasPathRules() bool {
	s := e.snap.Load()
	return len(s.userPathPrefixes) > 0 || len(s.userGlobs) > 0
}

// ClassifyDir returns the governance action for a directory.
// relPath should be volume-relative (e.g. "work/app/dist", without drive letter prefix).
// depth == 1 represents a directory directly under a volume root (e.g. C:\Windows, /usr).
// When depth <= 0 and relPath is non-empty, depth is inferred from relPath segments.
func (e *RuleEngine) ClassifyDir(name, relPath string, depth int) Action {
	act, _ := e.ClassifyDirWithUserMatch(name, relPath, depth)
	return act
}

// ClassifyDirWithUserMatch returns (action, matchedByUserRule).
// Used by Pruner so user rules outrank .xiftignore/.gitignore while built-in presets follow .xiftignore.
func (e *RuleEngine) ClassifyDirWithUserMatch(name, relPath string, depth int) (Action, bool) {
	if name == "" {
		return ActionKeep, false
	}
	s := e.snap.Load()
	if depth <= 0 && relPath != "" {
		depth = pathDepth(relPath)
	}
	if act, ok := s.matchUserDir(name, relPath); ok {
		return act, true
	}
	return e.classifyPresetDir(s, name, depth), false
}

// ClassifyFileWithUserMatch returns (action, matchedByUserRule).
func (e *RuleEngine) ClassifyFileWithUserMatch(name, relPath string) (Action, bool) {
	if name == "" {
		return ActionHardExclude, false
	}
	s := e.snap.Load()
	if act, ok := s.matchUserFile(name, relPath); ok {
		return act, true
	}
	return e.classifyPresetFile(s, name), false
}

// ClassifyFile returns the governance action for a regular file.
func (e *RuleEngine) ClassifyFile(name, relPath string) Action {
	act, _ := e.ClassifyFileWithUserMatch(name, relPath)
	return act
}

func (e *RuleEngine) classifyPresetDir(s *compiledRules, name string, depth int) Action {
	var best Action
	hasPreset := false
	if act, ok := lookupNameAction(s.presetDirs, name, s.caseInsensitive); ok {
		best = act
		hasPreset = true
	}
	if depth == 1 {
		if act, ok := lookupNameAction(s.presetRootDirs, name, s.caseInsensitive); ok {
			if !hasPreset || actionStrictness(act) > actionStrictness(best) {
				best = act
				hasPreset = true
			}
		}
	}
	if hasPreset {
		return best
	}
	return ActionKeep
}

func (e *RuleEngine) classifyPresetFile(s *compiledRules, name string) Action {
	if s.excludeNTFSMetaFiles && name[0] == '$' {
		return ActionHardExclude
	}
	if act, ok := lookupExtAction(s.presetExts, name); ok {
		return act
	}
	return ActionKeep
}

func (s *compiledRules) matchUserDir(name, relPath string) (Action, bool) {
	// Specificity order: PathPrefix > Glob > DirName
	if len(s.userPathPrefixes) > 0 && relPath != "" {
		normPath := normalizeRelPath(relPath, s.caseInsensitive)
		for i := range s.userPathPrefixes {
			if pathHasPrefix(normPath, s.userPathPrefixes[i].prefix) {
				return s.userPathPrefixes[i].action, true
			}
		}
	}
	if len(s.userGlobs) > 0 {
		var normPath string
		pathReady := false
		var normName string
		nameReady := false
		for i := range s.userGlobs {
			g := &s.userGlobs[i]
			if g.hasSlash {
				if relPath == "" {
					continue
				}
				if !pathReady {
					normPath = normalizeRelPath(relPath, s.caseInsensitive)
					pathReady = true
				}
				if matchPathGlob(g.pattern, normPath, false) {
					return g.action, true
				}
			} else {
				if !nameReady {
					if s.caseInsensitive {
						normName = strings.ToLower(name)
					} else {
						normName = name
					}
					nameReady = true
				}
				if matchSegmentGlob(g.pattern, normName, false) {
					return g.action, true
				}
			}
		}
	}
	if len(s.userDirNames) > 0 {
		if act, ok := lookupNameAction(s.userDirNames, name, s.caseInsensitive); ok {
			return act, true
		}
	}
	return ActionKeep, false
}

func (s *compiledRules) matchUserFile(name, relPath string) (Action, bool) {
	// Specificity order: PathPrefix > Glob > Ext
	if len(s.userPathPrefixes) > 0 && relPath != "" {
		normPath := normalizeRelPath(relPath, s.caseInsensitive)
		for i := range s.userPathPrefixes {
			if pathHasPrefix(normPath, s.userPathPrefixes[i].prefix) {
				return s.userPathPrefixes[i].action, true
			}
		}
	}
	if len(s.userGlobs) > 0 {
		var normPath string
		pathReady := false
		var normName string
		nameReady := false
		for i := range s.userGlobs {
			g := &s.userGlobs[i]
			if g.hasSlash {
				if relPath == "" {
					continue
				}
				if !pathReady {
					normPath = normalizeRelPath(relPath, s.caseInsensitive)
					pathReady = true
				}
				if matchPathGlob(g.pattern, normPath, false) {
					return g.action, true
				}
			} else {
				if !nameReady {
					if s.caseInsensitive {
						normName = strings.ToLower(name)
					} else {
						normName = name
					}
					nameReady = true
				}
				if matchSegmentGlob(g.pattern, normName, false) {
					return g.action, true
				}
			}
		}
	}
	if len(s.userExts) > 0 {
		if act, ok := lookupExtAction(s.userExts, name); ok {
			return act, true
		}
	}
	return ActionKeep, false
}

func (e *RuleEngine) recompileLocked() {
	c := &compiledRules{
		caseInsensitive:      e.caseInsensitive,
		excludeNTFSMetaFiles: e.excludeNTFSMetaFiles,
		gitignoreIgnoreFiles: e.gitignoreIgnoreFiles,
		userDirNames:         make(map[string]Action),
		userExts:             make(map[string]Action),
		presetDirs:           make(map[string]Action, 64),
		presetRootDirs:       make(map[string]Action, 32),
		presetExts:           make(map[string]Action, 16),
	}

	// Compile enabled built-in presets (strictest action wins on overlap).
	mergePreset := func(dst map[string]Action, key string, act Action) {
		k := key
		if c.caseInsensitive {
			k = strings.ToLower(k)
		}
		if prev, exists := dst[k]; !exists || actionStrictness(act) > actionStrictness(prev) {
			dst[k] = act
		}
	}

	for _, p := range e.profiles {
		if !e.profileEnabled[p.ID] {
			continue
		}
		for _, d := range p.HardExcludeDirs {
			mergePreset(c.presetDirs, d, ActionHardExclude)
		}
		for _, d := range p.FoldDirs {
			mergePreset(c.presetDirs, d, ActionFold)
		}
		for _, d := range p.SoftDemoteDirs {
			mergePreset(c.presetDirs, d, ActionSoftDemote)
		}
		for _, d := range p.SoftDemoteRootDirs {
			mergePreset(c.presetRootDirs, d, ActionSoftDemote)
		}
		for _, ext := range p.HardExcludeExts {
			normExt := normalizeExt(ext)
			if normExt != "" {
				c.presetExts[normExt] = ActionHardExclude
			}
		}
	}

	// Compile enabled user rules.
	type keyedRule struct {
		key      string
		action   Action
		priority int
		order    int
	}
	var dirRules []keyedRule
	var extRules []keyedRule

	for idx, r := range e.userRules {
		if !r.Enabled {
			continue
		}
		switch r.Kind {
		case RuleKindPathPrefix:
			norm := normalizeRelPath(r.Pattern, c.caseInsensitive)
			if norm == "" {
				continue
			}
			c.userPathPrefixes = append(c.userPathPrefixes, compiledPathRule{
				prefix:   norm,
				action:   r.Action,
				priority: r.Priority,
				order:    idx,
			})
		case RuleKindGlob:
			norm := normalizeGlobPattern(r.Pattern, c.caseInsensitive)
			if norm == "" {
				continue
			}
			c.userGlobs = append(c.userGlobs, compiledGlobRule{
				pattern:  norm,
				hasSlash: strings.IndexByte(norm, '/') >= 0,
				action:   r.Action,
				priority: r.Priority,
				order:    idx,
			})
		case RuleKindDirName:
			key := strings.TrimSpace(r.Pattern)
			if c.caseInsensitive {
				key = strings.ToLower(key)
			}
			if key != "" {
				dirRules = append(dirRules, keyedRule{key: key, action: r.Action, priority: r.Priority, order: idx})
			}
		case RuleKindExt:
			key := normalizeExt(r.Pattern)
			if key != "" {
				extRules = append(extRules, keyedRule{key: key, action: r.Action, priority: r.Priority, order: idx})
			}
		}
	}

	sort.Slice(c.userPathPrefixes, func(i, j int) bool {
		a, b := c.userPathPrefixes[i], c.userPathPrefixes[j]
		if a.priority != b.priority {
			return a.priority > b.priority
		}
		if len(a.prefix) != len(b.prefix) {
			return len(a.prefix) > len(b.prefix)
		}
		return a.order > b.order
	})
	sort.Slice(c.userGlobs, func(i, j int) bool {
		a, b := c.userGlobs[i], c.userGlobs[j]
		if a.priority != b.priority {
			return a.priority > b.priority
		}
		return a.order > b.order
	})

	sortKeyed := func(rs []keyedRule) {
		sort.Slice(rs, func(i, j int) bool {
			if rs[i].priority != rs[j].priority {
				return rs[i].priority < rs[j].priority
			}
			return rs[i].order < rs[j].order
		})
	}
	sortKeyed(dirRules)
	for _, r := range dirRules {
		c.userDirNames[r.key] = r.action
	}
	sortKeyed(extRules)
	for _, r := range extRules {
		c.userExts[r.key] = r.action
	}

	e.snap.Store(c)
}

func validateUserRule(r UserRule) error {
	if !r.Kind.Valid() {
		return fmt.Errorf("%w: unknown kind %q", ErrInvalidRule, r.Kind)
	}
	if !r.Action.Valid() {
		return fmt.Errorf("%w: unknown action %d", ErrInvalidRule, uint8(r.Action))
	}
	if strings.TrimSpace(r.Pattern) == "" {
		return fmt.Errorf("%w: empty pattern", ErrInvalidRule)
	}
	switch r.Kind {
	case RuleKindPathPrefix:
		if normalizeRelPath(r.Pattern, false) == "" {
			return fmt.Errorf("%w: degenerate path_prefix %q", ErrInvalidRule, r.Pattern)
		}
	case RuleKindGlob:
		if normalizeGlobPattern(r.Pattern, false) == "" {
			return fmt.Errorf("%w: degenerate glob %q", ErrInvalidRule, r.Pattern)
		}
	case RuleKindExt:
		if normalizeExt(r.Pattern) == "" {
			return fmt.Errorf("%w: degenerate ext %q", ErrInvalidRule, r.Pattern)
		}
	}
	return nil
}

func actionStrictness(a Action) int {
	switch a {
	case ActionHardExclude:
		return 3
	case ActionFold:
		return 2
	case ActionSoftDemote:
		return 1
	default:
		return 0
	}
}

func normalizeExt(ext string) string {
	ext = strings.TrimSpace(ext)
	if ext == "" {
		return ""
	}
	if ext[0] == '.' {
		ext = ext[1:]
	}
	if ext == "" || strings.ContainsAny(ext, "./\\") {
		return ""
	}
	return "." + strings.ToLower(ext)
}

func normalizeRelPath(p string, lower bool) string {
	p = strings.ReplaceAll(p, "\\", "/")
	p = collapseSlashes(p)
	p = strings.TrimLeft(p, "/")
	p = strings.TrimRight(p, "/")
	if lower {
		p = strings.ToLower(p)
	}
	return p
}

func collapseSlashes(p string) string {
	if !strings.Contains(p, "//") {
		return p
	}
	var b strings.Builder
	b.Grow(len(p))
	prevSlash := false
	for i := 0; i < len(p); i++ {
		c := p[i]
		if c == '/' {
			if prevSlash {
				continue
			}
			prevSlash = true
		} else {
			prevSlash = false
		}
		b.WriteByte(c)
	}
	return b.String()
}

// collapseDoubleStarSegments collapses consecutive "**" path segments at segment boundaries
// without corrupting patterns like "foo**/**bar".
func collapseDoubleStarSegments(p string) string {
	if !strings.Contains(p, "**") {
		return p
	}
	segs := strings.Split(p, "/")
	out := segs[:0]
	for _, seg := range segs {
		if seg == "**" && len(out) > 0 && out[len(out)-1] == "**" {
			continue
		}
		out = append(out, seg)
	}
	return strings.Join(out, "/")
}

func normalizeGlobPattern(p string, lower bool) string {
	p = strings.ReplaceAll(strings.TrimSpace(p), "\\", "/")
	p = collapseSlashes(p)
	p = strings.TrimLeft(p, "/")
	p = strings.TrimRight(p, "/")
	p = collapseDoubleStarSegments(p)
	if lower {
		p = strings.ToLower(p)
	}
	return p
}

func pathHasPrefix(path, prefix string) bool {
	if len(path) < len(prefix) {
		return false
	}
	if path[:len(prefix)] != prefix {
		return false
	}
	return len(path) == len(prefix) || path[len(prefix)] == '/'
}

func pathDepth(relPath string) int {
	depth := 0
	inSeg := false
	for i := 0; i < len(relPath); i++ {
		c := relPath[i]
		if c == '/' || c == '\\' {
			inSeg = false
			continue
		}
		if !inSeg {
			depth++
			inSeg = true
		}
	}
	return depth
}

// lookupNameAction looks up name in m with zero heap allocations for ASCII names <= 128 bytes.
func lookupNameAction(m map[string]Action, name string, caseInsensitive bool) (Action, bool) {
	if len(m) == 0 || name == "" {
		return ActionKeep, false
	}
	if !caseInsensitive {
		act, ok := m[name]
		return act, ok
	}
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
		act, ok := m[name]
		return act, ok
	}
	if !nonASCII && len(name) <= 128 {
		var buf [128]byte
		for i := 0; i < len(name); i++ {
			c := name[i]
			if c >= 'A' && c <= 'Z' {
				c += 'a' - 'A'
			}
			buf[i] = c
		}
		act, ok := m[string(buf[:len(name)])]
		return act, ok
	}
	act, ok := m[strings.ToLower(name)]
	return act, ok
}

// lookupExtAction extracts a lowercase extension (with leading '.') and looks it up with zero allocations for common ASCII extensions.
func lookupExtAction(m map[string]Action, name string) (Action, bool) {
	if len(m) == 0 {
		return ActionKeep, false
	}
	dot := strings.LastIndexByte(name, '.')
	if dot <= 0 || dot >= len(name)-1 {
		return ActionKeep, false
	}
	ext := name[dot:]
	needsLower := false
	nonASCII := false
	for i := 1; i < len(ext); i++ {
		c := ext[i]
		if c >= 0x80 {
			nonASCII = true
			break
		}
		if c >= 'A' && c <= 'Z' {
			needsLower = true
		}
	}
	if !needsLower && !nonASCII {
		act, ok := m[ext]
		return act, ok
	}
	if !nonASCII && len(ext) <= 32 {
		var buf [32]byte
		buf[0] = '.'
		for i := 1; i < len(ext); i++ {
			c := ext[i]
			if c >= 'A' && c <= 'Z' {
				c += 'a' - 'A'
			}
			buf[i] = c
		}
		act, ok := m[string(buf[:len(ext)])]
		return act, ok
	}
	act, ok := m[strings.ToLower(ext)]
	return act, ok
}

// matchPathGlob matches a slash-separated glob pattern (supporting *, ?, [abc], **) against a slash-separated path
// in O(len(pSegs) * len(tSegs)) time without exponential backtracking.
// Per gitignore semantics, a trailing "/**" (e.g. "build/**") matches everything INSIDE "build" (>= 1 trailing segment),
// never "build" itself.
func matchPathGlob(pattern, path string, caseInsensitive bool) bool {
	if caseInsensitive {
		pattern = strings.ToLower(pattern)
		path = strings.ToLower(path)
	}
	if pattern == "" {
		return path == ""
	}
	pSegs := strings.Split(pattern, "/")
	if len(pSegs) >= 2 && pSegs[len(pSegs)-1] == "**" {
		// Rewrite trailing "foo/**" -> "foo/*/**" so at least one child segment is required.
		pSegs[len(pSegs)-1] = "*"
		pSegs = append(pSegs, "**")
	}
	tSegs := strings.Split(path, "/")

	// Iterative two-pointer + star bookmarking over segments (strictly polynomial, zero exponential blowup).
	pi, ti := 0, 0
	starPI, starTI := -1, -1
	for ti < len(tSegs) {
		if pi < len(pSegs) && pSegs[pi] == "**" {
			starPI = pi
			starTI = ti
			pi++
			continue
		}
		if pi < len(pSegs) && matchSegmentGlob(pSegs[pi], tSegs[ti], false) {
			pi++
			ti++
			continue
		}
		if starPI != -1 {
			pi = starPI + 1
			starTI++
			ti = starTI
			continue
		}
		return false
	}
	for pi < len(pSegs) && pSegs[pi] == "**" {
		pi++
	}
	return pi == len(pSegs)
}

// matchSegmentGlob matches a single UTF-8 path segment (no '/') against a glob supporting *, ?, and [...] character classes
// using non-backtracking two-pointer wildcard matching in O(len(pattern)*len(text)).
func matchSegmentGlob(pattern, text string, caseInsensitive bool) bool {
	if caseInsensitive {
		pattern = strings.ToLower(pattern)
		text = strings.ToLower(text)
	}
	pi, ti := 0, 0
	starPI, starTI := -1, -1

	for ti < len(text) {
		tr, tSize := utf8.DecodeRuneInString(text[ti:])
		if pi < len(pattern) {
			switch pattern[pi] {
			case '*':
				for pi < len(pattern) && pattern[pi] == '*' {
					pi++
				}
				starPI = pi
				starTI = ti
				continue
			case '?':
				pi++
				ti += tSize
				continue
			case '[':
				if nextPI, matched := matchCharClass(pattern, pi, tr); nextPI > pi {
					if matched {
						pi = nextPI
						ti += tSize
						continue
					}
				} else if tr == '[' {
					// Unclosed '[': treat literally
					pi++
					ti += tSize
					continue
				}
			case '\\':
				if pi+1 < len(pattern) {
					pr, pSize := utf8.DecodeRuneInString(pattern[pi+1:])
					if pr == tr {
						pi += 1 + pSize
						ti += tSize
						continue
					}
				}
			default:
				pr, pSize := utf8.DecodeRuneInString(pattern[pi:])
				if pr == tr {
					pi += pSize
					ti += tSize
					continue
				}
			}
		}
		if starPI != -1 {
			pi = starPI
			_, starSize := utf8.DecodeRuneInString(text[starTI:])
			starTI += starSize
			ti = starTI
			continue
		}
		return false
	}
	for pi < len(pattern) && pattern[pi] == '*' {
		pi++
	}
	return pi == len(pattern)
}

// matchCharClass evaluates a UTF-8 bracket expression [...] starting at pattern[pos] == '['.
// Returns (index after ']', matched). Returns (pos, false) if unclosed.
func matchCharClass(pattern string, pos int, ch rune) (int, bool) {
	i := pos + 1
	if i >= len(pattern) {
		return pos, false
	}
	negated := false
	if pattern[i] == '!' || pattern[i] == '^' {
		negated = true
		i++
	}
	matched := false
	first := true
	for i < len(pattern) {
		if pattern[i] == ']' && !first {
			if negated {
				matched = !matched
			}
			return i + 1, matched
		}
		first = false

		lo, loSize := decodeClassRune(pattern, i)
		i += loSize

		if i < len(pattern) && pattern[i] == '-' && i+1 < len(pattern) && pattern[i+1] != ']' {
			i++ // skip '-'
			hi, hiSize := decodeClassRune(pattern, i)
			i += hiSize
			if ch >= lo && ch <= hi {
				matched = true
			}
			continue
		}
		if ch == lo {
			matched = true
		}
	}
	return pos, false
}

func decodeClassRune(pattern string, pos int) (rune, int) {
	if pattern[pos] == '\\' && pos+1 < len(pattern) {
		r, sz := utf8.DecodeRuneInString(pattern[pos+1:])
		return r, 1 + sz
	}
	return utf8.DecodeRuneInString(pattern[pos:])
}

func atomicWriteFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("filter: mkdir %s: %w", dir, err)
	}
	tmp, err := os.CreateTemp(dir, ".xift-filter-tmp-*")
	if err != nil {
		return fmt.Errorf("filter: create temp file: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("filter: write temp file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("filter: sync temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("filter: close temp file: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("filter: rename temp file: %w", err)
	}
	cleanup = false
	return nil
}
