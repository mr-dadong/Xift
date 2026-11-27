package filter

import (
	"path/filepath"
	"strings"
	"sync/atomic"
)

// TopDownDecision is the result of evaluating a directory or file during hierarchical traversal (macOS / Linux).
type TopDownDecision struct {
	Keep    bool   // Whether the node should be written to the index.
	Descend bool   // For directories: whether traversal should descend into children.
	Flags   uint16 // FlagDir / FlagFolded / FlagSystem bitmask aligned with storage.HotNode.Flags.
}

// Pass2Record is the lightweight input record evaluated in O(1) during Pass 2 of unordered MFT scans.
type Pass2Record struct {
	NodeKey   uint64
	ParentKey uint64
	Name      string
	RelPath   string // Optional; only needed if user path/glob rules are active.
	IsDir     bool
}

// Pass2Decision is the O(1) filtering outcome for a Pass 2 record.
type Pass2Decision struct {
	Keep  bool
	Flags uint16 // FlagDir / FlagFolded / FlagSystem
}

// EventOp identifies the type of real-time filesystem event arriving at the source interceptor.
type EventOp uint8

const (
	EventCreate EventOp = iota + 1
	EventDelete
	EventModifyMeta
	EventRename
)

// EventInput describes a single normalized kernel event before entering the 150ms Batcher and WAL.
type EventInput struct {
	VolID        uint16
	Op           EventOp
	NodeKey      uint64
	ParentKey    uint64
	NewParentKey uint64 // For EventRename; 0 or equal to ParentKey means same-directory rename.
	Name         string
	RelPath      string // Optional; only needed for path-prefix, glob, or project ignore rules.
	Depth        int    // Optional directory depth (1 = direct child of volume root).
	IsDir        bool
}

// InterceptAction is the source-level routing decision for a live filesystem event.
type InterceptAction uint8

const (
	// DecisionDrop discards the event at the source (0 Batcher / 0 WAL / 0 index writes).
	DecisionDrop InterceptAction = iota
	// DecisionPass forwards the event to the Batcher with the computed Flags.
	DecisionPass
	// DecisionDeleteFromIndex indicates a previously indexed node moved into a pruned subtree
	// or was renamed to a hard-excluded name and must be deleted from the index.
	DecisionDeleteFromIndex
	// DecisionRescanDir indicates a directory moved out of a pruned subtree or was un-folded
	// and requires forwarding plus a targeted directory rescan to populate its children.
	DecisionRescanDir
)

// EventDecision is returned by Pruner.InterceptEvent.
type EventDecision struct {
	Action        InterceptAction
	Flags         uint16
	PrunedSubtree bool // True when an existing visible directory just became folded/excluded or moved into a pruned dir.
}

// Pruner is the unified noise governance and source-level interception facade across all scan and watch modes.
type Pruner struct {
	rules         *RuleEngine
	set           *PrunedDirSet
	onIgnoreError atomic.Pointer[func(path string, err error)]
}

// NewPruner creates a Pruner backed by rules and set (allocating defaults if nil).
func NewPruner(rules *RuleEngine, set *PrunedDirSet) *Pruner {
	if rules == nil {
		rules = DefaultRuleEngine()
	}
	if set == nil {
		set = NewPrunedDirSet(4096)
	}
	return &Pruner{
		rules: rules,
		set:   set,
	}
}

// SetIgnoreErrorHandler registers an optional concurrent-safe callback invoked when reading
// a .gitignore/.xiftignore fails (e.g. oversized file or permission error). Errors never abort scanning.
func (p *Pruner) SetIgnoreErrorHandler(fn func(path string, err error)) {
	if fn == nil {
		p.onIgnoreError.Store(nil)
		return
	}
	p.onIgnoreError.Store(&fn)
}

func (p *Pruner) reportIgnoreError(path string, err error) {
	if fnPtr := p.onIgnoreError.Load(); fnPtr != nil && *fnPtr != nil {
		(*fnPtr)(path, err)
	}
}

// Rules returns the underlying RuleEngine.
func (p *Pruner) Rules() *RuleEngine {
	return p.rules
}

// PrunedDirSet returns the resident zero-GC PrunedDirSet.
func (p *Pruner) PrunedDirSet() *PrunedDirSet {
	return p.set
}

// InstallVolumeSet atomically replaces volID's resident entries with those built during a full scan (e.g. MFT Pass 1).
func (p *Pruner) InstallVolumeSet(volID uint16, src *PrunedDirSet) {
	p.set.ReplaceVolume(volID, src)
}

// ApplyDynamicDirRule persists/updates a user rule in RuleEngine and immediately updates PrunedDirSet for (volID, dirKey).
// Designed for UI right-click "Fold this directory" / "Exclude this path" closed-loop updates.
func (p *Pruner) ApplyDynamicDirRule(volID uint16, dirKey uint64, rule UserRule) error {
	if err := p.rules.AddUserRule(rule); err != nil {
		return err
	}
	if dirKey == 0 {
		return nil
	}
	if !rule.Enabled {
		// Disabling a dynamic rule clears any pruned flags previously enforced by it.
		p.set.ClearFlags(volID, dirKey, DirFlagsPrunedMask)
		return nil
	}
	switch rule.Action {
	case ActionHardExclude:
		p.set.Set(volID, dirKey, DirFlagHardExcluded)
	case ActionFold:
		prev, _ := p.set.GetFlags(volID, dirKey)
		df := DirFlagFoldedShell
		if prev.IsSystem() {
			df |= DirFlagSystem
		}
		p.set.Set(volID, dirKey, df)
	case ActionSoftDemote:
		// Replace pruned bits cleanly with Level 2 soft-demotion.
		p.set.Set(volID, dirKey, DirFlagSystem)
	case ActionKeep:
		p.set.ClearFlags(volID, dirKey, DirFlagsPrunedMask)
	}
	return nil
}

// EnterDirIgnoreScope inspects entries in a directory during top-down traversal and pushes readable ignore files:
//   - .xiftignore is an explicit Xift-specific opt-in file and is loaded whenever present.
//   - .gitignore is loaded ONLY when inside a detected project boundary (either an ancestor or this directory
//     contains a project marker such as package.json, go.mod, Cargo.toml, pyproject.toml, or .git), preventing
//     stray home-directory .gitignore files from folding unrelated user folders.
//
// Callers MUST defer stack.RestoreScope(scope) when exiting the directory.
func (p *Pruner) EnterDirIgnoreScope(stack *IgnoreStack, diskDirPath, relDirPath string, entries []string) IgnoreScope {
	if stack == nil {
		return IgnoreScope{}
	}
	scope := stack.SaveScope()
	ci := p.rules.CaseInsensitive()

	if DetectProjectRoot(entries, ci) {
		stack.EnterProject()
	}

	var xiftName, gitName string
	for _, entry := range entries {
		if equalsName(entry, ".xiftignore", ci) {
			xiftName = entry
		} else if equalsName(entry, ".gitignore", ci) {
			gitName = entry
		}
	}

	if xiftName != "" {
		full := filepath.Join(diskDirPath, xiftName)
		m, err := LoadIgnoreFile(full, relDirPath, IgnoreKindXift, ci)
		if err != nil {
			p.reportIgnoreError(full, err)
		} else {
			stack.Push(m)
		}
	}
	if gitName != "" && stack.InProject() {
		full := filepath.Join(diskDirPath, gitName)
		m, err := LoadIgnoreFile(full, relDirPath, IgnoreKindGit, ci)
		if err != nil {
			p.reportIgnoreError(full, err)
		} else {
			stack.Push(m)
		}
	}
	return scope
}

func equalsName(a, b string, caseInsensitive bool) bool {
	if caseInsensitive {
		return strings.EqualFold(a, b)
	}
	return a == b
}

// classifyDir applies strict precedence:
// 1. UserRules -> 2. .xiftignore -> 3. Built-in Presets -> 4. .gitignore -> 5. ActionKeep.
func (p *Pruner) classifyDir(name, relPath string, depth int, ignoreStack *IgnoreStack) Action {
	act, fromUser := p.rules.ClassifyDirWithUserMatch(name, relPath, depth)
	if fromUser {
		return act
	}
	if ignoreStack != nil && relPath != "" {
		ignAct, decided, fromXift := ignoreStack.Evaluate(relPath, true)
		if fromXift && decided {
			return ignAct
		}
		if act != ActionKeep {
			return act
		}
		if decided {
			return ignAct
		}
		return ActionKeep
	}
	return act
}

// classifyFile applies strict precedence:
// 1. UserRules -> 2. .xiftignore -> 3. Built-in Presets -> 4. .gitignore (if enabled) -> 5. ActionKeep.
func (p *Pruner) classifyFile(name, relPath string, ignoreStack *IgnoreStack) Action {
	act, fromUser := p.rules.ClassifyFileWithUserMatch(name, relPath)
	if fromUser {
		return act
	}
	if ignoreStack != nil && relPath != "" {
		ignAct, decided, fromXift := ignoreStack.Evaluate(relPath, false)
		if fromXift && decided {
			return ignAct
		}
		if act != ActionKeep {
			return act
		}
		if decided {
			return ignAct
		}
		return ActionKeep
	}
	return act
}

// EvalTopDownDir evaluates a directory during top-down traversal (macOS getattrlistbulk / Linux getdents64),
// updating PrunedDirSet cleanly (including clearing stale pruned entries during targeted rescans after unfold/move-out).
func (p *Pruner) EvalTopDownDir(volID uint16, dirKey uint64, name, relPath string, depth int, parentSystem bool, ignoreStack *IgnoreStack) TopDownDecision {
	if depth == 0 && name == "" {
		return TopDownDecision{Keep: true, Descend: true, Flags: FlagDir}
	}
	act := p.classifyDir(name, relPath, depth, ignoreStack)
	switch act {
	case ActionHardExclude:
		if dirKey != 0 {
			p.set.Set(volID, dirKey, DirFlagHardExcluded)
		}
		return TopDownDecision{Keep: false, Descend: false}
	case ActionFold:
		df := DirFlagFoldedShell
		flags := FlagDir | FlagFolded
		if parentSystem {
			df |= DirFlagSystem
			flags |= FlagSystem
		}
		if dirKey != 0 {
			p.set.Set(volID, dirKey, df)
		}
		return TopDownDecision{Keep: true, Descend: false, Flags: flags}
	case ActionSoftDemote:
		if dirKey != 0 {
			p.set.Set(volID, dirKey, DirFlagSystem)
		}
		return TopDownDecision{Keep: true, Descend: true, Flags: FlagDir | FlagSystem}
	default:
		flags := FlagDir
		if parentSystem {
			flags |= FlagSystem
			if dirKey != 0 {
				p.set.Set(volID, dirKey, DirFlagSystem)
			}
		} else if dirKey != 0 {
			// Guard with RLock check so initial clean scans incur zero write-lock contention,
			// while rescans after move-out/unfold clear stale DirFlagPrunedDescendant entries.
			if _, had := p.set.GetFlags(volID, dirKey); had {
				p.set.Remove(volID, dirKey)
			}
		}
		return TopDownDecision{Keep: true, Descend: true, Flags: flags}
	}
}

// EvalTopDownFile evaluates a regular file during top-down traversal.
func (p *Pruner) EvalTopDownFile(name, relPath string, parentSystem bool, ignoreStack *IgnoreStack) TopDownDecision {
	act := p.classifyFile(name, relPath, ignoreStack)
	if act == ActionHardExclude {
		return TopDownDecision{Keep: false}
	}
	var flags uint16
	if parentSystem || act == ActionSoftDemote {
		flags = FlagSystem
	}
	return TopDownDecision{Keep: true, Flags: flags}
}

// InterceptEvent performs O(1) source-level interception for real-time kernel events
// before they enter the 150ms Batcher and WAL.
func (p *Pruner) InterceptEvent(ev EventInput, ignoreStack *IgnoreStack) EventDecision {
	var parentFlags DirFlags
	if ev.ParentKey != 0 && ev.ParentKey != ev.NodeKey {
		parentFlags, _ = p.set.GetFlags(ev.VolID, ev.ParentKey)
	}

	// Fast path: parent directory is already pruned (HardExcluded, FoldedShell, or PrunedDescendant).
	if parentFlags.IsPruned() {
		if ev.Op == EventRename {
			dstParent := ev.NewParentKey
			if dstParent == 0 {
				dstParent = ev.ParentKey
			}
			if dstParent != ev.ParentKey {
				var dstFlags DirFlags
				if dstParent != 0 && dstParent != ev.NodeKey {
					dstFlags, _ = p.set.GetFlags(ev.VolID, dstParent)
				}
				if !dstFlags.IsPruned() {
					// Moved OUT of a pruned subtree into a visible directory!
					return p.evalMovedOutOfPruned(ev, dstFlags.IsSystem(), ignoreStack)
				}
			}
			// Rename staying inside pruned subtree
			if ev.IsDir && ev.NodeKey != 0 {
				p.set.AddFlags(ev.VolID, ev.NodeKey, DirFlagPrunedDescendant)
			}
			return EventDecision{Action: DecisionDrop}
		}
		if ev.IsDir && ev.NodeKey != 0 {
			switch ev.Op {
			case EventCreate:
				p.set.AddFlags(ev.VolID, ev.NodeKey, DirFlagPrunedDescendant)
			case EventDelete:
				p.set.Remove(ev.VolID, ev.NodeKey)
			}
		}
		return EventDecision{Action: DecisionDrop}
	}

	// Parent directory is visible (not pruned).
	effectiveSys := parentFlags.IsSystem()

	if ev.Op == EventRename && ev.NewParentKey != 0 && ev.NewParentKey != ev.ParentKey {
		var dstFlags DirFlags
		if ev.NewParentKey != ev.NodeKey {
			dstFlags, _ = p.set.GetFlags(ev.VolID, ev.NewParentKey)
		}
		if dstFlags.IsPruned() {
			// Moved INTO a pruned subtree from a visible directory.
			var oldFlags DirFlags
			if ev.IsDir && ev.NodeKey != 0 {
				oldFlags, _ = p.set.GetFlags(ev.VolID, ev.NodeKey)
				p.set.Set(ev.VolID, ev.NodeKey, DirFlagPrunedDescendant)
				if oldFlags&(DirFlagHardExcluded|DirFlagPrunedDescendant) != 0 {
					// Directory was never in the index.
					return EventDecision{Action: DecisionDrop}
				}
				return EventDecision{
					Action:        DecisionDeleteFromIndex,
					PrunedSubtree: !oldFlags.IsPruned(), // FoldedShell had no indexed descendants
				}
			}
			return EventDecision{Action: DecisionDeleteFromIndex}
		}
		effectiveSys = dstFlags.IsSystem()
	}

	if ev.Op == EventDelete {
		if ev.IsDir {
			var oldFlags DirFlags
			var had bool
			if ev.NodeKey != 0 {
				oldFlags, had = p.set.GetFlags(ev.VolID, ev.NodeKey)
				if had {
					p.set.Remove(ev.VolID, ev.NodeKey)
				}
			}
			if oldFlags&(DirFlagHardExcluded|DirFlagPrunedDescendant) != 0 {
				// Hard-excluded dir or delayed delete of a pruned descendant whose parent was already removed.
				return EventDecision{Action: DecisionDrop}
			}
			return EventDecision{Action: DecisionPass, Flags: FlagDir}
		}
		if p.classifyFile(ev.Name, ev.RelPath, ignoreStack) == ActionHardExclude {
			return EventDecision{Action: DecisionDrop}
		}
		return EventDecision{Action: DecisionPass}
	}

	if ev.IsDir {
		var oldFlags DirFlags
		if ev.NodeKey != 0 {
			oldFlags, _ = p.set.GetFlags(ev.VolID, ev.NodeKey)
		}
		if ev.Op == EventModifyMeta {
			if oldFlags&(DirFlagHardExcluded|DirFlagPrunedDescendant) != 0 {
				return EventDecision{Action: DecisionDrop}
			}
			flags := FlagDir
			if oldFlags&DirFlagFoldedShell != 0 {
				flags |= FlagFolded
			}
			if oldFlags.IsSystem() || effectiveSys {
				flags |= FlagSystem
			}
			return EventDecision{Action: DecisionPass, Flags: flags}
		}

		act := p.classifyDir(ev.Name, ev.RelPath, ev.Depth, ignoreStack)
		switch act {
		case ActionHardExclude:
			if ev.NodeKey != 0 {
				p.set.Set(ev.VolID, ev.NodeKey, DirFlagHardExcluded)
			}
			if ev.Op == EventRename && oldFlags&(DirFlagHardExcluded|DirFlagPrunedDescendant) == 0 {
				return EventDecision{
					Action:        DecisionDeleteFromIndex,
					PrunedSubtree: !oldFlags.IsPruned(),
				}
			}
			return EventDecision{Action: DecisionDrop}
		case ActionFold:
			df := DirFlagFoldedShell
			flags := FlagDir | FlagFolded
			if effectiveSys {
				df |= DirFlagSystem
				flags |= FlagSystem
			}
			if ev.NodeKey != 0 {
				p.set.Set(ev.VolID, ev.NodeKey, df)
			}
			wasNormal := ev.Op == EventRename && !oldFlags.IsPruned()
			return EventDecision{Action: DecisionPass, Flags: flags, PrunedSubtree: wasNormal}
		case ActionSoftDemote:
			wasPruned := oldFlags.IsPruned()
			if ev.NodeKey != 0 {
				p.set.Set(ev.VolID, ev.NodeKey, DirFlagSystem)
			}
			if ev.Op == EventRename && wasPruned {
				return EventDecision{Action: DecisionRescanDir, Flags: FlagDir | FlagSystem}
			}
			return EventDecision{Action: DecisionPass, Flags: FlagDir | FlagSystem}
		default: // ActionKeep
			wasPruned := oldFlags.IsPruned()
			flags := FlagDir
			if effectiveSys {
				flags |= FlagSystem
				if ev.NodeKey != 0 {
					p.set.Set(ev.VolID, ev.NodeKey, DirFlagSystem)
				}
			} else if oldFlags != 0 && ev.NodeKey != 0 {
				p.set.Remove(ev.VolID, ev.NodeKey)
			}
			if ev.Op == EventRename && wasPruned {
				return EventDecision{Action: DecisionRescanDir, Flags: flags}
			}
			return EventDecision{Action: DecisionPass, Flags: flags}
		}
	}

	// Regular file (Create / Rename / ModifyMeta) under visible parent
	act := p.classifyFile(ev.Name, ev.RelPath, ignoreStack)
	if act == ActionHardExclude {
		if ev.Op == EventRename {
			return EventDecision{Action: DecisionDeleteFromIndex}
		}
		return EventDecision{Action: DecisionDrop}
	}
	var flags uint16
	if effectiveSys || act == ActionSoftDemote {
		flags = FlagSystem
	}
	return EventDecision{Action: DecisionPass, Flags: flags}
}

func (p *Pruner) evalMovedOutOfPruned(ev EventInput, dstSystem bool, ignoreStack *IgnoreStack) EventDecision {
	if !ev.IsDir {
		act := p.classifyFile(ev.Name, ev.RelPath, ignoreStack)
		if act == ActionHardExclude {
			return EventDecision{Action: DecisionDrop}
		}
		var flags uint16
		if dstSystem || act == ActionSoftDemote {
			flags = FlagSystem
		}
		return EventDecision{Action: DecisionPass, Flags: flags}
	}

	// Directory moved out of pruned subtree: remove stale pruned state and re-classify at destination.
	if ev.NodeKey != 0 {
		p.set.Remove(ev.VolID, ev.NodeKey)
	}
	act := p.classifyDir(ev.Name, ev.RelPath, ev.Depth, ignoreStack)
	switch act {
	case ActionHardExclude:
		if ev.NodeKey != 0 {
			p.set.Set(ev.VolID, ev.NodeKey, DirFlagHardExcluded)
		}
		return EventDecision{Action: DecisionDrop}
	case ActionFold:
		df := DirFlagFoldedShell
		flags := FlagDir | FlagFolded
		if dstSystem {
			df |= DirFlagSystem
			flags |= FlagSystem
		}
		if ev.NodeKey != 0 {
			p.set.Set(ev.VolID, ev.NodeKey, df)
		}
		return EventDecision{Action: DecisionPass, Flags: flags}
	case ActionSoftDemote:
		if ev.NodeKey != 0 {
			p.set.Set(ev.VolID, ev.NodeKey, DirFlagSystem)
		}
		return EventDecision{Action: DecisionRescanDir, Flags: FlagDir | FlagSystem}
	default:
		flags := FlagDir
		if dstSystem {
			flags |= FlagSystem
			if ev.NodeKey != 0 {
				p.set.Set(ev.VolID, ev.NodeKey, DirFlagSystem)
			}
		}
		return EventDecision{Action: DecisionRescanDir, Flags: flags}
	}
}

// =============================================================================
// Mode B: Unordered MFT Two-Pass Skeleton Builder (Windows NTFS)
// =============================================================================

type skeletonDirEntry struct {
	dirKey    uint64
	parentKey uint64
	name      string
}

type pass2IgnoreScope struct {
	relDir string
	stack  *IgnoreStack
}

// SkeletonIgnoreLoader is an optional read-only callback invoked on candidate project directories during Build().
type SkeletonIgnoreLoader func(dirKey uint64, relPath string) (xiftM *IgnoreMatcher, gitM *IgnoreMatcher)

// SkeletonBuilder collects unordered directory records during Pass 1 and propagates pruning/system
// flags via cycle-safe BFS, producing a Pass2Evaluator whose PrunedDirSet can be installed directly into a live Pruner.
type SkeletonBuilder struct {
	volID        uint16
	rootDirKey   uint64
	rules        *RuleEngine
	dirs         []skeletonDirEntry
	idxByKey     map[uint64]int
	projectDirs  map[uint64]struct{}
	ignoreLoader SkeletonIgnoreLoader
}

// NewSkeletonBuilder creates a Pass 1 unordered directory skeleton builder for volID.
// Pass rootDirKey = 5 on standard NTFS volumes (or 0 if roots are identified solely by self-parenting).
func NewSkeletonBuilder(volID uint16, rootDirKey uint64, rules *RuleEngine) *SkeletonBuilder {
	if rules == nil {
		rules = DefaultRuleEngine()
	}
	return &SkeletonBuilder{
		volID:      volID,
		rootDirKey: rootDirKey,
		rules:      rules,
		dirs:       make([]skeletonDirEntry, 0, 4096),
		idxByKey:   make(map[uint64]int, 4096),
	}
}

// SetIgnoreLoader attaches an optional read-only loader for project .xiftignore / .gitignore files.
func (b *SkeletonBuilder) SetIgnoreLoader(fn SkeletonIgnoreLoader) {
	b.ignoreLoader = fn
}

// AddDir records a directory encountered in Pass 1 (order-independent). Duplicate dirKeys overwrite earlier entries.
func (b *SkeletonBuilder) AddDir(dirKey, parentKey uint64, name string) {
	if existing, dup := b.idxByKey[dirKey]; dup {
		b.dirs[existing] = skeletonDirEntry{
			dirKey:    dirKey,
			parentKey: parentKey,
			name:      name,
		}
		return
	}
	b.idxByKey[dirKey] = len(b.dirs)
	b.dirs = append(b.dirs, skeletonDirEntry{
		dirKey:    dirKey,
		parentKey: parentKey,
		name:      name,
	})
}

// MarkProjectDir hints that dirKey is a project root or contains an ignore file.
func (b *SkeletonBuilder) MarkProjectDir(dirKey uint64) {
	if b.projectDirs == nil {
		b.projectDirs = make(map[uint64]struct{}, 64)
	}
	b.projectDirs[dirKey] = struct{}{}
}

// DirCount returns the number of unique directories collected so far.
func (b *SkeletonBuilder) DirCount() int {
	return len(b.dirs)
}

// Build resolves depths safely (with cycle protection), classifies all directories (including optional
// project ignore rules without touching already-pruned subtrees), runs BFS propagation, and returns
// a Pass2Evaluator backed by a zero-GC PrunedDirSet.
func (b *SkeletonBuilder) Build() *Pass2Evaluator {
	n := len(b.dirs)
	idxByKey := b.idxByKey
	children := make(map[uint64][]int, n/2+16)

	for i := range b.dirs {
		d := &b.dirs[i]
		if d.parentKey != d.dirKey {
			children[d.parentKey] = append(children[d.parentKey], i)
		}
	}

	// Compute cycle-safe depth for every directory (root == 0, direct child of root == 1, cycle/orphan >= 2).
	depths := make([]int, n)
	visitState := make([]uint8, n) // 0=unvisited, 1=visiting, 2=done
	var chain []int

	for i := 0; i < n; i++ {
		if visitState[i] == 2 {
			continue
		}
		chain = chain[:0]
		cur := i
		baseDepth := 0
		for {
			if visitState[cur] == 2 {
				baseDepth = depths[cur]
				break
			}
			if visitState[cur] == 1 {
				// Parent cycle detected: assign non-root safe depth (>= 2) and terminate.
				baseDepth = 2
				break
			}
			visitState[cur] = 1
			chain = append(chain, cur)

			d := &b.dirs[cur]
			if d.dirKey == d.parentKey || (b.rootDirKey != 0 && d.dirKey == b.rootDirKey) {
				baseDepth = -1 // So cur gets depth 0
				break
			}
			if b.rootDirKey != 0 && d.parentKey == b.rootDirKey {
				baseDepth = 0 // Direct child of volume root gets depth 1
				break
			}
			pIdx, ok := idxByKey[d.parentKey]
			if !ok {
				// Detached parent outside collected skeleton: treat as non-root (depth >= 2)
				// so orphaned folders are never misclassified as volume-root system directories.
				baseDepth = 1
				break
			}
			cur = pIdx
		}
		for k := len(chain) - 1; k >= 0; k-- {
			idx := chain[k]
			baseDepth++
			depths[idx] = baseDepth
			visitState[idx] = 2
		}
	}

	hasPathRules := b.rules.HasPathRules()
	needPaths := hasPathRules || (b.ignoreLoader != nil && len(b.projectDirs) > 0)
	var relPaths []string
	var relComputed []bool
	var seenGen []uint32
	var curGen uint32

	var getRelPath func(idx int) string
	if needPaths {
		relPaths = make([]string, n)
		relComputed = make([]bool, n)
		seenGen = make([]uint32, n)
		var segs []string
		getRelPath = func(idx int) string {
			if relComputed[idx] {
				return relPaths[idx]
			}
			curGen++
			if curGen == 0 {
				clear(seenGen)
				curGen = 1
			}
			segs = segs[:0]
			cur := idx
			for {
				if relComputed[cur] {
					if relPaths[cur] != "" {
						segs = append(segs, relPaths[cur])
					}
					break
				}
				if seenGen[cur] == curGen {
					break
				}
				seenGen[cur] = curGen
				d := &b.dirs[cur]
				if depths[cur] == 0 {
					break
				}
				if d.name != "" {
					segs = append(segs, d.name)
				}
				if d.parentKey == d.dirKey || (b.rootDirKey != 0 && d.parentKey == b.rootDirKey) {
					break
				}
				pIdx, ok := idxByKey[d.parentKey]
				if !ok {
					break
				}
				cur = pIdx
			}
			for l, r := 0, len(segs)-1; l < r; l, r = l+1, r-1 {
				segs[l], segs[r] = segs[r], segs[l]
			}
			rp := strings.Join(segs, "/")
			relPaths[idx] = rp
			relComputed[idx] = true
			return rp
		}
	}

	// Stage 1: Classify all directories using RuleEngine (UserRules + Built-in Presets).
	actions := make([]Action, n)
	fromUser := make([]bool, n)
	for i := range b.dirs {
		if depths[i] == 0 {
			actions[i] = ActionKeep
			continue
		}
		var rp string
		if hasPathRules {
			rp = getRelPath(i)
		}
		actions[i], fromUser[i] = b.rules.ClassifyDirWithUserMatch(b.dirs[i].name, rp, depths[i])
	}

	// Stage 2: Pre-propagate preset/user HardExclude & Fold via cycle-safe BFS BEFORE loading
	// project ignore files, so we never perform disk I/O inside pruned subtrees like node_modules/<pkg>.
	flags := make([]DirFlags, n)
	var pruneQueue []int
	var sysQueue []int

	propagatePrune := func() {
		for head := 0; head < len(pruneQueue); head++ {
			curKey := b.dirs[pruneQueue[head]].dirKey
			for _, childIdx := range children[curKey] {
				if flags[childIdx]&DirFlagsPrunedMask == 0 {
					flags[childIdx] |= DirFlagPrunedDescendant
					pruneQueue = append(pruneQueue, childIdx)
				}
			}
		}
		pruneQueue = pruneQueue[:0]
	}

	clearDescendantPrune := func(startIdx int) {
		q := []int{startIdx}
		for head := 0; head < len(q); head++ {
			curKey := b.dirs[q[head]].dirKey
			for _, cIdx := range children[curKey] {
				if flags[cIdx]&(DirFlagHardExcluded|DirFlagFoldedShell) != 0 {
					continue
				}
				if flags[cIdx]&DirFlagPrunedDescendant != 0 {
					flags[cIdx] &^= DirFlagPrunedDescendant
					q = append(q, cIdx)
				}
			}
		}
	}

	for i := 0; i < n; i++ {
		switch actions[i] {
		case ActionHardExclude:
			flags[i] |= DirFlagHardExcluded
			pruneQueue = append(pruneQueue, i)
		case ActionFold:
			flags[i] |= DirFlagFoldedShell
			pruneQueue = append(pruneQueue, i)
		case ActionSoftDemote:
			flags[i] |= DirFlagSystem
			sysQueue = append(sysQueue, i)
		}
	}
	propagatePrune()

	// Stage 3: Optional project .gitignore / .xiftignore pass ONLY over visible (non-pruned) project roots.
	// Retain sparse pass2IgnoreScope entries so Pass2Evaluator can enforce file-level .xiftignore/.gitignore rules.
	var projectScopes map[uint64]pass2IgnoreScope
	if b.ignoreLoader != nil && len(b.projectDirs) > 0 {
		ci := b.rules.CaseInsensitive()
		ignFiles := b.rules.GitignoreIgnoreFiles()

		type frame struct {
			idx   int
			stack *IgnoreStack
		}
		visitedProj := make([]bool, n)

		// Process shallower project roots first so nested projects inherit parent matchers cleanly.
		projIdxs := make([]int, 0, len(b.projectDirs))
		for key := range b.projectDirs {
			if idx, ok := idxByKey[key]; ok {
				projIdxs = append(projIdxs, idx)
			}
		}
		for i := 1; i < len(projIdxs); i++ {
			cur := projIdxs[i]
			j := i - 1
			for j >= 0 && depths[projIdxs[j]] > depths[cur] {
				projIdxs[j+1] = projIdxs[j]
				j--
			}
			projIdxs[j+1] = cur
		}

		for _, rootIdx := range projIdxs {
			if visitedProj[rootIdx] || flags[rootIdx].IsPruned() {
				continue
			}
			rootRP := getRelPath(rootIdx)
			xiftM, gitM := b.ignoreLoader(b.dirs[rootIdx].dirKey, rootRP)
			if xiftM == nil && gitM == nil {
				continue
			}
			baseStack := NewIgnoreStack(ci, ignFiles)
			baseStack.EnterProject()
			baseStack.Push(xiftM)
			baseStack.Push(gitM)

			if projectScopes == nil {
				projectScopes = make(map[uint64]pass2IgnoreScope, 64)
			}
			projectScopes[b.dirs[rootIdx].dirKey] = pass2IgnoreScope{relDir: rootRP, stack: baseStack}

			queue := []frame{{idx: rootIdx, stack: baseStack}}
			visitedProj[rootIdx] = true

			for head := 0; head < len(queue); head++ {
				fr := queue[head]
				dirKey := b.dirs[fr.idx].dirKey
				for _, childIdx := range children[dirKey] {
					if visitedProj[childIdx] {
						continue
					}
					visitedProj[childIdx] = true

					childRP := getRelPath(childIdx)
					if !fromUser[childIdx] {
						ignAct, decided, fromXift := fr.stack.Evaluate(childRP, true)
						if fromXift && decided {
							// .xiftignore outranks built-in presets (including !pattern negation).
							actions[childIdx] = ignAct
							switch ignAct {
							case ActionKeep:
								wasPruned := flags[childIdx].IsPruned()
								flags[childIdx] &^= DirFlagsPrunedMask
								if wasPruned {
									clearDescendantPrune(childIdx)
								}
							case ActionHardExclude:
								flags[childIdx] = (flags[childIdx] &^ DirFlagsPrunedMask) | DirFlagHardExcluded
								pruneQueue = append(pruneQueue, childIdx)
							case ActionFold:
								flags[childIdx] = (flags[childIdx] &^ DirFlagsPrunedMask) | DirFlagFoldedShell
								pruneQueue = append(pruneQueue, childIdx)
							}
						} else if decided && !flags[childIdx].IsPruned() && actions[childIdx] == ActionKeep {
							actions[childIdx] = ignAct
							if ignAct == ActionFold {
								flags[childIdx] |= DirFlagFoldedShell
								pruneQueue = append(pruneQueue, childIdx)
							} else if ignAct == ActionHardExclude {
								flags[childIdx] |= DirFlagHardExcluded
								pruneQueue = append(pruneQueue, childIdx)
							}
						}
					}

					if flags[childIdx].IsPruned() {
						continue // Do not descend into pruned subtrees
					}

					childStack := fr.stack
					if _, isNested := b.projectDirs[b.dirs[childIdx].dirKey]; isNested {
						nx, ng := b.ignoreLoader(b.dirs[childIdx].dirKey, childRP)
						if nx != nil || ng != nil {
							childStack = fr.stack.Clone()
							childStack.Push(nx)
							childStack.Push(ng)
						}
					}
					projectScopes[b.dirs[childIdx].dirKey] = pass2IgnoreScope{relDir: childRP, stack: childStack}
					queue = append(queue, frame{idx: childIdx, stack: childStack})
				}
			}
		}

		// Propagate any newly discovered ignore-based HardExclude/Fold dirs down their subtrees.
		if len(pruneQueue) > 0 {
			propagatePrune()
		}
	}

	// Stage 4: Propagate Level 2 SoftDemote (DirFlagSystem) via cycle-safe BFS.
	for head := 0; head < len(sysQueue); head++ {
		curKey := b.dirs[sysQueue[head]].dirKey
		for _, childIdx := range children[curKey] {
			if flags[childIdx]&DirFlagSystem == 0 {
				flags[childIdx] |= DirFlagSystem
				sysQueue = append(sysQueue, childIdx)
			}
		}
	}

	// Materialize into a compact zero-GC PrunedDirSet.
	tracked := 0
	for i := range flags {
		if flags[i] != 0 {
			tracked++
		}
	}
	set := NewPrunedDirSet(tracked)
	for i, fl := range flags {
		if fl != 0 {
			set.Set(b.volID, b.dirs[i].dirKey, fl)
		}
	}

	return &Pass2Evaluator{
		volID:         b.volID,
		rules:         b.rules,
		set:           set,
		projectScopes: projectScopes,
	}
}

// Pass2Evaluator provides O(1), zero-allocation Pass 2 filtering over flat MFT streams.
type Pass2Evaluator struct {
	volID         uint16
	rules         *RuleEngine
	set           *PrunedDirSet
	projectScopes map[uint64]pass2IgnoreScope
}

// PrunedDirSet returns the built zero-GC PrunedDirSet, which can be installed directly into a live Pruner.
func (e *Pass2Evaluator) PrunedDirSet() *PrunedDirSet {
	return e.set
}

// Evaluate decides in O(1) time whether a Pass 2 record is kept and with what HotNode Flags.
func (e *Pass2Evaluator) Evaluate(rec Pass2Record) Pass2Decision {
	var parentFlags DirFlags
	if rec.ParentKey != 0 && rec.ParentKey != rec.NodeKey {
		parentFlags, _ = e.set.GetFlags(e.volID, rec.ParentKey)
		if parentFlags.IsPruned() {
			return Pass2Decision{Keep: false}
		}
	}

	if rec.IsDir {
		var selfFlags DirFlags
		if rec.NodeKey != 0 {
			selfFlags, _ = e.set.GetFlags(e.volID, rec.NodeKey)
		}
		if selfFlags&DirFlagHardExcluded != 0 ||
			(selfFlags&DirFlagPrunedDescendant != 0 && selfFlags&DirFlagFoldedShell == 0) {
			return Pass2Decision{Keep: false}
		}
		out := FlagDir
		if selfFlags&DirFlagFoldedShell != 0 {
			out |= FlagFolded
		}
		if selfFlags.IsSystem() || parentFlags.IsSystem() {
			out |= FlagSystem
		}
		return Pass2Decision{Keep: true, Flags: out}
	}

	act, fromUser := e.rules.ClassifyFileWithUserMatch(rec.Name, rec.RelPath)
	if !fromUser && len(e.projectScopes) > 0 {
		if sc, ok := e.projectScopes[rec.ParentKey]; ok && sc.stack != nil {
			relPath := rec.RelPath
			if relPath == "" {
				if sc.relDir == "" {
					relPath = rec.Name
				} else {
					relPath = sc.relDir + "/" + rec.Name
				}
			}
			ignAct, decided, fromXift := sc.stack.Evaluate(relPath, false)
			if fromXift && decided {
				act = ignAct
			} else if act == ActionKeep && decided {
				act = ignAct
			}
		}
	}

	if act == ActionHardExclude {
		return Pass2Decision{Keep: false}
	}
	var out uint16
	if parentFlags.IsSystem() || act == ActionSoftDemote {
		out = FlagSystem
	}
	return Pass2Decision{Keep: true, Flags: out}
}
