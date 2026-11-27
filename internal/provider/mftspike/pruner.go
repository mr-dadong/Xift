package mftspike

// RawRecord represents a single normalized NTFS MFT / synthetic record.
type RawRecord struct {
	FRN       uint64
	ParentFRN uint64
	Name      string
	IsDir     bool
	Hidden    bool
}

// Pass2Decision is the O(1) filtering outcome for a record during Pass 2.
type Pass2Decision struct {
	Keep   bool
	Folded bool
	System bool
}

type dirSkeletonEntry struct {
	frn       uint64
	parentFRN uint64
	action    FilterAction
}

// SkeletonBuilder collects directory records during Pass 1 and propagates pruning/system flags via BFS.
type SkeletonBuilder struct {
	rules *RuleSet
	dirs  []dirSkeletonEntry
}

// NewSkeletonBuilder creates a Pass 1 directory skeleton collector.
func NewSkeletonBuilder(rules *RuleSet) *SkeletonBuilder {
	if rules == nil {
		rules = DefaultRules()
	}
	return &SkeletonBuilder{
		rules: rules,
		dirs:  make([]dirSkeletonEntry, 0, 65536),
	}
}

// AddDir records a directory encountered in Pass 1 (order-independent).
func (b *SkeletonBuilder) AddDir(frn, parentFRN uint64, name string) {
	b.dirs = append(b.dirs, dirSkeletonEntry{
		frn:       frn,
		parentFRN: parentFRN,
		action:    b.rules.ClassifyDir(name),
	})
}

// DirCount returns the number of directories collected in Pass 1.
func (b *SkeletonBuilder) DirCount() int {
	return len(b.dirs)
}

// Build runs BFS propagation from matched directories down to all descendants, producing
// O(1) lookup sets for Pass 2 filtering. Self-references and parent cycles terminate safely.
func (b *SkeletonBuilder) Build() *Pruner {
	children := make(map[uint64][]uint64, len(b.dirs)/2+16)
	prunedSet := make(map[uint64]struct{}, 4096)
	hardExcludedSelf := make(map[uint64]struct{}, 1024)
	foldedSelf := make(map[uint64]struct{}, 1024)
	systemSet := make(map[uint64]struct{}, 4096)

	var pruneQueue []uint64
	var sysQueue []uint64

	for _, d := range b.dirs {
		if d.parentFRN != d.frn {
			children[d.parentFRN] = append(children[d.parentFRN], d.frn)
		}
		switch d.action {
		case ActionHardExclude:
			hardExcludedSelf[d.frn] = struct{}{}
			if _, seen := prunedSet[d.frn]; !seen {
				prunedSet[d.frn] = struct{}{}
				pruneQueue = append(pruneQueue, d.frn)
			}
		case ActionFold:
			foldedSelf[d.frn] = struct{}{}
			if _, seen := prunedSet[d.frn]; !seen {
				prunedSet[d.frn] = struct{}{}
				pruneQueue = append(pruneQueue, d.frn)
			}
		case ActionSoftDemote:
			if _, seen := systemSet[d.frn]; !seen {
				systemSet[d.frn] = struct{}{}
				sysQueue = append(sysQueue, d.frn)
			}
		}
	}

	// BFS propagate PrunedDirSet down all descendants (cycle-safe via visited set check)
	for head := 0; head < len(pruneQueue); head++ {
		cur := pruneQueue[head]
		for _, child := range children[cur] {
			if _, seen := prunedSet[child]; !seen {
				prunedSet[child] = struct{}{}
				pruneQueue = append(pruneQueue, child)
			}
		}
	}

	// BFS propagate SystemDirSet down all descendants (cycle-safe via visited set check)
	for head := 0; head < len(sysQueue); head++ {
		cur := sysQueue[head]
		for _, child := range children[cur] {
			if _, seen := systemSet[child]; !seen {
				systemSet[child] = struct{}{}
				sysQueue = append(sysQueue, child)
			}
		}
	}

	return &Pruner{
		rules:            b.rules,
		prunedDirs:       prunedSet,
		hardExcludedDirs: hardExcludedSelf,
		foldedDirs:       foldedSelf,
		systemDirs:       systemSet,
	}
}

// Pruner provides O(1) Pass 2 filtering decisions over flat MFT streams.
type Pruner struct {
	rules            *RuleSet
	prunedDirs       map[uint64]struct{}
	hardExcludedDirs map[uint64]struct{}
	foldedDirs       map[uint64]struct{}
	systemDirs       map[uint64]struct{}
}

// PrunedDirCount returns the size of PrunedDirSet.
func (p *Pruner) PrunedDirCount() int {
	return len(p.prunedDirs)
}

// SystemDirCount returns the size of SystemDirSet.
func (p *Pruner) SystemDirCount() int {
	return len(p.systemDirs)
}

// IsPrunedDir reports whether a directory FRN is inside PrunedDirSet.
func (p *Pruner) IsPrunedDir(frn uint64) bool {
	_, ok := p.prunedDirs[frn]
	return ok
}

// IsSystemDir reports whether a directory FRN is inside SystemDirSet.
func (p *Pruner) IsSystemDir(frn uint64) bool {
	_, ok := p.systemDirs[frn]
	return ok
}

// Evaluate decides in O(1) time whether a Pass 2 record is kept and with what flags.
func (p *Pruner) Evaluate(rec RawRecord) Pass2Decision {
	// If parent directory is pruned (HardExclude or Fold), drop immediately unless self-parent root.
	if rec.ParentFRN != rec.FRN {
		if _, parentPruned := p.prunedDirs[rec.ParentFRN]; parentPruned {
			return Pass2Decision{Keep: false}
		}
	}

	if rec.IsDir {
		if _, hard := p.hardExcludedDirs[rec.FRN]; hard {
			return Pass2Decision{Keep: false}
		}
		_, folded := p.foldedDirs[rec.FRN]
		_, sys := p.systemDirs[rec.FRN]
		if !sys {
			_, sys = p.systemDirs[rec.ParentFRN]
		}
		return Pass2Decision{
			Keep:   true,
			Folded: folded,
			System: sys,
		}
	}

	// File record
	if p.rules.ClassifyFile(rec.Name) == ActionHardExclude {
		return Pass2Decision{Keep: false}
	}
	_, sys := p.systemDirs[rec.ParentFRN]
	return Pass2Decision{
		Keep:   true,
		Folded: false,
		System: sys,
	}
}
