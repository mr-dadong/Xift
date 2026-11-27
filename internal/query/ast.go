package query

// ExprKind identifies the type of node in a parsed DSL expression tree.
type ExprKind uint8

const (
	ExprAtom ExprKind = iota + 1
	ExprAnd
	ExprOr
	ExprNot
)

// AtomKind identifies the semantic kind of a leaf predicate in the DSL.
type AtomKind uint8

const (
	AtomName AtomKind = iota + 1
	AtomWildcard
	AtomExt
	AtomSize
	AtomModTime
	AtomPath
	AtomType
)

// SizeRange represents an inclusive byte size range [Min, Max].
type SizeRange struct {
	HasMin bool
	HasMax bool
	Min    int64
	Max    int64
}

// Matches reports whether size falls within the inclusive range.
func (r SizeRange) Matches(size int64) bool {
	if r.HasMin && size < r.Min {
		return false
	}
	if r.HasMax && size > r.Max {
		return false
	}
	return true
}

// TimeRange represents an inclusive Unix nanosecond timestamp range [MinNano, MaxNano].
type TimeRange struct {
	HasMin  bool
	HasMax  bool
	MinNano int64
	MaxNano int64
}

// Matches reports whether unixNano falls within the inclusive range.
func (r TimeRange) Matches(unixNano int64) bool {
	if r.HasMin && unixNano < r.MinNano {
		return false
	}
	if r.HasMax && unixNano > r.MaxNano {
		return false
	}
	return true
}

// Atom is a single normalized DSL leaf predicate.
type Atom struct {
	Kind        AtomKind
	Value       string   // Lowercased normalized value for Name / Wildcard / Path
	Raw         string   // Original un-lowercased value
	PureExt     string   // For pure "*.ext" wildcards, stores lowercase "ext" for O(1) ExtID fast path
	Exts        []string // For AtomExt: normalized lowercase extensions without leading dot
	Size        SizeRange
	ModTime     TimeRange
	HasWildcard bool // For AtomPath: true when Value contains '*' or '?'
	WantDir     bool // For AtomType: true for dir/folder, false for file
}

// Expr is a node in the parsed DSL boolean expression tree.
type Expr struct {
	Kind     ExprKind
	Atom     Atom
	Child    *Expr   // Used by ExprNot
	Children []*Expr // Used by ExprAnd / ExprOr
}

// Query is the top-level parsed representation of a Xift DSL query string.
type Query struct {
	Raw           string
	Root          *Expr    // Nil means match all live nodes (empty filter)
	IncludeSystem bool     // Set by all: or sys:
	InDirs        []string // Folded/named directories requested by in:<dir>
	ContentTerms  []string // Reserved for Phase 3 P3-02 L2 full-text search
	Warnings      []string // Non-fatal diagnostics (e.g. L2 content: not yet enabled)
}

// SimpleAndAtoms returns the flat list of positive leaf atoms if q.Root is a single
// atom or a pure AND of positive atoms (no OR, no nested groups, no NOT).
func (q *Query) SimpleAndAtoms() ([]Atom, bool) {
	if q == nil || q.Root == nil {
		return nil, true
	}
	switch q.Root.Kind {
	case ExprAtom:
		return []Atom{q.Root.Atom}, true
	case ExprAnd:
		out := make([]Atom, 0, len(q.Root.Children))
		for _, ch := range q.Root.Children {
			if ch.Kind != ExprAtom {
				return nil, false
			}
			out = append(out, ch.Atom)
		}
		return out, true
	default:
		return nil, false
	}
}
