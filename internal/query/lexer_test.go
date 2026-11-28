package query

import (
	"testing"
	"time"
)

func TestParseCompoundANDORNegation(t *testing.T) {
	now := time.Date(2026, 11, 27, 15, 0, 0, 0, time.UTC)
	q, err := ParseAt(`report 2026 ext:PDF,.docx !draft | *.md path:docs`, now)
	if err != nil {
		t.Fatalf("ParseAt failed: %v", err)
	}
	if q.Root == nil || q.Root.Kind != ExprOr || len(q.Root.Children) != 2 {
		t.Fatalf("expected top-level OR with 2 branches, got %#v", q.Root)
	}

	left := q.Root.Children[0]
	if left.Kind != ExprAnd || len(left.Children) != 4 {
		t.Fatalf("expected left AND with 4 children, got %#v", left)
	}
	if left.Children[0].Atom.Kind != AtomName || left.Children[0].Atom.Value != "report" {
		t.Fatalf("unexpected child 0: %#v", left.Children[0])
	}
	if left.Children[1].Atom.Kind != AtomName || left.Children[1].Atom.Value != "2026" {
		t.Fatalf("unexpected child 1: %#v", left.Children[1])
	}
	extAtom := left.Children[2].Atom
	if extAtom.Kind != AtomExt || len(extAtom.Exts) != 2 || extAtom.Exts[0] != "pdf" || extAtom.Exts[1] != "docx" {
		t.Fatalf("unexpected ext atom: %#v", extAtom)
	}
	notDraft := left.Children[3]
	if notDraft.Kind != ExprNot || notDraft.Child == nil || notDraft.Child.Atom.Value != "draft" {
		t.Fatalf("unexpected negated atom: %#v", notDraft)
	}

	right := q.Root.Children[1]
	if right.Kind != ExprAnd || len(right.Children) != 2 {
		t.Fatalf("expected right AND with 2 children, got %#v", right)
	}
	wc := right.Children[0].Atom
	if wc.Kind != AtomWildcard || wc.PureExt != "md" {
		t.Fatalf("expected pure *.md wildcard with PureExt=md, got %#v", wc)
	}
	pAtom := right.Children[1].Atom
	if pAtom.Kind != AtomPath || pAtom.Value != "docs" {
		t.Fatalf("unexpected path atom: %#v", pAtom)
	}
}

func TestParseSizeAndModTime(t *testing.T) {
	now := time.Date(2026, 11, 27, 15, 0, 0, 0, time.UTC)

	q, err := ParseAt(`size:>50mb size:1kb..10mb dm:7d dm:2026-01-01..2026-01-31`, now)
	if err != nil {
		t.Fatalf("ParseAt failed: %v", err)
	}
	atoms, ok := q.SimpleAndAtoms()
	if !ok || len(atoms) != 4 {
		t.Fatalf("expected 4 simple AND atoms, ok=%v len=%d", ok, len(atoms))
	}

	s1 := atoms[0].Size
	const mb = int64(1024 * 1024)
	if !s1.HasMin || s1.Min != 50*mb+1 || s1.HasMax {
		t.Fatalf("unexpected size:>50mb: %+v", s1)
	}
	s2 := atoms[1].Size
	if !s2.HasMin || s2.Min != 1024 || !s2.HasMax || s2.Max != 10*mb {
		t.Fatalf("unexpected size:1kb..10mb: %+v", s2)
	}

	d1 := atoms[2].ModTime
	wantMin := now.Add(-7 * 24 * time.Hour).UnixNano()
	if !d1.HasMin || d1.MinNano != wantMin || d1.HasMax {
		t.Fatalf("unexpected dm:7d: %+v (want min %d)", d1, wantMin)
	}
	d2 := atoms[3].ModTime
	wantStart := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	wantEnd := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC).UnixNano() - 1
	if !d2.HasMin || d2.MinNano != wantStart || !d2.HasMax || d2.MaxNano != wantEnd {
		t.Fatalf("unexpected dm range: %+v (want %d..%d)", d2, wantStart, wantEnd)
	}
}

func TestParseSystemAndFoldedModifiers(t *testing.T) {
	now := time.Date(2026, 11, 27, 15, 0, 0, 0, time.UTC)

	q1, err := ParseAt(`all:kernel32`, now)
	if err != nil {
		t.Fatalf("ParseAt all:kernel32: %v", err)
	}
	if !q1.IncludeSystem {
		t.Fatalf("expected IncludeSystem=true for all:kernel32")
	}
	atoms, ok := q1.SimpleAndAtoms()
	if !ok || len(atoms) != 1 || atoms[0].Value != "kernel32" {
		t.Fatalf("expected kernel32 term, got %#v", atoms)
	}

	q2, err := ParseAt(`sys: ntdll`, now)
	if err != nil {
		t.Fatalf("ParseAt sys: ntdll: %v", err)
	}
	if !q2.IncludeSystem {
		t.Fatalf("expected IncludeSystem=true for sys:")
	}
	atoms2, ok := q2.SimpleAndAtoms()
	if !ok || len(atoms2) != 1 || atoms2[0].Value != "ntdll" {
		t.Fatalf("expected ntdll term, got %#v", atoms2)
	}

	q3, err := ParseAt(`in:node_modules lodash ext:js`, now)
	if err != nil {
		t.Fatalf("ParseAt in:node_modules: %v", err)
	}
	if len(q3.InDirs) != 1 || q3.InDirs[0] != "node_modules" {
		t.Fatalf("expected InDirs=[node_modules], got %#v", q3.InDirs)
	}
	atoms3, ok := q3.SimpleAndAtoms()
	if !ok || len(atoms3) != 2 {
		t.Fatalf("expected 2 filter atoms alongside in:, got %#v", atoms3)
	}
}

func TestParseQuotedParensAndType(t *testing.T) {
	now := time.Date(2026, 11, 27, 15, 0, 0, 0, time.UTC)
	q, err := ParseAt(`path:"Program Files\My App" ("hello world" | foo) type:dir`, now)
	if err != nil {
		t.Fatalf("ParseAt failed: %v", err)
	}
	if q.Root == nil || q.Root.Kind != ExprAnd || len(q.Root.Children) != 3 {
		t.Fatalf("expected 3 AND children, got %#v", q.Root)
	}
	if q.Root.Children[0].Atom.Value != "program files/my app" {
		t.Fatalf("expected normalized path value, got %q", q.Root.Children[0].Atom.Value)
	}
	if q.Root.Children[1].Kind != ExprOr {
		t.Fatalf("expected grouped OR, got %#v", q.Root.Children[1])
	}
	if q.Root.Children[2].Atom.Kind != AtomType || !q.Root.Children[2].Atom.WantDir {
		t.Fatalf("expected type:dir atom, got %#v", q.Root.Children[2])
	}
}

func TestParseMalformedGracefully(t *testing.T) {
	now := time.Date(2026, 11, 27, 15, 0, 0, 0, time.UTC)

	badInputs := []string{
		`size:>xyz`,
		`dm:notadate`,
		`ext:`,
		`size:`,
		`dm:`,
		`type:socket`,
	}
	for _, in := range badInputs {
		if _, err := ParseAt(in, now); err == nil {
			t.Errorf("expected error for malformed input %q, got nil", in)
		}
	}

	// Unclosed quote should not panic and should return an error.
	if _, err := ParseAt(`"unterminated`, now); err == nil {
		t.Errorf("expected error for unterminated quote")
	}

	// Empty / whitespace query is valid (matches all nodes).
	q, err := ParseAt("   ", now)
	if err != nil {
		t.Fatalf("empty query should succeed, got %v", err)
	}
	if q.Root != nil {
		t.Fatalf("expected nil Root for empty query, got %#v", q.Root)
	}

	// content: parses into a real atom that ANDs with the other conditions.
	qc, err := ParseAt(`content:"mutex" ext:go`, now)
	if err != nil {
		t.Fatalf("content: should parse cleanly, got %v", err)
	}
	if len(qc.ContentTerms) != 1 || qc.ContentTerms[0] != "mutex" || len(qc.Warnings) != 0 {
		t.Fatalf("expected content term and no warning, got terms=%#v warnings=%#v", qc.ContentTerms, qc.Warnings)
	}
	if qc.Root == nil || qc.Root.Kind != ExprAnd || len(qc.Root.Children) != 2 {
		t.Fatalf("expected content: to AND with ext:, got %#v", qc.Root)
	}
	if qc.Root.Children[0].Atom.Kind != AtomContent {
		t.Fatalf("expected first child to be a content atom, got %#v", qc.Root.Children[0].Atom)
	}
}
