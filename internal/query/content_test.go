package query

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/mr-dadong/Xift/internal/storage"
)

// fakeContentSearcher returns canned L1 node sets per folded term.
type fakeContentSearcher struct {
	sets  map[string][]uint32
	err   error
	calls []string
}

func (f *fakeContentSearcher) Search(term []byte, _ int) ([]uint32, bool, error) {
	if f.err != nil {
		return nil, false, f.err
	}
	f.calls = append(f.calls, string(term))
	return f.sets[string(term)], false, nil
}

func buildContentStorage(t *testing.T) (*storage.Engine, map[string]uint32) {
	t.Helper()
	now := time.Date(2026, 11, 28, 9, 0, 0, 0, time.UTC)
	eng, err := storage.Open(t.TempDir(), storage.Options{DisableWALSync: true})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })

	b, err := eng.BeginDenseSegment()
	if err != nil {
		t.Fatalf("BeginDenseSegment: %v", err)
	}
	root, err := b.AppendDense(1, 1, storage.RootParentIdx, "proj", storage.FlagDir, 0, 0, now.UnixNano())
	if err != nil {
		t.Fatal(err)
	}
	ids := make(map[string]uint32)
	for i, name := range []string{"a.go", "b.go", "notes.txt", "mutex_guide.md"} {
		id, err := b.AppendDense(1, uint64(i+2), root, name, 0, 0, 128, now.UnixNano())
		if err != nil {
			t.Fatal(err)
		}
		ids[name] = id
	}
	meta, err := b.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.CommitSegment(meta, nil); err != nil {
		t.Fatal(err)
	}
	return eng, ids
}

func namesOf(items []ResultItem) []string {
	out := make([]string, len(items))
	for i := range items {
		out[i] = items[i].Name
	}
	return out
}

func hasName(items []ResultItem, name string) bool {
	for _, it := range items {
		if it.Name == name {
			return true
		}
	}
	return false
}

// TestParseContentAtom verifies content: becomes an AST atom instead of a
// warning, keeps quoted spaces, and composes with AND / OR.
func TestParseContentAtom(t *testing.T) {
	q, err := Parse(`content:"mutex lock"`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if q.Root == nil || q.Root.Kind != ExprAtom || q.Root.Atom.Kind != AtomContent {
		t.Fatalf("root = %+v, want a content atom", q.Root)
	}
	if q.Root.Atom.Value != "mutex lock" || q.Root.Atom.Raw != "mutex lock" {
		t.Fatalf("atom = %+v, want quoted term preserved", q.Root.Atom)
	}
	if len(q.ContentTerms) != 1 || q.ContentTerms[0] != "mutex lock" {
		t.Fatalf("ContentTerms = %v", q.ContentTerms)
	}
	if len(q.Warnings) != 0 {
		t.Fatalf("warnings = %v, want none", q.Warnings)
	}

	q, err = Parse(`content:"a b" content:"c"`)
	if err != nil {
		t.Fatalf("Parse AND: %v", err)
	}
	if q.Root == nil || q.Root.Kind != ExprAnd || len(q.Root.Children) != 2 {
		t.Fatalf("AND root = %+v", q.Root)
	}
	for _, ch := range q.Root.Children {
		if ch.Atom.Kind != AtomContent {
			t.Fatalf("child = %+v, want content atom", ch.Atom)
		}
	}

	q, err = Parse(`content:"a" | content:"b"`)
	if err != nil {
		t.Fatalf("Parse OR: %v", err)
	}
	if q.Root == nil || q.Root.Kind != ExprOr || len(q.Root.Children) != 2 {
		t.Fatalf("OR root = %+v", q.Root)
	}

	if _, err := Parse(`content:`); err == nil {
		t.Fatal("empty content: term must fail to parse")
	}
}

// TestContentFilterIntersection verifies L2 hits intersect the L1 conditions,
// and a content-only query uses the L2 node set as candidates.
func TestContentFilterIntersection(t *testing.T) {
	eng, ids := buildContentStorage(t)
	searcher := &fakeContentSearcher{sets: map[string][]uint32{
		"mutex": {ids["a.go"], ids["mutex_guide.md"]},
	}}
	qe := NewEngine(eng, WithContentSearcher(searcher))
	ctx := context.Background()

	res, err := qe.Search(ctx, `content:"MUTEX" ext:go`, SearchOptions{Limit: 20})
	if err != nil {
		t.Fatalf("Search ext:go: %v", err)
	}
	if res.Total != 1 || !hasName(res.Items, "a.go") {
		t.Fatalf("content+ext results = %v, want only a.go", namesOf(res.Items))
	}
	for _, call := range searcher.calls {
		if call != "mutex" {
			t.Fatalf("searcher terms = %v, want ASCII-folded mutex", searcher.calls)
		}
	}

	res, err = qe.Search(ctx, `content:"mutex"`, SearchOptions{Limit: 20})
	if err != nil {
		t.Fatalf("Search content-only: %v", err)
	}
	if res.Total != 2 || !hasName(res.Items, "a.go") || !hasName(res.Items, "mutex_guide.md") {
		t.Fatalf("content-only results = %v", namesOf(res.Items))
	}

	res, err = qe.Search(ctx, `content:"absent"`, SearchOptions{Limit: 20})
	if err != nil {
		t.Fatalf("Search absent: %v", err)
	}
	if res.Total != 0 {
		t.Fatalf("absent content matched %d rows", res.Total)
	}

	n, err := qe.Count(ctx, `content:"mutex" ext:go`, SearchOptions{})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 1 {
		t.Fatalf("Count = %d, want 1", n)
	}
}

// TestContentDisabledWarning verifies a content: query without L2 returns empty
// results plus an explicit warning instead of silently ignoring the condition.
func TestContentDisabledWarning(t *testing.T) {
	eng, _ := buildContentStorage(t)
	qe := NewEngine(eng)

	res, err := qe.Search(context.Background(), `content:"mutex"`, SearchOptions{})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if res.Total != 0 || len(res.Items) != 0 {
		t.Fatalf("disabled L2 returned rows: %+v", res.Items)
	}
	if len(res.Warnings) == 0 || !strings.Contains(strings.Join(res.Warnings, " "), "not enabled") {
		t.Fatalf("warnings = %v, want an explicit content-disabled warning", res.Warnings)
	}

	n, err := qe.Count(context.Background(), `content:"mutex" ext:go`, SearchOptions{})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 0 {
		t.Fatalf("Count with disabled L2 = %d, want 0", n)
	}
}

// TestContentFileLevelResults verifies results stay file-level (no line or
// snippet fields) and content hits never widen the name condition.
func TestContentFileLevelResults(t *testing.T) {
	eng, ids := buildContentStorage(t)
	searcher := &fakeContentSearcher{sets: map[string][]uint32{
		"mutex": {ids["a.go"], ids["notes.txt"]},
	}}
	qe := NewEngine(eng, WithContentSearcher(searcher))
	ctx := context.Background()

	res, err := qe.Search(ctx, `a content:"mutex"`, SearchOptions{Limit: 20})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if res.Total != 1 || res.Items[0].Name != "a.go" {
		t.Fatalf("results = %v, want only a.go", namesOf(res.Items))
	}
	item := res.Items[0]
	if len(item.Highlights) == 0 {
		t.Fatal("name match must still carry filename highlights")
	}
	nameLen := len(item.Name)
	for _, h := range item.Highlights {
		if h.Start < 0 || h.End > nameLen || h.Start >= h.End {
			t.Fatalf("highlight %+v outside name %q", h, item.Name)
		}
	}

	raw, err := json.Marshal(item)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	for key := range decoded {
		lower := strings.ToLower(key)
		if strings.Contains(lower, "line") || strings.Contains(lower, "snippet") || strings.Contains(lower, "fragment") {
			t.Fatalf("file-level row leaked %q: %s", key, raw)
		}
	}

	// The name condition still restricts results: b.go matches `b` but its body
	// is not in the L2 hit set.
	res, err = qe.Search(ctx, `b content:"mutex"`, SearchOptions{Limit: 20})
	if err != nil {
		t.Fatalf("Search b: %v", err)
	}
	if res.Total != 0 {
		t.Fatalf("content hit widened name condition: %v", namesOf(res.Items))
	}
}
