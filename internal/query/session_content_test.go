package query

import (
	"context"
	"testing"
)

// TestSessionContentQuery verifies daemon-style session queries route through
// the L2 content searcher, both for content-only and mixed predicates.
func TestSessionContentQuery(t *testing.T) {
	eng, ids := buildContentStorage(t)
	searcher := &fakeContentSearcher{sets: map[string][]uint32{
		"mutex": {ids["mutex_guide.md"]},
	}}
	qe := NewEngine(eng, WithContentSearcher(searcher))
	sess := qe.NewSession()
	ctx := context.Background()

	res, err := sess.Search(ctx, `content:"mutex"`, SearchOptions{Limit: 10})
	if err != nil {
		t.Fatalf("session content search: %v", err)
	}
	if res.Total != 1 || len(res.Items) != 1 || res.Items[0].Name != "mutex_guide.md" {
		t.Fatalf("content-only results = %+v, want mutex_guide.md", res.Items)
	}
	if len(searcher.calls) != 1 || searcher.calls[0] != "mutex" {
		t.Fatalf("searcher calls = %v, want [mutex]", searcher.calls)
	}

	// A subsequent mixed query on the same session must still attach the
	// content candidate set instead of scanning with an empty one.
	res, err = sess.Search(ctx, `content:"mutex" ext:md`, SearchOptions{Limit: 10})
	if err != nil {
		t.Fatalf("session mixed search: %v", err)
	}
	if res.Total != 1 || len(res.Items) != 1 || res.Items[0].Name != "mutex_guide.md" {
		t.Fatalf("mixed results = %+v, want mutex_guide.md", res.Items)
	}

	res, err = sess.Search(ctx, `content:"absent"`, SearchOptions{Limit: 10})
	if err != nil {
		t.Fatalf("session miss search: %v", err)
	}
	if res.Total != 0 {
		t.Fatalf("absent term total = %d, want 0", res.Total)
	}
}

// TestSessionContentWithoutSearcher verifies the warning path is preserved for
// sessions when L2 is disabled.
func TestSessionContentWithoutSearcher(t *testing.T) {
	eng, _ := buildContentStorage(t)
	res, err := NewEngine(eng).NewSession().Search(context.Background(), `content:"mutex"`, SearchOptions{Limit: 10})
	if err != nil {
		t.Fatalf("session search: %v", err)
	}
	if res.Total != 0 {
		t.Fatalf("total = %d, want 0 without L2", res.Total)
	}
	if len(res.Warnings) == 0 {
		t.Fatal("expected a content-disabled warning")
	}
}
