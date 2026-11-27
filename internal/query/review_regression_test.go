package query

import (
	"context"
	"testing"
)

type staticFrecency map[uint32]float64

func (m staticFrecency) Score(nodeID uint32, _ string) float64 {
	return m[nodeID]
}

func TestRegressionNegatedGroupsAndColdSorts(t *testing.T) {
	store, now := buildTestStorage(t)
	qe := NewEngine(store)

	// 1. Negated parenthesized OR group: !(*.go | *.rs) type:file
	res, err := qe.Search(context.Background(), `type:file !(*.go | *.rs)`, SearchOptions{Now: now, Limit: 50})
	if err != nil {
		t.Fatalf("Search negated group: %v", err)
	}
	for _, it := range res.Items {
		if it.ExtID == store.ExtPool().Intern("go") || it.ExtID == store.ExtPool().Intern("rs") {
			t.Fatalf("negated group leaked excluded extension: %+v", it)
		}
	}

	// 2. Negated path filter: config.ini !path:windows
	resPath, err := qe.Search(context.Background(), `config.ini !path:windows`, SearchOptions{Now: now, Limit: 10})
	if err != nil {
		t.Fatalf("Search !path: %v", err)
	}
	if resPath.Total != 1 || resPath.Items[0].System {
		t.Fatalf("expected only non-windows config.ini, got %+v", resPath.Items)
	}

	// 3. SortBySize descending + pagination offset/limit.
	resSize, err := qe.Search(context.Background(), `type:file`, SearchOptions{
		Now:   now,
		Sort:  SortBySize,
		Desc:  true,
		Limit: 3,
	})
	if err != nil {
		t.Fatalf("Search SortBySize: %v", err)
	}
	if len(resSize.Items) != 3 {
		t.Fatalf("expected 3 items, got %d", len(resSize.Items))
	}
	if !(resSize.Items[0].Size >= resSize.Items[1].Size && resSize.Items[1].Size >= resSize.Items[2].Size) {
		t.Fatalf("expected descending size order, got %d, %d, %d",
			resSize.Items[0].Size, resSize.Items[1].Size, resSize.Items[2].Size)
	}

	// 4. SortByModTime descending: live_memtable.go (5 min ago) must be first.
	resMT, err := qe.Search(context.Background(), `type:file`, SearchOptions{
		Now:   now,
		Sort:  SortByModTime,
		Desc:  true,
		Limit: 1,
	})
	if err != nil {
		t.Fatalf("Search SortByModTime: %v", err)
	}
	if resMT.Returned != 1 || resMT.Items[0].Name != "live_memtable.go" {
		t.Fatalf("expected newest file live_memtable.go first, got %+v", resMT.Items)
	}

	// 5. dm:today matches files modified on 2026-11-27 (00:00..15:00).
	resToday, err := qe.Search(context.Background(), `dm:today type:file`, SearchOptions{Now: now, Limit: 20})
	if err != nil {
		t.Fatalf("Search dm:today: %v", err)
	}
	if resToday.Total < 3 {
		t.Fatalf("expected at least 3 files modified today, got %d", resToday.Total)
	}

	// 6. FrecencyProvider hook boosts target nodeID to top.
	resAll, err := qe.Search(context.Background(), `ext:go`, SearchOptions{Now: now, Limit: 10})
	if err != nil || len(resAll.Items) < 2 {
		t.Fatalf("expected >= 2 .go files, got %v / %d", err, len(resAll.Items))
	}
	boostID := resAll.Items[len(resAll.Items)-1].NodeID
	qeBoost := NewEngine(store, WithFrecency(staticFrecency{boostID: 5000}))
	resBoost, err := qeBoost.Search(context.Background(), `ext:go`, SearchOptions{Now: now, Limit: 10})
	if err != nil {
		t.Fatalf("Search with frecency: %v", err)
	}
	if resBoost.Items[0].NodeID != boostID {
		t.Fatalf("expected frecency-boosted nodeID %d at top, got %d", boostID, resBoost.Items[0].NodeID)
	}
}
