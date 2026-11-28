package query

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mr-dadong/Xift/internal/storage"
)

// DefaultMaxRetained is the default upper bound on ranked rows retained per ResultSet (8 MB).
const DefaultMaxRetained = 1_000_000

// ErrResultSetClosed is returned when Viewport is called on a closed ResultSet.
var ErrResultSetClosed = errors.New("query: result set closed")

// rankedRow is a pointer-free 8-byte entry kept in ResultSet for zero-GC viewport paging.
type rankedRow struct {
	nodeID uint32
	score  float32
}

// ResultSet holds the ranked node IDs and point-in-time storage snapshot from a query.Start call,
// allowing repeated Viewport fetches with snapshot consistency and viewport-only cold-column reads.
type ResultSet struct {
	mu          sync.RWMutex
	closed      bool
	snap        *storage.Snapshot
	query       string
	plan        *compiledPlan
	pathSep     byte
	total       int
	truncated   bool
	rows        []rankedRow
	foldedItems []ResultItem
	isFolded    bool
	cost        time.Duration
	incremental bool
	warnings    []string
	coldReads   atomic.Int64
}

// Query returns the original query string.
func (rs *ResultSet) Query() string {
	return rs.query
}

// Total returns the total number of matched nodes before MaxRetained truncation.
func (rs *ResultSet) Total() int {
	return rs.total
}

// Retained returns the number of ranked rows retained in this ResultSet.
func (rs *ResultSet) Retained() int {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	if rs.isFolded {
		return len(rs.foldedItems)
	}
	return len(rs.rows)
}

// Truncated reports whether the result set is incomplete: either the candidate
// budget was exhausted (Total is then a lower bound) or Total exceeded the
// retained Top-K rows.
func (rs *ResultSet) Truncated() bool {
	return rs.truncated || rs.total > rs.Retained()
}

// Cost returns the elapsed duration of the Start phase (parse + scan + rank).
func (rs *ResultSet) Cost() time.Duration {
	return rs.cost
}

// Incremental reports whether the query was satisfied by narrowing the session's previous cache.
func (rs *ResultSet) Incremental() bool {
	return rs.incremental
}

// Warnings returns any parser warnings associated with the query.
func (rs *ResultSet) Warnings() []string {
	return rs.warnings
}

// ColdReads returns the cumulative number of cold-column reads performed during Start and all Viewport calls.
func (rs *ResultSet) ColdReads() int {
	return int(rs.coldReads.Load())
}

// Close releases the held storage snapshot and frees retained rows. It is idempotent and safe for concurrent use.
func (rs *ResultSet) Close() {
	if rs == nil {
		return
	}
	rs.mu.Lock()
	if !rs.closed {
		rs.closed = true
		if rs.snap != nil {
			rs.snap.Release()
			rs.snap = nil
		}
		rs.rows = nil
		rs.foldedItems = nil
		rs.plan = nil
	}
	rs.mu.Unlock()
}

// Viewport materializes rows [offset, offset+limit) using the snapshot captured at Start time.
// Cold metadata is read strictly for the returned rows.
func (rs *ResultSet) Viewport(ctx context.Context, offset, limit int) ([]ResultItem, error) {
	if rs == nil {
		return nil, ErrResultSetClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		rs.mu.RLock()
		closed := rs.closed
		rs.mu.RUnlock()
		if closed {
			return nil, ErrResultSetClosed
		}
		return []ResultItem{}, nil
	}

	rs.mu.RLock()
	defer rs.mu.RUnlock()

	if rs.closed {
		return nil, ErrResultSetClosed
	}

	if rs.isFolded {
		n := len(rs.foldedItems)
		if offset >= n {
			return []ResultItem{}, nil
		}
		end := offset + limit
		if end > n {
			end = n
		}
		out := make([]ResultItem, end-offset)
		copy(out, rs.foldedItems[offset:end])
		return out, nil
	}

	n := len(rs.rows)
	if offset >= n {
		return []ResultItem{}, nil
	}
	end := offset + limit
	if end > n {
		end = n
	}
	vp := rs.rows[offset:end]
	items := make([]ResultItem, 0, len(vp))

	var posNames []compiledAtom
	if rs.plan != nil {
		posNames = rs.plan.positiveNames
	}

	for i := range vp {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		r := vp[i]
		node, nameBytes, ok := rs.snap.LookupNode(r.nodeID)
		if !ok {
			continue
		}
		pathStr, err := rs.snap.ResolvePathWithSep(r.nodeID, rs.pathSep)
		if err != nil {
			pathStr = string(nameBytes)
		}
		var size, mtime int64
		rs.coldReads.Add(1)
		if cm, err := rs.snap.ReadColdMeta(r.nodeID); err == nil {
			size = cm.Size
			mtime = cm.ModTime
		}
		highlights := computeHighlights(nameBytes, (node.Flags&storage.FlagNonASCII) != 0, posNames)
		item := ResultItem{
			NodeID:     r.nodeID,
			Path:       pathStr,
			Name:       string(nameBytes),
			Flags:      node.Flags,
			ExtID:      node.ExtID,
			VolID:      node.VolID,
			IsDir:      (node.Flags & storage.FlagDir) != 0,
			Folded:     (node.Flags & storage.FlagFolded) != 0,
			System:     (node.Flags & storage.FlagSystem) != 0,
			Size:       size,
			ModTime:    mtime,
			Score:      float64(r.score),
			Highlights: highlights,
		}
		if label, _, ok := rs.snap.SnapshotSource(r.nodeID); ok {
			item.SnapshotVol = label
			item.Offline = true
		}
		items = append(items, item)
	}
	return items, nil
}

// toSearchResult builds a legacy SearchResult from a ResultSet and a materialized viewport slice.
func (rs *ResultSet) toSearchResult(items []ResultItem, offset, limit int, start time.Time) *SearchResult {
	cost := time.Since(start)
	res := &SearchResult{
		Query:       rs.query,
		Total:       rs.total,
		Returned:    len(items),
		Offset:      offset,
		Limit:       limit,
		Cost:        cost,
		CostMs:      float64(cost.Microseconds()) / 1000.0,
		Incremental: rs.incremental,
		ColdReads:   rs.ColdReads(),
		Warnings:    rs.warnings,
		Items:       items,
		Truncated:   rs.Truncated(),
	}
	if rs.truncated {
		res.TotalLowerBound = rs.total
	}
	return res
}

// selectTopK places the k smallest elements (according to cmp) in items[:k] in sorted order
// using O(N) 3-way quickselect followed by sorting the top k elements.
func selectTopK[T any](items []T, k int, cmp func(a, b T) int) []T {
	if k <= 0 {
		return items[:0]
	}
	if len(items) <= k {
		slices.SortFunc(items, cmp)
		return items
	}

	lo, hi := 0, len(items)-1
	target := k
	for lo < hi {
		// Median-of-three pivot selection to avoid worst-case on sorted/reversed inputs.
		mid := lo + (hi-lo)/2
		if cmp(items[mid], items[lo]) < 0 {
			items[lo], items[mid] = items[mid], items[lo]
		}
		if cmp(items[hi], items[lo]) < 0 {
			items[lo], items[hi] = items[hi], items[lo]
		}
		if cmp(items[hi], items[mid]) < 0 {
			items[mid], items[hi] = items[hi], items[mid]
		}
		pivot := items[mid]

		// 3-way Dutch National Flag partition: [lo..lt-1] < pivot, [lt..gt] == pivot, [gt+1..hi] > pivot.
		lt, i, gt := lo, lo, hi
		for i <= gt {
			c := cmp(items[i], pivot)
			if c < 0 {
				items[lt], items[i] = items[i], items[lt]
				lt++
				i++
			} else if c > 0 {
				items[i], items[gt] = items[gt], items[i]
				gt--
			} else {
				i++
			}
		}

		if target <= lt {
			hi = lt - 1
		} else if target > gt+1 {
			lo = gt + 1
		} else {
			break
		}
	}

	top := items[:k]
	slices.SortFunc(top, cmp)
	return top
}
