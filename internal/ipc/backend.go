package ipc

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mr-dadong/Xift/internal/filter"
	"github.com/mr-dadong/Xift/internal/query"
	"github.com/mr-dadong/Xift/internal/storage"
)

// QueryBackendOption customizes optional governance and Frecency extensions on QueryBackend.
type QueryBackendOption func(*QueryBackend)

// WithFrecencyStore attaches a persistent FrecencyStore to handle RecordOpen RPCs.
func WithFrecencyStore(fs *query.FrecencyStore) QueryBackendOption {
	return func(b *QueryBackend) {
		b.frecency = fs
	}
}

// WithPruner attaches a filter.Pruner and rules persistence path to handle ApplyFilterRule RPCs.
func WithPruner(pruner *filter.Pruner, rulesPath string) QueryBackendOption {
	return func(b *QueryBackend) {
		b.pruner = pruner
		b.rulesPath = rulesPath
	}
}

// QueryBackend adapts storage.Engine + query.Engine to the IPC Backend interface.
type QueryBackend struct {
	store          *storage.Engine
	engine         *query.Engine
	frecency       *query.FrecencyStore
	pruner         *filter.Pruner
	rulesPath      string
	l2             AdminL2
	watch          AdminWatch
	govMu          sync.Mutex
	totalColdReads atomic.Int64
}

// NewQueryBackend creates a production IPC Backend over store and engine.
func NewQueryBackend(store *storage.Engine, engine *query.Engine, opts ...QueryBackendOption) *QueryBackend {
	b := &QueryBackend{
		store:  store,
		engine: engine,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(b)
		}
	}
	return b
}

// RecordOpen records a file open event in the persistent FrecencyStore.
func (b *QueryBackend) RecordOpen(_ context.Context, path string) error {
	if b.frecency == nil {
		return nil
	}
	b.frecency.Record(path)
	return b.frecency.Save()
}

// ApplyFilterRule persists a dynamic Fold or HardExclude rule, updates PrunedDirSet,
// and logically tombstones affected nodes in the index without touching user disk files.
func (b *QueryBackend) ApplyFilterRule(ctx context.Context, req ApplyFilterRuleReq) (uint32, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if b.store == nil {
		return 0, &RPCError{Code: ErrInternal, Message: "storage engine unavailable"}
	}

	targetNorm := normalizeIPCPath(req.TargetPath)
	if targetNorm == "" {
		return 0, &RPCError{Code: ErrBadRequest, Message: "empty target path"}
	}

	var action filter.Action
	switch req.Action {
	case FilterActionFold:
		action = filter.ActionFold
	case FilterActionHardExclude:
		action = filter.ActionHardExclude
	default:
		return 0, &RPCError{Code: ErrBadRequest, Message: fmt.Sprintf("unsupported filter action %d", req.Action)}
	}

	b.govMu.Lock()
	defer b.govMu.Unlock()

	snap := b.store.AcquireSnapshot()
	var (
		foundTarget  bool
		targetNodeID uint32
		targetNode   storage.HotNode
		targetName   string
		children     = make(map[uint32][]uint32)
	)

	// Fast check by hint NodeID first.
	if req.NodeID != 0 {
		if node, name, ok := snap.LookupNode(req.NodeID); ok {
			if resolved, err := snap.ResolvePathWithSep(req.NodeID, '/'); err == nil && pathsEqualLoose(resolved, targetNorm) {
				foundTarget = true
				targetNodeID = req.NodeID
				targetNode = node
				targetName = string(name)
			}
		}
	}

	// Build parent->children map and locate targetNodeID if not matched by hint.
	snap.ForEachLive(func(nodeID uint32, node storage.HotNode, name []byte) bool {
		if node.ParentIdx != storage.RootParentIdx && node.ParentIdx != storage.OrphanParentIdx {
			children[node.ParentIdx] = append(children[node.ParentIdx], nodeID)
		}
		if !foundTarget {
			if resolved, err := snap.ResolvePathWithSep(nodeID, '/'); err == nil && pathsEqualLoose(resolved, targetNorm) {
				foundTarget = true
				targetNodeID = nodeID
				targetNode = node
				targetName = string(name)
			}
		}
		return true
	})

	// Collect all descendant NodeIDs via BFS.
	descIDs := make(map[uint32]struct{})
	if foundTarget && len(children[targetNodeID]) > 0 {
		visited := map[uint32]struct{}{targetNodeID: {}}
		queue := append([]uint32(nil), children[targetNodeID]...)
		for head := 0; head < len(queue); head++ {
			id := queue[head]
			if _, seen := visited[id]; seen {
				continue
			}
			visited[id] = struct{}{}
			descIDs[id] = struct{}{}
			queue = append(queue, children[id]...)
		}
	}

	// Resolve NodeID -> NodeKey for target, target's parent (if Fold), and all descendants.
	needKeys := make(map[uint32]struct{}, len(descIDs)+2)
	for id := range descIDs {
		needKeys[id] = struct{}{}
	}
	if foundTarget {
		needKeys[targetNodeID] = struct{}{}
		if action == filter.ActionFold && targetNode.ParentIdx != storage.RootParentIdx && targetNode.ParentIdx != storage.OrphanParentIdx {
			needKeys[targetNode.ParentIdx] = struct{}{}
		}
	}

	nodeKeys := make(map[uint32]uint64, len(needKeys))
	if len(needKeys) > 0 {
		if mem := snap.MemTable(); mem != nil {
			mem.ForEachLive(func(nodeID uint32, _ storage.HotNode, _ []byte) bool {
				if _, ok := needKeys[nodeID]; ok {
					if slot, okSlot := mem.GetByNodeID(nodeID); okSlot {
						nodeKeys[nodeID] = mem.KeyAt(slot)
						delete(needKeys, nodeID)
					}
				}
				return len(needKeys) > 0
			})
		}
		if len(needKeys) > 0 {
			segs := snap.Segments()
			for i := len(segs) - 1; i >= 0 && len(needKeys) > 0; i-- {
				segs[i].ForEachKey(func(_ uint16, nodeKey uint64, nodeID uint32) bool {
					if _, ok := needKeys[nodeID]; ok {
						nodeKeys[nodeID] = nodeKey
						delete(needKeys, nodeID)
					}
					return len(needKeys) > 0
				})
			}
		}
	}

	var targetCold storage.ColdMeta
	if foundTarget && action == filter.ActionFold {
		targetCold, _ = snap.ReadColdMeta(targetNodeID)
	}
	snap.Release()

	volID := req.VolID
	var targetKey uint64
	if foundTarget {
		volID = targetNode.VolID
		targetKey = nodeKeys[targetNodeID]
	}

	if b.pruner != nil {
		rule := filter.UserRule{
			Kind:    filter.RuleKindPathPrefix,
			Pattern: targetNorm,
			Action:  action,
			Enabled: true,
		}
		if err := b.pruner.ApplyDynamicDirRule(volID, targetKey, rule); err != nil {
			return 0, &RPCError{Code: ErrBadRequest, Message: err.Error()}
		}
		if b.rulesPath != "" {
			if err := b.pruner.Rules().SaveUserRules(b.rulesPath); err != nil {
				return 0, &RPCError{Code: ErrInternal, Message: fmt.Sprintf("persist rules: %v", err)}
			}
		}
	}

	if !foundTarget || targetKey == 0 {
		return 0, nil
	}

	muts := make([]storage.Mutation, 0, len(descIDs)+1)
	for id := range descIDs {
		if k, ok := nodeKeys[id]; ok && k != 0 {
			muts = append(muts, storage.Mutation{
				Op:      storage.OpDelete,
				VolID:   volID,
				NodeKey: k,
			})
		}
	}

	affected := uint32(len(muts))
	if action == filter.ActionFold {
		var parentKey uint64
		if targetNode.ParentIdx != storage.RootParentIdx && targetNode.ParentIdx != storage.OrphanParentIdx {
			parentKey = nodeKeys[targetNode.ParentIdx]
		}
		muts = append(muts, storage.Mutation{
			Op:        storage.OpCreate,
			VolID:     volID,
			NodeKey:   targetKey,
			ParentKey: parentKey,
			Name:      targetName,
			Flags:     targetNode.Flags | storage.FlagDir | storage.FlagFolded,
			Size:      targetCold.Size,
			ModTime:   targetCold.ModTime,
		})
	} else {
		muts = append(muts, storage.Mutation{
			Op:      storage.OpDelete,
			VolID:   volID,
			NodeKey: targetKey,
		})
		affected++
	}

	if len(muts) > 0 {
		if err := b.store.ApplyBatch(muts); err != nil {
			return 0, &RPCError{Code: ErrInternal, Message: fmt.Sprintf("apply index rule: %v", err)}
		}
	}
	return affected, nil
}

func normalizeIPCPath(p string) string {
	s := strings.TrimSpace(p)
	if s == "" {
		return ""
	}
	s = strings.ReplaceAll(s, "\\", "/")
	for len(s) > 1 && strings.HasSuffix(s, "/") {
		s = strings.TrimSuffix(s, "/")
	}
	return s
}

func pathsEqualLoose(a, b string) bool {
	na := strings.TrimPrefix(normalizeIPCPath(a), "/")
	nb := strings.TrimPrefix(normalizeIPCPath(b), "/")
	return strings.EqualFold(na, nb)
}

// NewSession returns a per-connection SessionBackend preserving incremental query refinement.
func (b *QueryBackend) NewSession() SessionBackend {
	return &querySessionBackend{
		parent: b,
		sess:   b.engine.NewSession(),
	}
}

// Status returns the current index live node count.
func (b *QueryBackend) Status() BackendStatus {
	var live uint64
	if b.store != nil {
		live = b.store.LiveCount()
	}
	return BackendStatus{LiveCount: live}
}

// TotalColdReads returns the cumulative cold-column reads observed across all results created by this backend.
func (b *QueryBackend) TotalColdReads() int64 {
	return b.totalColdReads.Load()
}

type querySessionBackend struct {
	parent *QueryBackend
	sess   *query.Session
}

func (s *querySessionBackend) Start(ctx context.Context, req QueryRequest) (Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, err := query.ParseAt(req.Text, time.Now()); err != nil {
		return nil, &RPCError{Code: ErrQueryParse, Message: err.Error()}
	}
	rs, err := s.sess.Start(ctx, req.Text, query.SearchOptions{
		Sort:    query.SortOrder(req.Sort),
		Desc:    req.Desc,
		PathSep: req.PathSep,
	})
	if err != nil {
		return nil, err
	}
	initialCold := int64(rs.ColdReads())
	if initialCold > 0 {
		s.parent.totalColdReads.Add(initialCold)
	}
	return &queryResultAdapter{
		parent:        s.parent,
		rs:            rs,
		lastColdReads: initialCold,
	}, nil
}

func (s *querySessionBackend) Close() {
	s.sess.Cancel()
	s.sess.Reset()
}

type queryResultAdapter struct {
	parent        *QueryBackend
	rs            *query.ResultSet
	lastColdReads int64
}

func (a *queryResultAdapter) Total() uint64      { return uint64(a.rs.Total()) }
func (a *queryResultAdapter) Retained() uint32   { return uint32(a.rs.Retained()) }
func (a *queryResultAdapter) Truncated() bool    { return a.rs.Truncated() }
func (a *queryResultAdapter) Incremental() bool  { return a.rs.Incremental() }
func (a *queryResultAdapter) Warnings() []string { return a.rs.Warnings() }
func (a *queryResultAdapter) Close()             { a.rs.Close() }

func (a *queryResultAdapter) CostMicros() uint32 {
	us := a.rs.Cost().Microseconds()
	if us < 0 {
		return 0
	}
	if us > 0xFFFFFFFF {
		return 0xFFFFFFFF
	}
	return uint32(us)
}

func (a *queryResultAdapter) Viewport(ctx context.Context, offset, limit int) ([]ViewportRow, error) {
	before := int64(a.rs.ColdReads())
	items, err := a.rs.Viewport(ctx, offset, limit)
	after := int64(a.rs.ColdReads())
	if delta := after - before; delta > 0 {
		a.parent.totalColdReads.Add(delta)
	}
	if err != nil {
		return nil, err
	}
	rows := make([]ViewportRow, len(items))
	for i := range items {
		it := &items[i]
		flags := it.Flags
		if it.IsDir {
			flags |= storage.FlagDir
		}
		if it.Folded {
			flags |= storage.FlagFolded
		}
		if it.System {
			flags |= storage.FlagSystem
		}
		if it.FromFoldedScan {
			flags |= RowFlagFromFoldedScan
		}
		var hls []HighlightRange
		if len(it.Highlights) > 0 {
			hls = make([]HighlightRange, len(it.Highlights))
			for j, h := range it.Highlights {
				hls[j] = HighlightRange{Start: uint32(h.Start), End: uint32(h.End)}
			}
		}
		rows[i] = ViewportRow{
			NodeID:     it.NodeID,
			Flags:      flags,
			ExtID:      it.ExtID,
			VolID:      it.VolID,
			Size:       it.Size,
			ModTime:    it.ModTime,
			Score:      it.Score,
			Path:       it.Path,
			Name:       it.Name,
			Highlights: hls,
		}
	}
	return rows, nil
}
