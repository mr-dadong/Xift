package ipc

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/mr-dadong/Xift/internal/l2search"
	"github.com/mr-dadong/Xift/internal/storage"
)

// L2Status is the wire form of the L2 indexer counters.
type L2Status struct {
	Roots    []string       `json:"roots"`
	Indexed  int            `json:"indexed"`
	Deleted  int            `json:"deleted"`
	Batches  int            `json:"batches"`
	Skipped  map[string]int `json:"skipped,omitempty"`
	Paused   bool           `json:"paused"`
	DiskUsed int64          `json:"disk_used"`
	DiskCap  int64          `json:"disk_cap"`
}

// AdminL2 is implemented by the daemon's L2 content pipeline.
type AdminL2 interface {
	AddRoot(dir string) ([]string, error)
	RemoveRoot(dir string) ([]string, error)
	Status() l2search.Stats
}

// WithL2Service attaches the L2 whitelist service handled by admin RPCs.
func WithL2Service(l2 AdminL2) QueryBackendOption {
	return func(b *QueryBackend) { b.l2 = l2 }
}

// Admin serves administrative JSON operations: snapshot packages and the L2
// content whitelist. Unknown operations are rejected without side effects.
func (b *QueryBackend) Admin(ctx context.Context, op string, payload []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	switch op {
	case "l2.add", "l2.remove":
		var req struct {
			Dir string `json:"dir"`
		}
		if err := decodeAdminReq(payload, &req); err != nil {
			return nil, err
		}
		if req.Dir == "" {
			return nil, &RPCError{Code: ErrBadRequest, Message: "dir is required"}
		}
		if b.l2 == nil {
			return nil, &RPCError{Code: ErrBadRequest, Message: "L2 content index is not enabled on this daemon"}
		}
		var (
			roots []string
			err   error
		)
		if op == "l2.add" {
			roots, err = b.l2.AddRoot(req.Dir)
		} else {
			roots, err = b.l2.RemoveRoot(req.Dir)
		}
		if err != nil {
			return nil, fmt.Errorf("ipc: %s: %w", op, err)
		}
		return json.Marshal(struct {
			Roots []string `json:"roots"`
		}{Roots: roots})

	case "l2.status":
		if b.l2 == nil {
			return json.Marshal(L2Status{Roots: []string{}})
		}
		st := b.l2.Status()
		out := L2Status{
			Roots:    st.Roots,
			Indexed:  st.Indexed,
			Deleted:  st.Deleted,
			Batches:  st.Batches,
			Skipped:  make(map[string]int, len(st.Skipped)),
			Paused:   st.Paused,
			DiskUsed: st.DiskUsed,
			DiskCap:  st.DiskCap,
		}
		for reason, n := range st.Skipped {
			if n > 0 {
				out.Skipped[reason.String()] = n
			}
		}
		if out.Roots == nil {
			out.Roots = []string{}
		}
		return json.Marshal(out)

	case "snapshot.list":
		if b.store == nil {
			return nil, &RPCError{Code: ErrInternal, Message: "storage engine unavailable"}
		}
		return json.Marshal(struct {
			Snapshots []storage.SnapshotInfo `json:"snapshots"`
		}{Snapshots: b.store.Snapshots()})

	case "snapshot.export":
		var req struct {
			VolumeID uint16 `json:"volume_id"`
			OutDir   string `json:"out_dir"`
			Label    string `json:"label"`
			RootPath string `json:"root_path"`
		}
		if err := decodeAdminReq(payload, &req); err != nil {
			return nil, err
		}
		if req.OutDir == "" {
			return nil, &RPCError{Code: ErrBadRequest, Message: "out_dir is required"}
		}
		if b.store == nil {
			return nil, &RPCError{Code: ErrInternal, Message: "storage engine unavailable"}
		}
		pkg, err := b.store.ExportVolumeSnapshot(req.VolumeID, req.OutDir, storage.ExportOptions{
			VolumeLabel: req.Label,
			RootPath:    req.RootPath,
		})
		if err != nil {
			return nil, fmt.Errorf("ipc: snapshot.export: %w", err)
		}
		return json.Marshal(pkg)

	case "snapshot.import":
		var req struct {
			Package string `json:"package"`
		}
		if err := decodeAdminReq(payload, &req); err != nil {
			return nil, err
		}
		if req.Package == "" {
			return nil, &RPCError{Code: ErrBadRequest, Message: "package is required"}
		}
		if b.store == nil {
			return nil, &RPCError{Code: ErrInternal, Message: "storage engine unavailable"}
		}
		info, err := b.store.ImportVolumeSnapshot(req.Package)
		if err != nil {
			return nil, fmt.Errorf("ipc: snapshot.import: %w", err)
		}
		return json.Marshal(info)

	case "snapshot.remove":
		var req struct {
			ID uint32 `json:"id"`
		}
		if err := decodeAdminReq(payload, &req); err != nil {
			return nil, err
		}
		if b.store == nil {
			return nil, &RPCError{Code: ErrInternal, Message: "storage engine unavailable"}
		}
		if err := b.store.RemoveSnapshot(req.ID); err != nil {
			return nil, fmt.Errorf("ipc: snapshot.remove: %w", err)
		}
		return []byte(`{"removed":true}`), nil

	default:
		return nil, &RPCError{Code: ErrBadRequest, Message: "unknown admin operation " + op}
	}
}

func decodeAdminReq(payload []byte, dst any) error {
	if len(payload) == 0 {
		payload = []byte("{}")
	}
	if err := json.Unmarshal(payload, dst); err != nil {
		return &RPCError{Code: ErrBadRequest, Message: "invalid admin payload: " + err.Error()}
	}
	return nil
}
