package daemon

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/mr-dadong/Xift/internal/filter"
	"github.com/mr-dadong/Xift/internal/ipc"
	"github.com/mr-dadong/Xift/internal/provider"
	"github.com/mr-dadong/Xift/internal/storage"
)

const watchEventBuffer = 256

// WatchConfig configures the daemon's live kernel-event delivery.
type WatchConfig struct {
	// Provider supplies per-volume Watch streams. Required when Enabled.
	Provider provider.FSProvider
	// Volumes is the set of volumes enumerated at start-up.
	Volumes []provider.VolumeInfo
	// Enabled toggles live delivery. Disabled keeps the static index only.
	Enabled bool
	// Window overrides the 150ms micro-batch window (tests use a shorter one).
	Window time.Duration
	// RetryDelay is the base backoff for a failed volume stream (default 1s,
	// capped at 30s). Tests use a shorter delay.
	RetryDelay time.Duration
	// Pruner applies source-level pruning before batching.
	Pruner *filter.Pruner
	// OnRescanDir rebuilds a directory after overload degradation.
	OnRescanDir func(ev provider.FileEvent) error
}

type watchState string

const (
	watchStateWatching    watchState = "watching"
	watchStateRebuilding  watchState = "rebuilding"
	watchStateRetrying    watchState = "retrying"
	watchStateUnsupported watchState = "unsupported"
	watchStateDisabled    watchState = "disabled"
)

// volumeWatchState is the externally reported state of one volume stream.
type volumeWatchState struct {
	VolID   uint16
	Root    string
	State   watchState
	Cursor  uint64
	LastErr string
	Rebuilt uint64
	mu      sync.Mutex
}

func (s *volumeWatchState) set(state watchState, err error) {
	s.mu.Lock()
	s.State = state
	if err != nil {
		s.LastErr = err.Error()
	}
	s.mu.Unlock()
}

func (s *volumeWatchState) setCursor(cursor uint64) {
	s.mu.Lock()
	if cursor > s.Cursor {
		s.Cursor = cursor
	}
	s.mu.Unlock()
}

// liveWatch supervises one Watch stream per volume and fans batches out to the
// L1 storage sink and the optional L2 content pipeline.
type liveWatch struct {
	store *storage.Engine
	cfg   WatchConfig
	sink  *provider.StorageEventSink
	l2    *L2Pipeline

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu     sync.Mutex
	states map[uint16]*volumeWatchState
}

func startLiveWatch(parent context.Context, store *storage.Engine, cfg WatchConfig, l2 *L2Pipeline) (*liveWatch, error) {
	if !cfg.Enabled {
		return &liveWatch{cfg: cfg, states: map[uint16]*volumeWatchState{}}, nil
	}
	if cfg.Provider == nil {
		return nil, errors.New("daemon: live watch requires a provider")
	}
	if store == nil {
		return nil, errors.New("daemon: live watch requires the storage engine")
	}
	volRoots := make(map[uint16]string, len(cfg.Volumes))
	for _, vol := range cfg.Volumes {
		volRoots[vol.VolID] = vol.RootPath
	}
	ctx, cancel := context.WithCancel(parent)
	w := &liveWatch{
		store:  store,
		cfg:    cfg,
		sink:   provider.NewStorageEventSink(store, volRoots, cfg.OnRescanDir),
		l2:     l2,
		ctx:    ctx,
		cancel: cancel,
		states: make(map[uint16]*volumeWatchState, len(cfg.Volumes)),
	}
	for _, vol := range cfg.Volumes {
		st := &volumeWatchState{VolID: vol.VolID, Root: vol.RootPath, State: watchStateWatching}
		w.states[vol.VolID] = st
		w.wg.Add(1)
		go func(vol provider.VolumeInfo, st *volumeWatchState) {
			defer w.wg.Done()
			w.runVolume(vol, st)
		}(vol, st)
	}
	return w, nil
}

func (w *liveWatch) runVolume(vol provider.VolumeInfo, st *volumeWatchState) {
	base := w.cfg.RetryDelay
	if base <= 0 {
		base = time.Second
	}
	backoff := base
	for {
		if w.ctx.Err() != nil {
			return
		}
		err := w.watchOnce(vol, st)
		if err == nil || errors.Is(err, context.Canceled) || w.ctx.Err() != nil {
			return
		}
		if errors.Is(err, provider.ErrCursorExpired) {
			st.set(watchStateRebuilding, nil)
			if rerr := w.rebuildVolume(w.ctx, vol, st); rerr == nil {
				st.mu.Lock()
				st.Rebuilt++
				st.mu.Unlock()
				st.set(watchStateWatching, nil)
				backoff = base
				continue
			} else {
				st.set(watchStateRetrying, rerr)
			}
		} else {
			st.set(watchStateRetrying, err)
		}
		if !w.sleep(backoff) {
			return
		}
		backoff *= 2
		if cap := 30 * time.Second; backoff > cap {
			backoff = cap
		}
	}
}

func (w *liveWatch) sleep(d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-w.ctx.Done():
		return false
	}
}

// watchOnce runs one Watch subscription until it fails or the daemon stops.
func (w *liveWatch) watchOnce(vol provider.VolumeInfo, st *volumeWatchState) error {
	st.set(watchStateWatching, nil)
	batcher := provider.NewBatcher(provider.BatcherOptions{
		Window:  w.cfg.Window,
		Pruner:  w.cfg.Pruner,
		OnBatch: w.dispatch,
	})
	batcher.Start(w.ctx)
	defer batcher.Close()

	since := w.sinceCursor(vol.VolID)
	st.setCursor(since)
	out := make(chan provider.FileEvent, watchEventBuffer)
	watchErr := make(chan error, 1)
	go func() {
		watchErr <- w.cfg.Provider.Watch(w.ctx, vol, since, out)
	}()

	for {
		select {
		case ev := <-out:
			if ev.VolID == 0 {
				ev.VolID = vol.VolID
			}
			batcher.Submit(ev)
			if ev.Cursor > 0 {
				st.setCursor(ev.Cursor)
			}
		case err := <-watchErr:
			return err
		case <-w.ctx.Done():
			// Drain events already handed to us before the last flush so the
			// final 150ms window is not lost on shutdown.
			for {
				select {
				case ev := <-out:
					if ev.VolID == 0 {
						ev.VolID = vol.VolID
					}
					batcher.Submit(ev)
				default:
					return w.ctx.Err()
				}
			}
		}
	}
}

// rebuildVolume invalidates every row of vol, rescans it from scratch, and
// commits the new journal cursor.
func (w *liveWatch) rebuildVolume(ctx context.Context, vol provider.VolumeInfo, st *volumeWatchState) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var refs []storage.NodeKeyRef
	snap := w.store.AcquireSnapshot()
	snap.ForEachKey(func(volID uint16, nodeKey uint64, _ uint32) bool {
		if volID == vol.VolID {
			refs = append(refs, storage.NodeKeyRef{VolID: volID, NodeKey: nodeKey})
		}
		return true
	})
	snap.Release()

	const deleteBatch = 4096
	for start := 0; start < len(refs); start += deleteBatch {
		end := start + deleteBatch
		if end > len(refs) {
			end = len(refs)
		}
		muts := make([]storage.Mutation, 0, end-start)
		for _, ref := range refs[start:end] {
			muts = append(muts, storage.Mutation{Op: storage.OpDelete, VolID: ref.VolID, NodeKey: ref.NodeKey})
		}
		if err := w.store.ApplyBatch(muts); err != nil {
			return err
		}
	}

	sw, err := provider.NewStorageSegmentWriter(w.store, vol)
	if err != nil {
		return err
	}
	cursor, err := w.cfg.Provider.FullScan(ctx, vol, sw)
	if err != nil {
		sw.Abort()
		return err
	}
	if _, err := sw.Commit(cursor); err != nil {
		return err
	}
	st.setCursor(cursor)
	return nil
}

// sinceCursor returns the persisted journal cursor for volID, or zero when the
// volume has never been indexed.
func (w *liveWatch) sinceCursor(volID uint16) uint64 {
	if vc, ok := w.store.VolumeCursor(volID); ok {
		return vc.Cursor
	}
	return 0
}

// dispatch delivers one coalesced batch to L1 first, then to L2.
func (w *liveWatch) dispatch(batch provider.EventBatch) {
	w.sink.HandleBatch(batch)
	if w.l2 != nil {
		w.l2.HandleBatch(batch)
	}
	for volID, cursor := range batch.Cursors {
		if st := w.state(volID); st != nil {
			st.setCursor(cursor)
		}
	}
}

func (w *liveWatch) state(volID uint16) *volumeWatchState {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.states[volID]
}

// Close stops all watchers and flushes buffered events.
func (w *liveWatch) Close() {
	if w.cancel == nil {
		return
	}
	w.cancel()
	w.wg.Wait()
}

// Enabled reports whether live delivery is active.
func (w *liveWatch) Enabled() bool { return w != nil && w.cancel != nil }

// volumeSnapshots returns a stable copy of per-volume states for status output.
func (w *liveWatch) volumeSnapshots() []volumeStatus {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]volumeStatus, 0, len(w.states))
	for _, st := range w.states {
		st.mu.Lock()
		out = append(out, volumeStatus{
			VolID:   st.VolID,
			Root:    st.Root,
			State:   string(st.State),
			Cursor:  st.Cursor,
			LastErr: st.LastErr,
			Rebuilt: st.Rebuilt,
		})
		st.mu.Unlock()
	}
	return out
}

// WatchStatus implements ipc.AdminWatch for the daemon status RPC.
func (w *liveWatch) WatchStatus() ipc.WatchStatus {
	internal := w.status()
	out := ipc.WatchStatus{
		Enabled: internal.Enabled,
		Volumes: make([]ipc.VolumeWatchStatus, 0, len(internal.Volumes)),
	}
	for _, v := range internal.Volumes {
		out.Volumes = append(out.Volumes, ipc.VolumeWatchStatus{
			VolID:   v.VolID,
			Root:    v.Root,
			State:   v.State,
			Cursor:  v.Cursor,
			LastErr: v.LastErr,
			Rebuilt: v.Rebuilt,
		})
	}
	return out
}

// volumeStatus is the JSON shape of one volume's watch state.
type volumeStatus struct {
	VolID   uint16 `json:"vol_id"`
	Root    string `json:"root"`
	State   string `json:"state"`
	Cursor  uint64 `json:"cursor"`
	LastErr string `json:"last_error,omitempty"`
	Rebuilt uint64 `json:"rebuilt"`
}

// watchStatus is the JSON shape of the `watch.status` admin operation.
type watchStatus struct {
	Enabled bool           `json:"enabled"`
	Volumes []volumeStatus `json:"volumes"`
}

func (w *liveWatch) status() watchStatus {
	if w == nil {
		return watchStatus{Enabled: false, Volumes: []volumeStatus{}}
	}
	vols := w.volumeSnapshots()
	if vols == nil {
		vols = []volumeStatus{}
	}
	return watchStatus{Enabled: w.Enabled(), Volumes: vols}
}
