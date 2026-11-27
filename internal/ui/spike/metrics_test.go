package spike

import (
	"testing"
	"time"
)

func TestMetricsTracker_FPSAndFrameTime(t *testing.T) {
	tracker := NewMetricsTracker(250 * time.Millisecond)
	base := time.Date(2026, 11, 27, 10, 0, 0, 0, time.UTC)

	// 61 frames spaced 16ms apart => ~62.5 FPS
	for i := 0; i < 61; i++ {
		tracker.RecordFrame(base.Add(time.Duration(i) * 16 * time.Millisecond))
	}

	snap := tracker.Snapshot()
	if snap.FrameCount != 61 {
		t.Fatalf("FrameCount = %d, want 61", snap.FrameCount)
	}
	if snap.FPS < 58.0 || snap.FPS > 67.0 {
		t.Fatalf("FPS = %.2f, want in [58.0, 67.0]", snap.FPS)
	}
	if snap.LastFrameMs < 15.0 || snap.LastFrameMs > 17.0 {
		t.Fatalf("LastFrameMs = %.2f, want ~16.0", snap.LastFrameMs)
	}
}

func TestMetricsTracker_MemStatsThrottled(t *testing.T) {
	calls := 0
	fakeReader := func() (uint64, uint64) {
		calls++
		return uint64(calls) * (4 << 20), uint64(calls) * (16 << 20)
	}

	tracker := newMetricsTrackerWithReader(250*time.Millisecond, fakeReader)
	base := time.Date(2026, 11, 27, 10, 0, 0, 0, time.UTC)

	tracker.RecordFrame(base)
	tracker.RecordFrame(base.Add(50 * time.Millisecond))
	tracker.RecordFrame(base.Add(150 * time.Millisecond))
	tracker.RecordFrame(base.Add(249 * time.Millisecond))

	if calls != 1 {
		t.Fatalf("mem reader calls within 249ms = %d, want 1", calls)
	}
	snap1 := tracker.Snapshot()
	if snap1.HeapAllocBytes != 4<<20 || snap1.SysBytes != 16<<20 {
		t.Fatalf("snap1 mem = (%d, %d), want (%d, %d)", snap1.HeapAllocBytes, snap1.SysBytes, 4<<20, 16<<20)
	}

	tracker.RecordFrame(base.Add(250 * time.Millisecond))
	if calls != 2 {
		t.Fatalf("mem reader calls at 250ms = %d, want 2", calls)
	}
	snap2 := tracker.Snapshot()
	if snap2.HeapAllocBytes != 8<<20 || snap2.SysBytes != 32<<20 {
		t.Fatalf("snap2 mem = (%d, %d), want (%d, %d)", snap2.HeapAllocBytes, snap2.SysBytes, 8<<20, 32<<20)
	}
}
