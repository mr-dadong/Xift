package spike

import (
	"runtime"
	"time"
)

// MetricsSnapshot holds the latest computed FPS, frame duration, and memory telemetry.
type MetricsSnapshot struct {
	FrameCount     uint64
	FPS            float64
	LastFrameMs    float64
	HeapAllocBytes uint64
	SysBytes       uint64
}

type memReaderFunc func() (heapAlloc uint64, sys uint64)

func defaultMemReader() (uint64, uint64) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc, ms.Sys
}

// MetricsTracker tracks frame rate over a fixed-capacity ring buffer (zero allocations per frame)
// and throttles runtime.ReadMemStats calls to avoid introducing STW pauses into the render loop.
type MetricsTracker struct {
	memInterval time.Duration
	readMem     memReaderFunc

	frameCount  uint64
	lastFrameAt time.Time
	lastFrameMs float64

	// Fixed-size ring buffer of recent frame timestamps (120 slots, zero heap churn).
	ring      [120]time.Time
	ringHead  int
	ringCount int

	lastMemAt time.Time
	hasMem    bool
	heapAlloc uint64
	sysBytes  uint64
}

// NewMetricsTracker creates a telemetry tracker with the given memory sampling interval.
func NewMetricsTracker(memInterval time.Duration) *MetricsTracker {
	return newMetricsTrackerWithReader(memInterval, defaultMemReader)
}

func newMetricsTrackerWithReader(memInterval time.Duration, reader memReaderFunc) *MetricsTracker {
	if memInterval <= 0 {
		memInterval = 250 * time.Millisecond
	}
	if reader == nil {
		reader = defaultMemReader
	}
	return &MetricsTracker{
		memInterval: memInterval,
		readMem:     reader,
	}
}

// RecordFrame records a rendered frame at timestamp now and refreshes memory stats if due.
func (m *MetricsTracker) RecordFrame(now time.Time) {
	m.frameCount++
	if !m.lastFrameAt.IsZero() {
		dt := now.Sub(m.lastFrameAt)
		if dt > 0 {
			m.lastFrameMs = float64(dt) / float64(time.Millisecond)
		}
	}
	m.lastFrameAt = now

	m.ring[m.ringHead] = now
	m.ringHead = (m.ringHead + 1) % len(m.ring)
	if m.ringCount < len(m.ring) {
		m.ringCount++
	}

	if !m.hasMem || now.Sub(m.lastMemAt) >= m.memInterval {
		m.heapAlloc, m.sysBytes = m.readMem()
		m.lastMemAt = now
		m.hasMem = true
	}
}

// Snapshot returns the current FPS, frame time, and throttled memory metrics.
func (m *MetricsTracker) Snapshot() MetricsSnapshot {
	var fps float64
	if m.ringCount >= 2 {
		oldestIdx := (m.ringHead - m.ringCount + len(m.ring)) % len(m.ring)
		newestIdx := (m.ringHead - 1 + len(m.ring)) % len(m.ring)
		span := m.ring[newestIdx].Sub(m.ring[oldestIdx])
		if span > 0 {
			fps = float64(m.ringCount-1) / span.Seconds()
		}
	}
	return MetricsSnapshot{
		FrameCount:     m.frameCount,
		FPS:            fps,
		LastFrameMs:    m.lastFrameMs,
		HeapAllocBytes: m.heapAlloc,
		SysBytes:       m.sysBytes,
	}
}
