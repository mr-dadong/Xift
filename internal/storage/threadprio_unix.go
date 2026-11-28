//go:build unix

package storage

import (
	"runtime"

	"golang.org/x/sys/unix"
)

// lockOSThreadLowPriority pins the calling goroutine to its OS thread and
// lowers that thread's nice value while background maintenance runs. The
// returned function restores the previous nice value and unlocks the thread.
func lockOSThreadLowPriority() func() {
	runtime.LockOSThread()
	prev, err := unix.Getpriority(unix.PRIO_PROCESS, 0)
	if err != nil {
		return runtime.UnlockOSThread
	}
	const niceStep = 5
	next := prev + niceStep
	if next > 19 {
		next = 19
	}
	if err := unix.Setpriority(unix.PRIO_PROCESS, 0, next); err != nil {
		return runtime.UnlockOSThread
	}
	return func() {
		if err := unix.Setpriority(unix.PRIO_PROCESS, 0, prev); err != nil {
			// Raising priority back needs CAP_SYS_NICE; if it fails, leave the
			// thread locked so the runtime retires it on goroutine exit instead
			// of returning a permanently low-priority thread to the pool.
			return
		}
		runtime.UnlockOSThread()
	}
}
