//go:build linux

package provider

import (
	"runtime"

	"golang.org/x/sys/unix"
)

const (
	ioprioWhoProcess = 1
	ioprioClassIdle  = 3
	ioprioClassShift = 13
)

func ioprioPrioValue(class, data int) uintptr {
	return uintptr((class << ioprioClassShift) | data)
}

// SetLowPriorityThread locks the current goroutine to its OS thread, raises its nice value (+10),
// and best-effort sets Linux IOPRIO_CLASS_IDLE. If restoring the original nice value fails due to
// non-root EACCES, the OS thread remains locked so it exits cleanly instead of returning a
// permanently niced thread to Go's general M pool.
func SetLowPriorityThread() (restore func()) {
	runtime.LockOSThread()

	tid := unix.Gettid()
	prevNice, errNice := unix.Getpriority(unix.PRIO_PROCESS, tid)
	niceChanged := false
	if errNice == nil {
		target := prevNice + 10
		if target > 19 {
			target = 19
		}
		if target != prevNice && unix.Setpriority(unix.PRIO_PROCESS, tid, target) == nil {
			niceChanged = true
		}
	}

	prevIOPrio, _, errGetIO := unix.Syscall(unix.SYS_IOPRIO_GET, ioprioWhoProcess, 0, 0)
	_, _, _ = unix.Syscall(unix.SYS_IOPRIO_SET, ioprioWhoProcess, 0, ioprioPrioValue(ioprioClassIdle, 0))

	restored := false
	return func() {
		if restored {
			return
		}
		restored = true
		if errGetIO == 0 {
			_, _, _ = unix.Syscall(unix.SYS_IOPRIO_SET, ioprioWhoProcess, 0, prevIOPrio)
		}
		if niceChanged {
			if err := unix.Setpriority(unix.PRIO_PROCESS, tid, prevNice); err != nil {
				// Non-root Linux cannot lower nice back; leave thread locked so Go retires it on goroutine exit.
				return
			}
		}
		runtime.UnlockOSThread()
	}
}
