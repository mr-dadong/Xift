//go:build windows

package provider

import (
	"runtime"

	"golang.org/x/sys/windows"
)

const (
	// threadPriorityBelowNormal matches Win32 THREAD_PRIORITY_BELOW_NORMAL (-1).
	threadPriorityBelowNormal = ^uintptr(0) // int32(-1)
	threadPriorityErrorReturn = 0x7FFFFFFF
)

var (
	modKernel32Priority   = windows.NewLazySystemDLL("kernel32.dll")
	procGetThreadPriority = modKernel32Priority.NewProc("GetThreadPriority")
	procSetThreadPriority = modKernel32Priority.NewProc("SetThreadPriority")
)

// SetLowPriorityThread locks the current goroutine to its OS thread and lowers its
// Windows scheduling priority to THREAD_PRIORITY_BELOW_NORMAL.
// Callers MUST invoke the returned restore function before exiting.
func SetLowPriorityThread() (restore func()) {
	runtime.LockOSThread()

	thread := windows.CurrentThread()
	prev, _, _ := procGetThreadPriority.Call(uintptr(thread))
	_, _, _ = procSetThreadPriority.Call(uintptr(thread), threadPriorityBelowNormal)

	restored := false
	return func() {
		if restored {
			return
		}
		restored = true
		if prev != threadPriorityErrorReturn {
			_, _, _ = procSetThreadPriority.Call(uintptr(thread), prev)
		}
		runtime.UnlockOSThread()
	}
}
