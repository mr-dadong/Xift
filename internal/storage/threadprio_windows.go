//go:build windows

package storage

import (
	"runtime"

	"golang.org/x/sys/windows"
)

var procSetThreadPriority = windows.NewLazySystemDLL("kernel32.dll").NewProc("SetThreadPriority")

const (
	threadPriorityLowest = uintptr(0xFFFFFFFE) // (DWORD)-2, THREAD_PRIORITY_LOWEST
	threadPriorityNormal = uintptr(0)
)

// lockOSThreadLowPriority pins the calling goroutine to its OS thread and
// drops that thread to the lowest priority while background maintenance runs.
// The returned function restores normal priority and unlocks the thread.
func lockOSThreadLowPriority() func() {
	runtime.LockOSThread()
	thread, err := windows.GetCurrentThread()
	if err != nil {
		return runtime.UnlockOSThread
	}
	_, _, _ = procSetThreadPriority.Call(uintptr(thread), threadPriorityLowest)
	return func() {
		_, _, _ = procSetThreadPriority.Call(uintptr(thread), threadPriorityNormal)
		runtime.UnlockOSThread()
	}
}
