//go:build windows

package main

import (
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

var procGetProcessMemoryInfo = windows.NewLazySystemDLL("psapi.dll").NewProc("GetProcessMemoryInfo")

// processMemoryCounters mirrors PROCESS_MEMORY_COUNTERS.
type processMemoryCounters struct {
	Cb                         uint32
	PageFaultCount             uint32
	PeakWorkingSetSize         uintptr
	WorkingSetSize             uintptr
	QuotaPeakPagedPoolUsage    uintptr
	QuotaPagedPoolUsage        uintptr
	QuotaPeakNonPagedPoolUsage uintptr
	QuotaNonPagedPoolUsage     uintptr
	PagefileUsage              uintptr
	PeakPagefileUsage          uintptr
}

// residentBytes returns the process working set in bytes, or 0 on failure.
func residentBytes() uint64 {
	var c processMemoryCounters
	c.Cb = uint32(unsafe.Sizeof(c))
	proc, err := windows.GetCurrentProcess()
	if err != nil {
		return 0
	}
	r, _, _ := procGetProcessMemoryInfo.Call(uintptr(proc), uintptr(unsafe.Pointer(&c)), uintptr(c.Cb))
	if r == 0 {
		return 0
	}
	_ = syscall.Errno(0)
	return uint64(c.WorkingSetSize)
}
