//go:build windows

package mftspike

import (
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

type mmapRegion struct {
	file    *os.File
	mapping windows.Handle
	addr    uintptr
	data    []byte
}

func openReadOnlyMmap(path string) (*mmapRegion, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	size := fi.Size()
	if size == 0 {
		return &mmapRegion{file: f}, nil
	}

	mapping, err := windows.CreateFileMapping(
		windows.Handle(f.Fd()),
		nil,
		windows.PAGE_READONLY,
		0,
		0,
		nil,
	)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("CreateFileMapping(%s): %w", path, err)
	}

	addr, err := windows.MapViewOfFile(mapping, windows.FILE_MAP_READ, 0, 0, uintptr(size))
	if err != nil {
		_ = windows.CloseHandle(mapping)
		_ = f.Close()
		return nil, fmt.Errorf("MapViewOfFile(%s): %w", path, err)
	}

	// Convert OS-returned virtual memory address to a Go slice cleanly for go vet.
	data := unsafe.Slice((*byte)(unsafe.Add(nil, addr)), size)
	return &mmapRegion{
		file:    f,
		mapping: mapping,
		addr:    addr,
		data:    data,
	}, nil
}

func (r *mmapRegion) Close() error {
	var firstErr error
	if r.addr != 0 {
		if err := windows.UnmapViewOfFile(r.addr); err != nil && firstErr == nil {
			firstErr = err
		}
		r.addr = 0
		r.data = nil
	}
	if r.mapping != 0 {
		if err := windows.CloseHandle(r.mapping); err != nil && firstErr == nil {
			firstErr = err
		}
		r.mapping = 0
	}
	if r.file != nil {
		if err := r.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		r.file = nil
	}
	return firstErr
}

type processMemoryCounters struct {
	CB                         uint32
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

var (
	kernel32DLL              = windows.NewLazySystemDLL("kernel32.dll")
	procK32GetProcessMemInfo = kernel32DLL.NewProc("K32GetProcessMemoryInfo")
)

// ProcessWorkingSetBytes returns the current Windows process WorkingSetSize in bytes (pure Go).
func ProcessWorkingSetBytes() (workingSet uint64, peakWorkingSet uint64, err error) {
	var pmc processMemoryCounters
	pmc.CB = uint32(unsafe.Sizeof(pmc))
	ret, _, callErr := procK32GetProcessMemInfo.Call(
		uintptr(windows.CurrentProcess()),
		uintptr(unsafe.Pointer(&pmc)),
		uintptr(pmc.CB),
	)
	if ret == 0 {
		return 0, 0, fmt.Errorf("K32GetProcessMemoryInfo: %w", callErr)
	}
	return uint64(pmc.WorkingSetSize), uint64(pmc.PeakWorkingSetSize), nil
}
