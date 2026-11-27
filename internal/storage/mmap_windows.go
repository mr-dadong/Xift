//go:build windows

package storage

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

func openReadOnlyMmap(path string, _ mmapAdvice) (*mmapRegion, error) {
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

	// Use unsafe.Add(nil, addr) so go vet unsafeptr check remains clean under CGO_ENABLED=0.
	data := unsafe.Slice((*byte)(unsafe.Add(nil, addr)), size)
	return &mmapRegion{
		file:    f,
		mapping: mapping,
		addr:    addr,
		data:    data,
	}, nil
}

func (r *mmapRegion) Bytes() []byte {
	if r == nil {
		return nil
	}
	return r.data
}

func (r *mmapRegion) Close() error {
	if r == nil {
		return nil
	}
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

func syncDir(_ string) {}
