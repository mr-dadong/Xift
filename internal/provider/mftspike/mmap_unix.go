//go:build darwin || linux

package mftspike

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

type mmapRegion struct {
	file *os.File
	data []byte
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
	size := int(fi.Size())
	if size == 0 {
		return &mmapRegion{file: f}, nil
	}

	data, err := unix.Mmap(int(f.Fd()), 0, size, unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("unix.Mmap(%s): %w", path, err)
	}
	return &mmapRegion{file: f, data: data}, nil
}

func (r *mmapRegion) Close() error {
	var firstErr error
	if len(r.data) > 0 {
		if err := unix.Munmap(r.data); err != nil && firstErr == nil {
			firstErr = err
		}
		r.data = nil
	}
	if r.file != nil {
		if err := r.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		r.file = nil
	}
	return firstErr
}

// ProcessWorkingSetBytes returns 0 on non-Windows platforms in this Windows MFT spike.
func ProcessWorkingSetBytes() (uint64, uint64, error) {
	return 0, 0, nil
}
