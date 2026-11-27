//go:build darwin || linux

package storage

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

type mmapRegion struct {
	file *os.File
	data []byte
}

func openReadOnlyMmap(path string, advice mmapAdvice) (*mmapRegion, error) {
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

	switch advice {
	case mmapAdviceSequential:
		_ = unix.Madvise(data, unix.MADV_SEQUENTIAL)
	case mmapAdviceRandom:
		_ = unix.Madvise(data, unix.MADV_RANDOM)
	}

	return &mmapRegion{file: f, data: data}, nil
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

func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
}
