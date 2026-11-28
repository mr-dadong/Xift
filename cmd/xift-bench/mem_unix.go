//go:build unix

package main

import "golang.org/x/sys/unix"

// residentBytes returns the process maximum resident set size in bytes.
func residentBytes() uint64 {
	var ru unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	// Linux reports KiB, macOS reports bytes.
	if ru.Maxrss > 1<<30 {
		return uint64(ru.Maxrss)
	}
	return uint64(ru.Maxrss) * 1024
}
