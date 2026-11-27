//go:build !windows

package mftspike

import "errors"

// ErrUnsupportedPlatform is returned when EnumerateNTFSMFT is called on a non-Windows OS.
var ErrUnsupportedPlatform = errors.New("NTFS MFT enumeration is only supported on Windows")

// EnumerateNTFSMFT is a stub on non-Windows platforms.
func EnumerateNTFSMFT(drive string, emit func(RawRecord)) error {
	return ErrUnsupportedPlatform
}

// EnumerateNTFSMFTFiltered is a stub on non-Windows platforms.
func EnumerateNTFSMFTFiltered(
	drive string,
	shouldDecode func(frn, parentFRN uint64, isDir bool) bool,
	emit func(RawRecord),
) (int, error) {
	return 0, ErrUnsupportedPlatform
}
