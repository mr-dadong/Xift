//go:build windows

package mftspike

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"unsafe"

	"golang.org/x/sys/windows"
)

const (
	fsctlEnumUsnData = 0x000900b3
	mftIOCTLBufSize  = 1024 * 1024 // 1 MiB batch buffer for fewer kernel transitions
)

// mftEnumDataV0 matches Windows MFT_ENUM_DATA_V0 (24 bytes), guaranteeing USN_RECORD_V2 output.
type mftEnumDataV0 struct {
	StartFileReferenceNumber uint64
	LowUsn                   int64
	HighUsn                  int64
}

// EnumerateNTFSMFT opens the specified NTFS drive strictly READ-ONLY (GENERIC_READ)
// and streams all MFT records via DeviceIoControl(FSCTL_ENUM_USN_DATA).
func EnumerateNTFSMFT(drive string, emit func(RawRecord)) error {
	_, err := EnumerateNTFSMFTFiltered(drive, nil, emit)
	return err
}

// EnumerateNTFSMFTFiltered opens the volume READ-ONLY and skips UTF-16 filename decoding
// for any MFT record where shouldDecode returns false, returning total MFT records seen.
func EnumerateNTFSMFTFiltered(
	drive string,
	shouldDecode func(frn, parentFRN uint64, isDir bool) bool,
	emit func(RawRecord),
) (int, error) {
	letter := normalizeDriveLetter(drive)
	if letter == "" {
		return 0, fmt.Errorf("invalid drive letter %q (expected e.g. C or C:)", drive)
	}

	volPath, err := windows.UTF16PtrFromString(`\\.\` + letter + `:`)
	if err != nil {
		return 0, err
	}

	// Iron Law #1: Strictly read-only handle (GENERIC_READ only, never GENERIC_WRITE).
	handle, err := windows.CreateFile(
		volPath,
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		0,
		0,
	)
	if err != nil {
		if errors.Is(err, windows.ERROR_ACCESS_DENIED) {
			return 0, fmt.Errorf("访问卷 %s: 被拒绝（需要管理员权限）：请以“管理员身份”运行 PowerShell 后重试 (%w)", letter, err)
		}
		return 0, fmt.Errorf("打开卷 %s: 失败: %w", letter, err)
	}
	defer windows.CloseHandle(handle)

	inData := mftEnumDataV0{
		StartFileReferenceNumber: 0,
		LowUsn:                   0,
		HighUsn:                  math.MaxInt64,
	}

	buf := make([]byte, mftIOCTLBufSize)
	var bytesReturned uint32
	totalSeen := 0

	for {
		err := windows.DeviceIoControl(
			handle,
			fsctlEnumUsnData,
			(*byte)(unsafe.Pointer(&inData)),
			uint32(unsafe.Sizeof(inData)),
			&buf[0],
			uint32(len(buf)),
			&bytesReturned,
			nil,
		)
		if err != nil {
			if errors.Is(err, windows.ERROR_HANDLE_EOF) {
				break
			}
			if errors.Is(err, windows.ERROR_ACCESS_DENIED) {
				return totalSeen, fmt.Errorf("枚举卷 %s: MFT 权限不足（需要管理员权限）: %w", letter, err)
			}
			return totalSeen, fmt.Errorf("DeviceIoControl(FSCTL_ENUM_USN_DATA) on %s: failed: %w", letter, err)
		}
		if bytesReturned <= 8 {
			break
		}

		nextFRN, seen, parseErr := ParseUSNBufferFiltered(buf[:bytesReturned], shouldDecode, emit)
		totalSeen += seen
		if parseErr != nil {
			return totalSeen, parseErr
		}
		if nextFRN <= inData.StartFileReferenceNumber {
			break
		}
		inData.StartFileReferenceNumber = nextFRN
	}

	return totalSeen, nil
}

func normalizeDriveLetter(drive string) string {
	s := strings.TrimSpace(drive)
	s = strings.TrimSuffix(s, `\`)
	s = strings.TrimSuffix(s, `/`)
	s = strings.TrimSuffix(s, `:`)
	if len(s) != 1 {
		return ""
	}
	ch := s[0]
	if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') {
		return strings.ToUpper(s)
	}
	return ""
}
