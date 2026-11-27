//go:build windows

package windows

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"runtime"
	"strings"
	"unsafe"

	"golang.org/x/sys/windows"

	"github.com/mr-dadong/Xift/internal/filter"
	"github.com/mr-dadong/Xift/internal/provider"
	"github.com/mr-dadong/Xift/internal/storage"
)

const (
	fsctlQueryUsnJournal = 0x000900f4
	fsctlEnumUsnData     = 0x000900b3
	fsctlReadUsnJournal  = 0x000900bb

	mftIOCTLBufSize = 1024 * 1024 // 1 MiB batch buffer
	usnWatchBufSize = 64 * 1024   // 64 KiB real-time journal buffer

	errJournalDeleteInProgress windows.Errno = 1178
	errJournalNotActive        windows.Errno = 1179
	errJournalEntryDeleted     windows.Errno = 1181
)

type mftEnumDataV0 struct {
	StartFileReferenceNumber uint64
	LowUsn                   int64
	HighUsn                  int64
}

type usnJournalDataV0 struct {
	UsnJournalID    uint64
	FirstUsn        int64
	NextUsn         int64
	LowestValidUsn  int64
	MaxUsn          int64
	MaximumSize     uint64
	AllocationDelta uint64
}

type readUsnJournalDataV0 struct {
	StartUsn          int64
	ReasonMask        uint32
	ReturnOnlyOnClose uint32
	Timeout           uint64
	BytesToWaitFor    uint64
	UsnJournalID      uint64
}

// openVolumeReadOnly opens \\.\<drive>: with strictly GENERIC_READ access (Iron Law #1).
func openVolumeReadOnly(driveLetter string, overlapped bool) (windows.Handle, error) {
	volPath, err := windows.UTF16PtrFromString(`\\.\` + driveLetter + `:`)
	if err != nil {
		return windows.InvalidHandle, err
	}
	var flags uint32
	if overlapped {
		flags = windows.FILE_FLAG_OVERLAPPED
	}
	h, err := windows.CreateFile(
		volPath,
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		flags,
		0,
	)
	if err != nil {
		if errors.Is(err, windows.ERROR_ACCESS_DENIED) {
			return windows.InvalidHandle, fmt.Errorf("volume %s: %w (%v)", driveLetter, provider.ErrPermissionDenied, err)
		}
		return windows.InvalidHandle, err
	}
	return h, nil
}

// queryUSNJournal queries NTFS USN journal state, using an Overlapped struct when handle
// was opened with FILE_FLAG_OVERLAPPED (C2 fix).
func queryUSNJournal(handle windows.Handle, overlapped bool) (usnJournalDataV0, error) {
	var jd usnJournalDataV0
	var returned uint32
	var ov *windows.Overlapped
	var evHandle windows.Handle
	if overlapped {
		var err error
		evHandle, err = windows.CreateEvent(nil, 1, 0, nil)
		if err != nil {
			return usnJournalDataV0{}, err
		}
		defer windows.CloseHandle(evHandle)
		ov = &windows.Overlapped{HEvent: evHandle}
	}

	err := windows.DeviceIoControl(
		handle,
		fsctlQueryUsnJournal,
		nil,
		0,
		(*byte)(unsafe.Pointer(&jd)),
		uint32(unsafe.Sizeof(jd)),
		&returned,
		ov,
	)
	if overlapped && errors.Is(err, windows.ERROR_IO_PENDING) {
		err = windows.GetOverlappedResult(handle, ov, &returned, true)
	}
	if err != nil {
		if errors.Is(err, errJournalNotActive) || errors.Is(err, errJournalDeleteInProgress) {
			return usnJournalDataV0{}, fmt.Errorf("usn journal inactive: %w (%v)", provider.ErrCursorExpired, err)
		}
		return usnJournalDataV0{}, err
	}
	return jd, nil
}

// resolveNTFSRootFRN queries the real 64-bit NTFS root FRN (sequence<<48 | 5) read-only.
func resolveNTFSRootFRN(driveLetter string, hint uint64) uint64 {
	if hint != 0 && hint != 5 {
		return hint
	}
	if k, _, _, _, ok := getFileKeyAndMetaReadOnly(driveLetter + `:\`); ok && k != 0 {
		return k
	}
	return defaultNTFSRootFRN
}

func (p *Provider) scanNTFSMFT(
	ctx context.Context,
	vol provider.VolumeInfo,
	driveLetter string,
	writer provider.SegmentWriter,
) (uint64, error) {
	handle, err := openVolumeReadOnly(driveLetter, false)
	if err != nil {
		return 0, err
	}
	defer windows.CloseHandle(handle)

	var startCursor uint64
	if jd, qErr := queryUSNJournal(handle, false); qErr == nil && jd.NextUsn > 0 {
		startCursor = uint64(jd.NextUsn)
	}

	rootFRN := resolveNTFSRootFRN(driveLetter, vol.RootKey)

	sb := filter.NewSkeletonBuilder(vol.VolID, rootFRN, p.pruner.Rules())
	ci := p.pruner.Rules().CaseInsensitive()

	// Attach read-only project ignore loader for Pass 1 visible project roots.
	volRoot := driveLetter + `:\`
	sb.SetIgnoreLoader(func(_ uint64, relPath string) (*filter.IgnoreMatcher, *filter.IgnoreMatcher) {
		diskDir := volRoot
		if relPath != "" {
			diskDir = filepath.Join(volRoot, filepath.FromSlash(relPath))
		}
		var xiftM, gitM *filter.IgnoreMatcher
		if m, err := filter.LoadIgnoreFile(filepath.Join(diskDir, ".xiftignore"), relPath, filter.IgnoreKindXift, ci); err == nil {
			xiftM = m
		}
		if m, err := filter.LoadIgnoreFile(filepath.Join(diskDir, ".gitignore"), relPath, filter.IgnoreKindGit, ci); err == nil {
			gitM = m
		}
		return xiftM, gitM
	})

	// Collect raw directory metadata during Pass 1 so surviving dirs can be emitted in topological order.
	rawDirs := make([]mftDirInfo, 0, 4096)
	rootSeen := false
	buf := make([]byte, mftIOCTLBufSize)

	// Pass 1: Decode directories plus candidate project marker / ignore filenames (fast UTF-16 length filter).
	pass1Filter := func(_ uint64, _ uint64, isDir bool, nameLenBytes int) bool {
		if isDir {
			return true
		}
		return isCandidateMarkerUTF16Len(nameLenBytes)
	}

	err = enumerateMFTStream(ctx, handle, buf, pass1Filter, func(rec USNRecord) {
		if !rec.IsDir {
			if isProjectOrIgnoreFileName(rec.Name) {
				sb.MarkProjectDir(rec.ParentFRN)
			}
			return
		}
		// Auto-align rootFRN if MFT records reference a root ParentFRN with index 5.
		if rec.ParentFRN&ntfsMFTIndexMask == 5 && rec.ParentFRN != rootFRN {
			rootFRN = rec.ParentFRN
		}
		if rec.FRN == rootFRN {
			rootSeen = true
		}
		if strings.EqualFold(rec.Name, ".git") {
			sb.MarkProjectDir(rec.ParentFRN)
		}
		sb.AddDir(rec.FRN, rec.ParentFRN, rec.Name)
		var f uint16 = storage.FlagDir
		if rec.Hidden {
			f |= storage.FlagHidden
		}
		if rec.IsReparsePoint {
			f |= storage.FlagSymlink
		}
		rawDirs = append(rawDirs, mftDirInfo{
			frn:       rec.FRN,
			parentFRN: rec.ParentFRN,
			name:      rec.Name,
			flags:     f,
		})
	})
	if err != nil {
		return 0, err
	}

	if !rootSeen {
		rootName := driveLetter + ":"
		sb.AddDir(rootFRN, rootFRN, rootName)
		rawDirs = append(rawDirs, mftDirInfo{
			frn:       rootFRN,
			parentFRN: rootFRN,
			name:      rootName,
			flags:     storage.FlagDir,
		})
	}

	eval := sb.Build()

	// Filter surviving directories and emit in cycle-safe root-to-leaf BFS order so ParentIdx resolves O(1).
	survivingDirs := make([]mftDirInfo, 0, len(rawDirs))
	for i := range rawDirs {
		d := &rawDirs[i]
		if d.frn == rootFRN {
			survivingDirs = append(survivingDirs, *d)
			continue
		}
		dec := eval.Evaluate(filter.Pass2Record{
			NodeKey:   d.frn,
			ParentKey: d.parentFRN,
			Name:      d.name,
			IsDir:     true,
		})
		if !dec.Keep {
			continue
		}
		d.flags = (d.flags &^ (storage.FlagDir | storage.FlagFolded | storage.FlagSystem)) | dec.Flags
		survivingDirs = append(survivingDirs, *d)
	}
	orderedDirs := orderDirsTopologically(rootFRN, survivingDirs)

	for i := range orderedDirs {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		d := &orderedDirs[i]
		if err := writer.WriteRecord(provider.ScanRecord{
			VolID:     vol.VolID,
			NodeKey:   d.frn,
			ParentKey: d.parentFRN,
			Name:      d.name,
			Flags:     d.flags,
		}); err != nil {
			return 0, err
		}
	}

	// Pass 2: Stream non-directory files, skipping UTF-16 decode for pruned parents in O(1).
	prunedSet := eval.PrunedDirSet()
	pass2Filter := func(_ uint64, parentFRN uint64, isDir bool, _ int) bool {
		if isDir {
			return false
		}
		return !prunedSet.IsPruned(vol.VolID, parentFRN)
	}

	var writeErr error
	err = enumerateMFTStream(ctx, handle, buf, pass2Filter, func(rec USNRecord) {
		if writeErr != nil {
			return
		}
		dec := eval.Evaluate(filter.Pass2Record{
			NodeKey:   rec.FRN,
			ParentKey: rec.ParentFRN,
			Name:      rec.Name,
			IsDir:     false,
		})
		if !dec.Keep {
			return
		}
		flags := dec.Flags
		if rec.Hidden {
			flags |= storage.FlagHidden
		}
		if rec.IsReparsePoint {
			flags |= storage.FlagSymlink
		}
		writeErr = writer.WriteRecord(provider.ScanRecord{
			VolID:     vol.VolID,
			NodeKey:   rec.FRN,
			ParentKey: rec.ParentFRN,
			Name:      rec.Name,
			Flags:     flags,
			Size:      provider.SizeUnknown,
			ModTime:   rec.ModTimeUnixNano,
		})
	})
	if err != nil {
		return 0, err
	}
	if writeErr != nil {
		return 0, writeErr
	}

	// Hand off Pass 1 PrunedDirSet atomically into the resident live Pruner.
	p.pruner.InstallVolumeSet(vol.VolID, prunedSet)
	return startCursor, nil
}

func enumerateMFTStream(
	ctx context.Context,
	handle windows.Handle,
	buf []byte,
	shouldDecode func(frn, parentFRN uint64, isDir bool, nameLenBytes int) bool,
	emit func(USNRecord),
) error {
	inData := mftEnumDataV0{
		StartFileReferenceNumber: 0,
		LowUsn:                   0,
		HighUsn:                  math.MaxInt64,
	}
	var bytesReturned uint32

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
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
				return nil
			}
			if errors.Is(err, windows.ERROR_ACCESS_DENIED) {
				return fmt.Errorf("mft enum: %w (%v)", provider.ErrPermissionDenied, err)
			}
			return err
		}
		if bytesReturned <= 8 {
			return nil
		}

		nextFRN, _, parseErr := ParseUSNBufferWithNameLen(buf[:bytesReturned], shouldDecode, emit)
		if parseErr != nil {
			return parseErr
		}
		if nextFRN <= inData.StartFileReferenceNumber {
			return nil
		}
		inData.StartFileReferenceNumber = nextFRN
	}
}

// overlappedWaiter coordinates cancellable Win32 overlapped I/O using a single manual-reset
// cancel event and WaitForMultipleObjects, pinning both the OVERLAPPED struct and I/O buffer
// on the heap via runtime.Pinner so Go goroutine stack growth can never relocate them while
// a Windows kernel IRP is in flight (C3 + Go 1.27 stack-allocation fix).
type overlappedWaiter struct {
	ioEvent     windows.Handle
	cancelEvent windows.Handle
	doneCh      chan struct{}
	joinedCh    chan struct{}
	ov          windows.Overlapped
	buf         []byte
	pinner      runtime.Pinner
}

func newOverlappedWaiter(ctx context.Context, bufSize int) (*overlappedWaiter, error) {
	ioEv, err := windows.CreateEvent(nil, 1, 0, nil)
	if err != nil {
		return nil, err
	}
	cancelEv, err := windows.CreateEvent(nil, 1, 0, nil)
	if err != nil {
		_ = windows.CloseHandle(ioEv)
		return nil, err
	}
	w := &overlappedWaiter{
		ioEvent:     ioEv,
		cancelEvent: cancelEv,
		doneCh:      make(chan struct{}),
		joinedCh:    make(chan struct{}),
		buf:         make([]byte, bufSize),
	}
	// Pin both OVERLAPPED and I/O buffer in heap memory for the lifetime of the watch session.
	w.pinner.Pin(&w.ov)
	if len(w.buf) > 0 {
		w.pinner.Pin(&w.buf[0])
	}
	go func() {
		defer close(w.joinedCh)
		select {
		case <-ctx.Done():
			_ = windows.SetEvent(w.cancelEvent)
		case <-w.doneCh:
		}
	}()
	return w, nil
}

func (w *overlappedWaiter) reset() error {
	w.ov = windows.Overlapped{HEvent: w.ioEvent}
	return windows.ResetEvent(w.ioEvent)
}

func (w *overlappedWaiter) wait(handle windows.Handle) (uint32, error) {
	handles := [2]windows.Handle{w.ioEvent, w.cancelEvent}
	evt, err := windows.WaitForMultipleObjects(handles[:], false, windows.INFINITE)
	if err != nil {
		return 0, err
	}
	var bytesReturned uint32
	if evt == windows.WAIT_OBJECT_0+1 {
		// Context cancelled: cancel pending IRP and drain its completion before returning.
		_ = windows.CancelIoEx(handle, &w.ov)
		_ = windows.GetOverlappedResult(handle, &w.ov, &bytesReturned, true)
		return 0, windows.ERROR_OPERATION_ABORTED
	}
	if err := windows.GetOverlappedResult(handle, &w.ov, &bytesReturned, true); err != nil {
		return 0, err
	}
	return bytesReturned, nil
}

func (w *overlappedWaiter) close() {
	close(w.doneCh)
	<-w.joinedCh
	w.pinner.Unpin()
	_ = windows.CloseHandle(w.cancelEvent)
	_ = windows.CloseHandle(w.ioEvent)
}

func (p *Provider) watchUSNJournal(
	ctx context.Context,
	vol provider.VolumeInfo,
	driveLetter string,
	sinceCursor uint64,
	out chan<- provider.FileEvent,
) error {
	// Pin to one OS thread so non-IOCP overlapped DeviceIoControl IRPs are never aborted by OS thread exit.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	handle, err := openVolumeReadOnly(driveLetter, true)
	if err != nil {
		return err
	}
	defer windows.CloseHandle(handle)

	jd, err := queryUSNJournal(handle, true)
	if err != nil {
		return err
	}

	startUsn := jd.NextUsn
	if sinceCursor > 0 {
		if int64(sinceCursor) < jd.FirstUsn || int64(sinceCursor) > jd.NextUsn {
			return fmt.Errorf("cursor %d outside valid USN range [%d, %d]: %w", sinceCursor, jd.FirstUsn, jd.NextUsn, provider.ErrCursorExpired)
		}
		startUsn = int64(sinceCursor)
	}

	waiter, err := newOverlappedWaiter(ctx, usnWatchBufSize)
	if err != nil {
		return err
	}
	defer waiter.close()

	buf := waiter.buf
	norm := NewUSNEventNormalizer(vol.VolID)

	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		inData := readUsnJournalDataV0{
			StartUsn:          startUsn,
			ReasonMask:        0xFFFFFFFF,
			ReturnOnlyOnClose: 0,
			Timeout:           0,
			BytesToWaitFor:    1,
			UsnJournalID:      jd.UsnJournalID,
		}
		if err := waiter.reset(); err != nil {
			return err
		}

		var bytesReturned uint32
		err := windows.DeviceIoControl(
			handle,
			fsctlReadUsnJournal,
			(*byte)(unsafe.Pointer(&inData)),
			uint32(unsafe.Sizeof(inData)),
			&buf[0],
			uint32(len(buf)),
			&bytesReturned,
			&waiter.ov,
		)
		if err != nil {
			if errors.Is(err, windows.ERROR_IO_PENDING) {
				bytesReturned, err = waiter.wait(handle)
			}
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				if errors.Is(err, errJournalEntryDeleted) || errors.Is(err, errJournalNotActive) || errors.Is(err, errJournalDeleteInProgress) {
					return fmt.Errorf("usn journal wrapped/reset: %w (%v)", provider.ErrCursorExpired, err)
				}
				return err
			}
		}

		if bytesReturned <= 8 {
			continue
		}

		var sendCancelled bool
		nextUsn, _, parseErr := ParseUSNBuffer(buf[:bytesReturned], nil, func(rec USNRecord) {
			if sendCancelled {
				return
			}
			norm.Consume(rec, func(ev provider.FileEvent) {
				select {
				case out <- ev:
				case <-ctx.Done():
					sendCancelled = true
				}
			})
		})
		if parseErr != nil {
			return parseErr
		}
		if sendCancelled {
			return nil
		}
		if int64(nextUsn) > startUsn {
			startUsn = int64(nextUsn)
		}
	}
}

func extractDriveLetterRoot(rootPath string) (string, bool) {
	s := strings.TrimSpace(rootPath)
	if len(s) == 2 && s[1] == ':' {
		if (s[0] >= 'a' && s[0] <= 'z') || (s[0] >= 'A' && s[0] <= 'Z') {
			return strings.ToUpper(s[:1]), true
		}
	}
	if len(s) == 3 && s[1] == ':' && (s[2] == '\\' || s[2] == '/') {
		if (s[0] >= 'a' && s[0] <= 'z') || (s[0] >= 'A' && s[0] <= 'Z') {
			return strings.ToUpper(s[:1]), true
		}
	}
	return "", false
}
