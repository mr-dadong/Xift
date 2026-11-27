//go:build darwin

package darwin

import (
	"context"
	"fmt"
	"path"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/ebitengine/purego"
	"golang.org/x/sys/unix"

	"github.com/mr-dadong/Xift/internal/provider"
)

const (
	kCFStringEncodingUTF8         uint32 = 0x08000100
	kFSEventStreamEventIdSinceNow uint64 = ^uint64(0)

	kFSEventStreamCreateFlagNoDefer    uint32 = 0x00000002
	kFSEventStreamCreateFlagFileEvents uint32 = 0x00000010

	defaultFSEventLatencySeconds float64 = 0.15
)

type fsEventStreamContext struct {
	version         uintptr
	info            uintptr
	retain          uintptr
	release         uintptr
	copyDescription uintptr
}

type fsEventSession struct {
	ctx     context.Context
	volID   uint16
	volRoot string
	deduper *FirmlinkDeduper
	cache   *PathKeyCache
	out     chan<- provider.FileEvent
	errCh   chan error
}

var (
	fsEventsLoadOnce sync.Once
	fsEventsLoadErr  error

	fnCFStringCreateWithCString func(alloc uintptr, cStr string, encoding uint32) uintptr
	fnCFArrayCreate             func(alloc uintptr, values unsafe.Pointer, numValues int64, callBacks uintptr) uintptr
	fnCFRelease                 func(cf uintptr)
	cfTypeArrayCallBacksPtr     uintptr

	fnDispatchQueueCreate func(label string, attr uintptr) uintptr
	fnDispatchSyncF       func(queue, ctx, work uintptr)
	fnDispatchRelease     func(obj uintptr)
	noopDispatchWorkPtr   uintptr

	fnFSEventsGetCurrentEventId func() uint64
	fnFSEventStreamCreate       func(allocator, callback uintptr, ctxPtr unsafe.Pointer, pathsToWatch uintptr, sinceWhen uint64, latency float64, flags uint32) uintptr
	fnFSEventStreamSetDispatchQ func(streamRef, queue uintptr)
	fnFSEventStreamStart        func(streamRef uintptr) bool
	fnFSEventStreamStop         func(streamRef uintptr)
	fnFSEventStreamInvalidate   func(streamRef uintptr)
	fnFSEventStreamRelease      func(streamRef uintptr)
	globalFSEventCallbackPtr    uintptr

	sessionRegistry sync.Map // map[uintptr]*fsEventSession
	nextSessionID   atomic.Uintptr
)

func noopDispatchBarrier(_ uintptr) uintptr { return 0 }

func initFSEventsBindings() error {
	fsEventsLoadOnce.Do(func() {
		cf, err := purego.Dlopen("/System/Library/Frameworks/CoreFoundation.framework/CoreFoundation", purego.RTLD_LAZY|purego.RTLD_GLOBAL)
		if err != nil {
			fsEventsLoadErr = fmt.Errorf("dlopen CoreFoundation: %w", err)
			return
		}
		cs, err := purego.Dlopen("/System/Library/Frameworks/CoreServices.framework/CoreServices", purego.RTLD_LAZY|purego.RTLD_GLOBAL)
		if err != nil {
			fsEventsLoadErr = fmt.Errorf("dlopen CoreServices: %w", err)
			return
		}
		libSys, err := purego.Dlopen("/usr/lib/libSystem.B.dylib", purego.RTLD_LAZY|purego.RTLD_GLOBAL)
		if err != nil {
			fsEventsLoadErr = fmt.Errorf("dlopen libSystem.B.dylib: %w", err)
			return
		}

		purego.RegisterLibFunc(&fnCFStringCreateWithCString, cf, "CFStringCreateWithCString")
		purego.RegisterLibFunc(&fnCFArrayCreate, cf, "CFArrayCreate")
		purego.RegisterLibFunc(&fnCFRelease, cf, "CFRelease")
		if sym, symErr := purego.Dlsym(cf, "kCFTypeArrayCallBacks"); symErr == nil {
			cfTypeArrayCallBacksPtr = sym
		}

		purego.RegisterLibFunc(&fnDispatchQueueCreate, libSys, "dispatch_queue_create")
		purego.RegisterLibFunc(&fnDispatchSyncF, libSys, "dispatch_sync_f")
		purego.RegisterLibFunc(&fnDispatchRelease, libSys, "dispatch_release")
		noopDispatchWorkPtr = purego.NewCallback(noopDispatchBarrier)

		purego.RegisterLibFunc(&fnFSEventsGetCurrentEventId, cs, "FSEventsGetCurrentEventId")
		purego.RegisterLibFunc(&fnFSEventStreamCreate, cs, "FSEventStreamCreate")
		purego.RegisterLibFunc(&fnFSEventStreamSetDispatchQ, cs, "FSEventStreamSetDispatchQueue")
		purego.RegisterLibFunc(&fnFSEventStreamStart, cs, "FSEventStreamStart")
		purego.RegisterLibFunc(&fnFSEventStreamStop, cs, "FSEventStreamStop")
		purego.RegisterLibFunc(&fnFSEventStreamInvalidate, cs, "FSEventStreamInvalidate")
		purego.RegisterLibFunc(&fnFSEventStreamRelease, cs, "FSEventStreamRelease")

		globalFSEventCallbackPtr = purego.NewCallback(fsEventsCallbackTrampoline)
	})
	return fsEventsLoadErr
}

func currentFSEventID() uint64 {
	if err := initFSEventsBindings(); err != nil || fnFSEventsGetCurrentEventId == nil {
		return 0
	}
	return fnFSEventsGetCurrentEventId()
}

// fsEventsCallbackTrampoline receives CoreServices FSEventStreamCallback invocations
// via purego.NewCallback (zero CGO) and routes them to the active watch session.
func fsEventsCallbackTrampoline(
	_ uintptr,
	clientCallBackInfo uintptr,
	numEvents uintptr,
	eventPathsPtr uintptr,
	eventFlagsPtr uintptr,
	eventIDsPtr uintptr,
) uintptr {
	val, ok := sessionRegistry.Load(clientCallBackInfo)
	if !ok || numEvents == 0 {
		return 0
	}
	sess := val.(*fsEventSession)
	if sess.ctx.Err() != nil {
		return 0
	}

	n := int(numEvents)
	cPaths := unsafe.Slice((*uintptr)(unsafe.Pointer(eventPathsPtr)), n)
	cFlags := unsafe.Slice((*uint32)(unsafe.Pointer(eventFlagsPtr)), n)
	cIDs := unsafe.Slice((*uint64)(unsafe.Pointer(eventIDsPtr)), n)

	raw := make([]RawFSEvent, n)
	for i := 0; i < n; i++ {
		raw[i] = RawFSEvent{
			EventID: cIDs[i],
			Path:    cStringToGo(cPaths[i]),
			Flags:   cFlags[i],
		}
	}

	events, err := NormalizeFSEventsWithCache(sess.volID, sess.volRoot, raw, sess.deduper, sess.cache, darwinLstatLookup)
	if err != nil {
		select {
		case sess.errCh <- err:
		default:
		}
		return 0
	}

	for i := range events {
		if sess.ctx.Err() != nil {
			return 0
		}
		select {
		case sess.out <- events[i]:
		case <-sess.ctx.Done():
			return 0
		}
	}
	return 0
}

func cStringToGo(ptr uintptr) string {
	if ptr == 0 {
		return ""
	}
	const maxLen = 4096
	raw := unsafe.Slice((*byte)(unsafe.Pointer(ptr)), maxLen)
	for i := 0; i < maxLen; i++ {
		if raw[i] == 0 {
			return string(raw[:i])
		}
	}
	return string(raw)
}

// darwinLstatLookup resolves live APFS inodes strictly read-only.
// Even when fullPath has been deleted, it still lstats path.Dir(fullPath) so ParentKey is a real inode.
func darwinLstatLookup(fullPath string) (nodeKey, parentKey uint64, size, modTime int64, isDir, ok bool) {
	var pSt unix.Stat_t
	if err := unix.Lstat(path.Dir(fullPath), &pSt); err == nil {
		parentKey = pSt.Ino
	}
	var st unix.Stat_t
	if err := unix.Lstat(fullPath, &st); err != nil {
		return 0, parentKey, 0, 0, false, false
	}
	isDir = (st.Mode & unix.S_IFMT) == unix.S_IFDIR
	modTime = st.Mtim.Nano()
	return st.Ino, parentKey, st.Size, modTime, isDir, true
}

func (p *Provider) watchFSEvents(
	ctx context.Context,
	vol provider.VolumeInfo,
	sinceCursor uint64,
	out chan<- provider.FileEvent,
) error {
	if err := initFSEventsBindings(); err != nil {
		return err
	}

	rootPath := normalizeUnixPath(vol.RootPath)
	if rootPath == "" {
		return provider.ErrInvalidVolume
	}

	cfPath := fnCFStringCreateWithCString(0, rootPath, kCFStringEncodingUTF8)
	if cfPath == 0 {
		return fmt.Errorf("CFStringCreateWithCString failed for %q", rootPath)
	}
	defer fnCFRelease(cfPath)

	values := [1]uintptr{cfPath}
	cfPaths := fnCFArrayCreate(0, unsafe.Pointer(&values[0]), 1, cfTypeArrayCallBacksPtr)
	if cfPaths == 0 {
		return fmt.Errorf("CFArrayCreate failed")
	}
	defer fnCFRelease(cfPaths)

	token := nextSessionID.Add(1)
	sess := &fsEventSession{
		ctx:     ctx,
		volID:   vol.VolID,
		volRoot: rootPath,
		deduper: p.deduper,
		cache:   p.getOrCreatePathCache(vol.VolID),
		out:     out,
		errCh:   make(chan error, 1),
	}
	sessionRegistry.Store(token, sess)

	sinceWhen := sinceCursor
	if sinceWhen == 0 {
		sinceWhen = kFSEventStreamEventIdSinceNow
	}

	streamCtx := fsEventStreamContext{
		version: 0,
		info:    token,
	}

	stream := fnFSEventStreamCreate(
		0,
		globalFSEventCallbackPtr,
		unsafe.Pointer(&streamCtx),
		cfPaths,
		sinceWhen,
		defaultFSEventLatencySeconds,
		kFSEventStreamCreateFlagFileEvents|kFSEventStreamCreateFlagNoDefer,
	)
	if stream == 0 {
		sessionRegistry.Delete(token)
		return fmt.Errorf("FSEventStreamCreate failed for %q", rootPath)
	}

	queue := fnDispatchQueueCreate("com.xift.fsevents", 0)
	if queue == 0 {
		fnFSEventStreamRelease(stream)
		sessionRegistry.Delete(token)
		return fmt.Errorf("dispatch_queue_create failed")
	}

	fnFSEventStreamSetDispatchQ(stream, queue)
	if !fnFSEventStreamStart(stream) {
		fnFSEventStreamInvalidate(stream)
		fnFSEventStreamRelease(stream)
		fnDispatchRelease(queue)
		sessionRegistry.Delete(token)
		return fmt.Errorf("FSEventStreamStart failed for %q", rootPath)
	}

	defer func() {
		fnFSEventStreamStop(stream)
		fnFSEventStreamInvalidate(stream)
		// Synchronize with serial queue so any in-flight callback completes before
		// releasing resources or returning (preventing closed-channel send races).
		if fnDispatchSyncF != nil && noopDispatchWorkPtr != 0 {
			fnDispatchSyncF(queue, 0, noopDispatchWorkPtr)
		}
		fnFSEventStreamRelease(stream)
		fnDispatchRelease(queue)
		sessionRegistry.Delete(token)
	}()

	select {
	case <-ctx.Done():
		return nil
	case err := <-sess.errCh:
		return err
	}
}
