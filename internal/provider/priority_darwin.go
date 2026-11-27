//go:build darwin

package provider

import (
	"runtime"
	"sync"

	"github.com/ebitengine/purego"
)

const (
	iopolTypeDisk    = 0
	iopolScopeThread = 1
	iopolDefault     = 0
	iopolThrottle    = 3

	// macOS <sys/qos.h> QOS_CLASS_BACKGROUND (0x09) and QOS_CLASS_DEFAULT (0x15).
	qosClassBackground uint32 = 0x09
	qosClassDefault    uint32 = 0x15
)

var (
	darwinPriorityOnce sync.Once
	setiopolicyNP      func(iotype, scope, policy int32) int32
	setQoSClassSelfNP  func(qosClass uint32, relativePriority int32) int32
)

func initDarwinThreadPriority() {
	darwinPriorityOnce.Do(func() {
		libc, err := purego.Dlopen("/usr/lib/libSystem.B.dylib", purego.RTLD_LAZY|purego.RTLD_GLOBAL)
		if err != nil {
			return
		}
		purego.RegisterLibFunc(&setiopolicyNP, libc, "setiopolicy_np")
		purego.RegisterLibFunc(&setQoSClassSelfNP, libc, "pthread_set_qos_class_self_np")
	})
}

// SetLowPriorityThread locks the current goroutine to its OS thread and applies
// strictly thread-scoped macOS QOS_CLASS_BACKGROUND + IOPOL_THROTTLE via purego (zero CGO).
// Unlike setpriority(PRIO_PROCESS), this never throttles unrelated query/IPC threads and
// restores cleanly without root privileges.
func SetLowPriorityThread() (restore func()) {
	runtime.LockOSThread()
	initDarwinThreadPriority()

	if setQoSClassSelfNP != nil {
		_ = setQoSClassSelfNP(qosClassBackground, 0)
	}
	if setiopolicyNP != nil {
		_ = setiopolicyNP(iopolTypeDisk, iopolScopeThread, iopolThrottle)
	}

	restored := false
	return func() {
		if restored {
			return
		}
		restored = true
		if setiopolicyNP != nil {
			_ = setiopolicyNP(iopolTypeDisk, iopolScopeThread, iopolDefault)
		}
		if setQoSClassSelfNP != nil {
			_ = setQoSClassSelfNP(qosClassDefault, 0)
		}
		runtime.UnlockOSThread()
	}
}
