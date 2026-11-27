//go:build !windows && !linux && !darwin

package provider

// SetLowPriorityThread is a no-op fallback on unsupported platforms.
func SetLowPriorityThread() (restore func()) {
	return func() {}
}
