//go:build linux

package ipc

import (
	"fmt"
	"net"

	"golang.org/x/sys/unix"
)

func getPeerUID(uc *net.UnixConn) (uint32, error) {
	raw, err := uc.SyscallConn()
	if err != nil {
		return 0, err
	}
	var (
		uid   uint32
		scErr error
	)
	if err := raw.Control(func(fd uintptr) {
		cred, gerr := unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
		if gerr != nil {
			scErr = gerr
			return
		}
		uid = cred.Uid
	}); err != nil {
		return 0, err
	}
	if scErr != nil {
		return 0, fmt.Errorf("getsockopt SO_PEERCRED: %w", scErr)
	}
	return uid, nil
}
