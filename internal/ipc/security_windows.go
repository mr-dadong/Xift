//go:build windows

package ipc

import (
	"fmt"
	"unsafe"

	"golang.org/x/sys/windows"
)

var (
	modKernel32Pipe                 = windows.NewLazySystemDLL("kernel32.dll")
	procGetNamedPipeClientProcessId = modKernel32Pipe.NewProc("GetNamedPipeClientProcessId")
	procGetNamedPipeServerProcessId = modKernel32Pipe.NewProc("GetNamedPipeServerProcessId")
)

func currentProcessUserSID() (string, error) {
	var tok windows.Token
	if err := windows.OpenProcessToken(windows.CurrentProcess(), windows.TOKEN_QUERY, &tok); err != nil {
		return "", fmt.Errorf("ipc: open current process token: %w", err)
	}
	defer tok.Close()

	tu, err := tok.GetTokenUser()
	if err != nil {
		return "", fmt.Errorf("ipc: query current token user: %w", err)
	}
	return tu.User.Sid.String(), nil
}

func processUserSID(pid uint32) (string, error) {
	hProc, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, pid)
	if err != nil {
		return "", fmt.Errorf("ipc: open peer process %d: %w", pid, err)
	}
	defer windows.CloseHandle(hProc)

	var tok windows.Token
	if err := windows.OpenProcessToken(hProc, windows.TOKEN_QUERY, &tok); err != nil {
		return "", fmt.Errorf("ipc: open peer process %d token: %w", pid, err)
	}
	defer tok.Close()

	tu, err := tok.GetTokenUser()
	if err != nil {
		return "", fmt.Errorf("ipc: query peer process %d token user: %w", pid, err)
	}
	return tu.User.Sid.String(), nil
}

func namedPipeClientSID(pipe windows.Handle) (string, error) {
	var pid uint32
	r1, _, e1 := procGetNamedPipeClientProcessId.Call(uintptr(pipe), uintptr(unsafe.Pointer(&pid)))
	if r1 == 0 {
		return "", fmt.Errorf("ipc: GetNamedPipeClientProcessId: %w", e1)
	}
	return processUserSID(pid)
}

func namedPipeServerSID(pipe windows.Handle) (string, error) {
	var pid uint32
	r1, _, e1 := procGetNamedPipeServerProcessId.Call(uintptr(pipe), uintptr(unsafe.Pointer(&pid)))
	if r1 == 0 {
		return "", fmt.Errorf("ipc: GetNamedPipeServerProcessId: %w", e1)
	}
	return processUserSID(pid)
}

func sameUserSecurityDescriptor(sidStr string) (*windows.SECURITY_DESCRIPTOR, *windows.SecurityAttributes, error) {
	// Protected DACL granting Generic All exclusively to the current user SID.
	sddl := fmt.Sprintf("O:%sD:P(A;;GA;;;%s)", sidStr, sidStr)
	sd, err := windows.SecurityDescriptorFromString(sddl)
	if err != nil {
		return nil, nil, fmt.Errorf("ipc: SecurityDescriptorFromString: %w", err)
	}
	sa := &windows.SecurityAttributes{
		Length:             uint32(unsafe.Sizeof(windows.SecurityAttributes{})),
		SecurityDescriptor: sd,
		InheritHandle:      0,
	}
	return sd, sa, nil
}
