//go:build windows

package ipc

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

func uniqueTestPipeName(t *testing.T) string {
	t.Helper()
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	return `\\.\pipe\xift-test-` + hex.EncodeToString(b[:])
}

func TestWindowsDefaultEndpointEmbedsUserSID(t *testing.T) {
	sid, err := currentProcessUserSID()
	if err != nil {
		t.Fatalf("currentProcessUserSID: %v", err)
	}
	ep, err := DefaultEndpoint()
	if err != nil {
		t.Fatalf("DefaultEndpoint: %v", err)
	}
	want := `\\.\pipe\xift-daemon-` + sid
	if ep != want {
		t.Fatalf("DefaultEndpoint() = %q, want %q", ep, want)
	}
}

func TestWindowsPipeProtectedDACLOnlyOwner(t *testing.T) {
	ep := uniqueTestPipeName(t)
	ln, err := Listen(ep)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()

	// Read security descriptor directly from the created pipe handle.
	wl := ln.(*winPipeListener)
	wl.mu.Lock()
	h := wl.nextPipe
	wl.mu.Unlock()
	sd, err := windows.GetSecurityInfo(
		h,
		windows.SE_KERNEL_OBJECT,
		windows.OWNER_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION,
	)
	if err != nil {
		t.Fatalf("GetSecurityInfo: %v", err)
	}

	ctrl, _, err := sd.Control()
	if err != nil {
		t.Fatalf("sd.Control: %v", err)
	}
	if ctrl&windows.SE_DACL_PROTECTED == 0 {
		t.Fatalf("expected protected DACL (SE_DACL_PROTECTED), got control=0x%x", ctrl)
	}

	selfSIDStr, err := currentProcessUserSID()
	if err != nil {
		t.Fatal(err)
	}
	owner, _, err := sd.Owner()
	if err != nil {
		t.Fatalf("sd.Owner: %v", err)
	}
	if owner.String() != selfSIDStr {
		t.Fatalf("pipe owner SID = %s, want %s", owner.String(), selfSIDStr)
	}

	dacl, _, err := sd.DACL()
	if err != nil {
		t.Fatalf("sd.DACL: %v", err)
	}
	if dacl == nil {
		t.Fatalf("expected non-nil DACL")
	}

	// Verify SDDL string contains protected DACL with only the owner SID allow ACE.
	// Note: Windows SDDL abbreviates the built-in Local Administrator SID (*-500) as "LA".
	sddl := sd.String()
	if !strings.Contains(sddl, "D:P(") {
		t.Fatalf("SDDL missing protected DACL D:P(...): %s", sddl)
	}
	isLocalAdmin := strings.HasSuffix(selfSIDStr, "-500")
	if !strings.Contains(sddl, selfSIDStr) && !(isLocalAdmin && strings.Contains(sddl, ";;;LA)")) {
		t.Fatalf("SDDL missing owner SID %s: %s", selfSIDStr, sddl)
	}
	// Ensure no Everyone (WD), Authenticated Users (AU), or Users (BU) ACEs exist.
	for _, forbidden := range []string{";;;WD)", ";;;AU)", ";;;BU)", ";;;AN)"} {
		if strings.Contains(sddl, forbidden) {
			t.Fatalf("SDDL grants forbidden principal %s: %s", forbidden, sddl)
		}
	}

	// Also walk ACL entries directly to confirm exactly 1 allow ACE for selfSID.
	var aclSizeInfo struct {
		AceCount      uint32
		AclBytesInUse uint32
		AclBytesFree  uint32
	}
	const aclSizeInformation = 2
	modAdvapi32 := windows.NewLazySystemDLL("advapi32.dll")
	procGetAclInfo := modAdvapi32.NewProc("GetAclInformation")
	r1, _, e1 := procGetAclInfo.Call(
		uintptr(unsafe.Pointer(dacl)),
		uintptr(unsafe.Pointer(&aclSizeInfo)),
		unsafe.Sizeof(aclSizeInfo),
		aclSizeInformation,
	)
	if r1 == 0 {
		t.Fatalf("GetAclInformation failed: %v", e1)
	}
	if aclSizeInfo.AceCount != 1 {
		t.Fatalf("expected exactly 1 ACE in pipe DACL, got %d (SDDL=%s)", aclSizeInfo.AceCount, sddl)
	}
}

func TestWindowsDuplicateListenFailsFast(t *testing.T) {
	ep := uniqueTestPipeName(t)
	l1, err := Listen(ep)
	if err != nil {
		t.Fatalf("first Listen: %v", err)
	}
	defer l1.Close()

	l2, err := Listen(ep)
	if !errors.Is(err, ErrEndpointInUse) {
		if l2 != nil {
			_ = l2.Close()
		}
		t.Fatalf("second Listen: got %v, want ErrEndpointInUse", err)
	}
}

func TestWindowsSameUserRoundTripAndPeerVerifier(t *testing.T) {
	ep := uniqueTestPipeName(t)
	ln, err := Listen(ep)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()

	errCh := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer c.Close()
		buf := make([]byte, 12)
		if _, err := io.ReadFull(c, buf); err != nil {
			errCh <- err
			return
		}
		_, err = c.Write([]byte("hello-client"))
		errCh <- err
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cc, err := DialTransport(ctx, ep)
	if err != nil {
		t.Fatalf("DialTransport: %v", err)
	}
	if _, err := cc.Write([]byte("hello-server")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	reply := make([]byte, 12)
	if _, err := io.ReadFull(cc, reply); err != nil {
		t.Fatalf("ReadFull: %v", err)
	}
	_ = cc.Close()
	if string(reply) != "hello-client" {
		t.Fatalf("got %q, want hello-client", string(reply))
	}
	if err := <-errCh; err != nil {
		t.Fatalf("server: %v", err)
	}

	// Client rejects impostor server when peerVerifier fails.
	if bad, err := dialTransportWithOptions(ctx, DialOptions{
		Endpoint: ep,
		peerVerifier: func(peerID, selfID string) error {
			return ErrPeerIdentityMismatch
		},
	}); !errors.Is(err, ErrPeerIdentityMismatch) {
		if bad != nil {
			_ = bad.Close()
		}
		t.Fatalf("impostor check: got %v, want ErrPeerIdentityMismatch", err)
	}

	// Server rejects mismatched client without sending any bytes and increments RejectedCount.
	ep2 := uniqueTestPipeName(t)
	ln2, err := listenWithOptions(ListenOptions{
		Endpoint: ep2,
		peerVerifier: func(peerID, selfID string) error {
			return ErrPeerIdentityMismatch
		},
	})
	if err != nil {
		t.Fatalf("listenWithOptions: %v", err)
	}
	defer ln2.Close()

	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		c, err := ln2.Accept()
		if err == nil && c != nil {
			_ = c.Close()
		}
	}()

	rawClient, err := DialTransport(ctx, ep2)
	if err != nil {
		t.Fatalf("dial ep2: %v", err)
	}
	var one [1]byte
	n, rerr := rawClient.Read(one[:])
	_ = rawClient.Close()
	if n != 0 || rerr == nil {
		t.Fatalf("expected closed connection with 0 bytes, got n=%d err=%v", n, rerr)
	}
	_ = ln2.Close()
	<-acceptDone

	if ln2.RejectedCount() < 1 {
		t.Fatalf("RejectedCount() = %d, want >= 1", ln2.RejectedCount())
	}
}

func TestWindowsConcurrentIOAndCloseCancelsBlockedRead(t *testing.T) {
	ep := uniqueTestPipeName(t)
	ln, err := Listen(ep)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()

	srvConnCh := make(chan io.ReadWriteCloser, 1)
	go func() {
		c, err := ln.Accept()
		if err == nil {
			srvConnCh <- c
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cliConn, err := DialTransport(ctx, ep)
	if err != nil {
		t.Fatalf("DialTransport: %v", err)
	}
	srvConn := <-srvConnCh
	defer srvConn.Close()

	// Exercise concurrent bidirectional writes/reads first.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			msg := fmt.Sprintf("msg-%04d", i)
			if _, err := cliConn.Write([]byte(msg)); err != nil {
				t.Errorf("cli write: %v", err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		buf := make([]byte, 8)
		for i := 0; i < 50; i++ {
			if _, err := io.ReadFull(srvConn, buf); err != nil {
				t.Errorf("srv read: %v", err)
				return
			}
		}
	}()
	wg.Wait()

	// Now start a blocked Read on cliConn and interrupt it immediately via Close().
	readDone := make(chan error, 1)
	go func() {
		buf := make([]byte, 64)
		_, rerr := cliConn.Read(buf)
		readDone <- rerr
	}()

	time.Sleep(30 * time.Millisecond)
	closeStart := time.Now()
	_ = cliConn.Close()

	select {
	case rerr := <-readDone:
		if rerr == nil {
			t.Fatalf("expected error from interrupted Read")
		}
		if elapsed := time.Since(closeStart); elapsed > 2*time.Second {
			t.Fatalf("Close took too long to unblock Read: %v", elapsed)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("blocked Read was not interrupted by Close")
	}
}
