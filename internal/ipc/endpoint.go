package ipc

import (
	"context"
	"errors"
	"net"
)

var (
	// ErrEndpointInUse is returned when another live process already owns the IPC endpoint.
	ErrEndpointInUse = errors.New("ipc: endpoint already in use")

	// ErrPeerIdentityMismatch is returned when the peer's user identity does not match the current user.
	ErrPeerIdentityMismatch = errors.New("ipc: peer identity mismatch")

	// ErrInsecureDir is returned on Unix when the socket runtime directory has unsafe permissions or owner.
	ErrInsecureDir = errors.New("ipc: runtime directory has insecure permissions or wrong owner")

	// ErrListenerClosed is returned by Accept after the listener has been closed.
	ErrListenerClosed = errors.New("ipc: listener closed")
)

// Listener accepts authenticated same-user IPC connections.
type Listener interface {
	Accept() (net.Conn, error)
	Close() error
	Addr() string
	RejectedCount() uint64
}

// ListenOptions configures transport-level IPC listening.
type ListenOptions struct {
	Endpoint     string
	peerVerifier func(peerID, selfID string) error
}

// DialOptions configures transport-level IPC dialing.
type DialOptions struct {
	Endpoint     string
	peerVerifier func(peerID, selfID string) error
}

// DefaultEndpoint returns the per-user local IPC endpoint path/name for the current OS.
func DefaultEndpoint() (string, error) {
	return defaultEndpoint()
}

// Listen starts a same-user IPC listener on endpoint (or DefaultEndpoint if empty).
func Listen(endpoint string) (Listener, error) {
	return listenWithOptions(ListenOptions{Endpoint: endpoint})
}

// DialTransport connects to a same-user IPC endpoint (or DefaultEndpoint if empty)
// and verifies the server's user identity before returning the connection.
func DialTransport(ctx context.Context, endpoint string) (net.Conn, error) {
	return dialTransportWithOptions(ctx, DialOptions{Endpoint: endpoint})
}

func verifyPeer(peerID, selfID string, custom func(peerID, selfID string) error) error {
	if custom != nil {
		return custom(peerID, selfID)
	}
	if peerID == "" || selfID == "" || peerID != selfID {
		return ErrPeerIdentityMismatch
	}
	return nil
}
