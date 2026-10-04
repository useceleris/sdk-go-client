//go:build linux || darwin

package celeris

import (
	"net"
	"syscall"
	"testing"
)

// Every socket the transport dials probes when idle, so a silently dead path
// fails it. Idle time is not read back: its option has a different name on
// each system.
func TestSocketsProbeWhenIdle(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")

	if err != nil {
		t.Fatal(err)
	}

	defer func() { _ = listener.Close() }()

	conn, err := httpTransport().DialContext(t.Context(), "tcp", listener.Addr().String())

	if err != nil {
		t.Fatal(err)
	}

	defer func() { _ = conn.Close() }()

	raw, err := conn.(*net.TCPConn).SyscallConn()

	if err != nil {
		t.Fatal(err)
	}

	options := map[string][2]int{
		"SO_KEEPALIVE":  {syscall.SOL_SOCKET, syscall.SO_KEEPALIVE},
		"TCP_KEEPINTVL": {syscall.IPPROTO_TCP, syscall.TCP_KEEPINTVL},
		"TCP_KEEPCNT":   {syscall.IPPROTO_TCP, syscall.TCP_KEEPCNT},
	}

	want := map[string]int{
		"SO_KEEPALIVE":  1,
		"TCP_KEEPINTVL": int(keepAliveInterval.Seconds()),
		"TCP_KEEPCNT":   keepAliveCount,
	}

	for name, option := range options {
		var value int
		var readErr error

		if err := raw.Control(func(descriptor uintptr) {
			value, readErr = syscall.GetsockoptInt(int(descriptor), option[0], option[1])
		}); err != nil {
			t.Fatal(err)
		}

		if readErr != nil {
			t.Fatalf("%s: %v", name, readErr)
		}

		// Linux reports SO_KEEPALIVE as any nonzero value.
		if name == "SO_KEEPALIVE" && value != 0 {
			value = 1
		}

		if value != want[name] {
			t.Fatalf("%s is %d, want %d", name, value, want[name])
		}
	}
}
