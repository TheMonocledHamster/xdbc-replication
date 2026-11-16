package transfer

import (
	"errors"
	"net"
	"testing"
	"time"
)

func TestAcceptConnsCollectsWholeSession(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	for i := 0; i < 3; i++ {
		go func() {
			if c, err := net.Dial("tcp", l.Addr().String()); err == nil {
				defer c.Close()
				time.Sleep(200 * time.Millisecond)
			}
		}()
	}
	conns, err := AcceptConns(l, 3, time.Second)
	if err != nil || len(conns) != 3 {
		t.Fatalf("AcceptConns() = %d conns, %v; want 3", len(conns), err)
	}
	closeAll(conns)
}

// A client that opens too few connections must not wedge the server, and the
// listener must stay usable for the next session.
func TestAcceptConnsTimesOutAndRecovers(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	c, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	conns, err := AcceptConns(l, 2, 100*time.Millisecond)
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() || conns != nil {
		t.Fatalf("AcceptConns() = %v, %v; want a timeout and no connections", conns, err)
	}
	// The accepted connection was closed on failure.
	_ = c.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := c.Read(make([]byte, 1)); err == nil {
		t.Fatal("partial session connection left open")
	}
	go func() {
		for i := 0; i < 2; i++ {
			if c, err := net.Dial("tcp", l.Addr().String()); err == nil {
				defer c.Close()
			}
		}
		time.Sleep(200 * time.Millisecond)
	}()
	conns, err = AcceptConns(l, 2, time.Second)
	if err != nil || len(conns) != 2 {
		t.Fatalf("listener unusable after a timeout: %d conns, %v", len(conns), err)
	}
	closeAll(conns)
}
