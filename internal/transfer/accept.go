package transfer

import (
	"net"
	"time"
)

// AcceptConns accepts the n connections that make up one transfer session.
func AcceptConns(l net.Listener, n int, timeout time.Duration) (conns []net.Conn, err error) {
	// Set a deadline, throwing an error if something goes off
	deadliner, _ := l.(interface{ SetDeadline(time.Time) error })
	defer func() {
		if deadliner != nil {
			_ = deadliner.SetDeadline(time.Time{})
		}
		if err != nil {
			closeAll(conns)
			conns = nil
		}
	}()

	// Spin up the n listening threads
	for len(conns) < n {
		conn, err := l.Accept()
		if err != nil {
			return conns, err
		}

		// Add the established connections to a metadata array for tracking
		conns = append(conns, conn)
		if len(conns) == 1 && deadliner != nil {

			// First connection handles closing of all connections upon timeout
			_ = deadliner.SetDeadline(time.Now().Add(timeout))
		}
	}
	return conns, nil
}
