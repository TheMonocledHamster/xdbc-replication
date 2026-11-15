package transfer

import (
	"net"
	"time"
)

// Accept n connections for one transfer session.
func AcceptConns(l net.Listener, n int, timeout time.Duration) (conns []net.Conn, err error) {
	// Set a deadline, throwing an error if something's gone off
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

	// Start the n listening threads sequentially
	for len(conns) < n {
		conn, err := l.Accept()
		if err != nil {
			return conns, err
		}

		// Add established conns to metadata array for tracking
		conns = append(conns, conn)
		if len(conns) == 1 && deadliner != nil {

			// First conn sets deadline to accept the remaining requests
			_ = deadliner.SetDeadline(time.Now().Add(timeout))
		}
	}
	return conns, nil
}
