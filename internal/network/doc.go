// Package network implements the XDBC-Go V1 TCP handshake and streaming stages.
// Workers exchange pool buffer IDs through bounded queues. Each connection is
// owned by one worker; pipeline coordinators manage connections and queue closure.
package network
