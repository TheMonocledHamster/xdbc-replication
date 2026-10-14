// Package queue provides bounded, context-aware queues of pool buffer IDs for
// exchanging buffers between pipeline stages. Closing a queue signals completion;
// consumers receive io.EOF after draining the queued IDs.
package queue
