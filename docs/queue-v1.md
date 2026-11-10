# Queues

`internal/queue` wraps a bounded Go channel of buffer IDs. A full queue blocks its producer, providing backpressure between pipeline stages.

| Operation | Behavior |
|---|---|
| `New(capacity)` | Create a queue; capacity must be positive |
| `Push(ctx, id)` | Wait for space or cancellation |
| `Pop(ctx)` | Wait for an ID or cancellation; return `io.EOF` after closure and draining |
| `close(q)` | Signal that no more IDs will be produced |

A successful push transfers buffer ownership to the downstream stage. If push fails, the caller still owns the buffer and must release it.

The producer's coordinator closes its output after production finishes. If several producers share a queue, close it only after all have finished. Sending to a closed queue panics; cancellation alone does not close a queue.

On failure, the [runtime](architecture.md#ownership-and-failures) cancels workers, waits for them, and releases IDs left in the queues. Queue capacity and pool size must be [configured together](configuration.md#buffer-sizing).

Run `make test` for blocking, cancellation, ordering, and closure checks.
