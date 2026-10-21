# XDBC-Go Wire Protocol V1

Integers encoded in network byte order (big endian).

## Frame Header

The frame header is exactly 16 bytes.

| Offset | Size | Field |
|---|---:|---|
| 0 | 4 | Magic (`XDBC`) |
| 4 | 1 | Version |
| 5 | 1 | Message type |
| 6 | 1 | Compression |
| 7 | 1 | Format |
| 8 | 4 | Payload length |
| 12 | 4 | Record count |

## Message types

1. HELLO
2. HELLO_ACK
3. DATA
4. END
5. ERROR

The implementation is in `internal/network`, with Go package name `network`.
It follows the TCP send/receive stages and buffer-ID queues in the reference paper.

The current row payload includes a per-row NULL bitmap; see [row-v1.md](row-v1.md).
Its replacement of the earlier bitmap-free layout requires both peers to upgrade
together. The frame header and `FormatRow` ID have not changed, and the handshake
does not detect an old row-layout implementation.

## Handshake

The client sends HELLO; the server replies with HELLO_ACK after validating the
request, or ERROR when the request is incompatible. HELLO and HELLO_ACK each
contain exactly six payload bytes:

| Offset | Size | Field |
|---|---:|---|
| 0 | 4 | Maximum logical-buffer payload capacity, big endian |
| 4 | 1 | Compression (`0` = none, `1` = Snappy) |
| 5 | 1 | Intermediate format (`0` = row) |

The accepted capacity is the smaller of the requested and supported capacities,
excluding the 32-byte pool header. Capacity must be positive and leave room for
that header in the wire header's uint32 payload length. Compression and format
must match on both sides. Control-frame headers use compression `0`, format `0`,
and record count `0`.

The handshake negotiates transport settings only. Dataset selection, source
credentials, and starting the upstream pipeline belong to the coordinating
runtime. Prepare the source pipeline before acknowledging readiness, and build
its buffers using the accepted capacity.

## DATA, END, and ERROR

DATA contains the existing **32-byte pool header followed by exactly PayloadLen
bytes** from the pool buffer. The frame's PayloadSize includes both. The pool
header retains its existing little-endian encoding; the enclosing 16-byte frame
header uses big endian. Sequence, next-record, uncompressed-length, protocol,
and reserved fields are preserved verbatim. `BufferHeader.Protocol` remains pool
metadata; the outer header independently carries the wire version and format.

The outer RecordCount and Compression must equal the pool header fields, and
the inner PayloadLen must equal PayloadSize minus 32. Receiver capacity checks
occur before reading into a buffer. Compression is performed by a separate stage:
send and receive transport the already encoded bytes unchanged.

END has zero payload and zero record count. Each sender emits it once its shared
input queue is closed and drained. Each receiver requires END to complete
successfully; socket EOF before END is an incomplete transfer. ERROR carries a
diagnostic of at most 1024 bytes and causes the receiving operation to fail.
Malformed headers, unnegotiated encodings, unexpected message types, inconsistent
metadata, and truncated payloads also fail the operation.

## Extending codecs, formats, and messages

The header codec handles the fixed V1 envelope and preserves message,
compression, and format IDs as bytes. It checks size, magic, and wire version;
it does not maintain a list of installed codecs or layouts. `Header.Validate`
separately checks builtin V1 message rules. The handshake and receive stages
apply that validation and enforce the configured encoding IDs and capacity.

The pipeline owns codec/layout support. To add one, implement it in its pipeline
stage, assign a stable unused byte ID, and configure both peers with that ID.
`Config{Compression: Compression(42), Format: Format(77), PayloadCap: 65536}`
can be negotiated without modifying the header codec or network workers.
A peer configured with different IDs rejects that request, and DATA must match
the accepted IDs. Choosing an ID does not install or implement its codec.

New message IDs also round-trip through the header and generic frame helpers.
Using a new message in the builtin transfer protocol requires adding its rules
to `Header.Validate` and its handling to the applicable handshake/transfer stage.
Unhandled messages are rejected there. A change to the envelope itself requires
a new wire version. Existing numeric IDs must remain stable.

## Go API and buffer ownership

```go
type Config struct {
	PayloadCap  int
	Compression Compression
	Format      Format
}

func ClientHandshake(ctx context.Context, conn net.Conn, requested Config) (Config, error)
func ServerHandshake(ctx context.Context, conn net.Conn, supported Config) (Config, error)
func Send(ctx context.Context, conn net.Conn, p *pool.Pool, input queue.Queue, cfg Config) error
func Receive(ctx context.Context, conn net.Conn, p *pool.Pool, output queue.Queue, cfg Config) error
```

Pass the accepted handshake configuration to Send/Receive. Both local pools
must accommodate its PayloadCap. Compression and decompression remain separate
pipeline stages; network workers transport their bytes unchanged.

Send takes ownership of each popped source ID, writes only its used bytes, and
releases it on success or failure. Closing and draining its input produces END.
Receive reads directly into an acquired target buffer and transfers ownership
to output only after validating the complete frame. Read, validation, and push
failures return the acquired buffer to the pool even after cancellation.
Downstream consumers release IDs after processing. Pool exhaustion and bounded
queue capacity provide backpressure without extra data-buffer allocations.

Use one connection per sender/receiver pair. Parallel workers may share their
local queues and pools. The coordinator closes input after all producers finish
and output after all receivers finish. Workers never close shared queues or
connections. On failure, cancel the transfer, close connections, wait for workers,
and drain/release queued IDs that remain owned by the pipeline. Never push into
a closed queue. Socket EOF without END fails the transfer; already forwarded
buffers still belong to downstream consumers.

Each operation exclusively owns its connection's I/O and deadlines while it runs.
Context cancellation/deadlines interrupt blocked socket I/O and queue/pool waits;
connection deadlines are cleared on return. Close the connection after protocol
or I/O errors because its framing may be incomplete. TCP preserves order within
each connection; use the retained sequence metadata for ordering across workers.

For example, on the source (with its upstream producer running concurrently):

```go
cfg, err := network.ServerHandshake(ctx, conn, network.Config{
	PayloadCap: sourcePool.PayloadCap(),
	Compression: network.CompressionNone,
	Format: network.FormatRow,
})
if err != nil {
	return err
}
// Upstream uses cfg.PayloadCap when filling buffers and closes sourceInput.
return network.Send(ctx, conn, sourcePool, sourceInput, cfg)
```

On the target, run downstream consumers concurrently:

```go
cfg, err := network.ClientHandshake(ctx, conn, network.Config{
	PayloadCap: targetPool.PayloadCap(),
	Compression: network.CompressionNone,
	Format: network.FormatRow,
})
if err != nil {
	return err
}
return network.Receive(ctx, conn, targetPool, targetOutput, cfg)
// The coordinator joins all receivers before closing targetOutput.
```

`Header.Encode`, `DecodeHeader`, `ReadHeader`, and `WriteFrame` support framing
through `io.Reader`/`io.Writer`, including partial reads and writes.

## Tests

Run `go test -race -cover -count=1 -timeout 60s ./...`. The network suite covers
exact wire bytes, partial I/O, handshake negotiation/rejection, real loopback TCP
transfers with none/Snappy payloads, independent pools, shared-queue concurrent
workers, malformed/truncated input, missing END, buffer cleanup, cancellation,
deadlines, and queue/pool backpressure.
