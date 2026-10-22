# TCP protocol V1

`internal/network` transports encoded batches over TCP. Compression and row conversion happen in separate stages. The runtime uses one connection per transfer; the network package also supports separate connections sharing queues.

## Frame header

Each frame begins with this 16-byte header. Multi-byte integers use big endian.

| Offset | Bytes | Field |
|---|---|---|
| 0 | 4 | Magic: `XDBC` |
| 4 | 1 | Wire version: `1` |
| 5 | 1 | Message type |
| 6 | 1 | Actual compression ID |
| 7 | 1 | Format ID |
| 8 | 4 | `PayloadSize` |
| 12 | 4 | `RecordCount` |

| Message | ID | Payload |
|---|---|---|
| HELLO | 1 | Requested settings |
| HELLO_ACK | 2 | Accepted settings |
| DATA | 3 | Pool header and used payload bytes |
| END | 4 | Empty |
| ERROR | 5 | Diagnostic, at most 1024 bytes |

Control frames use compression `0`, format `0`, and record count `0`.

## Handshake

The client sends HELLO. The server responds with HELLO_ACK or rejects the request with ERROR. Both configuration payloads contain six bytes:

| Offset | Bytes | Setting |
|---|---|---|
| 0 | 4 | Maximum batch payload capacity, big endian |
| 4 | 1 | Compression: `0` none, `1` Snappy |
| 5 | 1 | Format: `0` fixed-width row |

Compression and format must match. The accepted capacity is the smaller peer capacity, excluding the 32-byte pool header. It must be positive and leave room for that header in the frame's uint32 payload length.

The handshake does not send credentials, table names, or schemas. Both processes load their [shared configuration](configuration.md) separately. The runtime allocates its session buffers using the accepted capacity.

## Data and completion

A DATA payload contains a [32-byte pool header](pool-v1.md#batch-header) followed by exactly `PayloadLen` bytes. The pool header retains its little-endian encoding inside the big-endian frame envelope.

The receiver checks capacity before reading the payload and validates:

- Frame `PayloadSize` equals `32 + PayloadLen`.
- Frame record count and compression match the pool header.
- Format and compression are allowed by the negotiated settings.

A Snappy session permits both Snappy and uncompressed fallback batches. An uncompressed session rejects Snappy. The actual encoding is recorded in both headers; bytes are transported unchanged.

A sender emits END after its input closes and drains, unless cancelled. Receiving END completes reception, not database writing. EOF before END, truncated frames, invalid metadata, and unexpected message types fail the transfer. ERROR reports a peer diagnostic; it is not a retry instruction.

## Go API and ownership

`ClientHandshake` and `ServerHandshake` return the accepted `network.Config`. Pass it to `Send` or `Receive`; local pools must accommodate its `PayloadCap`.

- `Send` pops IDs, writes only used bytes, and releases each ID on success or failure.
- `Receive` acquires a buffer and pushes its ID only after validating the complete frame. It releases the buffer if reading, validation, or forwarding fails.
- The coordinator closes queues, manages connections, waits for workers, and drains remaining IDs. Network workers do not close shared queues.

Cancellation and deadlines interrupt blocked transport operations. Close connections after protocol or I/O errors because framing may be incomplete. TCP preserves per-connection order; the current runtime does not reorder batches across multiple connections.

`Header.Encode`, `DecodeHeader`, `ReadHeader`, and `WriteFrame` provide lower-level framing helpers. Encoding/decoding checks the envelope; `Header.Validate` applies built-in message rules. The generic helpers preserve encoding IDs, but choosing a new ID does not implement a codec or format. Envelope changes require a new wire version.

The NULL bitmap was added without changing `FormatRow` or the wire version. Older bitmap-free peers are incompatible, and the handshake cannot detect that layout difference. Upgrade both peers together.

Run `make test` for framing, handshake, transport, fallback, cancellation, and backpressure tests.
