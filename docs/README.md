# Developer guide

Start with the [project README](../README.md) to run the application.

| Guide | Covers |
|---|---|
| [Architecture](architecture.md) | Services, HTTP API, transfer lifecycle, and errors |
| [Configuration](configuration.md) | Shared schema, runtime settings, and buffer sizing |
| [Source adapter](../internal/postgres/README.md) | Reading PostgreSQL rows into buffers |
| [Sink adapter](target-v1.md) | Writing buffers through PostgreSQL COPY |
| [Row format](row-v1.md) | Field encodings, NULLs, and payload validation |
| [Compression](compression-v1.md) | Snappy stages and uncompressed fallback |
| [Network protocol](network-v1.md) | Handshake, frames, and completion rules |
| [Buffer pool](pool-v1.md) | Allocation, ownership, and batch headers |
| [Queues](queue-v1.md) | Backpressure, closure, and cancellation |
| [IoT fixture](../testdata/iot/README.md) | Data origin and verification |

The `*-v1.md` filenames refer to component contracts. Both PostgreSQL adapters support the full row scalar type set, including NULL. The wire handshake still requires both peers to be configured with the same ordered schema.

The original [proposal](references/InitialProposal.pdf) and [paper](references/FastDataTransfer.pdf) are reference material. The guides above describe the current Go implementation.
