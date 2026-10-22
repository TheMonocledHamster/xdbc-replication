package network

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"time"

	"github.com/TheMonocledHamster/xdbc-replication/internal/pool"
	"github.com/TheMonocledHamster/xdbc-replication/internal/queue"
)

// Config describes one connection's transfer. PayloadCap excludes the pool header.
// Compression and Format are opaque wire IDs for implementations supplied by
// the pipeline. Both peers must explicitly configure the same IDs; the network
// stage neither applies codecs nor converts formats.
type Config struct {
	PayloadCap  int
	Compression Compression
	Format      Format
}

func (c Config) validate() error {
	if c.PayloadCap < 1 || uint64(c.PayloadCap) > math.MaxUint32-pool.HeaderSize {
		return fmt.Errorf("invalid payload capacity %d", c.PayloadCap)
	}
	return nil
}

// ClientHandshake requests a configuration and waits for the server to be ready.
// The returned configuration must be used for Receive on this connection.
func ClientHandshake(ctx context.Context, conn net.Conn, requested Config) (accepted Config, err error) {
	if err = requested.validate(); err != nil {
		return accepted, err
	}
	finish, err := interruptOnCancel(ctx, conn)
	if err != nil {
		return accepted, err
	}
	defer func() { finish(); err = contextError(ctx, err) }()
	if err = writeConfig(conn, MessageHello, requested); err != nil {
		return accepted, err
	}
	if accepted, err = readConfig(conn, MessageHelloAck); err != nil {
		return accepted, err
	}
	if accepted.PayloadCap > requested.PayloadCap || accepted.Compression != requested.Compression || accepted.Format != requested.Format {
		return Config{}, fmt.Errorf("server acknowledged an incompatible configuration")
	}
	return accepted, nil
}

// ServerHandshake accepts the smaller of the peers' capacities and acknowledges
// readiness. Compression and format must match; unsupported requests get ERROR.
func ServerHandshake(ctx context.Context, conn net.Conn, supported Config) (accepted Config, err error) {
	if err = supported.validate(); err != nil {
		return accepted, err
	}
	finish, err := interruptOnCancel(ctx, conn)
	if err != nil {
		return accepted, err
	}
	defer func() { finish(); err = contextError(ctx, err) }()
	requested, err := readConfig(conn, MessageHello)
	if err == nil && (requested.Compression != supported.Compression || requested.Format != supported.Format) {
		err = fmt.Errorf("requested compression or format is not supported")
	}
	if err != nil {
		// A peer awaiting HELLO_ACK can consume this bounded diagnostic.
		payload := []byte(err.Error())
		if len(payload) > maxErrorSize {
			payload = payload[:maxErrorSize]
		}
		_ = WriteFrame(conn, Header{Type: MessageError, PayloadSize: uint32(len(payload))}, payload)
		return Config{}, err
	}
	accepted = requested
	accepted.PayloadCap = min(requested.PayloadCap, supported.PayloadCap)
	err = writeConfig(conn, MessageHelloAck, accepted)
	return accepted, err
}

func writeConfig(w io.Writer, typ MessageType, c Config) error {
	var b [configSize]byte
	binary.BigEndian.PutUint32(b[:4], uint32(c.PayloadCap))
	b[4], b[5] = byte(c.Compression), byte(c.Format)
	return WriteFrame(w, Header{Type: typ, PayloadSize: configSize}, b[:])
}

func readConfig(r io.Reader, typ MessageType) (Config, error) {
	h, err := ReadHeader(r)
	if err != nil {
		return Config{}, err
	}
	if err := h.Validate(); err != nil {
		return Config{}, err
	}
	if h.Type == MessageError {
		return Config{}, readRemoteError(r, h)
	}
	if h.Type != typ || h.PayloadSize != configSize {
		return Config{}, fmt.Errorf("expected message %d with %d configuration bytes", typ, configSize)
	}
	var b [configSize]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return Config{}, err
	}
	c := Config{PayloadCap: int(binary.BigEndian.Uint32(b[:4])), Compression: Compression(b[4]), Format: Format(b[5])}
	return c, c.validate()
}

func readRemoteError(r io.Reader, h Header) error {
	if h.PayloadSize > maxErrorSize {
		return fmt.Errorf("remote ERROR exceeds %d bytes", maxErrorSize)
	}
	b := make([]byte, int(h.PayloadSize))
	if _, err := io.ReadFull(r, b); err != nil {
		return err
	}
	return fmt.Errorf("remote error: %s", b)
}

// Send runs one TCP sender worker. It takes ownership of each popped buffer and
// releases it after writing, including on failure. A closed, drained input queue
// produces END. Each parallel worker must have its own connection.
func Send(ctx context.Context, conn net.Conn, p *pool.Pool, input queue.Queue, cfg Config) (err error) {
	if err = checkPool(p, cfg); err != nil {
		return err
	}
	finish, err := interruptOnCancel(ctx, conn)
	if err != nil {
		return err
	}
	defer func() { finish(); err = contextError(ctx, err) }()
	for {
		id, popErr := input.Pop(ctx)
		if errors.Is(popErr, io.EOF) {
			if err := ctx.Err(); err != nil {
				return err
			}
			return WriteFrame(conn, Header{Type: MessageEnd}, nil)
		}
		if popErr != nil {
			return popErr
		}
		sendErr := sendBuffer(conn, p, id, cfg)
		// Cleanup must remain possible after cancellation.
		releaseErr := p.Release(id)
		if err := errors.Join(sendErr, releaseErr); err != nil {
			return err
		}
	}
}

func sendBuffer(w io.Writer, p *pool.Pool, id int, cfg Config) error {
	b, err := p.Buffer(id)
	if err != nil {
		return err
	}
	bh := pool.DecodeHeader(b)
	if uint64(bh.PayloadLen) > uint64(cfg.PayloadCap) || !acceptsCompression(cfg.Compression, Compression(bh.Compression)) {
		return fmt.Errorf("buffer %d does not match negotiated configuration", id)
	}
	h := Header{Type: MessageData, Compression: Compression(bh.Compression), Format: cfg.Format,
		PayloadSize: pool.HeaderSize + bh.PayloadLen, RecordCount: bh.RecordCount}
	return WriteFrame(w, h, b[:int(h.PayloadSize)])
}

// Receive runs one TCP receiver worker, reading directly into acquired slab
// buffers. Ownership transfers to output only after a complete validated DATA
// frame. END returns success; EOF before END is a failed transfer. The coordinator
// must close output after ALL receivers finish, and close the connection on error.
func Receive(ctx context.Context, conn net.Conn, p *pool.Pool, output queue.Queue, cfg Config) (err error) {
	if err = checkPool(p, cfg); err != nil {
		return err
	}
	finish, err := interruptOnCancel(ctx, conn)
	if err != nil {
		return err
	}
	defer func() { finish(); err = contextError(ctx, err) }()
	for {
		h, err := ReadHeader(conn)
		if errors.Is(err, io.EOF) {
			return io.ErrUnexpectedEOF
		}
		if err != nil {
			return err
		}
		if err := h.Validate(); err != nil {
			return err
		}
		switch h.Type {
		case MessageEnd:
			return nil
		case MessageError:
			return readRemoteError(conn, h)
		case MessageData:
			if h.PayloadSize < pool.HeaderSize || uint64(h.PayloadSize) > uint64(cfg.PayloadCap)+pool.HeaderSize || !acceptsCompression(cfg.Compression, h.Compression) || h.Format != cfg.Format {
				return fmt.Errorf("DATA frame violates negotiated size / encoding constraints")
			}
			if err := receiveBuffer(ctx, conn, p, output, h); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unexpected message %d during transfer", h.Type)
		}
	}
}

func receiveBuffer(ctx context.Context, r io.Reader, p *pool.Pool, output queue.Queue, h Header) (err error) {
	id, err := p.Acquire(ctx)
	if err != nil {
		return err
	}
	forwarded := false
	defer func() {
		if !forwarded {
			err = errors.Join(err, p.Release(id))
		}
	}()
	b, err := p.Buffer(id)
	if err != nil {
		return err
	}
	if _, err = io.ReadFull(r, b[:int(h.PayloadSize)]); err != nil {
		return err
	}
	bh := pool.DecodeHeader(b)
	if bh.PayloadLen != h.PayloadSize-pool.HeaderSize || bh.RecordCount != h.RecordCount || bh.Compression != uint8(h.Compression) {
		return fmt.Errorf("DATA metadata does not match pool header")
	}
	if err = output.Push(ctx, id); err != nil {
		return err
	}
	forwarded = true
	return nil
}

func checkPool(p *pool.Pool, cfg Config) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	if p == nil || cfg.PayloadCap > p.PayloadCap() {
		return fmt.Errorf("negotiated payload capacity exceeds pool capacity")
	}
	return nil
}

// The worker exclusively owns connection I/O and deadlines until it returns.
// Waiting for an in-flight callback before resetting avoids a late cancellation
// deadline affecting a subsequent operation on the connection.
func interruptOnCancel(ctx context.Context, conn net.Conn) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return nil, err
	}
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = conn.SetDeadline(time.Now())
		close(done)
	})
	return func() {
		if !stop() {
			<-done
		}
		_ = conn.SetDeadline(time.Time{})
	}, nil
}

func contextError(ctx context.Context, err error) error {
	if err != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// A Snappy session permits uncompressed fallback for incompressible batches.
func acceptsCompression(negotiated, actual Compression) bool {
	return actual == negotiated || negotiated == CompressionSnappy && actual == CompressionNone
}
