package network

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/TheMonocledHamster/xdbc-replication/internal/pool"
	"github.com/TheMonocledHamster/xdbc-replication/internal/queue"
	"github.com/golang/snappy"
)

func testPool(t *testing.T, count, capacity int) *pool.Pool {
	t.Helper()
	p, err := pool.New(pool.Config{NumBuffers: count, PayloadCap: capacity})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func testQueue(t *testing.T, capacity int) queue.Queue {
	t.Helper()
	q, err := queue.New(capacity)
	if err != nil {
		t.Fatal(err)
	}
	return *q
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func pipe(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	a, b := net.Pipe()
	t.Cleanup(func() { a.Close(); b.Close() })
	return a, b
}

func checkReleased(t *testing.T, p *pool.Pool, count int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ids := make([]int, 0, count)
	seen := make(map[int]bool)
	for i := 0; i < count; i++ {
		id, err := p.Acquire(ctx)
		if err != nil {
			t.Fatalf("buffer leaked: %v", err)
		}
		if seen[id] {
			t.Fatalf("buffer %d was released twice", id)
		}
		seen[id] = true
		ids = append(ids, id)
	}
	for _, id := range ids {
		if err := p.Release(id); err != nil {
			t.Fatal(err)
		}
	}
}

func putBuffer(t *testing.T, ctx context.Context, p *pool.Pool, q queue.Queue, seq uint64, payload []byte, comp Compression) pool.BufferHeader {
	t.Helper()
	id, err := p.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.Buffer(id)
	if err != nil {
		t.Fatal(err)
	}
	h := pool.BufferHeader{Seq: seq, NextRecord: seq * 10, RecordCount: 2, PayloadLen: uint32(len(payload)),
		UncompressedLen: 37, Compression: uint8(comp), Protocol: 1, Unused1: 0xa5, Unused2: 0x5a}
	h.Encode(b)
	copy(b[pool.HeaderSize:], payload)
	if err := q.Push(ctx, id); err != nil {
		t.Fatal(err)
	}
	return h
}

func TestHandshakeNegotiation(t *testing.T) {
	for _, comp := range []Compression{CompressionNone, CompressionSnappy} {
		t.Run(string(rune('0'+comp)), func(t *testing.T) {
			ctx := testContext(t)
			a, b := pipe(t)
			result := make(chan error, 1)
			go func() {
				cfg, err := ServerHandshake(ctx, a, Config{PayloadCap: 64, Compression: comp})
				if err == nil && cfg != (Config{PayloadCap: 64, Compression: comp}) {
					err = errors.New("wrong server configuration")
				}
				result <- err
			}()
			cfg, err := ClientHandshake(ctx, b, Config{PayloadCap: 128, Compression: comp})
			if err != nil || cfg != (Config{PayloadCap: 64, Compression: comp}) {
				t.Fatalf("client: %+v, %v", cfg, err)
			}
			if err := <-result; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestHandshakeRejectsIncompatibleCompression(t *testing.T) {
	ctx := testContext(t)
	a, b := pipe(t)
	result := make(chan error, 1)
	go func() { _, err := ServerHandshake(ctx, a, Config{PayloadCap: 64}); result <- err }()
	_, err := ClientHandshake(ctx, b, Config{PayloadCap: 64, Compression: CompressionSnappy})
	if err == nil || !strings.Contains(err.Error(), "remote error") {
		t.Fatalf("client: %v", err)
	}
	if err := <-result; err == nil {
		t.Fatal("server accepted incompatible compression")
	}
}

func TestHandshakeRejectsInvalidAcknowledgment(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  MessageType
		cfg  Config
	}{
		{"larger capacity", MessageHelloAck, Config{PayloadCap: 128}},
		{"wrong compression", MessageHelloAck, Config{PayloadCap: 64, Compression: CompressionSnappy}},
		{"zero capacity", MessageHelloAck, Config{}},
		{"unrequested encoding", MessageHelloAck, Config{PayloadCap: 64, Compression: 99}},
		{"unrequested format", MessageHelloAck, Config{PayloadCap: 64, Format: 99}},
		{"wrong message", MessageHello, Config{PayloadCap: 64}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testContext(t)
			a, b := pipe(t)
			result := make(chan error, 1)
			go func() {
				_, err := readConfig(a, MessageHello)
				if err == nil {
					err = writeConfig(a, tc.typ, tc.cfg)
				}
				result <- err
			}()
			if _, err := ClientHandshake(ctx, b, Config{PayloadCap: 64}); err == nil {
				t.Fatal("accepted invalid acknowledgment")
			}
			// A rejected header may leave the reply's payload unread.
			b.Close()
			if err := <-result; err != nil && !errors.Is(err, io.ErrClosedPipe) {
				t.Fatal(err)
			}
		})
	}
}

func TestInvalidConfiguration(t *testing.T) {
	for _, cfg := range []Config{
		{}, {PayloadCap: -1},
	} {
		if err := cfg.validate(); err == nil {
			t.Fatalf("accepted %+v", cfg)
		}
	}
	p := testPool(t, 1, 64)
	if err := checkPool(p, Config{PayloadCap: 65}); err == nil {
		t.Fatal("accepted capacity exceeding pool")
	}
	if err := checkPool(nil, Config{PayloadCap: 64}); err == nil {
		t.Fatal("accepted nil pool")
	}
}

func TestParallelWorkersShareQueues(t *testing.T) {
	ctx := testContext(t)
	const workers, frames = 3, 6
	source, target := testPool(t, frames, 64), testPool(t, 2, 64)
	input, output := testQueue(t, frames), testQueue(t, 1)
	for i := 0; i < frames; i++ {
		putBuffer(t, ctx, source, input, uint64(i), []byte{byte(i)}, CompressionNone)
	}
	close(input)
	result := make(chan error, workers*2)
	for i := 0; i < workers; i++ {
		a, b := pipe(t)
		go func() { result <- Send(ctx, a, source, input, Config{PayloadCap: 64}) }()
		go func() { result <- Receive(ctx, b, target, output, Config{PayloadCap: 64}) }()
	}
	seen := make(map[uint64]bool)
	for i := 0; i < frames; i++ {
		id, err := output.Pop(ctx)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := target.Buffer(id)
		h := pool.DecodeHeader(b)
		if h.Seq >= frames || seen[h.Seq] || b[pool.HeaderSize] != byte(h.Seq) {
			t.Fatalf("duplicate or corrupted frame: %+v", h)
		}
		seen[h.Seq] = true
		if err := target.Release(id); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < workers*2; i++ {
		if err := <-result; err != nil {
			t.Fatal(err)
		}
	}
	// Workers leave the shared output open for its coordinator.
	close(output)
	if _, err := output.Pop(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("coordinator close: %v", err)
	}
	checkReleased(t, source, frames)
	checkReleased(t, target, 2)
}

func TestDeadlineUnblocksReceive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, b := pipe(t)
	err := Receive(ctx, b, testPool(t, 1, 64), testQueue(t, 1), Config{PayloadCap: 64})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline: %v", err)
	}
}

// Uses real TCP and independent pools. Snappy bytes are transported unchanged;
// compression/decompression remain separate stages, as specified by the paper.
func TestTCPTransfer(t *testing.T) {
	for _, comp := range []Compression{CompressionNone, CompressionSnappy} {
		t.Run(string(rune('0'+comp)), func(t *testing.T) {
			ctx := testContext(t)
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { listener.Close() })
			source := testPool(t, 3, 128)
			target := testPool(t, 1, 64)
			input, output := testQueue(t, 3), testQueue(t, 1)
			payload := []byte("rows crossing TCP with reusable memory")
			if comp == CompressionSnappy {
				payload = snappy.Encode(nil, payload)
			}
			want := make([]pool.BufferHeader, 3)
			for i := range want {
				want[i] = putBuffer(t, ctx, source, input, uint64(i), payload, comp)
			}
			close(input)
			server := make(chan error, 1)
			go func() {
				conn, err := listener.Accept()
				if err != nil {
					server <- err
					return
				}
				defer conn.Close()
				cfg, err := ServerHandshake(ctx, conn, Config{PayloadCap: 128, Compression: comp})
				if err == nil {
					err = Send(ctx, conn, source, input, cfg)
				}
				server <- err
			}()
			conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { conn.Close() })
			cfg, err := ClientHandshake(ctx, conn, Config{PayloadCap: 64, Compression: comp})
			if err != nil {
				t.Fatal(err)
			}
			receiver := make(chan error, 1)
			go func() { receiver <- Receive(ctx, conn, target, output, cfg) }()
			for _, h := range want {
				id, err := output.Pop(ctx)
				if err != nil {
					t.Fatal(err)
				}
				b, _ := target.Buffer(id)
				if got := pool.DecodeHeader(b); got != h {
					t.Fatalf("metadata: %+v, want %+v", got, h)
				}
				if !bytes.Equal(b[pool.HeaderSize:pool.HeaderSize+len(payload)], payload) {
					t.Fatal("payload changed")
				}
				if err := target.Release(id); err != nil {
					t.Fatal(err)
				}
			}
			if err := <-receiver; err != nil {
				t.Fatal(err)
			}
			if err := <-server; err != nil {
				t.Fatal(err)
			}
			checkReleased(t, source, 3)
			checkReleased(t, target, 1)
		})
	}
}

func TestReceiveRejectsBadFrames(t *testing.T) {
	valid := make([]byte, pool.HeaderSize+3)
	pool.BufferHeader{PayloadLen: 3, RecordCount: 1}.Encode(valid)
	copy(valid[pool.HeaderSize:], "abc")
	for _, tc := range []struct {
		name    string
		h       Header
		payload []byte
	}{
		{"oversized", Header{Type: MessageData, PayloadSize: pool.HeaderSize + 65}, nil},
		{"missing pool header", Header{Type: MessageData, PayloadSize: 3}, nil},
		{"wrong compression", Header{Type: MessageData, Compression: CompressionSnappy, PayloadSize: uint32(len(valid))}, nil},
		{"wrong message", Header{Type: MessageHello}, nil},
		{"unhandled message", Header{Type: 99}, nil},
		{"unnegotiated format", Header{Type: MessageData, PayloadSize: uint32(len(valid)), Format: 99}, nil},
		{"control data metadata", Header{Type: MessageEnd, Format: 99}, nil},
		{"record mismatch", Header{Type: MessageData, PayloadSize: uint32(len(valid)), RecordCount: 2}, valid},
		{"payload length mismatch", Header{Type: MessageData, PayloadSize: pool.HeaderSize, RecordCount: 1}, valid[:pool.HeaderSize]},
		{"compression mismatch", Header{Type: MessageData, Compression: CompressionSnappy, PayloadSize: uint32(len(valid)), RecordCount: 1}, valid},
		{"truncated payload", Header{Type: MessageData, PayloadSize: uint32(len(valid)), RecordCount: 1}, valid[:len(valid)-1]},
		{"remote error", Header{Type: MessageError, PayloadSize: 4}, []byte("oops")},
		{"oversized error", Header{Type: MessageError, PayloadSize: maxErrorSize + 1}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testContext(t)
			a, b := pipe(t)
			p, q := testPool(t, 1, 64), testQueue(t, 1)
			cfg := Config{PayloadCap: 64}
			if tc.name == "compression mismatch" {
				cfg.Compression = CompressionSnappy
			}
			written := make(chan struct{})
			go func() {
				defer close(written)
				defer a.Close()
				var raw [HeaderSize]byte
				_ = tc.h.Encode(raw[:])
				_, _ = a.Write(raw[:])
				if len(tc.payload) > 0 {
					_, _ = a.Write(tc.payload)
				}
			}()
			err := Receive(ctx, b, p, q, cfg)
			b.Close()
			<-written
			if err == nil {
				t.Fatal("accepted bad frame")
			}
			if tc.name == "truncated payload" && !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("truncation: %v", err)
			}
			if tc.name == "remote error" && !strings.Contains(err.Error(), "oops") {
				t.Fatalf("remote error: %v", err)
			}
			if len(q) != 0 {
				t.Fatal("forwarded invalid buffer")
			}
			checkReleased(t, p, 1)
		})
	}
}

// The pipeline can introduce new codec and layout IDs without changing header
// encoding or the transport. Both peers must explicitly opt into those IDs.
func TestNegotiatedExtensionTransfer(t *testing.T) {
	ctx := testContext(t)
	a, b := pipe(t)
	cfg := Config{PayloadCap: 64, Compression: 42, Format: 77}
	source, target := testPool(t, 1, 64), testPool(t, 1, 64)
	input, output := testQueue(t, 1), testQueue(t, 1)
	payload := []byte("opaque custom codec bytes")
	want := putBuffer(t, ctx, source, input, 9, payload, cfg.Compression)
	close(input)
	server := make(chan error, 1)
	go func() {
		accepted, err := ServerHandshake(ctx, a, cfg)
		if err == nil {
			err = Send(ctx, a, source, input, accepted)
		}
		server <- err
	}()
	accepted, err := ClientHandshake(ctx, b, cfg)
	if err != nil || accepted != cfg {
		t.Fatalf("extension handshake: %+v, %v", accepted, err)
	}
	if err := Receive(ctx, b, target, output, accepted); err != nil {
		t.Fatal(err)
	}
	id, err := output.Pop(ctx)
	if err != nil {
		t.Fatal(err)
	}
	buf, err := target.Buffer(id)
	if err != nil {
		t.Fatal(err)
	}
	if got := pool.DecodeHeader(buf); got != want {
		t.Fatalf("extension metadata: %+v, want %+v", got, want)
	}
	if !bytes.Equal(buf[pool.HeaderSize:pool.HeaderSize+len(payload)], payload) {
		t.Fatal("extension payload changed")
	}
	if err := target.Release(id); err != nil {
		t.Fatal(err)
	}
	if err := <-server; err != nil {
		t.Fatal(err)
	}
	checkReleased(t, source, 1)
	checkReleased(t, target, 1)
}

func TestReceiveRequiresEnd(t *testing.T) {
	ctx := testContext(t)
	a, b := pipe(t)
	p, q := testPool(t, 1, 64), testQueue(t, 1)
	go func() {
		defer a.Close()
		data := make([]byte, pool.HeaderSize)
		_ = WriteFrame(a, Header{Type: MessageData, PayloadSize: pool.HeaderSize}, data)
	}()
	err := Receive(ctx, b, p, q, Config{PayloadCap: 64})
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("EOF without END: %v", err)
	}
	id, err := q.Pop(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Release(id); err != nil {
		t.Fatal(err)
	}
	checkReleased(t, p, 1)
}

func TestCancellationUnblocksIO(t *testing.T) {
	for _, mode := range []string{"receive header", "receive payload", "send socket", "send queue", "handshake"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(testContext(t))
			defer cancel()
			a, b := pipe(t)
			p, q := testPool(t, 1, 64), testQueue(t, 1)
			cfg := Config{PayloadCap: 64}
			result := make(chan error, 1)
			switch mode {
			case "send socket", "send queue":
				if mode == "send socket" {
					putBuffer(t, ctx, p, q, 1, []byte("abc"), CompressionNone)
				}
				go func() { result <- Send(ctx, b, p, q, cfg) }()
				if mode == "send socket" {
					if _, err := ReadHeader(a); err != nil {
						t.Fatal(err)
					}
				}
			case "handshake":
				go func() { _, err := ClientHandshake(ctx, b, cfg); result <- err }()
				if _, err := readConfig(a, MessageHello); err != nil {
					t.Fatal(err)
				}
			default:
				go func() { result <- Receive(ctx, b, p, q, cfg) }()
				if mode == "receive payload" {
					var raw [HeaderSize]byte
					_ = (Header{Type: MessageData, PayloadSize: pool.HeaderSize + 3}).Encode(raw[:])
					if _, err := a.Write(raw[:]); err != nil {
						t.Fatal(err)
					}
				}
			}
			cancel()
			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancel: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("cancellation did not unblock worker")
			}
			checkReleased(t, p, 1)
		})
	}
}

func TestSendRejectsInvalidBufferAndReleases(t *testing.T) {
	for _, mode := range []string{"length", "compression", "write failure"} {
		t.Run(mode, func(t *testing.T) {
			ctx := testContext(t)
			a, b := pipe(t)
			p, q := testPool(t, 1, 64), testQueue(t, 1)
			putBuffer(t, ctx, p, q, 1, []byte("abc"), CompressionNone)
			buf, _ := p.Buffer(0)
			h := pool.DecodeHeader(buf)
			switch mode {
			case "length":
				h.PayloadLen = 65
			case "compression":
				h.Compression = uint8(CompressionSnappy)
			case "write failure":
				// Close only after Send has taken ownership and written its header.
				go func() { _, _ = ReadHeader(a); a.Close() }()
			}
			h.Encode(buf)
			if err := Send(ctx, b, p, q, Config{PayloadCap: 64}); err == nil {
				t.Fatal("accepted bad buffer/write")
			}
			checkReleased(t, p, 1)
		})
	}
}

func TestReceiveQueueBackpressure(t *testing.T) {
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	a, b := pipe(t)
	p, q := testPool(t, 1, 64), testQueue(t, 1)
	q <- -1 // Occupy the downstream queue independently of the receiver's pool.
	result := make(chan error, 1)
	go func() { result <- Receive(ctx, b, p, q, Config{PayloadCap: 64}) }()
	data := make([]byte, pool.HeaderSize+3)
	pool.BufferHeader{PayloadLen: 3}.Encode(data)
	if err := WriteFrame(a, Header{Type: MessageData, PayloadSize: uint32(len(data))}, data); err != nil {
		t.Fatal(err)
	}
	// The completed read owns the only buffer while it waits for queue capacity.
	blocked, stop := context.WithTimeout(ctx, 20*time.Millisecond)
	defer stop()
	if _, err := p.Acquire(blocked); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("receiver did not hold buffer under backpressure: %v", err)
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel blocked push: %v", err)
	}
	if id := <-q; id != -1 {
		t.Fatalf("overwrote occupied queue: %d", id)
	}
	checkReleased(t, p, 1)
}

func TestReceivePoolBackpressure(t *testing.T) {
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	a, b := pipe(t)
	p, q := testPool(t, 1, 64), testQueue(t, 1)
	id, err := p.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- Receive(ctx, b, p, q, Config{PayloadCap: 64}) }()
	var raw [HeaderSize]byte
	_ = (Header{Type: MessageData, PayloadSize: pool.HeaderSize}).Encode(raw[:])
	if _, err := a.Write(raw[:]); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel blocked acquire: %v", err)
	}
	if err := p.Release(id); err != nil {
		t.Fatal(err)
	}
	checkReleased(t, p, 1)
}
