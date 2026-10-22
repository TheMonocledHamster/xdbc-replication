package compression

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/TheMonocledHamster/xdbc-replication/internal/pool"
	"github.com/TheMonocledHamster/xdbc-replication/internal/protocol"
	"github.com/TheMonocledHamster/xdbc-replication/internal/queue"
	"golang.org/x/sync/errgroup"
)

// Starts the Compress/Decompress stage with `workers` worker threads
// Switch `isDecompress` to true to create a Decompress stage (versus Compress)
func StartCompressStage(ctx context.Context, workers int,
	input queue.Queue, output queue.Queue, p *pool.Pool,
	isDecompress bool, codec Codec) error {

	// Early checks before starting workers
	if workers <= 0 {
		return errors.New("(de-)compression: workers must be > 0")
	}
	if p == nil || codec == nil {
		return errors.New("(de-)compression: pool and codec are required")
	}

	eg, ctx := errgroup.WithContext(ctx)
	for i := 1; i <= workers; i++ {
		eg.Go(func() error {
			if err := compressStage(ctx, input, output, p, isDecompress, codec); err != nil {
				return fmt.Errorf("StartCompressStage worker %d: %w", i, err)
			}
			return nil
		})
	}

	return eg.Wait()
}

// Wrapper for consumeBuffer that waits for input buffers and consumes them.
// Switch `isDecompress` to true to create a Decompress stage (versus Compress)
func compressStage(ctx context.Context, input queue.Queue,
	output queue.Queue, p *pool.Pool, isDecompress bool,
	codec Codec) error {

	for {
		// Pick new buffer id from queue
		id, err := input.Pop(ctx)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		// Handle that buffer's contents
		if err := consumeBuffer(ctx, id, output, p, isDecompress, codec); err != nil {
			return err
		}
	}
}

func consumeBuffer(ctx context.Context, id int, output queue.Queue, p *pool.Pool, isDecompress bool, codec Codec) error {
	errLabel := "compress"
	if isDecompress {
		errLabel = "decompress"
	}

	// Make sure the context has not yet expired/cancelled/errored
	select {
	case <-ctx.Done():
		p.Release(id)
		return ctx.Err()
	default:
	}

	// Get the buffer contents and do length validation check
	b, err := p.Buffer(id)
	if err != nil {
		p.Release(id)
		return fmt.Errorf("%s: access buffer %d: %w", errLabel, id, err)
	}
	if len(b) < pool.HeaderSize {
		p.Release(id)
		return fmt.Errorf("%s: buffer is shorter than its header", errLabel)
	}

	// Get the header and run validation checks
	h := pool.DecodeHeader(b)
	if uint64(h.PayloadLen) > uint64(p.PayloadCap()) {
		p.Release(id)
		return fmt.Errorf("%s: buffer %d payload length %d exceeds capacity %d", errLabel, id, h.PayloadLen, p.PayloadCap())
	}

	if pool.HeaderSize+int(h.PayloadLen) > len(b) {
		p.Release(id)
		return fmt.Errorf("%s: buffer %d payload length %d exceeds actual buffer size", errLabel, id, h.PayloadLen)
	}

	// If the input is uncompressed, there's no work for us to do (hooray!)
	if h.Compression == uint8(protocol.CompressionNone) {
		err := output.Push(ctx, id)
		if err != nil {
			p.Release(id)
			return err
		}
		return nil
	}

	// Check for Codec mismatches
	if h.Compression != uint8(codec.ID()) {
		p.Release(id)
		return fmt.Errorf("%s: buffer header declared compression %d, but codec is compression %d", errLabel, h.Compression, codec.ID())
	}
	compErr := doCompression(ctx, id, output, p, codec, isDecompress)

	return compErr
}

func doCompression(ctx context.Context, inputID int,
	output queue.Queue, p *pool.Pool, codec Codec, isDecompress bool) error {
	srcBuf, err := p.Buffer(inputID)
	if err != nil {
		p.Release(inputID)
		return err
	}

	// Fetch header and start dstH with same info
	// Items like uncompressedLen, Protocol, etc. should be the same
	srcH := pool.DecodeHeader(srcBuf)

	// Quick check: if we cannot decompress the input buffer into one output buffer, throw an error
	if isDecompress && srcH.UncompressedLen > uint32(p.PayloadCap()) {
		p.Release(inputID)
		return errors.New("decompress: Buffer too small to hold uncompressed contents")
	}

	// All pre-flight checks cleared, can set up output buffer
	dst, err := p.Acquire(ctx)
	if err != nil {
		// Calls that return an error always yield dst=0, no need to release dst
		p.Release(inputID)
		return err
	}

	dstBuf, err := p.Buffer(dst)
	if err != nil {
		p.Release(inputID)
		p.Release(dst)
		return err
	}

	copy(dstBuf[:pool.HeaderSize], srcBuf[:pool.HeaderSize])

	srcEnd := pool.HeaderSize + int(srcH.PayloadLen)

	dstH := pool.DecodeHeader(dstBuf)
	if isDecompress {
		// TODO: Ensure Read stage chunks up input in a way uncompressed length < p.cfg.PayloadCap
		// For now, assuming Read chunked things so decompressed length of inputs fits in one buffer
		decompressedBytes, err := codec.Decompress(dstBuf[pool.HeaderSize:], srcBuf[pool.HeaderSize:srcEnd])
		if err != nil {
			p.Release(inputID)
			p.Release(dst)
			return err
		}

		// See if the actual decompressed size matches expectations
		if uint32(decompressedBytes) != srcH.UncompressedLen {
			p.Release(inputID)
			p.Release(dst)
			return fmt.Errorf("decompress: got %d bytes, header says %d", decompressedBytes, srcH.UncompressedLen)
		}

		dstH.Compression = uint8(protocol.CompressionNone)
		dstH.PayloadLen = uint32(decompressedBytes)
	} else {
		compressedBytes, err := codec.Compress(dstBuf[pool.HeaderSize:], srcBuf[pool.HeaderSize:srcEnd])
		// Some random error, stop now
		if err != nil && !errors.Is(err, io.ErrShortBuffer) {
			p.Release(inputID)
			p.Release(dst)
			return err
		}

		// io.ErrShortBuffer: For whatever reason, compression is larger than not compressing it. Skip it.
		if err != nil || compressedBytes >= int(srcH.PayloadLen) {
			// Didn't fit or didn't shrink: forward the payload uncompressed
			copy(dstBuf[pool.HeaderSize:], srcBuf[pool.HeaderSize:srcEnd])
			dstH.Compression = uint8(protocol.CompressionNone)
			dstH.PayloadLen = srcH.PayloadLen
		} else {
			// All good!
			dstH.Compression = uint8(codec.ID())
			dstH.PayloadLen = uint32(compressedBytes)
		}
	}
	dstH.Encode(dstBuf)

	// All set, send along the new buffer ID!
	err = output.Push(ctx, dst)
	if err != nil {
		p.Release(inputID)
		p.Release(dst)
		return err
	}

	p.Release(inputID)

	return nil
}
