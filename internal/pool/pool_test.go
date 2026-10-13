package pool

import (
	"context"
	"strings"
	"testing"
)

func TestNewNormalPool(t *testing.T) {
	cfg := Config{NumBuffers: 2, PayloadCap: 250}
	pool, err := New(cfg)

	if err != nil {
		t.Fatalf("Encountered unexpected error from initializing a pool: %s", err)
	}

	// Verify nothing shady was done with the configuration
	if pool.cfg != cfg {
		t.Fatalf("Pool cfg attribute does not match inputted cfg argument")
	}

	// Verify enough memory was allocated
	bufSize := HeaderSize + 250
	if len(pool.slab) != (2 * bufSize) {
		t.Fatalf("Incorrect allocated pool slab size: expected %d, but got %d", 2*bufSize, len(pool.slab))
	}

	// Verify pool structure's buffer size is correctly listed
	if pool.bufSize != bufSize {
		t.Fatalf("Incorrect buf size attribute of pool: expected %d, but got %d", bufSize, pool.bufSize)
	}

	// Verify free list starts with correct number of entries
	if len(pool.free) != 2 {
		t.Fatalf("Expected %d entries in pool's free list, got %d", 2, len(pool.free))
	}

	// Verify all buffers start as unused in the "used" attribute
	for i := 0; i < len(pool.used); i++ {
		if pool.used[i].Load() {
			t.Fatalf("Expected entries in pool's used list to be false, but pool.used[%d] is set to true", i)
		}
	}
}

func TestNewInvalidBufNumPool(t *testing.T) {
	negCfg := Config{NumBuffers: -1, PayloadCap: 250}
	_, negErr := New(negCfg)
	if negErr == nil {
		t.Fatal("Expected error for negative-buffer specification, but returned normally")
	}

	zeroCfg := Config{NumBuffers: 0, PayloadCap: 250}
	_, zeroErr := New(zeroCfg)
	if zeroErr == nil {
		t.Fatal("Expected error for 0-buffer specification, but returned normally")
	}
}

func TestNewInvalidPayloadCapPool(t *testing.T) {
	negCfg := Config{NumBuffers: 2, PayloadCap: -1}
	_, negErr := New(negCfg)
	if negErr == nil {
		t.Fatal("Expected error for negative-payload-cap specification, but returned normally")
	}

	zeroCfg := Config{NumBuffers: 2, PayloadCap: 0}
	_, zeroErr := New(zeroCfg)
	if zeroErr == nil {
		t.Fatal("Expected error for 0-payload-cap specification, but returned normally")
	}
}

func TestCheckID(t *testing.T) {
	cfg := Config{NumBuffers: 2, PayloadCap: 250}
	pool, err := New(cfg)

	if err != nil {
		t.Fatalf("Encountered unexpected error from initializing a pool: %s", err)
	}

	for i := range 2 {
		err = pool.checkID(i)
		if err != nil {
			t.Fatalf("Expected no error for checking ID for buffer #%d, but got an error: %s", i, err)
		}
	}

	if pool.checkID(-1) == nil {
		t.Fatal("Expected an error for checking ID for buffer #-1, but got no error")
	}

	if pool.checkID(3) == nil {
		t.Fatal("Expected an error for checking ID for buffer #3 (beyond num buffers), but got no error")
	}
}

func TestBufferContents(t *testing.T) {
	cfg := Config{NumBuffers: 2, PayloadCap: 250}
	pool, err := New(cfg)

	if err != nil {
		t.Fatalf("Encountered unexpected error from initializing a pool: %s", err)
	}

	myStr1 := "Hello"
	myStr2 := "World"

	ctx := context.Background()
	id1, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id2, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	buf1, err := pool.Buffer(id1)
	if err != nil {
		t.Fatal(err)
	}
	buf2, err := pool.Buffer(id2)
	if err != nil {
		t.Fatal(err)
	}
	copy(buf1[HeaderSize:HeaderSize+len(myStr1)], []byte(myStr1))
	copy(buf2[HeaderSize:HeaderSize+len(myStr2)], []byte(myStr2))

	if got := string(buf1[HeaderSize : HeaderSize+len(myStr1)]); got != myStr1 {
		t.Fatalf("Expected %s from getting buffer #0, got %s", myStr1, got)
	}

	if got := string(buf2[HeaderSize : HeaderSize+len(myStr2)]); got != myStr2 {
		t.Fatalf("Expected %s from getting buffer #1, got %s", myStr2, got)
	}
	if err := pool.Release(id1); err != nil {
		t.Fatal(err)
	}
	if err := pool.Release(id2); err != nil {
		t.Fatal(err)
	}
}

func TestAcquire(t *testing.T) {
	cfg := Config{NumBuffers: 1, PayloadCap: 250}
	pool, err := New(cfg)

	if err != nil {
		t.Fatalf("Encountered unexpected error from initializing a pool: %s", err)
	}

	ctx := context.Background()
	id, acqErr := pool.Acquire(ctx)
	if acqErr != nil {
		t.Fatalf("Encountered unexpected error from acquiring a buffer: %s", acqErr)
	}

	if id != 0 {
		t.Fatalf("Expected 0 as acquired buffer ID in single-buffer pool, got %d", id)
	}

	if len(pool.free) != 0 {
		t.Fatalf("Expected 0 entries in pool's free list after Acquire, got %d", len(pool.free))
	}

	if !(pool.used[0].Load()) {
		t.Fatal("Expected acquired buffer's 'used' value to be true, but got false")
	}
}

func TestRelease(t *testing.T) {
	cfg := Config{NumBuffers: 1, PayloadCap: 250}
	pool, err := New(cfg)

	if err != nil {
		t.Fatalf("Encountered unexpected error from initializing a pool: %s", err)
	}

	ctx := context.Background()
	id, acqErr := pool.Acquire(ctx)
	if acqErr != nil {
		t.Fatalf("Encountered unexpected error from acquiring a buffer: %s", acqErr)
	}

	relErr := pool.Release(id)
	if relErr != nil {
		t.Fatalf("Encountered unexpected error from releasing a buffer: %s", relErr)
	}

	if len(pool.free) != 1 {
		t.Fatalf("Expected 1 entry in pool's free list after Acquire then Release, got %d", len(pool.free))
	}

	if pool.used[0].Load() {
		t.Fatal("Expected acquired buffer's 'used' value to be false, but got true")
	}
}

func TestBufferAccess(t *testing.T) {
	p, err := New(Config{NumBuffers: 1, PayloadCap: 64})
	if err != nil {
		t.Fatal(err)
	}
	if p.PayloadCap() != 64 {
		t.Fatal("wrong payload capacity")
	}
	id, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.Buffer(id)
	if err != nil || len(b) != HeaderSize+64 || cap(b) != len(b) {
		t.Fatalf("buffer: length %d, %v", len(b), err)
	}
	b[HeaderSize] = 42
	again, err := p.Buffer(id)
	if err != nil || again[HeaderSize] != 42 {
		t.Fatal("accessor must return slab-backed bytes")
	}
	for _, invalid := range []int{-1, 1} {
		if _, err := p.Buffer(invalid); err == nil {
			t.Fatalf("accepted ID %d", invalid)
		}
	}
	if err := p.Release(id); err != nil {
		t.Fatal(err)
	}
}

func TestDoubleRelease(t *testing.T) {
	cfg := Config{NumBuffers: 1, PayloadCap: 250}
	pool, err := New(cfg)

	if err != nil {
		t.Fatalf("Encountered unexpected error from initializing a pool: %s", err)
	}

	ctx := context.Background()
	id, acqErr := pool.Acquire(ctx)
	if acqErr != nil {
		t.Fatalf("Encountered unexpected error from acquiring a buffer: %s", acqErr)
	}

	relErr := pool.Release(id)
	if relErr != nil {
		t.Fatalf("Encountered unexpected error from releasing a buffer: %s", relErr)
	}

	// Attempting double release
	relErr2 := pool.Release(id)
	if relErr2 == nil || strings.Compare(relErr2.Error(), "Attempted double-release of buffer") != 0 {
		t.Fatalf("Expected error of 'Attempted double-release of buffer', got %s", relErr2)
	}
}
