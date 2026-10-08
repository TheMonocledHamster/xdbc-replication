package compression

import (
	"testing"

	"github.com/golang/snappy"
)

func TestIDReferencing(t *testing.T) {
	nc, err := ByID(None)
	if err != nil {
		t.Fatal(err.Error())
	}

	sc, err := ByID(Snappy)
	if err != nil {
		t.Fatal(err.Error())
	}

	if nc.ID() != None {
		t.Fatalf("Expected ID 0 for None Codec, got %d", nc.ID())
	}

	if sc.ID() != Snappy {
		t.Fatalf("Expected ID 1 for Snappy Codec, got %d", sc.ID())
	}
}

func TestMaxEncodedLen(t *testing.T) {
	nc := NoneCodec{}
	sc := SnappyCodec{}

	if nc.MaxEncodedLen(6400) != 6400 {
		t.Fatalf("Expected 6400 for NoneCodec's MaxEncodedLen(6400), got %d", nc.MaxEncodedLen(6400))
	}

	if sc.MaxEncodedLen(6400) != snappy.MaxEncodedLen(6400) {
		t.Fatalf("Expected %d for NoneCodec's MaxEncodedLen(6400), got %d", snappy.MaxEncodedLen(6400), nc.MaxEncodedLen(6400))
	}
}

func TestCompress(t *testing.T) {

	t.Skip("Skipping Compression tests: Decompress methods not implemented yet")

	nc := NoneCodec{}
	sc := SnappyCodec{}

	nc_dst := make([]byte, 128)
	nc_src := "Hello World"

	sc_dst := make([]byte, 128)
	sc_src := "Hello World"

	nc_bytes, nc_err := nc.Compress(nc_dst, []byte(nc_src))
	sc_bytes, sc_err := sc.Compress(sc_dst, []byte(sc_src))

	if nc_err != nil {
		t.Fatal(nc_err)
	}

	if sc_err != nil {
		t.Fatal(sc_err)
	}

	if nc_bytes != len([]byte(nc_src)) {
		t.Fatalf("Expected %d bytes in compressed format, got %d", len([]byte(nc_src)), nc_bytes)
	}

	if sc_bytes != len([]byte(sc_src)) {
		t.Fatalf("Expected %d bytes in compressed format, got %d", len([]byte(nc_src)), nc_bytes)
	}

	if string(nc_dst) != "Hello World" {
		t.Fatalf("Expected compressed string to be %s, got %s", "Hello World", string(nc_dst))
	}

	if string(sc_dst) != "SNAPPY_EXAMPLE" {
		t.Fatalf("Expected compressed string to be %s, got %s", "SNAPPY EXAMPLE", string(nc_dst))
	}
}

func TestDecompress(t *testing.T) {

	t.Skip("Skipping Decompression tests: Decompress methods not implemented yet")

	nc := NoneCodec{}
	sc := SnappyCodec{}

	nc_dst := make([]byte, 128)
	nc_src := "Hello World"

	// Need to figure out snappy compressed example
	sc_dst := make([]byte, 128)
	sc_src := "BETTER SNAPPY EXAMPLE"

	// Add nc_bytes and sc_bytes after implementation
	nc_bytes, nc_err := nc.Decompress(nc_dst, []byte(nc_src))
	sc_bytes, sc_err := sc.Decompress(sc_dst, []byte(sc_src))

	if nc_err != nil {
		t.Fatal(nc_err)
	}

	if sc_err != nil {
		t.Fatal(sc_err)
	}

	if nc_bytes != len([]byte(nc_src)) {
		t.Fatalf("Expected %d bytes in decompressed format, got %d", len([]byte(nc_src)), nc_bytes)
	}

	if sc_bytes != len([]byte(sc_src)) {
		t.Fatalf("Expected %d bytes in decompressed format, got %d", len([]byte(nc_src)), nc_bytes)
	}

	if string(nc_dst) != "Hello World" {
		t.Fatalf("Expected decompressed string to be %s, got %s", "Hello World", string(nc_dst))
	}

	if string(sc_dst) != "SNAPPY_EXAMPLE" {
		t.Fatalf("Expected decompressed string to be %s, got %s", "SNAPPY EXAMPLE", string(nc_dst))
	}
}

func TestRoundTrip(t *testing.T) {
	t.Skip("Skipping Round Trip tests: Compress/Decompress methods not implemented yet")

	nc := NoneCodec{}
	sc := SnappyCodec{}

	nc_dst1 := make([]byte, 128)
	nc_dst2 := make([]byte, 128)
	nc_src := "Hello World"

	// Need to figure out snappy compressed example
	sc_dst1 := make([]byte, 128)
	sc_dst2 := make([]byte, 128)
	sc_src := "BETTER SNAPPY EXAMPLE"

	nc_comp_bytes, nc_comp_err := nc.Compress(nc_dst1, []byte(nc_src))
	sc_comp_bytes, sc_comp_err := nc.Compress(sc_dst1, []byte(nc_src))

	if nc_comp_err != nil {
		t.Fatal(nc_comp_err)
	}

	if sc_comp_err != nil {
		t.Fatal(sc_comp_err)
	}

	if nc_comp_bytes != len([]byte(nc_src)) {
		t.Fatalf("Expected %d bytes in decompressed format, got %d", len([]byte(nc_src)), nc_comp_bytes)
	}

	if sc_comp_bytes != len([]byte(sc_src)) {
		t.Fatalf("Expected %d bytes in decompressed format, got %d", len([]byte(nc_src)), sc_comp_bytes)
	}

	nc_decom_bytes, nc_decom_err := nc.Decompress(nc_dst2, nc_dst1)
	sc_decom_bytes, sc_decom_err := sc.Decompress(sc_dst2, sc_dst1)

	if nc_decom_err != nil {
		t.Fatal(nc_decom_err)
	}

	if sc_decom_err != nil {
		t.Fatal(sc_decom_err)
	}

	if nc_decom_bytes != len([]byte(nc_src)) {
		t.Fatalf("Expected %d bytes in decompressed format, got %d", len([]byte(nc_src)), nc_comp_bytes)
	}

	if sc_decom_bytes != len([]byte(sc_src)) {
		t.Fatalf("Expected %d bytes in decompressed format, got %d", len([]byte(nc_src)), sc_comp_bytes)
	}

	if string(nc_dst2) != nc_src {
		t.Fatalf("Expected returned value to be %s, got %s", nc_src, string(nc_dst2))
	}

	if string(sc_dst2) != sc_src {
		t.Fatalf("Expected returned value to be %s, got %s", sc_src, string(sc_dst2))
	}
}
