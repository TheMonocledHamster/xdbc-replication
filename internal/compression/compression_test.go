package compression

import (
	"bytes"
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
	nc := NoneCodec{}
	sc := SnappyCodec{}

	nc_dst := make([]byte, 128)
	nc_src := []byte("abcdabcdabcdabcdabcd")

	sc_dst := make([]byte, 128)
	sc_src := []byte("abcdabcdabcdabcdabcd")

	nc_bytes, nc_err := nc.Compress(nc_dst, nc_src)
	sc_bytes, sc_err := sc.Compress(sc_dst, sc_src)

	if nc_err != nil {
		t.Fatal(nc_err)
	}

	if sc_err != nil {
		t.Fatal(sc_err)
	}

	if nc_bytes != len(nc_src) {
		t.Fatalf("Expected %d bytes in compressed format, got %d", len(nc_src), nc_bytes)
	}

	// NOTE: Cannot meaningfully check for exact Snappy compressed example size
	if sc_bytes > snappy.MaxEncodedLen(len(sc_src)) {
		t.Fatalf("Expected at most %d bytes in compressed format, got %d", snappy.MaxEncodedLen(len(sc_src)), sc_bytes)
	}

	if !(bytes.Equal(nc_dst[:nc_bytes], nc_src)) {
		t.Fatalf("Expected none-compressed byte array to be %s, got %s", "abcdabcdabcdabcdabcd", string(nc_dst))
	}

	if !(bytes.Equal(sc_dst[:sc_bytes], snappy.Encode(nil, []byte("abcdabcdabcdabcdabcd")))) {
		t.Fatalf("Expected snappy-compressed byte array to be %s, got %s", "abcdabcdabcdabcdabcd", string(nc_dst))
	}
}

func TestDecompress(t *testing.T) {
	nc := NoneCodec{}
	sc := SnappyCodec{}

	nc_dst := make([]byte, 128)
	nc_src := []byte("abcdabcdabcdabcdabcd")

	// Need to figure out snappy compressed example
	sc_dst := make([]byte, 128)
	sc_src := snappy.Encode(nil, []byte("abcdabcdabcdabcdabcd"))

	// Add nc_bytes and sc_bytes after implementation
	nc_bytes, nc_err := nc.Decompress(nc_dst, nc_src)
	sc_bytes, sc_err := sc.Decompress(sc_dst, sc_src)

	if nc_err != nil {
		t.Fatal(nc_err)
	}

	if sc_err != nil {
		t.Fatal(sc_err)
	}

	if nc_bytes != len(nc_src) {
		t.Fatalf("Expected %d bytes in decompressed format, got %d", len(nc_src), nc_bytes)
	}

	expected_sc_bytes, _ := snappy.DecodedLen(sc_src)
	if sc_bytes != expected_sc_bytes {
		t.Fatalf("Expected %d bytes in decompressed format, got %d", expected_sc_bytes, sc_bytes)
	}

	if !(bytes.Equal(nc_dst[:nc_bytes], nc_src)) {
		t.Fatalf("Expected none-decompressed byte array to be %s, got %s", "abcdabcdabcdabcdabcd", nc_dst)
	}

	if !(bytes.Equal(sc_dst[:sc_bytes], []byte("abcdabcdabcdabcdabcd"))) {
		t.Fatalf("Expected snappy-decompressed byte array to be %s, got %s", "abcdabcdabcdabcdabcd", sc_dst)
	}
}

func TestRoundTrip(t *testing.T) {
	nc := NoneCodec{}
	sc := SnappyCodec{}

	nc_dst1 := make([]byte, 128)
	nc_dst2 := make([]byte, 128)
	nc_src := []byte("abcdabcdabcdabcdabcd")

	// Need to figure out snappy compressed example
	sc_dst1 := make([]byte, 128)
	sc_dst2 := make([]byte, 128)
	sc_src := []byte("abcdabcdabcdabcdabcd")

	nc_comp_bytes, nc_comp_err := nc.Compress(nc_dst1, nc_src)
	sc_comp_bytes, sc_comp_err := sc.Compress(sc_dst1, nc_src)

	if nc_comp_err != nil {
		t.Fatal(nc_comp_err)
	}

	if sc_comp_err != nil {
		t.Fatal(sc_comp_err)
	}

	if nc_comp_bytes != len(nc_src) {
		t.Fatalf("Expected %d bytes in compressed format, got %d", len(nc_src), nc_comp_bytes)
	}

	if sc_comp_bytes > snappy.MaxEncodedLen(len(sc_src)) {
		t.Fatalf("Expected at most %d bytes in compressed snappy format, got %d", len(sc_src), sc_comp_bytes)
	}

	nc_decom_bytes, nc_decom_err := nc.Decompress(nc_dst2, nc_dst1[:nc_comp_bytes])
	sc_decom_bytes, sc_decom_err := sc.Decompress(sc_dst2, sc_dst1[:sc_comp_bytes])

	if nc_decom_err != nil {
		t.Fatal(nc_decom_err)
	}

	if sc_decom_err != nil {
		t.Fatal(sc_decom_err)
	}

	if nc_decom_bytes != len([]byte(nc_src)) {
		t.Fatalf("Expected %d bytes in decompressed format, got %d", len(nc_src), nc_comp_bytes)
	}

	if sc_decom_bytes != len([]byte(sc_src)) {
		t.Fatalf("Expected %d bytes in decompressed format, got %d", len(nc_src), sc_comp_bytes)
	}

	if !(bytes.Equal(nc_dst2[:nc_decom_bytes], nc_src)) {
		t.Fatalf("Expected none byte array to be %s, got %s", "abcdabcdabcdabcdabcd", nc_dst2)
	}

	if !(bytes.Equal(sc_dst2[:sc_decom_bytes], sc_src)) {
		t.Fatalf("Expected snappy byte array to be %s, got %s", "abcdabcdabcdabcdabcd", sc_dst2)
	}
}
