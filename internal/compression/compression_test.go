package compression

import (
	"bytes"
	"testing"

	"github.com/golang/snappy"
)

func TestIDReferencing(t *testing.T) {
	sc, err := ByID(Snappy)
	if err != nil {
		t.Fatal(err.Error())
	}

	if sc.ID() != Snappy {
		t.Fatalf("Expected ID 1 for Snappy Codec, got %d", sc.ID())
	}
}

func TestMaxEncodedLen(t *testing.T) {
	sc := SnappyCodec{}

	if sc.MaxEncodedLen(6400) != snappy.MaxEncodedLen(6400) {
		t.Fatalf("Expected %d for NoneCodec's MaxEncodedLen(6400), got %d", snappy.MaxEncodedLen(6400), sc.MaxEncodedLen(6400))
	}
}

func TestCompress(t *testing.T) {
	sc := SnappyCodec{}

	sc_dst := make([]byte, 128)
	sc_src := []byte("abcdabcdabcdabcdabcd")

	sc_bytes, sc_err := sc.Compress(sc_dst, sc_src)

	if sc_err != nil {
		t.Fatal(sc_err)
	}

	// NOTE: Cannot meaningfully check for exact Snappy compressed example size
	if sc_bytes > snappy.MaxEncodedLen(len(sc_src)) {
		t.Fatalf("Expected at most %d bytes in compressed format, got %d", snappy.MaxEncodedLen(len(sc_src)), sc_bytes)
	}

	if !(bytes.Equal(sc_dst[:sc_bytes], snappy.Encode(nil, []byte("abcdabcdabcdabcdabcd")))) {
		t.Fatalf("Expected snappy-compressed byte array to be %s, got %s", "abcdabcdabcdabcdabcd", string(sc_dst))
	}
}

func TestDecompress(t *testing.T) {
	sc := SnappyCodec{}

	// Need to figure out snappy compressed example
	sc_dst := make([]byte, 128)
	sc_src := snappy.Encode(nil, []byte("abcdabcdabcdabcdabcd"))

	// Add nc_bytes and sc_bytes after implementation
	sc_bytes, sc_err := sc.Decompress(sc_dst, sc_src)

	if sc_err != nil {
		t.Fatal(sc_err)
	}

	expected_sc_bytes, _ := snappy.DecodedLen(sc_src)
	if sc_bytes != expected_sc_bytes {
		t.Fatalf("Expected %d bytes in decompressed format, got %d", expected_sc_bytes, sc_bytes)
	}

	if !(bytes.Equal(sc_dst[:sc_bytes], []byte("abcdabcdabcdabcdabcd"))) {
		t.Fatalf("Expected snappy-decompressed byte array to be %s, got %s", "abcdabcdabcdabcdabcd", sc_dst)
	}
}

func TestRoundTrip(t *testing.T) {
	sc := SnappyCodec{}

	// Need to figure out snappy compressed example
	sc_dst1 := make([]byte, 128)
	sc_dst2 := make([]byte, 128)
	sc_src := []byte("abcdabcdabcdabcdabcd")

	sc_comp_bytes, sc_comp_err := sc.Compress(sc_dst1, sc_src)

	if sc_comp_err != nil {
		t.Fatal(sc_comp_err)
	}

	if sc_comp_bytes > snappy.MaxEncodedLen(len(sc_src)) {
		t.Fatalf("Expected at most %d bytes in compressed snappy format, got %d", len(sc_src), sc_comp_bytes)
	}

	sc_decom_bytes, sc_decom_err := sc.Decompress(sc_dst2, sc_dst1[:sc_comp_bytes])

	if sc_decom_err != nil {
		t.Fatal(sc_decom_err)
	}

	if sc_decom_bytes != len([]byte(sc_src)) {
		t.Fatalf("Expected %d bytes in decompressed format, got %d", len(sc_src), sc_comp_bytes)
	}

	if !(bytes.Equal(sc_dst2[:sc_decom_bytes], sc_src)) {
		t.Fatalf("Expected snappy byte array to be %s, got %s", "abcdabcdabcdabcdabcd", sc_dst2)
	}
}
