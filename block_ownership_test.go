package lz4_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/pierrec/lz4/v4"
)

// TestBlockShortBufferAPI exercises the contract through the exported root
// package: a too-small destination gives the exported ShortBufferError with
// the guaranteed size, while errors.Is keeps matching the old sentinel.
func TestBlockShortBufferAPI(t *testing.T) {
	src := bytes.Repeat([]byte("the quick brown fox jumps over the lazy dog. "), 64)
	need := lz4.CompressBlockBound(len(src))

	var c lz4.Compressor
	full := make([]byte, need)
	n, err := c.CompressBlock(src, full)
	if err != nil || n <= 0 {
		t.Fatalf("bound-sized dst: n=%d err=%v", n, err)
	}

	short := make([]byte, n-1)
	got, err := c.CompressBlock(src, short)
	if got != 0 {
		t.Fatalf("failed compression must return zero bytes, got %d", got)
	}
	if !errors.Is(err, lz4.ErrInvalidSourceShortBuffer) {
		t.Fatalf("errors.Is(ErrInvalidSourceShortBuffer): got %v", err)
	}
	var sbe *lz4.ShortBufferError
	if !errors.As(err, &sbe) {
		t.Fatalf("errors.As(*ShortBufferError): got %T", err)
	}
	if sbe.Need != need || sbe.Have != n-1 {
		t.Fatalf("ShortBufferError = {Have:%d Need:%d}, want {Have:%d Need:%d}", sbe.Have, sbe.Need, n-1, need)
	}

	// Retrying with the advertised size succeeds and round-trips.
	retry := make([]byte, sbe.Need)
	rn, err := c.CompressBlock(src, retry)
	if err != nil || rn <= 0 {
		t.Fatalf("retry: n=%d err=%v", rn, err)
	}
	out := make([]byte, len(src))
	dn, err := lz4.UncompressBlock(retry[:rn], out)
	if err != nil || dn != len(src) || !bytes.Equal(out, src) {
		t.Fatalf("retry round trip: n=%d err=%v", dn, err)
	}

	// A short decompression destination reports the same type with Need 0.
	_, err = lz4.UncompressBlock(retry[:rn], make([]byte, 4))
	var decErr *lz4.ShortBufferError
	if !errors.As(err, &decErr) || decErr.Have != 4 || decErr.Need != 0 {
		t.Fatalf("decompress short dst: got %v", err)
	}
	if !errors.Is(err, lz4.ErrInvalidSourceShortBuffer) {
		t.Fatalf("decompress errors.Is sentinel: got %v", err)
	}
}
