//go:build !nounsafe && !purego

package lz4_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/pierrec/lz4/v4"
)

// TestBlockOverlapAPI checks the exported overlap rejection for both
// directions of the block APIs. Overlap detection requires comparing backing
// arrays, so it exists only in the unsafe build; the nounsafe/purego builds
// skip this file by build constraint.
func TestBlockOverlapAPI(t *testing.T) {
	backing := bytes.Repeat([]byte("the quick brown fox jumps. "), 16)
	src := backing[:len(backing)/2+4]
	dst := backing[len(backing)/2-4:]

	var c lz4.Compressor
	if _, err := c.CompressBlock(src, dst); !errors.Is(err, lz4.ErrOverlappingBuffers) {
		t.Fatalf("compress overlap: got %v, want ErrOverlappingBuffers", err)
	}
	var hc lz4.CompressorHC
	if _, err := hc.CompressBlock(src, dst); !errors.Is(err, lz4.ErrOverlappingBuffers) {
		t.Fatalf("HC compress overlap: got %v, want ErrOverlappingBuffers", err)
	}

	// Build a real block, then decode it into a buffer aliasing the input.
	plain := bytes.Repeat([]byte("compressible lz4 block contents "), 32)
	zbuf := make([]byte, lz4.CompressBlockBound(len(plain)))
	n, err := c.CompressBlock(plain, zbuf)
	if err != nil {
		t.Fatal(err)
	}
	flat := make([]byte, n+len(plain))
	copy(flat, zbuf)
	if _, err := lz4.UncompressBlock(flat[:n], flat[n-2:n-2+len(plain)]); !errors.Is(err, lz4.ErrOverlappingBuffers) {
		t.Fatalf("uncompress overlap: got %v, want ErrOverlappingBuffers", err)
	}
}
