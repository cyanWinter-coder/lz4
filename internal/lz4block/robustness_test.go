package lz4block

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/pierrec/lz4/v4/internal/lz4errors"
)

// The tests in this file check boundary conditions and hostile inputs. Every
// decode runs against both decodeBlock (assembly where available) and
// decodeBlockGo, which must agree.

var testDecoders = []struct {
	name string
	fn   func(dst, src, dict []byte) int
}{
	{"decodeBlock", decodeBlock},
	{"decodeBlockGo", decodeBlockGo},
}

const guardByte = 0xA5

// guardedDecode decodes src into a buffer of dstLen bytes followed by guard
// bytes, and fails if the decoder writes past dstLen or returns more than it.
func guardedDecode(t testing.TB, fn func(dst, src, dict []byte) int, src, dict []byte, dstLen int) ([]byte, int) {
	t.Helper()
	const guard = 64
	buf := make([]byte, dstLen+guard)
	for i := range buf {
		buf[i] = guardByte
	}
	n := fn(buf[:dstLen], src, dict)
	if n > dstLen {
		t.Fatalf("decoder returned %d for a %d byte destination", n, dstLen)
	}
	for i := dstLen; i < len(buf); i++ {
		if buf[i] != guardByte {
			t.Fatalf("guard byte %d overwritten (n=%d)", i-dstLen, n)
		}
	}
	if n < 0 {
		return nil, n
	}
	return buf[:n], n
}

// appendLenExt appends the LSIC extension bytes for a length whose 4-bit
// token field is saturated at 15.
func appendLenExt(b []byte, n int) []byte {
	for ; n >= 255; n -= 255 {
		b = append(b, 255)
	}
	return append(b, byte(n))
}

// appendSeq appends one LZ4 sequence. mlen == 0 means a final, literals only
// sequence.
func appendSeq(b, lits []byte, off, mlen int) []byte {
	var tok byte
	ln := len(lits)
	if ln >= 15 {
		tok = 0xF0
	} else {
		tok = byte(ln) << 4
	}
	ml := mlen - minMatch
	if mlen > 0 {
		if ml >= 15 {
			tok |= 0x0F
		} else {
			tok |= byte(ml)
		}
	}
	b = append(b, tok)
	if ln >= 15 {
		b = appendLenExt(b, ln-15)
	}
	b = append(b, lits...)
	if mlen == 0 {
		return b
	}
	b = append(b, byte(off), byte(off>>8))
	if ml >= 15 {
		b = appendLenExt(b, ml-15)
	}
	return b
}

// refDecodeSeq is what appendSeq's sequence decodes to, given the output so far
// and the dictionary; ok is false if the offset is out of range.
func refDecodeSeq(out, dict, lits []byte, off, mlen int) (_ []byte, ok bool) {
	out = append(out, lits...)
	if mlen == 0 {
		return out, true
	}
	if off == 0 || off > len(out)+len(dict) {
		return nil, false
	}
	for range mlen {
		pos := len(dict) + len(out) - off
		if pos < len(dict) {
			out = append(out, dict[pos])
		} else {
			out = append(out, out[pos-len(dict)])
		}
	}
	return out, true
}

func testPattern(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7) ^ seed
	}
	return b
}

func TestDecodeLengthBoundaries(t *testing.T) {
	// Literal and match lengths on either side of the 4-bit token limit and of
	// each extra length byte.
	litLens := []int{0, 1, 14, 15, 16, 17, 15 + 254, 15 + 255, 15 + 256, 15 + 255 + 255, 1000}
	matchLens := []int{4, 5, 18, 19, 20, 19 + 254, 19 + 255, 19 + 256, 19 + 255 + 255, 1000}
	offsets := []int{1, 2, 3, 4, 7, 8, 9, 15, 16, 17, 31, 32, 33}
	tail := []byte("final")

	for _, ll := range litLens {
		for _, ml := range matchLens {
			for _, off := range offsets {
				lits := testPattern(ll, 0x3C)
				src := appendSeq(nil, lits, off, ml)
				src = appendSeq(src, tail, 0, 0)
				want, ok := refDecodeSeq(nil, nil, lits, off, ml)
				if ok {
					want = append(want, tail...)
				}
				name := fmt.Sprintf("lit%d/match%d/off%d", ll, ml, off)
				for _, d := range testDecoders {
					for _, slack := range []int{-1, 0, 1, 8, 32} {
						dstLen := len(want) + slack
						if !ok {
							dstLen = 2048 + slack
						}
						if dstLen < 0 {
							continue
						}
						got, n := guardedDecode(t, d.fn, src, nil, dstLen)
						switch {
						case !ok || slack < 0:
							if n >= 0 {
								t.Fatalf("%s %s slack=%d: want error, got %d bytes", d.name, name, slack, n)
							}
						case n < 0:
							t.Fatalf("%s %s slack=%d: unexpected error %d", d.name, name, slack, n)
						case !bytes.Equal(got, want):
							t.Fatalf("%s %s slack=%d: output mismatch", d.name, name, slack)
						}
					}
				}
			}
		}
	}
}

func TestDecodeDictBoundaries(t *testing.T) {
	tail := []byte("final")
	for _, dictLen := range []int{0, 1, 3, 4, 15, 16, 17, 1<<16 - 1, 1 << 16, 1<<16 + 1, 1 << 20} {
		dict := testPattern(dictLen, 0x5A)
		for _, ll := range []int{0, 3, 20} {
			for _, off := range []int{1, 3, 4, 16, dictLen - 1, dictLen, dictLen + ll, dictLen + ll + 1, 1<<16 - 1} {
				if off < 1 || off > 1<<16-1 {
					continue
				}
				for _, ml := range []int{4, 19, 100} {
					lits := testPattern(ll, 0xC3)
					src := appendSeq(nil, lits, off, ml)
					src = appendSeq(src, tail, 0, 0)
					want, ok := refDecodeSeq(nil, dict, lits, off, ml)
					if ok {
						want = append(want, tail...)
					}
					name := fmt.Sprintf("dict%d/lit%d/off%d/match%d", dictLen, ll, off, ml)
					for _, d := range testDecoders {
						got, n := guardedDecode(t, d.fn, src, dict, ll+ml+len(tail))
						switch {
						case !ok:
							if n >= 0 {
								t.Fatalf("%s %s: want error, got %d bytes", d.name, name, n)
							}
						case n < 0:
							t.Fatalf("%s %s: unexpected error %d", d.name, name, n)
						case !bytes.Equal(got, want):
							t.Fatalf("%s %s: output mismatch\n got %x\nwant %x", d.name, name, got, want)
						}
					}
				}
			}
		}
	}
}

// Every prefix of a valid block, and every single byte corruption of it, must
// be handled identically by both decoders without writing out of bounds.
func TestDecodeTruncatedAndCorruptBlocks(t *testing.T) {
	var src []byte
	src = appendSeq(src, testPattern(20, 1), 3, 40)
	src = appendSeq(src, testPattern(300, 2), 17, 300)
	src = appendSeq(src, nil, 1, 4)
	src = appendSeq(src, testPattern(7, 3), 0, 0)
	dict := testPattern(100, 4)

	check := func(name string, blk []byte) {
		t.Helper()
		for _, dstLen := range []int{0, 1, 100, 671, 672, 1024} {
			for _, dc := range [][]byte{nil, dict} {
				want, wn := guardedDecode(t, decodeBlockGo, blk, dc, dstLen)
				got, gn := guardedDecode(t, decodeBlock, blk, dc, dstLen)
				if (gn < 0) != (wn < 0) || (gn >= 0 && (gn != wn || !bytes.Equal(got, want))) {
					t.Fatalf("%s dst=%d dict=%d: decodeBlock=%d decodeBlockGo=%d", name, dstLen, len(dc), gn, wn)
				}
			}
		}
	}
	for i := 0; i <= len(src); i++ {
		check(fmt.Sprintf("prefix%d", i), src[:i])
	}
	blk := make([]byte, len(src))
	for i := range src {
		for _, v := range []byte{0, 1, 0x0F, 0x10, 0x7F, 0xF0, 0xFF, src[i] ^ 0x80} {
			copy(blk, src)
			blk[i] = v
			check(fmt.Sprintf("byte%d=%#x", i, v), blk)
		}
	}
}

func compressionInputs() map[string][]byte {
	rnd := rand.New(rand.NewSource(1))
	random := func(n int) []byte {
		b := make([]byte, n)
		rnd.Read(b)
		return b
	}
	text := bytes.Repeat([]byte("the quick brown fox jumps over the lazy dog. "), 2000)
	in := map[string][]byte{
		"empty":        {},
		"text64K":      text[:1<<16],
		"text64K+1":    text[:1<<16+1],
		"random1K":     random(1024),
		"random64K":    random(1 << 16),
		"zeros64K":     make([]byte, 1<<16),
		"zeros64K+100": make([]byte, 1<<16+100),
		"mixed":        append(append(random(3000), make([]byte, 3000)...), random(3000)...),
	}
	// Sizes around mfLimit and minMatch, where the compressors switch to
	// emitting only literals.
	for _, n := range []int{1, 2, 3, 4, 5, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20} {
		in[fmt.Sprint("zeros", n)] = make([]byte, n)
		in[fmt.Sprint("random", n)] = random(n)
	}
	return in
}

// Compressors must not panic or overrun dst whatever its size, must always
// succeed with CompressBlockBound, and must produce decodable output.
func TestCompressBlockDstBoundaries(t *testing.T) {
	dict := []byte("the quick brown fox jumps over the lazy dog 0123456789")
	compressors := []struct {
		name string
		fn   func(src, dst []byte) (int, error)
		dict []byte
	}{
		{"fast", CompressBlock, nil},
		{"fast+dict", func(src, dst []byte) (int, error) { return CompressBlockWithDict(src, dst, dict) }, dict},
		{"hc1", func(src, dst []byte) (int, error) { return CompressBlockHC(src, dst, 1<<9) }, nil},
		{"hc9", func(src, dst []byte) (int, error) { return CompressBlockHC(src, dst, 1<<17) }, nil},
		{"hc0", func(src, dst []byte) (int, error) { return CompressBlockHC(src, dst, 0) }, nil},
	}
	for name, src := range compressionInputs() {
		for _, c := range compressors {
			bound := CompressBlockBound(len(src))
			full := make([]byte, bound)
			fn, err := c.fn(src, full)
			if err != nil || fn <= 0 {
				t.Fatalf("%s %s: with CompressBlockBound got n=%d err=%v", c.name, name, fn, err)
			}
			for _, dstLen := range []int{0, 1, fn - 1, fn, fn + 1, len(src) - 1, len(src), len(src) + 1, bound - 1, bound} {
				if dstLen < 0 || dstLen > bound {
					continue
				}
				buf := make([]byte, dstLen+64)
				for i := range buf {
					buf[i] = guardByte
				}
				n, err := c.fn(src, buf[:dstLen])
				for i := dstLen; i < len(buf); i++ {
					if buf[i] != guardByte {
						t.Fatalf("%s %s dst=%d: wrote past dst", c.name, name, dstLen)
					}
				}
				if n < 0 || n > dstLen {
					t.Fatalf("%s %s dst=%d: n=%d out of range", c.name, name, dstLen, n)
				}
				if dstLen == bound && (err != nil || n == 0) {
					t.Fatalf("%s %s dst=bound: n=%d err=%v", c.name, name, n, err)
				}
				if err != nil || n == 0 {
					continue
				}
				dec := make([]byte, len(src))
				dn, err := UncompressBlock(buf[:n], dec, c.dict)
				if err != nil || !bytes.Equal(dec[:dn], src) {
					t.Fatalf("%s %s dst=%d: round trip failed: %v", c.name, name, dstLen, err)
				}
			}
		}
	}
}

// CompressBlockWithDict reports incompressible input to a short dst the way
// CompressBlock does: (0, nil) rather than an error.
func TestCompressBlockWithDictIncompressible(t *testing.T) {
	src := make([]byte, 1000)
	rand.New(rand.NewSource(4)).Read(src)
	copy(src, "abcdefghabcdefgh") // one early match
	dict := []byte("zzzzzzzzzzzzzzzzzzzz")
	for _, dstLen := range []int{len(src) - 1, len(src), len(src) + 1, len(src) + 5} {
		var c Compressor
		n0, err0 := c.CompressBlock(src, make([]byte, dstLen))
		n1, err1 := c.CompressBlockWithDict(src, make([]byte, dstLen), dict)
		if (err0 == nil) != (err1 == nil) || (n0 == 0) != (n1 == 0) {
			t.Errorf("dst=%d: CompressBlock n=%d err=%v, CompressBlockWithDict n=%d err=%v", dstLen, n0, err0, n1, err1)
		}
	}
}

// allCompressors returns every block compression entry point, each wrapped
// to the same signature so alias and short-buffer tests cover them all.
func allCompressors() []struct {
	name string
	fn   func(src, dst []byte) (int, error)
} {
	var fast Compressor
	var hc CompressorHC
	var cc CompressorCCompat
	return []struct {
		name string
		fn   func(src, dst []byte) (int, error)
	}{
		{"fast/pooled", CompressBlock},
		{"fast/method", fast.CompressBlock},
		{"hc/pooled", func(src, dst []byte) (int, error) { return CompressBlockHC(src, dst, 1<<9) }},
		{"hc/method", func(src, dst []byte) (int, error) { return hc.CompressBlock(src, dst, 1<<9) }},
		{"ccompat/method", func(src, dst []byte) (int, error) {
			return cc.CompressBlock(src, dst, 1)
		}},
		{"ccompat/pooled", CompressBlockCCompat},
	}
}

// A backing array split into overlapping src and dst slices of every
// relative position: dst before src, after it, and sub-slices both ways.
func aliasPairs(backing []byte) []struct {
	name     string
	src, dst []byte
} {
	mid := len(backing) / 2
	return []struct {
		name     string
		src, dst []byte
	}{
		{"dst-then-src", backing[:mid], backing[mid:]},       // adjacent, no shared byte
		{"src-then-dst", backing[mid:], backing[:mid]},       // adjacent, reversed
		{"overlap-prefix", backing[:mid+1], backing[mid-1:]}, // share two middle bytes
		{"same-region", backing[:mid], backing[:mid]},        // identical
		{"src-contains-dst", backing, backing[2:6]},
		{"dst-contains-src", backing[2:6], backing},
	}
}

func TestAliasesDetected(t *testing.T) {
	if !aliasCheckEnabled {
		t.Skip("overlap detection requires the unsafe build; nounsafe/purego only keep bounds checks")
	}
	backing := bytes.Repeat([]byte("the quick brown fox jumps. "), 8)
	t.Run("compress", func(t *testing.T) {
		for _, pair := range aliasPairs(backing) {
			for _, c := range allCompressors() {
				t.Run(c.name+"/"+pair.name, func(t *testing.T) {
					n, err := c.fn(pair.src, pair.dst)
					wantAlias := aliases(pair.src, pair.dst)
					if wantAlias {
						if !errors.Is(err, lz4errors.ErrOverlappingBuffers) {
							t.Fatalf("got n=%d err=%v, want ErrOverlappingBuffers", n, err)
						}
						if n != 0 {
							t.Fatalf("an overlapping call must write nothing, got n=%d", n)
						}
					} else if errors.Is(err, lz4errors.ErrOverlappingBuffers) {
						t.Fatalf("adjacent non-overlapping slices falsely rejected")
					}
				})
			}
		}
	})

	t.Run("compress-with-dict", func(t *testing.T) {
		// One backing array shared by dict and dst; src is a separate slice.
		shared := make([]byte, 128)
		dict := shared[:64]
		dst := shared[32:128] // shares bytes 32..63 with dict
		src := bytes.Repeat([]byte("separate, unrelated source block "), 4)
		var c Compressor
		if _, err := c.CompressBlockWithDict(src, dst, dict); !errors.Is(err, lz4errors.ErrOverlappingBuffers) {
			t.Fatalf("dict/dst alias: got %v, want ErrOverlappingBuffers", err)
		}
		// src and dst overlap while a separate dict is given.
		backing2 := bytes.Repeat([]byte("the quick brown fox jumps. "), 8)
		if _, err := c.CompressBlockWithDict(backing2[:40], backing2[20:60], []byte("unrelated dictionary bytes")); !errors.Is(err, lz4errors.ErrOverlappingBuffers) {
			t.Fatalf("src/dst alias: got %v, want ErrOverlappingBuffers", err)
		}
	})

	t.Run("uncompress", func(t *testing.T) {
		src := bytes.Repeat([]byte("compressible lz4 block input data "), 32)
		z := make([]byte, CompressBlockBound(len(src)))
		n, err := CompressBlock(src, z)
		if err != nil {
			t.Fatal(err)
		}
		z = z[:n]
		// Decoding into a buffer that shares memory with the compressed input.
		flat := make([]byte, n+len(src))
		copy(flat, z)
		if _, err := UncompressBlock(flat[:n], flat[n:n+len(src)], nil); err != nil {
			t.Fatalf("adjacent non-overlapping slices must work: %v", err)
		}
		if _, err := UncompressBlock(flat[:n], flat[n-4:n-4+len(src)], nil); !errors.Is(err, lz4errors.ErrOverlappingBuffers) {
			t.Fatalf("got %v, want ErrOverlappingBuffers", err)
		}

		// A dictionary that shares the destination's backing array is rejected.
		dict := bytes.Repeat([]byte("dictionary contents for overlap test. "), 4)
		dsrc := append(append([]byte{}, dict[:32]...), []byte(" then fresh block bytes")...)
		zb := make([]byte, CompressBlockBound(len(dsrc)))
		dn, derr := CompressBlockWithDict(dsrc, zb, dict)
		if derr != nil || dn == 0 {
			t.Fatalf("CompressBlockWithDict: n=%d err=%v", dn, derr)
		}
		shared := make([]byte, len(dict)+len(dsrc))
		copy(shared, dict)
		if _, err := UncompressBlock(zb[:dn], shared[len(dict)-8:len(dict)-8+len(dsrc)], shared[:len(dict)]); !errors.Is(err, lz4errors.ErrOverlappingBuffers) {
			t.Fatalf("dict/dst alias: got %v, want ErrOverlappingBuffers", err)
		}
	})
}

// TestShortBufferErrorContract checks the bounded-cleanup contract: a failed
// compression returns zero bytes, a structured error carrying the guaranteed
// sufficient length, and errors.Is still matches the historical sentinel.
func TestShortBufferErrorContract(t *testing.T) {
	src := bytes.Repeat([]byte("the quick brown fox jumps over the lazy dog. "), 64)
	need := CompressBlockBound(len(src))
	cases := []struct {
		name string
		fn   func(dst []byte) (int, error)
	}{
		{"fast", func(dst []byte) (int, error) { return CompressBlock(src, dst) }},
		{"fast-method", func(dst []byte) (int, error) { var c Compressor; return c.CompressBlock(src, dst) }},
		{"dict", func(dst []byte) (int, error) {
			var c Compressor
			return c.CompressBlockWithDict(src, dst, []byte("a reusable dictionary of well over sixteen bytes"))
		}},
		{"hc", func(dst []byte) (int, error) { return CompressBlockHC(src, dst, 1<<9) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// First find a size that compresses, then ask for one byte less so
			// the run is guaranteed to run out of room mid-block.
			full := make([]byte, need)
			n, err := tc.fn(full)
			if err != nil || n <= 0 {
				t.Fatalf("bound-sized dst: n=%d err=%v", n, err)
			}
			dst := make([]byte, n-1)
			got, err := tc.fn(dst)
			if got != 0 {
				t.Fatalf("failed call returned %d bytes, want 0 so no half block looks valid", got)
			}
			if !errors.Is(err, lz4errors.ErrInvalidSourceShortBuffer) {
				t.Fatalf("errors.Is sentinel: got %v", err)
			}
			var short *lz4errors.ShortBufferError
			if !errors.As(err, &short) {
				t.Fatalf("errors.As *ShortBufferError: got %T", err)
			}
			if short.Need != need || short.Have != n-1 {
				t.Fatalf("ShortBufferError = {Have:%d Need:%d}, want {Have:%d Need:%d}", short.Have, short.Need, n-1, need)
			}
			// Retrying with the advertised size must succeed and round-trip.
			retry := make([]byte, short.Need)
			rn, err := tc.fn(retry)
			if err != nil || rn <= 0 {
				t.Fatalf("retry with Need bytes: n=%d err=%v", rn, err)
			}
			out := make([]byte, len(src))
			dn, derr := UncompressBlock(retry[:rn], out, nil)
			if derr != nil || !bytes.Equal(out[:dn], src) {
				t.Fatalf("retry round trip: %v", derr)
			}
		})
	}

	// Decompression cannot know the required size from a block, so Need is 0
	// but the error still carries the space it had and matches the sentinel.
	t.Run("uncompress", func(t *testing.T) {
		full := make([]byte, need)
		n, err := CompressBlock(src, full)
		if err != nil {
			t.Fatal(err)
		}
		_, err = UncompressBlock(full[:n], make([]byte, 4), nil)
		var short *lz4errors.ShortBufferError
		if !errors.As(err, &short) || short.Have != 4 || short.Need != 0 {
			t.Fatalf("got %v, want ShortBufferError{Have:4 Need:0}", err)
		}
		if !errors.Is(err, lz4errors.ErrInvalidSourceShortBuffer) {
			t.Fatalf("errors.Is sentinel: got %v", err)
		}
	})
}

// TestCompressorReuseClearsState verifies that reusing a compressor drops the
// previous block's table and dictionary state, so output is deterministic and
// no call matches into data the caller has since modified.
func TestCompressorReuseClearsState(t *testing.T) {
	var c Compressor
	dict := bytes.Repeat([]byte("dictionary-word-"), 8)
	first := bytes.Repeat([]byte("aaaa bbbb cccc dddd "), 32)
	second := bytes.Repeat([]byte("zzzz yyyy xxxx wwww "), 32)

	// Prime the compressor with a dictionary block.
	z1 := make([]byte, CompressBlockBound(len(first)))
	if _, err := c.CompressBlockWithDict(first, z1, dict); err != nil {
		t.Fatal(err)
	}
	if c.dict == nil {
		t.Fatal("CompressBlockWithDict must retain the dict for a repeat call")
	}

	// A plain CompressBlock on the same compressor must not keep referencing
	// the dictionary, and must match a fresh compressor's output exactly.
	z2a := make([]byte, CompressBlockBound(len(second)))
	n2a, err := c.CompressBlock(second, z2a)
	if err != nil {
		t.Fatal(err)
	}
	if c.dict != nil {
		t.Fatal("CompressBlock must clear a dictionary left by CompressBlockWithDict")
	}
	var fresh Compressor
	z2b := make([]byte, CompressBlockBound(len(second)))
	n2b, err := fresh.CompressBlock(second, z2b)
	if err != nil {
		t.Fatal(err)
	}
	if n2a != n2b || !bytes.Equal(z2a[:n2a], z2b[:n2b]) {
		t.Fatal("reused compressor output differs from a fresh one after CompressBlock")
	}

	// Mutating the old dictionary backing array must not change subsequent
	// output: nothing may still index it.
	for i := range dict {
		dict[i] = 0xFF
	}
	z2c := make([]byte, CompressBlockBound(len(second)))
	n2c, err := c.CompressBlock(second, z2c)
	if err != nil {
		t.Fatal(err)
	}
	if n2c != n2a || !bytes.Equal(z2c[:n2c], z2a[:n2a]) {
		t.Fatal("output changed after mutating a dictionary the compressor should have dropped")
	}

	// Repeated identical blocks through one compressor stay deterministic
	// (the table reset that issue #65 requires).
	for range 5 {
		z := make([]byte, CompressBlockBound(len(second)))
		n, err := c.CompressBlock(second, z)
		if err != nil || n != n2a || !bytes.Equal(z[:n], z2a[:n2a]) {
			t.Fatalf("repeated CompressBlock is non-deterministic: n=%d err=%v", n, err)
		}
	}
}

// TestConcurrentBlockCompression exercises the documented contract: many
// goroutines compress different blocks concurrently, each owning its scratch
// buffers while only the read-only input is shared. It runs under -race in CI.
func TestConcurrentBlockCompression(t *testing.T) {
	blocks := make([][]byte, 16)
	rnd := rand.New(rand.NewSource(7))
	for i := range blocks {
		b := make([]byte, 4096)
		rnd.Read(b)
		// Make half the blocks compressible with repeated runs.
		if i%2 == 0 {
			for j := 0; j < len(b); j += 64 {
				copy(b[j:], []byte("shared-readonly-prefix-"))
			}
		}
		blocks[i] = b
	}

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			// Per-goroutine compressor and scratch: nothing mutable is shared.
			var fast Compressor
			var hc CompressorHC
			var cc CompressorCCompat
			for round := range 25 {
				src := blocks[(g+round)%len(blocks)]
				bound := CompressBlockBound(len(src))
				for _, fn := range []func([]byte) (int, error){
					func(dst []byte) (int, error) { return fast.CompressBlock(src, dst) },
					func(dst []byte) (int, error) {
						return hc.CompressBlock(src, dst, 1<<9)
					},
					func(dst []byte) (int, error) { return cc.CompressBlock(src, dst, 1) },
					func(dst []byte) (int, error) { return CompressBlock(src, dst) },
				} {
					dst := make([]byte, bound)
					n, err := fn(dst)
					if err != nil {
						t.Errorf("compress: %v", err)
						return
					}
					if n == 0 {
						t.Errorf("bound-sized dst returned n=0")
						return
					}
					out := make([]byte, len(src))
					dn, err := UncompressBlock(dst[:n], out, nil)
					if err != nil || dn != len(src) || !bytes.Equal(out, src) {
						t.Errorf("round trip failed: n=%d err=%v", dn, err)
						return
					}
				}
			}
		}(g)
	}
	wg.Wait()
}
