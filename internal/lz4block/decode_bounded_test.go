package lz4block

import (
	"bytes"
	"errors"
	"testing"

	"github.com/pierrec/lz4/v4/internal/lz4errors"
)

// TestUncompressBlockBounded checks that the budget-aware decoder matches the
// ordinary decoder on valid input at every destination size, that it reports
// a distinct budget error with the in-budget prefix, and that malformed
// blocks keep their format error and deliver nothing.
func TestUncompressBlockBounded(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		data []byte
		dict []byte
	}{
		{"empty", nil, nil},
		{"literal", []byte("hello, hello, hello, hello!"), nil},
		{"repetitive", bytes.Repeat([]byte("lz4 block "), 500), nil},
		{"random64K", testBlockData(64<<10, 11), nil},
		{"withDict", []byte("the quick brown fox jumps"), bytes.Repeat([]byte("the quick brown "), 100)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			comp := make([]byte, CompressBlockBound(len(tc.data))+16)
			var c Compressor
			n, err := c.CompressBlockWithDict(tc.data, comp, tc.dict)
			if err != nil {
				t.Fatalf("compress: %v", err)
			}
			if len(tc.data) > 0 && n == 0 {
				t.Fatal("compress gave up with a bound-sized dst")
			}
			src := comp[:n]

			// A destination large enough must agree with UncompressBlock exactly.
			full := make([]byte, len(tc.data)+8)
			wantN, wantErr := UncompressBlock(src, full, tc.dict)
			got := make([]byte, len(tc.data)+8)
			gotN, gotErr := UncompressBlockBounded(src, got, tc.dict)
			if wantErr != gotErr || wantN != gotN || !bytes.Equal(full[:wantN], got[:gotN]) {
				t.Fatalf("full dst: UncompressBlock=(%d,%v) bounded=(%d,%v)", wantN, wantErr, gotN, gotErr)
			}

			// Every intermediate destination size: below the true size it must
			// report the limit error and hand back the exact prefix of the data.
			for size := 1; size < len(tc.data); size++ {
				dst := make([]byte, size)
				m, berr := UncompressBlockBounded(src, dst, tc.dict)
				if !errors.Is(berr, lz4errors.ErrOutputLimitExceeded) {
					t.Fatalf("size=%d: got %v, want ErrOutputLimitExceeded", size, berr)
				}
				if m != size {
					t.Fatalf("size=%d: delivered %d bytes", size, m)
				}
				if !bytes.Equal(dst, tc.data[:size]) {
					t.Fatalf("size=%d: prefix mismatch", size)
				}
			}
		})
	}
}

// TestDecodeBlockBoundedMalformed keeps format errors distinguishable from
// budget errors: a malformed block must report a format error and deliver
// nothing, whatever destination size is given.
func TestDecodeBlockBoundedMalformed(t *testing.T) {
	t.Parallel()

	malformed := [][]byte{
		{0x00, 0x00},                     // repeated zero token
		{0x20, 'a'},                      // truncated literal
		{0x01},                           // no room for offset
		{0xe1, 'a', 'b', 'c', 'd', 0, 0}, // zero offset
		{0x1b, '0', 0x10, 0x00, '0', '0', '0', '0'},  // offset past the block start
		{0xF0, 0xFF, 0xFF, 'A'},                      // truncated extended literal
		{0x10, 'a', 'b', 'c', 'd', 0x01, 0x00, 0x40}, // trailing garbage after a sequence
	}
	for i, src := range malformed {
		for _, size := range []int{1, 7, 1024} {
			dst := make([]byte, size)
			n, err := UncompressBlockBounded(src, dst, nil)
			if !errors.Is(err, lz4errors.ErrInvalidSourceShortBuffer) {
				t.Errorf("case %d size=%d: got (%d,%v), want ErrInvalidSourceShortBuffer", i, size, n, err)
			}
			if n != 0 {
				t.Errorf("case %d size=%d: malformed block delivered %d bytes", i, size, n)
			}
		}
	}
}

// TestDecodeBlockBoundedNoWritePastLimit proves the decoder never writes past
// the destination even when the block wants more, and that the prefix is exact.
func TestDecodeBlockBoundedNoWritePastLimit(t *testing.T) {
	t.Parallel()

	data := bytes.Repeat([]byte("abcdefgh"), 4000) // 32KiB of matches
	comp := make([]byte, CompressBlockBound(len(data)))
	n, _ := CompressBlock(data, comp)
	src := comp[:n]

	const size = 137
	dst := make([]byte, len(data))
	for i := range dst {
		dst[i] = 0xCC
	}
	dst = dst[:size]
	m, err := UncompressBlockBounded(src, dst, nil)
	if !errors.Is(err, lz4errors.ErrOutputLimitExceeded) {
		t.Fatalf("got %v, want limit error", err)
	}
	if m != size {
		t.Fatalf("delivered %d, want %d", m, size)
	}
	if !bytes.Equal(dst, data[:size]) {
		t.Fatal("in-budget prefix mismatch")
	}
}

// TestDecodeBlockBoundedFormatErrorAfterBudget proves a format error that
// appears after the budget is exhausted still wins over the budget error and
// delivers nothing. A naive decoder that stops once the destination is full
// would miss the bad byte at the tail and wrongly report ErrOutputLimitExceeded.
func TestDecodeBlockBoundedFormatErrorAfterBudget(t *testing.T) {
	t.Parallel()

	// A valid block that expands well past a one-byte budget, with a
	// well-formed first sequence, followed by a zero-offset match (illegal).
	// Token 0xF0 + ext 0x01 => 16 literals; then a match with offset 0.
	goodLiterals := []byte("0123456789abcdef")
	src := append(append([]byte{0xF0, 0x01}, goodLiterals...), 0x00, 0x00)

	for _, size := range []int{1, 4, 16} {
		dst := make([]byte, size)
		n, err := UncompressBlockBounded(src, dst, nil)
		if !errors.Is(err, lz4errors.ErrInvalidSourceShortBuffer) {
			t.Errorf("size=%d: got (%d,%v), want format error despite the over-budget prefix", size, n, err)
		}
		if n != 0 {
			t.Errorf("size=%d: malformed block delivered %d bytes", size, n)
		}
	}

	// Control: a valid compressed block that expands past the budget.
	data := bytes.Repeat([]byte("abcdefgh"), 2000)
	comp := make([]byte, CompressBlockBound(len(data)))
	cn, _ := CompressBlock(data, comp)
	dst := make([]byte, 1)
	n, err := UncompressBlockBounded(comp[:cn], dst, nil)
	if !errors.Is(err, lz4errors.ErrOutputLimitExceeded) || n != 1 || dst[0] != data[0] {
		t.Fatalf("control: got (%d,%v,%x), want limit error with the first byte", n, err, dst)
	}
}

// TestDecodeBlockBoundedDictionaryBudget exercises a match that starts in the
// dictionary, including when the budget runs out inside the dictionary part.
// The in-budget prefix must match the reference decoder byte for byte.
func TestDecodeBlockBoundedDictionaryBudget(t *testing.T) {
	t.Parallel()

	// No literals: an 8-byte match (nibble 4 + minMatch) at offset 10, so the
	// whole match is served from the dictionary.
	src := []byte{0x04, 0x0a, 0x00}
	dict := []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	full := make([]byte, 64)
	fn := decodeBlockGo(full, src, dict)
	if fn < 0 {
		t.Fatal("reference rejected a valid dictionary match")
	}
	for _, size := range []int{1, 2, 7, fn, fn + 1} {
		dst := make([]byte, size)
		n, err := UncompressBlockBounded(src, dst, dict)
		if size >= fn {
			if err != nil || n != fn || !bytes.Equal(dst[:fn], full[:fn]) {
				t.Fatalf("size=%d: got (%d,%v), want full %d bytes", size, n, err, fn)
			}
			continue
		}
		if !errors.Is(err, lz4errors.ErrOutputLimitExceeded) || n != size || !bytes.Equal(dst, full[:size]) {
			t.Fatalf("size=%d: got (%d,%v), want exact prefix of %d", size, n, err, fn)
		}
	}
}

// TestDecodeBlockBoundedGuarded runs malformed cases against a source buffer
// ending at an inaccessible page, so an out-of-bounds read faults instead of
// slipping past a check.
func TestDecodeBlockBoundedGuarded(t *testing.T) {
	for _, tc := range []struct {
		name string
		src  []byte
	}{
		{"truncatedLiteral", []byte{0x60, 'a'}},
		{"truncatedOffset", []byte{0x01}},
		{"zeroOffset", []byte{0xe1, 'a', 'b', 'c', 'd', 0, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := guardedTail(t, len(tc.src))
			copy(src, tc.src)
			dst := make([]byte, 4096)
			if _, err := UncompressBlockBounded(src, dst, nil); !errors.Is(err, lz4errors.ErrInvalidSourceShortBuffer) {
				t.Fatalf("got %v, want format error", err)
			}
		})
	}
}

// testBlockData returns deterministic pseudo-random bytes of length n.
func testBlockData(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		seed = seed*31 + 17
		b[i] = seed ^ byte(i>>3)
	}
	return b
}
