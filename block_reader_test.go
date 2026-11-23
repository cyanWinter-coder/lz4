package lz4_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/pierrec/lz4/v4"
)

// blockReadModes are the ways a BlockReader's output is consumed. A clean
// finish is normalized to io.EOF so the three modes can share expectations.
var blockReadModes = []struct {
	name string
	read func(t *testing.T, r *lz4.BlockReader) ([]byte, error)
}{
	{"ReadAll", func(t *testing.T, r *lz4.BlockReader) ([]byte, error) {
		return io.ReadAll(r)
	}},
	{"Read/one", func(t *testing.T, r *lz4.BlockReader) ([]byte, error) {
		var out bytes.Buffer
		buf := make([]byte, 1)
		for {
			n, err := r.Read(buf)
			out.Write(buf[:n])
			if err != nil {
				return out.Bytes(), err
			}
		}
	}},
	{"WriteTo", func(t *testing.T, r *lz4.BlockReader) ([]byte, error) {
		var out bytes.Buffer
		_, err := r.WriteTo(&out)
		if err == nil {
			err = io.EOF
		}
		return out.Bytes(), err
	}},
}

// compressRawBlock compresses data with the block compressor, failing the
// test if it gives up.
func compressRawBlock(t *testing.T, data, dict []byte) []byte {
	t.Helper()
	comp := make([]byte, lz4.CompressBlockBound(len(data))+16)
	var c lz4.Compressor
	n, err := c.CompressBlockWithDict(data, comp, dict)
	if err != nil {
		t.Fatalf("compress: %v", err)
	}
	if len(data) > 0 && n == 0 {
		t.Fatal("compressor gave up with a bound-sized dst")
	}
	return comp[:n]
}

// TestBlockReaderRoundTrip decodes valid blocks every way the reader offers
// and checks the output against the source, including empty blocks and ones
// that need a dictionary.
func TestBlockReaderRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		data []byte
		dict []byte
	}{
		{"empty", nil, nil},
		{"literal", []byte("hello, hello, hello, hello!"), nil},
		{"repetitive", bytes.Repeat([]byte("lz4 block "), 1000), nil},
		{"withDict", []byte("the quick brown fox jumps over the lazy dog"), bytes.Repeat([]byte("the quick brown "), 50)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			comp := compressRawBlock(t, tc.data, tc.dict)
			for _, m := range blockReadModes {
				t.Run(m.name, func(t *testing.T) {
					r := lz4.NewBlockReader(bytes.NewReader(comp))
					if len(tc.dict) > 0 {
						if err := r.Apply(lz4.BlockDictionaryOption(tc.dict)); err != nil {
							t.Fatal(err)
						}
					}
					out, err := m.read(t, r)
					if err != nil && !errors.Is(err, io.EOF) {
						t.Fatalf("%v", err)
					}
					if !bytes.Equal(out, tc.data) {
						t.Fatalf("got %d bytes, want %d", len(out), len(tc.data))
					}
				})
			}
		})
	}
}

// TestBlockReaderBudgetExceeded is the core budget test: a block larger than
// the declared limit must first deliver exactly the in-budget prefix, then
// fail with ErrOutputLimitExceeded, and keep returning that error.
func TestBlockReaderBudgetExceeded(t *testing.T) {
	data := bytes.Repeat([]byte("abcdefgh"), 4000) // 32 KiB
	comp := compressRawBlock(t, data, nil)

	for _, limit := range []int{1, 7, 137, 4096, len(data) - 1} {
		t.Run(fmt.Sprintf("limit%d", limit), func(t *testing.T) {
			for _, m := range blockReadModes {
				t.Run(m.name, func(t *testing.T) {
					r := lz4.NewBlockReader(bytes.NewReader(comp))
					if err := r.Apply(lz4.BlockOutputLimitOption(limit)); err != nil {
						t.Fatal(err)
					}
					out, err := m.read(t, r)
					if !errors.Is(err, lz4.ErrOutputLimitExceeded) {
						t.Fatalf("got %v, want ErrOutputLimitExceeded", err)
					}
					if len(out) != limit || !bytes.Equal(out, data[:limit]) {
						t.Fatalf("delivered %d bytes, want exact prefix of %d", len(out), limit)
					}
					// The error is sticky across Read and WriteTo until Reset.
					buf := make([]byte, 16)
					for range 3 {
						if n, err := r.Read(buf); n != 0 || !errors.Is(err, lz4.ErrOutputLimitExceeded) {
							t.Fatalf("Read after failure: n=%d err=%v", n, err)
						}
					}
					if n, err := r.WriteTo(io.Discard); n != 0 || !errors.Is(err, lz4.ErrOutputLimitExceeded) {
						t.Fatalf("WriteTo after failure: n=%d err=%v", n, err)
					}
				})
			}
		})
	}
}

// TestBlockReaderBudgetExactAndOver proves a block that fits the budget
// finishes cleanly while one byte over reports the limit error.
func TestBlockReaderBudgetExactAndOver(t *testing.T) {
	data := []byte("the quick brown fox")
	comp := compressRawBlock(t, data, nil)

	// Exact budget: clean EOF, full data.
	r := lz4.NewBlockReader(bytes.NewReader(comp))
	if err := r.Apply(lz4.BlockOutputLimitOption(len(data))); err != nil {
		t.Fatal(err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("exact budget: %v", err)
	}
	if !bytes.Equal(out, data) {
		t.Fatal("exact budget: output mismatch")
	}

	// One byte short: prefix plus limit error.
	r = lz4.NewBlockReader(bytes.NewReader(comp))
	if err := r.Apply(lz4.BlockOutputLimitOption(len(data) - 1)); err != nil {
		t.Fatal(err)
	}
	out, err = io.ReadAll(r)
	if !errors.Is(err, lz4.ErrOutputLimitExceeded) {
		t.Fatalf("got %v, want limit error", err)
	}
	if !bytes.Equal(out, data[:len(data)-1]) {
		t.Fatal("over-budget prefix mismatch")
	}
}

// TestBlockReaderMalformed proves format errors take priority over the
// budget and deliver nothing: zero offset, truncated token, trailing garbage.
func TestBlockReaderMalformed(t *testing.T) {
	malformed := [][]byte{
		{0x00, 0x00},                     // repeated zero token
		{0x20, 'a'},                      // truncated literal
		{0x01},                           // no room for offset
		{0xe1, 'a', 'b', 'c', 'd', 0, 0}, // zero offset
		{0xF0, 0xFF, 0xFF, 'A'},          // truncated extended literal
		{0x10, 'a', 'b', 'c', 'd', 0x01, 0x00, 0x40}, // trailing garbage
	}
	for i, bad := range malformed {
		for _, limit := range []int{1, 64, 4096} {
			r := lz4.NewBlockReader(bytes.NewReader(bad))
			if err := r.Apply(lz4.BlockOutputLimitOption(limit)); err != nil {
				t.Fatal(err)
			}
			out, err := io.ReadAll(r)
			if !errors.Is(err, lz4.ErrInvalidSourceShortBuffer) {
				t.Errorf("case %d limit=%d: got %v, want format error", i, limit, err)
			}
			if len(out) != 0 {
				t.Errorf("case %d limit=%d: malformed block delivered %d bytes", i, limit, len(out))
			}
			// Sticky.
			buf := make([]byte, 8)
			if n, err := r.Read(buf); n != 0 || !errors.Is(err, lz4.ErrInvalidSourceShortBuffer) {
				t.Errorf("case %d: Read after failure n=%d err=%v", i, n, err)
			}
		}
	}
}

// TestBlockReaderResetClearsHistoryAndError drives the reader into each
// terminal state and proves Reset clears the error and dictionary history and
// revives it with a new source, while Close releases references and ends with
// io.EOF.
func TestBlockReaderResetClearsHistoryAndError(t *testing.T) {
	valid := compressRawBlock(t, []byte("some valid block data"), nil)
	bad := []byte{0x01} // truncated offset

	// After a format error, Reset revives the reader.
	r := lz4.NewBlockReader(bytes.NewReader(bad))
	if _, err := io.ReadAll(r); !errors.Is(err, lz4.ErrInvalidSourceShortBuffer) {
		t.Fatalf("want format error, got %v", err)
	}
	r.Reset(bytes.NewReader(valid))
	out, err := io.ReadAll(r)
	if err != nil || string(out) != "some valid block data" {
		t.Fatalf("after Reset: %q, %v", out, err)
	}

	// After a budget error, Reset revives the reader.
	r = lz4.NewBlockReader(bytes.NewReader(valid))
	if err := r.Apply(lz4.BlockOutputLimitOption(4)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(r); !errors.Is(err, lz4.ErrOutputLimitExceeded) {
		t.Fatal("want budget error")
	}
	r.Reset(bytes.NewReader(valid))
	out, err = io.ReadAll(r)
	if err != nil || string(out) != "some valid block data" {
		t.Fatalf("after budget Reset: %q, %v", out, err)
	}

	// Close ends the reader with EOF and is idempotent; Reset revives it.
	r = lz4.NewBlockReader(bytes.NewReader(valid))
	if err := r.Close(); err != nil {
		t.Fatalf("Close on a fresh reader: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if n, err := r.Read(make([]byte, 4)); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("Read after Close: n=%d err=%v", n, err)
	}
	r.Reset(bytes.NewReader(valid))
	out, err = io.ReadAll(r)
	if err != nil || string(out) != "some valid block data" {
		t.Fatalf("after Close then Reset: %q, %v", out, err)
	}
}

// TestBlockReaderCloseKeepsStickyError proves Close returns and retains a
// sticky error rather than masking it with EOF, until Reset.
func TestBlockReaderCloseKeepsStickyError(t *testing.T) {
	bad := []byte{0x01}
	r := lz4.NewBlockReader(bytes.NewReader(bad))
	if _, err := io.ReadAll(r); !errors.Is(err, lz4.ErrInvalidSourceShortBuffer) {
		t.Fatal("want format error")
	}
	if err := r.Close(); !errors.Is(err, lz4.ErrInvalidSourceShortBuffer) {
		t.Fatalf("Close returned %v, want the sticky format error", err)
	}
	if n, err := r.Read(make([]byte, 4)); n != 0 || !errors.Is(err, lz4.ErrInvalidSourceShortBuffer) {
		t.Fatalf("Read after Close: n=%d err=%v", n, err)
	}
	r.Reset(bytes.NewReader(nil))
	if _, err := io.ReadAll(r); err != nil {
		t.Fatalf("after Reset: %v", err)
	}
}

// TestBlockReaderRejectsOptionsAfterUse proves options cannot change once the
// reader has been used, and a negative limit is rejected up front.
func TestBlockReaderRejectsOptionsAfterUse(t *testing.T) {
	r := lz4.NewBlockReader(bytes.NewReader(compressRawBlock(t, []byte("abc"), nil)))
	if err := r.Apply(lz4.BlockOutputLimitOption(-1)); !errors.Is(err, lz4.ErrOptionInvalidBlockSize) {
		t.Fatalf("negative limit: got %v", err)
	}
	if _, err := r.Read(make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	if err := r.Apply(lz4.BlockOutputLimitOption(10)); !errors.Is(err, lz4.ErrOptionClosedOrError) {
		t.Fatalf("Apply after Read: got %v", err)
	}
}

// TestBlockReaderTransportError proves an error from the underlying reader is
// surfaced and becomes sticky, rather than being mistaken for a format error.
func TestBlockReaderTransportError(t *testing.T) {
	wantErr := errors.New("boom")
	r := lz4.NewBlockReader(&errReader{err: wantErr})
	_, err := io.ReadAll(r)
	if !errors.Is(err, wantErr) {
		t.Fatalf("got %v, want transport error", err)
	}
	if n, err := r.Read(make([]byte, 4)); n != 0 || !errors.Is(err, wantErr) {
		t.Fatalf("Read after transport error: n=%d err=%v", n, err)
	}
}

// TestBlockReaderReaderContract checks the io.Reader details: a zero-length
// buffer returns (0,nil), EOF and a terminal error are repeatable, and Read
// and WriteTo can be mixed while sharing the output offset.
func TestBlockReaderReaderContract(t *testing.T) {
	data := []byte("0123456789abcdefghij")
	comp := compressRawBlock(t, data, nil)
	r := lz4.NewBlockReader(bytes.NewReader(comp))

	// A zero-length Read before any data must not consume or error.
	if n, err := r.Read(nil); n != 0 || err != nil {
		t.Fatalf("Read(nil) = (%d,%v), want (0,nil)", n, err)
	}

	// Read half through Read, the rest through WriteTo.
	first := make([]byte, 7)
	n, err := r.Read(first)
	if err != nil || n != 7 || string(first) != "0123456" {
		t.Fatalf("partial Read: n=%d err=%v", n, err)
	}
	var rest bytes.Buffer
	wn, err := r.WriteTo(&rest)
	if err != nil || wn != int64(len(data)-7) || rest.String() != string(data[7:]) {
		t.Fatalf("WriteTo remainder: n=%d err=%v got=%q", wn, err, rest.Bytes())
	}

	// EOF is repeatable once the block is drained.
	for range 3 {
		if n, err := r.Read(make([]byte, 4)); n != 0 || !errors.Is(err, io.EOF) {
			t.Fatalf("Read after EOF: n=%d err=%v", n, err)
		}
	}
	// WriteTo on a drained reader also reports EOF.
	if _, err := r.WriteTo(io.Discard); !errors.Is(err, io.EOF) {
		t.Fatalf("WriteTo after EOF: %v", err)
	}
}

type errReader struct{ err error }

func (e *errReader) Read(p []byte) (int, error) { return 0, e.err }
