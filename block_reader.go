package lz4

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/pierrec/lz4/v4/internal/lz4block"
	"github.com/pierrec/lz4/v4/internal/lz4errors"
)

// defaultBlockOutputLimit bounds the memory a BlockReader will allocate when
// the caller declares no output budget. A raw LZ4 block carries no size field,
// so without a cap a hostile block could make its decoder grow without bound.
const defaultBlockOutputLimit = 64 << 20 // 64 MiB

// Block reader lifecycle states. Unlike the frame Reader, a BlockReader must
// stay readable across many small Reads of one block, so its state does not
// advance on every successful Read: it moves only when the block is fully
// consumed, closed, or failed.
const (
	blockNew    = iota // before the first Read; options may still be applied
	blockRead          // the block has been decoded and bytes remain or are being handed out
	blockEOF           // the whole block has been delivered
	blockClosed        // Close was called
	blockError         // a sticky terminal error was recorded
)

// NewBlockReader returns a reader that decompresses a single raw LZ4 block
// (the block format, not an LZ4 frame) from r.
//
// Unlike UncompressBlock, which needs a destination sized up front, a
// BlockReader takes an io.Reader and owns its output buffer. The maximum
// number of bytes it may produce is declared with BlockOutputLimitOption;
// exceeding it makes the reader fail with ErrOutputLimitExceeded once the
// in-budget prefix has been delivered. Malformed input always reports
// ErrInvalidSourceShortBuffer instead, even when it also passes the budget.
//
// After a failure the reader is in a sticky error state: every Read and
// WriteTo returns the same error until Reset. Close releases the input and
// output references; Reset clears the error and the dictionary history.
//
// A BlockReader is not safe for concurrent use.
func NewBlockReader(r io.Reader) *BlockReader {
	br := &BlockReader{limit: defaultBlockOutputLimit, phase: blockNew}
	br.Reset(r)
	return br
}

// BlockReader decompresses one raw LZ4 block with an explicit output budget
// and sticky terminal errors.
type BlockReader struct {
	phase      aState
	sticky     error // terminal error, set once phase reaches blockError
	src        io.Reader
	limit      int
	presetDict []byte
	dict       []byte

	compressed []byte // fully read input; nil until the first Read needs it
	out        []byte // decoded bytes not yet handed to the caller
	idx        int
	decoded    bool
	// pending is a budget overrun discovered while decoding. It is kept out
	// of the sticky state until out has been delivered, so the in-budget
	// prefix reaches the caller before the error; it then becomes sticky.
	pending error
}

func (*BlockReader) private() {}

// Apply applies the given options. Options can only be set before the first
// Read; applying them while reading or after an error returns
// ErrOptionClosedOrError.
func (r *BlockReader) Apply(options ...BlockOption) error {
	if r.phase == blockError {
		return r.sticky
	}
	if r.phase != blockNew {
		return lz4errors.ErrOptionClosedOrError
	}
	for _, o := range options {
		if err := o(r); err != nil {
			return err
		}
	}
	return nil
}

// BlockOption configures a BlockReader.
type BlockOption func(*BlockReader) error

// BlockOutputLimitOption declares the maximum number of uncompressed bytes
// the BlockReader will produce. A value of zero means no limit besides the
// library's 64 MiB safety cap; a negative value is rejected.
//
// When the decoded block is larger than the limit, the reader first delivers
// the limit bytes that fit the budget and then fails with
// ErrOutputLimitExceeded. Every Read after that returns the same error until
// Reset.
func BlockOutputLimitOption(limit int) BlockOption {
	return func(r *BlockReader) error {
		if limit < 0 {
			return fmt.Errorf("%w: negative output limit %d", lz4errors.ErrOptionInvalidBlockSize, limit)
		}
		if limit == 0 {
			r.limit = defaultBlockOutputLimit
		} else {
			r.limit = limit
		}
		return nil
	}
}

// BlockDictionaryOption sets the dictionary used to decode the block. Only
// the last 64 KiB is used, and the same dictionary must have been used to
// compress it. The dictionary is copied and is also used after a Reset.
func BlockDictionaryOption(dict []byte) BlockOption {
	dict = slices.Clip(bytes.Clone(dict))
	return func(r *BlockReader) error {
		r.presetDict = dict
		r.dict = dict
		return nil
	}
}

// fail records a terminal error and moves to the sticky error phase.
func (r *BlockReader) fail(err error) error {
	r.sticky = err
	r.phase = blockError
	return err
}

// Read implements io.Reader. It buffers the whole compressed block on first
// use, since the raw block format has no framing that would allow streaming
// the input, then decodes it into the output buffer and hands bytes out.
//
// On a budget overrun the prefix within the budget is returned first, and
// ErrOutputLimitExceeded is reported once it has been delivered. A malformed
// block reports ErrInvalidSourceShortBuffer and delivers no decoded bytes.
// The terminal error is sticky: later calls return it unchanged until Reset.
func (r *BlockReader) Read(p []byte) (int, error) {
	switch r.phase {
	case blockError, blockClosed:
		return 0, r.sticky
	case blockEOF:
		return 0, io.EOF
	case blockNew:
		if err := r.decode(); err != nil {
			// A budget overrun is not terminal yet: its prefix must be
			// delivered first. Anything else is a sticky format error.
			if !errors.Is(err, lz4errors.ErrOutputLimitExceeded) {
				return 0, r.fail(err)
			}
		}
		r.phase = blockRead
	}
	if len(p) == 0 {
		return 0, nil
	}
	if r.idx < len(r.out) {
		n := copy(p, r.out[r.idx:])
		r.idx += n
		return n, nil
	}
	// All deliverable bytes are gone. Surface the pending budget error, or
	// finish cleanly at the end of a block that fit the budget.
	if r.pending != nil {
		err := r.pending
		r.pending = nil
		return 0, r.fail(err)
	}
	r.phase = blockEOF
	return 0, io.EOF
}

// decode reads and uncompresses the block in a single budget-aware pass, so
// the allocated output never exceeds the declared limit.
//
// The bounded decoder validates every sequence even after the budget is full:
//   - a malformed block (bad offset, truncated token, trailing garbage) fails
//     with ErrInvalidSourceShortBuffer and delivers nothing, regardless of
//     where the budget sits;
//   - a well-formed block larger than the limit fills dst and returns
//     ErrOutputLimitExceeded; dst then holds the exact in-budget prefix,
//     which is kept with the error recorded as pending so Read delivers the
//     prefix before repeating the error;
//   - a block that fits returns its decoded size with no error.
func (r *BlockReader) decode() error {
	if !r.decoded {
		// The raw block format has no length prefix, so the whole block must
		// be buffered before decoding. A transport error while reading is
		// returned as is; a block that is merely truncated is caught as a
		// format error by the decoder.
		src, err := io.ReadAll(r.src)
		if err != nil {
			return err
		}
		r.compressed = src
		r.decoded = true
	}

	dst := make([]byte, r.limit)
	n, err := lz4block.UncompressBlockBounded(r.compressed, dst, r.dict)
	if err != nil && !errors.Is(err, lz4errors.ErrOutputLimitExceeded) {
		return err
	}
	r.out = dst[:n]
	r.idx = 0
	if err != nil {
		// Well-formed block past the budget: hold the prefix, defer the error.
		r.pending = err
	}
	return err
}

// WriteTo copies the decoded block to w. After a budget overrun it writes the
// in-budget prefix and returns ErrOutputLimitExceeded, which then stays
// sticky for subsequent Reads. On success it returns nil: the block is fully
// consumed, so there is no io.EOF to report.
func (r *BlockReader) WriteTo(w io.Writer) (n int64, err error) {
	switch r.phase {
	case blockError, blockClosed:
		return 0, r.sticky
	case blockEOF:
		return 0, io.EOF
	case blockNew:
		if err := r.decode(); err != nil && !errors.Is(err, lz4errors.ErrOutputLimitExceeded) {
			return 0, r.fail(err)
		}
		r.phase = blockRead
	}
	bn, werr := w.Write(r.out[r.idx:])
	r.idx += bn
	n = int64(bn)
	if werr != nil {
		return n, r.fail(werr)
	}
	if r.pending != nil {
		err := r.pending
		r.pending = nil
		return n, r.fail(err)
	}
	r.phase = blockEOF
	return n, nil
}

// Reset returns the reader to its initial state, reading from reader instead.
// It clears any sticky or pending error and the decoded history, restores the
// preset dictionary and the default output budget, and drops references to
// the previous input and output. No access to reader is performed.
func (r *BlockReader) Reset(reader io.Reader) {
	r.compressed = nil
	r.out = nil
	r.idx = 0
	r.decoded = false
	r.pending = nil
	r.sticky = nil
	r.phase = blockNew
	r.limit = defaultBlockOutputLimit
	r.dict = slices.Clip(bytes.Clone(r.presetDict))
	r.src = reader
}

// Close releases the input and output references and ends the reader. It is
// idempotent: closing a reader that is not in an error state always returns
// nil, and a later Read returns io.EOF until Reset revives the reader. A
// sticky error, if the reader had failed, is returned on every Close and kept
// until Reset.
func (r *BlockReader) Close() error {
	err := r.sticky
	r.compressed = nil
	r.out = nil
	r.idx = 0
	r.src = nil
	r.pending = nil
	if r.phase == blockError {
		return err
	}
	// No failure to report: Close succeeds and stays idempotent. Reads after
	// Close report EOF via the closed phase.
	r.phase = blockClosed
	r.sticky = io.EOF
	return nil
}
