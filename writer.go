package lz4

import (
	"fmt"
	"io"
	"sync"

	"github.com/pierrec/lz4/v4/internal/lz4block"
	"github.com/pierrec/lz4/v4/internal/lz4errors"
	"github.com/pierrec/lz4/v4/internal/lz4stream"
)

var writerStates = []aState{
	noState:     newState,
	newState:    writeState,
	writeState:  closedState,
	closedState: newState,
	errorState:  newState,
}

// NewWriter returns a new LZ4 frame encoder.
func NewWriter(w io.Writer) *Writer {
	zw := &Writer{frame: lz4stream.NewFrame()}
	zw.state.init(writerStates)
	_ = zw.Apply(DefaultBlockSizeOption, DefaultChecksumOption, DefaultConcurrency, defaultOnBlockDone)
	zw.Reset(w)
	return zw
}

// Writer allows writing an LZ4 stream.
//
// Write, ReadFrom, Flush, Close, Reset and Apply are safe for concurrent use;
// a call holds a single lock for its duration, so two calls are serialized,
// never interleaved.
type Writer struct {
	mu         sync.Mutex
	state      _State
	src        io.Writer                 // destination writer
	level      lz4block.CompressionLevel // how hard to try
	num        int                       // concurrency level
	frame      *lz4stream.Frame          // frame being built
	data       []byte                    // pending data
	idx        int                       // size of pending data
	handler    func(int)
	legacy     bool
	dict       []byte
	compressor *lz4block.Compressor
}

func (*Writer) private() {}

func (w *Writer) Apply(options ...Option) (err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	defer w.state.check(&err)
	switch w.state.state {
	case newState:
	case errorState:
		return w.state.err
	default:
		return lz4errors.ErrOptionClosedOrError
	}
	w.reset(w.src)
	for _, o := range options {
		if err = o(w); err != nil {
			return
		}
	}
	return
}

func (w *Writer) isNotConcurrent() bool {
	return w.num == 1
}

// init sets up the Writer when in newState. It does not change the Writer state.
func (w *Writer) init() error {
	if len(w.dict) > 0 {
		w.num = 1
		if w.compressor == nil {
			w.compressor = new(lz4block.Compressor)
		}
	}
	if !w.legacy && !w.frame.Descriptor.Flags.BlockSizeIndex().IsValid() {
		// Block8Mb is only valid in legacy frames, and LegacyOption may be
		// applied after BlockSizeOption.
		return fmt.Errorf("%w: %d is only valid for legacy frames", lz4errors.ErrOptionInvalidBlockSize, lz4block.Block8Mb)
	}
	w.frame.InitW(w.src, w.num, w.legacy)
	size := w.frame.BlockSizeIndex()
	w.data = size.Get()
	w.idx = 0
	return w.frame.Descriptor.Write(w.frame, w.src)
}

// Write implements io.Writer. It only ever hands data it owns to the
// compressor: a full block is passed without a copy when the compression is
// synchronous (concurrency 1) and the call returns before the caller may reuse
// buf; every other path copies buf into a Writer-owned buffer first.
func (w *Writer) Write(buf []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.state.state == closedState {
		return 0, lz4errors.ErrWriterClosed
	}
	defer w.state.check(&err)
	switch w.state.state {
	case writeState:
	case errorState:
		return 0, w.state.err
	case newState:
		if err = w.init(); w.state.next(err) {
			return
		}
	default:
		return 0, w.state.fail()
	}

	zn := len(w.data)
	for len(buf) > 0 {
		if w.isNotConcurrent() && w.idx == 0 && len(buf) >= zn {
			// Avoid a copy as there is enough data for a block. The
			// compression is synchronous, so the block is fully written before
			// Write returns and the caller may reuse buf.
			if err = w.write(buf[:zn], false); err != nil {
				return
			}
			n += zn
			buf = buf[zn:]
			continue
		}
		// Accumulate the data to be compressed.
		m := copy(w.data[w.idx:], buf)
		n += m
		w.idx += m
		buf = buf[m:]

		if w.idx < len(w.data) {
			// Buffer not filled.
			return
		}

		// Buffer full.
		if err = w.write(w.data, true); err != nil {
			return
		}
		if !w.isNotConcurrent() {
			size := w.frame.BlockSizeIndex()
			w.data = size.Get()
		}
		w.idx = 0
	}
	return
}

// write compresses data and submits the resulting block. In concurrent mode it
// returns as soon as the block is queued: the block is written to the
// destination in submission order by a manager goroutine, and Flush or Close
// acts as a barrier that waits for it.
func (w *Writer) write(data []byte, safe bool) error {
	if w.isNotConcurrent() {
		block := w.frame.Blocks.Block
		err := block.CompressWithDict(w.frame, data, w.level, w.compressor, w.dict).Write(w.frame, w.src)
		w.handler(len(block.Data))
		return err
	}
	c := w.frame.Blocks.Submit()
	go func(c chan *lz4stream.FrameDataBlock, data []byte, safe bool) {
		b := lz4stream.NewFrameDataBlock(w.frame)
		b.Compress(w.frame, data, w.level)
		// Report the block before handing it over, so that Close, which waits
		// for every block to be written, returns after all the reports.
		w.handler(len(b.Data))
		c <- b
		<-c
		b.Close(w.frame)
		if safe {
			// safe to put it back as the last usage of it was FrameDataBlock.Write() called before c is closed
			lz4block.Put(data)
		}
	}(c, data, safe)

	return nil
}

// Flush establishes a barrier: when it returns nil, all data written before
// the call has been compressed and handed to the underlying writer, and data
// written afterwards starts a new block. It returns the first error that
// reached the underlying writer; once that happens the Writer is in a sticky
// error state and every later Write, ReadFrom, Flush or Close fails with the
// same error until Reset.
func (w *Writer) Flush() (err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.state.state == closedState {
		// A closed writer has nothing left to flush.
		return nil
	}
	defer w.state.check(&err)
	switch w.state.state {
	case writeState:
	case errorState:
		return w.state.err
	case newState:
		if err = w.init(); w.state.next(err) {
			return
		}
	default:
		return nil
	}

	if w.idx > 0 {
		if err = w.write(w.data[:w.idx], true); err != nil {
			return err
		}
		if !w.isNotConcurrent() {
			size := w.frame.Descriptor.Flags.BlockSizeIndex()
			w.data = size.Get()
		}
		w.idx = 0
	}
	// Wait for every queued block to reach the destination and surface its
	// error: a Flush is a barrier, not merely a queue insertion.
	return w.frame.Blocks.BarrierW()
}

// Close closes the Writer, flushing any unwritten data to the underlying writer
// without closing it. Close is bounded and idempotent: a second call does not
// re-flush or rewrite committed blocks and returns the first call's result.
// After an error the frame is torn down and the block buffers released before
// the sticky error is returned, without forging an end mark or success.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	switch w.state.state {
	case closedState:
		return nil
	case errorState:
		// Tear down whatever the failing call left behind and keep reporting
		// the sticky error; the frame is already invalid, so nothing is written.
		w.teardown()
		return w.state.err
	}

	// A frame with no Write or Flush yet still needs its descriptor.
	if w.state.state == newState {
		if err := w.init(); err != nil {
			w.teardown()
			w.state.next(err)
			return err
		}
		w.state.next(nil)
	}

	err := w.flushLocked()
	if err == nil {
		err = w.frame.CloseW(w.src, w.num)
	}
	if err != nil {
		// Drain in-flight compression goroutines and release the buffers; do
		// not write the end mark of a frame whose blocks already failed.
		w.teardown()
		w.state.next(err)
		// Return the original error; the sticky copy in w.state is wrapped
		// but preserves it through errors.Is.
		return err
	}
	// It is now safe to free the buffer.
	lz4block.Put(w.data)
	w.data = nil
	w.state.next(nil)
	return nil
}

// flushLocked emits the pending block, if any, and waits for every queued
// block to reach the destination, returning the first write error. The caller
// holds w.mu and the Writer must be in writeState.
func (w *Writer) flushLocked() error {
	if w.idx > 0 {
		if err := w.write(w.data[:w.idx], true); err != nil {
			return err
		}
		if !w.isNotConcurrent() {
			size := w.frame.Descriptor.Flags.BlockSizeIndex()
			w.data = size.Get()
		}
		w.idx = 0
	}
	return w.frame.Blocks.BarrierW()
}

// teardown stops the frame machinery and returns w.data to the pool. It is
// safe to call repeatedly and without a prior successful Close.
func (w *Writer) teardown() {
	w.frame.Reset(w.num)
	if w.data != nil {
		lz4block.Put(w.data)
		w.data = nil
	}
	w.idx = 0
}

// Reset clears the state of the Writer w such that it is equivalent to its
// initial state from NewWriter, but instead writing to writer.
// Reset keeps the previous options unless overwritten by the supplied ones.
// No access to writer is performed.
//
// w.Close must be called before Reset or pending data may be dropped.
func (w *Writer) Reset(writer io.Writer) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.reset(writer)
}

// reset is the lock-free part of Reset; the caller holds w.mu (or the Writer
// is still under construction).
func (w *Writer) reset(writer io.Writer) {
	w.teardown()
	w.state.reset()
	w.src = writer
}

// ReadFrom reads from r until EOF or an error and compresses the data into the
// Writer destination. As with Write, data that does not fill a block stays
// buffered until the next Write, ReadFrom, Flush or Close.
func (w *Writer) ReadFrom(r io.Reader) (n int64, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	switch w.state.state {
	case writeState:
	case closedState:
		return 0, lz4errors.ErrWriterClosed
	case errorState:
		return 0, w.state.err
	case newState:
		if err = w.init(); w.state.next(err) {
			return
		}
	default:
		return 0, w.state.fail()
	}
	defer w.state.check(&err)

	size := w.frame.BlockSizeIndex()
	for {
		var rn int
		rn, err = io.ReadFull(r, w.data[w.idx:])
		n += int64(rn)
		w.idx += rn
		switch err {
		case nil:
		case io.EOF, io.ErrUnexpectedEOF:
			return n, nil
		default:
			return
		}
		if err = w.write(w.data, true); err != nil {
			return
		}
		if !w.isNotConcurrent() {
			w.data = size.Get()
		}
		w.idx = 0
	}
}
