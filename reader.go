package lz4

import (
	"bytes"
	"io"
	"slices"

	"github.com/pierrec/lz4/v4/internal/lz4block"
	"github.com/pierrec/lz4/v4/internal/lz4errors"
	"github.com/pierrec/lz4/v4/internal/lz4stream"
)

var readerStates = []aState{
	noState:     newState,
	errorState:  newState,
	newState:    readState,
	readState:   closedState,
	closedState: newState,
}

// NewReader returns a new LZ4 frame decoder.
func NewReader(r io.Reader) *Reader {
	return newReader(r, false)
}

func newReader(r io.Reader, legacy bool) *Reader {
	zr := &Reader{frame: lz4stream.NewFrame()}
	zr.state.init(readerStates)
	_ = zr.Apply(DefaultConcurrency, defaultOnBlockDone)
	zr.Reset(r)
	return zr
}

// Reader allows reading an LZ4 stream.
//
// A stream may hold several frames, which are read in order; the checksums,
// descriptor and content size of each are checked against that frame alone.
// A frame that ends in the middle fails the read with io.ErrUnexpectedEOF (or
// a more specific error), and the Reader stays failed until Reset, so the
// bytes that follow a truncated frame are never taken for a new one. Every
// later call reports the same error, except Read and WriteTo, which report
// io.EOF once the whole stream has been read.
type Reader struct {
	state      _State
	src        io.Reader        // source reader
	num        int              // concurrency level requested by the options
	frameNum   int              // concurrency level the current frame was started with
	frame      *lz4stream.Frame // frame being read
	data       []byte           // block buffer allocated in non-concurrent mode
	reads      chan []byte      // pending data
	idx        int              // size of pending data
	handler    func(int)
	cum        uint32
	dict       []byte
	presetDict []byte
}

func (*Reader) private() {}

func (r *Reader) Apply(options ...Option) (err error) {
	defer r.state.check(&err)
	switch r.state.state {
	case newState:
	case errorState:
		return r.state.err
	default:
		return lz4errors.ErrOptionClosedOrError
	}
	for _, o := range options {
		if err = o(r); err != nil {
			return
		}
	}
	return
}

// Size returns the size of the current frame's uncompressed data, if set in the stream.
func (r *Reader) Size() int {
	switch r.state.state {
	case readState, closedState:
		if r.frame.Descriptor.Flags.Size() {
			return int(r.frame.Descriptor.ContentSize)
		}
	}
	return 0
}

func (r *Reader) isNotConcurrent() bool {
	return r.frameNum == 1
}

func (r *Reader) init() error {
	if len(r.dict) > 0 {
		r.num = 1
	}
	r.frameNum = r.num
	err := r.frame.ParseHeaders(r.src)
	if err != nil {
		return err
	}
	if !r.frame.Descriptor.Flags.BlockIndependence() {
		// We can't decompress dependent blocks concurrently.
		// Instead of throwing an error to the user, silently drop concurrency
		r.frameNum = 1
	}
	data, err := r.frame.InitR(r.src, r.frameNum)
	if err != nil {
		return err
	}
	r.reads = data
	r.idx = 0
	size := r.frame.BlockSizeIndex()
	r.data = size.Get()
	r.cum = 0
	return nil
}

// drainFrame is called when the frame's blocks are exhausted but the frame
// is not: it reads and verifies the frame's trailer (content checksum and
// content size). It reports whether the frame was legacy, which has no
// trailer, no end mark and is complete at the end of its blocks.
func (r *Reader) drainFrame() (legacy bool, err error) {
	if err := r.frame.CloseR(r.src); err != nil {
		return false, err
	}
	return r.frame.IsLegacy(), nil
}

// nextFrame ends the current frame and starts the next one. It reports
// io.EOF, without starting a frame, when the source ends cleanly here.
func (r *Reader) nextFrame() error {
	r.Reset(r.src)
	if err := r.init(); err != nil {
		if err == io.EOF {
			// A clean end of the stream is not a new frame.
			return io.EOF
		}
		return err
	}
	r.state.next(nil)
	return nil
}

// finish completes the reader: the stream was read to its end. It releases
// the block buffer and leaves every later Read and WriteTo reporting io.EOF.
func (r *Reader) finish() {
	r.state.end()
	lz4block.Put(r.data)
	r.data = nil
}

// Read reads up to len(buf) bytes from the stream.
//
// Once the whole stream has been read, Read returns io.EOF. After a frame
// fails, it returns the error of that frame, unchanged, on every later call
// until Reset.
func (r *Reader) Read(buf []byte) (n int, err error) {
	defer r.state.check(&err)
	switch r.state.state {
	case readState:
	case closedState:
		// The stream was read to its end.
		return 0, io.EOF
	case errorState:
		return 0, r.state.err
	case newState:
		// First initialization.
		if err = r.init(); r.state.next(err) {
			return
		}
	default:
		return 0, r.state.fail()
	}
	for len(buf) > 0 {
		var bn int
		if r.idx == 0 {
		read:
			if r.isNotConcurrent() {
				bn, err = r.read(buf)
			} else {
				lz4block.Put(r.data)
				var ok bool
				r.data, ok = <-r.reads
				if !ok {
					// The read loop has exited: something went wrong or we are done.
					// An empty block is not a signal, as it may precede an error.
					err = r.frame.Blocks.ErrorR()
				}
			}
			switch err {
			case nil:
			case lz4errors.ErrEndOfStream:
				// The frame's end mark was read: verify its trailer, then
				// start the next frame, if any.
				if _, err = r.drainFrame(); err != nil {
					return
				}
				if err = r.nextFrame(); err != nil {
					if err == io.EOF {
						// The stream is complete.
						r.finish()
					}
					return
				}
				goto read
			case io.EOF:
				// The frame's blocks end at the source's end: a legacy frame
				// is complete, but a non-legacy one is truncated before its
				// end mark.
				legacy, derr := r.drainFrame()
				if derr != nil {
					err = derr
					return
				}
				if !legacy {
					err = io.ErrUnexpectedEOF
					return
				}
				r.finish()
				return
			default:
				return
			}
		}
		if bn == 0 {
			// Fill buf with buffered data.
			bn = copy(buf, r.data[r.idx:])
			r.idx += bn
			if r.idx == len(r.data) {
				// All data read, get ready for the next Read.
				r.idx = 0
			}
		}
		buf = buf[bn:]
		n += bn
		r.handler(bn)
	}
	return
}

// read uncompresses the next block as follows:
//   - if buf has enough room, the block is uncompressed into it directly
//     and the length of used space is returned
//   - else, the uncompressed data is stored in r.data and 0 is returned
func (r *Reader) read(buf []byte) (int, error) {
	block := r.frame.Blocks.Block
	_, err := block.Read(r.frame, r.src, r.cum)
	if err != nil {
		return 0, err
	}
	var direct bool
	dst := r.data[:cap(r.data)]
	if len(buf) >= len(dst) {
		// Decompress directly into buf.
		// trim r.data as it is not needed now
		r.data = r.data[:0]
		direct = true
		// No more than a block, so that a block that decompresses to
		// more than the frame's block size is rejected as it is otherwise.
		dst = buf[:len(dst)]
	}
	dst, err = block.Uncompress(r.frame, dst, r.dict, true)
	if err != nil {
		return 0, err
	}
	if !r.frame.Descriptor.Flags.BlockIndependence() {
		if len(r.dict)+len(dst) > 128*1024 {
			preserveSize := max(64*1024-len(dst), 0)
			r.dict = r.dict[len(r.dict)-preserveSize:]
		}
		r.dict = append(r.dict, dst...)
	}
	r.cum += uint32(len(dst))
	if direct {
		r.data = r.data[:0]
		return len(dst), nil
	}
	r.data = dst
	return 0, nil
}

// Reset clears the state of the Reader r such that it is equivalent to its
// initial state from NewReader, but instead reading from reader.
// No access to reader is performed.
// With concurrency, Reset waits for a read of the previous source that is in
// progress to return; the previous source is then no longer used.
//
// Reset also discards the previous stream: a frame that failed or was left
// unfinished is not resumed, the frame's checksum and size state is cleared,
// and the dictionary is set back to the one from DictionaryOption, dropping
// the data dependent blocks accumulated from the previous stream.
func (r *Reader) Reset(reader io.Reader) {
	lz4block.Put(r.data)
	r.data = nil
	r.frame.Reset(r.frameNum)
	r.state.reset()
	r.src = reader
	r.reads = nil
	r.idx = 0
	r.frameNum = 0
	r.dict = slices.Clip(bytes.Clone(r.presetDict))
}

// WriteTo efficiently uncompresses the data from the Reader underlying source to w.
//
// Like Read, it verifies each frame separately and stops at the first broken
// frame; once the stream is complete it returns nil, and returns io.EOF if
// called again.
func (r *Reader) WriteTo(w io.Writer) (n int64, err error) {
	switch r.state.state {
	case readState:
	case closedState:
		return 0, io.EOF
	case errorState:
		return 0, r.state.err
	case newState:
		if err = r.init(); r.state.next(err) {
			if err == io.EOF {
				// An empty source is an empty stream; WriteTo reports
				// success rather than io.EOF.
				err = nil
				r.state.end()
			}
			return
		}
	default:
		return 0, r.state.fail()
	}
	// A successful WriteTo moves the state from readState to closedState,
	// unless the stream reached its end first. A failure is stored, so later
	// calls report it too.
	defer func() {
		switch r.state.state {
		case readState:
			r.state.next(err)
		case closedState, errorState:
		default:
			r.state.check(&err)
		}
	}()
	defer func() {
		if err != nil {
			// Don't leave a concurrent read-ahead using the source, as
			// after a failed write to w.
			r.frame.Blocks.StopR()
		}
	}()

	if r.idx > 0 {
		// A previous Read left part of the current block unconsumed.
		var bn int
		bn, err = w.Write(r.data[r.idx:])
		n += int64(bn)
		r.idx = 0
		if err != nil {
			return
		}
		r.handler(bn)
	}

	for {
		var bn int
		var dst []byte
	read:
		if r.isNotConcurrent() {
			// Uncompress into r.data, which init sizes for every frame.
			_, err = r.read(nil)
			dst = r.data
			bn = len(dst)
		} else {
			var ok bool
			dst, ok = <-r.reads
			bn = len(dst)
			if !ok {
				// The read loop has exited: something went wrong or we are done.
				err = r.frame.Blocks.ErrorR()
			}
		}
		switch err {
		case nil:
		case lz4errors.ErrEndOfStream:
			// The frame's end mark was read: verify its trailer, then
			// start the next frame, if any.
			if _, err = r.drainFrame(); err != nil {
				return
			}
			if err = r.nextFrame(); err != nil {
				if err == io.EOF {
					// The stream is complete; WriteTo reports success.
					err = nil
					r.state.end()
				}
				return
			}
			goto read
		case io.EOF:
			// The frame's blocks end at the source's end: a legacy frame is
			// complete, but a non-legacy one is truncated before its end
			// mark.
			legacy, derr := r.drainFrame()
			if derr != nil {
				err = derr
				return
			}
			if !legacy {
				err = io.ErrUnexpectedEOF
				return
			}
			// A legacy frame is complete at the end of its blocks; success.
			err = nil
			r.state.end()
			return
		default:
			return
		}
		r.handler(bn)
		bn, err = w.Write(dst)
		if !r.isNotConcurrent() {
			lz4block.Put(dst)
		}
		n += int64(bn)
		if err != nil {
			return
		}
	}
}

// ValidFrameHeader returns a bool indicating if the given bytes slice matches a LZ4 header.
func ValidFrameHeader(in []byte) (bool, error) {
	f := lz4stream.NewFrame()
	err := f.ParseHeaders(bytes.NewReader(in))
	if err == nil {
		return true, nil
	}
	if err == lz4errors.ErrInvalidFrame {
		return false, nil
	}
	return false, err
}
