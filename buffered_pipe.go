package lz4

import (
	"io"
	"sync"
)

// defaultPipeBuffer bounds how many compressed bytes one direction may queue
// ahead of its decompressor. It is only a bound on how far a producer can run
// ahead: a single frame write may be larger and is accepted in one piece.
const defaultPipeBuffer = 256 << 10

// bufferedPipe is a one-direction byte queue with a terminal state. Data
// written before the pipe is closed is always read out before io.EOF or the
// close error is reported, so accepted bytes are never dropped.
//
// With a positive capacity, Write blocks while the buffer is full; with a
// capacity of zero it never blocks, which lets a decompressor finish its frame
// even when the receiving end is not reading yet. One writer and one reader
// may use it concurrently; Close may be called from any goroutine.
type bufferedPipe struct {
	mu   sync.Mutex
	cond *sync.Cond

	buf  []byte // data not yet read; buf[off:] is pending
	off  int
	cap  int // 0 means unbounded
	done bool
	err  error // terminal error, reported after buf drains
}

func newBufferedPipe() *bufferedPipe {
	return newBufferedPipeCap(defaultPipeBuffer)
}

func newBufferedPipeCap(capacity int) *bufferedPipe {
	p := &bufferedPipe{cap: capacity, buf: make([]byte, 0, capacity)}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// ensureCond lazily binds the condition so the zero value is usable too.
// The caller must hold p.mu.
func (p *bufferedPipe) ensureCond() {
	if p.cond == nil {
		p.cond = sync.NewCond(&p.mu)
	}
}

func (p *bufferedPipe) setUnbounded() {
	p.mu.Lock()
	p.cap = 0
	p.mu.Unlock()
}

// Write queues b, blocking while a bounded buffer is full. Data is copied:
// callers reuse their frames and block buffers.
//
// All of b is accepted at once, or an error is returned, so a Write waiting on
// room never reports a partial count that the caller cannot attribute. A write
// larger than the capacity is accepted in one piece once there is room, which
// is what bounds how far a producer can run ahead without deadlocking on a
// single frame.
func (p *bufferedPipe) Write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ensureCond()

	for {
		if p.done {
			if p.err != nil {
				return 0, p.err
			}
			return 0, io.ErrClosedPipe
		}
		if p.cap > 0 && len(p.buf)-p.off >= p.cap && len(b) > 0 {
			p.cond.Wait()
			continue
		}
		break
	}
	if len(b) == 0 {
		return 0, nil
	}
	p.compactLocked()
	p.buf = append(p.buf, b...)
	p.cond.Broadcast()
	return len(b), nil
}

// compactLocked reclaims the consumed head of buf. The caller must hold p.mu.
func (p *bufferedPipe) compactLocked() {
	if p.off == 0 {
		return
	}
	p.buf = p.buf[:copy(p.buf, p.buf[p.off:])]
	p.off = 0
}

// Read returns queued bytes, blocking until some are available or the pipe
// closes. A close after buffered data drains reports io.EOF, or the error
// passed to CloseWithError.
func (p *bufferedPipe) Read(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ensureCond()

	for len(p.buf) == p.off {
		if p.done {
			if p.err != nil {
				return 0, p.err
			}
			return 0, io.EOF
		}
		if len(b) == 0 {
			return 0, nil
		}
		p.cond.Wait()
	}

	n := copy(b, p.buf[p.off:])
	p.off += n
	if p.off == len(p.buf) {
		p.buf = p.buf[:0]
		p.off = 0
	}
	p.cond.Broadcast()
	return n, nil
}

// Close finishes the pipe after its buffered data: Read then reports io.EOF.
func (p *bufferedPipe) Close() error { return p.close(nil) }

// CloseWithError is Close, but Read reports err once the buffer drains.
func (p *bufferedPipe) CloseWithError(err error) error { return p.close(err) }

func (p *bufferedPipe) close(err error) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ensureCond()
	if p.done {
		return nil
	}
	p.done = true
	p.err = err
	p.cond.Broadcast()
	return nil
}

// release drops the retained buffer after the pipe reached its terminal state
// and every goroutine using it has been joined. A later Write still fails on
// the terminal state; a later Read reports it, since len(buf) == off == 0.
func (p *bufferedPipe) release() {
	p.mu.Lock()
	p.buf = nil
	p.off = 0
	p.cap = 0
	p.cond.Broadcast()
	p.mu.Unlock()
}
