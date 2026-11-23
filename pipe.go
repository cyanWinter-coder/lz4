package lz4

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/pierrec/lz4/v4/internal/lz4errors"
)

// StreamPipe creates a synchronous, in-memory, bidirectional LZ4 stream. The
// two ends are peers of one pipe: what one end writes is compressed and
// returned, decompressed, by the other end's Read.
//
// Both peers share a single lifecycle with explicit state boundaries:
//
//   - the ends share one cancellation signal, but each direction keeps its own
//     real write and decompression error. The first failure of either becomes
//     the terminal state of the whole pipe, and every later Read, Write or
//     Close on either end returns it unchanged;
//   - a graceful Close first flushes and finishes the end's frame, so data
//     already accepted by Write is decompressed and delivered before the peer
//     sees io.EOF. The reverse direction stays usable until its own end closes;
//   - a failure in either direction, or a deadline, wakes both ends without
//     waiting for a timeout: blocked Reads and Writes return the terminal
//     error, and Done is closed;
//   - Close may be called concurrently from both ends and repeatedly; the
//     decompression goroutines and buffers are released exactly once. Wait
//     joins that teardown.
//
// An end is meant for one reader and one writer at a time, like the Reader and
// Writer types. Read and write deadlines are supported through SetDeadline.
//
// The options configure the frame writers and readers behind both ends. They
// are the usual Option values (BlockSizeOption, BlockChecksumOption,
// ChecksumOption, CompressionLevelOption, DictionaryOption and so on); each is
// applied to both writers and to the decompressors where it applies.
// Compression runs in the calling goroutine, so a Writer concurrency above one
// is rejected with ErrOptionNotApplicable: the pipe's lifecycle owns every
// goroutine it starts.
func StreamPipe(opts ...Option) (a, b *PipeEnd, err error) {
	life := newPipeLifecycle()
	p := &pipe{life: life}

	// ab carries the frames a compresses and b decompresses; ba mirrors it.
	ab := newDirection()
	ba := newDirection()

	// Each end compresses into the direction the peer decompresses, and reads
	// the bytes the peer compressed.
	a = &PipeEnd{
		pipe: p, index: 0,
		w:     NewWriter(ab.frame),
		r:     NewReader(ba.frame),
		frame: ab.frame,
		out:   ba.out,
	}
	b = &PipeEnd{
		pipe: p, index: 1,
		w:     NewWriter(ba.frame),
		r:     NewReader(ab.frame),
		frame: ba.frame,
		out:   ab.out,
	}
	p.ends = [2]*PipeEnd{a, b}
	p.dirs = [2]*direction{ab, ba}

	// The pipe's own lifecycle owns every goroutine: an inline compressor lets
	// Close and a failure reach a quiescent state without joining a pool. A
	// caller asking for concurrency is rejected rather than silently
	// overridden, so the teardown never waits on goroutines it does not own.
	if err = ConcurrencyOption(1)(a.w); err != nil {
		return nil, nil, err
	}
	if err = ConcurrencyOption(1)(b.w); err != nil {
		return nil, nil, err
	}
	for _, o := range opts {
		for _, w := range []*Writer{a.w, b.w} {
			if err = o(w); err != nil {
				return nil, nil, err
			}
		}
		if a.w.num != 1 || b.w.num != 1 {
			return nil, nil, fmt.Errorf("%w: StreamPipe compresses in the calling goroutine", lz4errors.ErrOptionNotApplicable)
		}
	}
	// The decompressors need the options that concern reading, such as the
	// dictionary; writer-only options are not errors for them. Concurrency is
	// rejected here too: a concurrent Reader starts its own read-ahead, which
	// the pipe lifecycle would not join.
	for _, o := range opts {
		for _, r := range []*Reader{a.r, b.r} {
			if err = o(r); err != nil && !errors.Is(err, lz4errors.ErrOptionNotApplicable) {
				return nil, nil, err
			}
		}
		if a.r.num != 1 || b.r.num != 1 {
			return nil, nil, fmt.Errorf("%w: StreamPipe decompresses in the pipe's own goroutines", lz4errors.ErrOptionNotApplicable)
		}
	}

	p.start(ab, b.r) // b decompresses a's frames
	p.start(ba, a.r) // a decompresses b's frames
	return a, b, nil
}

// direction is one half of the pipe: the compressed frame stream produced by
// one end and the decompressed bytes served to the other.
type direction struct {
	frame *bufferedPipe // bounded: compressed frames from the sending end
	out   *bufferedPipe // unbounded: decompressed bytes for the receiving end
}

func newDirection() *direction {
	d := &direction{frame: newBufferedPipe(), out: newBufferedPipe()}
	// The receiving end may read late, or never; an unbounded output lets the
	// decompressor finish its frame, so a graceful close never depends on the
	// peer draining. Only unread data is retained.
	d.out.setUnbounded()
	return d
}

// pipe is the shared lifecycle of two PipeEnd peers.
type pipe struct {
	life *pipeLifecycle
	ends [2]*PipeEnd
	dirs [2]*direction
	wg   sync.WaitGroup // the two decompression goroutines
}

// start runs the decompression loop of one direction: it copies the frames the
// peer compressed through d.frame into d.out. A real decode error is the
// pipe's terminal state; a clean end of the frame is a graceful completion.
func (p *pipe) start(d *direction, r *Reader) {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		_, err := r.WriteTo(d.out)
		if errors.Is(err, io.EOF) {
			// End of stream is the normal completion of a direction, whichever
			// path of the reader reports it.
			err = nil
		}
		if err != nil {
			// Record the real decompression error and wake both ends: abort
			// breaks the other direction's pipes with it, so a blocked Read or
			// Write on either end returns it.
			p.fail(err)
			return
		}
		_ = d.out.Close()
	}()
}

// fail records err as the pipe's terminal state, breaks both internal pipes so
// blocked Reads and Writes return it, and asks for the single teardown.
func (p *pipe) fail(err error) {
	if err == nil {
		return
	}
	p.life.fail(err)
	p.abort()
	p.requestTeardown()
}

// abort breaks every internal pipe with the terminal error. Idempotent.
func (p *pipe) abort() {
	err := p.life.terminalErr()
	for _, d := range p.dirs {
		_ = d.frame.CloseWithError(err)
		_ = d.out.CloseWithError(err)
	}
}

func (p *pipe) requestTeardown() {
	p.life.requestTeardown(p.teardown)
}

// Wait blocks until the single teardown has run: every decompression goroutine
// and frame finisher has exited and the compressors are closed.
func (p *pipe) Wait() {
	<-p.life.done
}

// teardown joins everything the lifecycle started. It runs once, from its own
// goroutine, after a failure or once both ends have closed gracefully.
func (p *pipe) teardown() {
	defer close(p.life.done)
	p.life.cancel()
	if p.life.failed() {
		p.abort()
	}
	// Join the graceful finishers first: each closes its writer and its frame
	// pipe, which is what lets the decompressors reach the end of the stream.
	p.life.waitFinishers()
	p.wg.Wait()
	// Every goroutine has exited. Compressed bytes still queued can no longer
	// be decoded, so release those buffers. After a failure no Read delivers
	// the decompressed bytes either; after a graceful close they are kept, as
	// the peer may drain them with a Read after Wait.
	failed := p.life.failed()
	for _, e := range p.ends {
		e.clearDeadlines()
		e.frame.release()
		if failed {
			e.out.release()
		}
		// A Writer that failed before Close never returned its block buffer.
		// Reset puts it back; writeMu orders this after any Write that was
		// still finishing when the pipe was aborted, and a later Write returns
		// the terminal error before touching the Writer.
		e.writeMu.Lock()
		e.w.Reset(io.Discard)
		e.writeMu.Unlock()
		e.r.Reset(nil)
	}
}

// PipeEnd is one end of a StreamPipe. Read decompresses what the peer wrote;
// Write compresses for the peer; Close finishes this end's write half.
type PipeEnd struct {
	pipe  *pipe
	index int
	w     *Writer       // compresses into the peer's source
	r     *Reader       // decompresses the peer's writes
	frame *bufferedPipe // this end's compressed frames, finished by Close
	out   *bufferedPipe // decompressed bytes this end's Read serves

	writeMu sync.Mutex // serializes compression and the frame finisher
	closeMu sync.Mutex // guards closed
	closed  bool       // this end's write half is closed

	deadlineMu sync.Mutex
	readTimer  *time.Timer
	writeTimer *time.Timer
}

var (
	_ io.ReadWriteCloser = (*PipeEnd)(nil)
	_ interface {
		SetReadDeadline(time.Time) error
		SetWriteDeadline(time.Time) error
		SetDeadline(time.Time) error
	} = (*PipeEnd)(nil)
)

// Done returns the channel closed when the pipe's shared cancellation signal
// fires, whether because an end failed, a deadline was reached, or both ends
// closed.
func (e *PipeEnd) Done() <-chan struct{} {
	return e.pipe.life.ctx.Done()
}

// Err returns the terminal error of the pipe shared by both ends, or nil while
// it is alive or was closed gracefully.
func (e *PipeEnd) Err() error {
	return e.pipe.life.err()
}

// Wait blocks until the pipe's single teardown has completed, so every
// goroutine it started has exited and its buffers are released.
func (e *PipeEnd) Wait() {
	e.pipe.Wait()
}

// Read decompresses data written by the peer. Data accepted before a graceful
// peer Close is delivered first, then io.EOF. After a failure, every Read
// returns the shared terminal error.
func (e *PipeEnd) Read(p []byte) (int, error) {
	if err := e.pipe.life.err(); err != nil {
		return 0, err
	}
	n, err := e.out.Read(p)
	if err != nil {
		// Report the lifecycle error, not the internal pipe's wrapper.
		if t := e.pipe.life.err(); t != nil {
			return n, t
		}
	}
	return n, err
}

// Write compresses p for the peer. It returns once the data is accepted, which
// may be before the peer reads it. Data accepted before a failure is still
// decompressed; after a failure Write returns the shared terminal error.
func (e *PipeEnd) Write(p []byte) (int, error) {
	if err := e.pipe.life.err(); err != nil {
		return 0, err
	}
	e.writeMu.Lock()
	defer e.writeMu.Unlock()

	// close reads the flag under its own lock; holding writeMu across the
	// check and the write keeps the finisher from closing the Writer between
	// them.
	e.closeMu.Lock()
	closed := e.closed
	e.closeMu.Unlock()
	if closed {
		if err := e.pipe.life.err(); err != nil {
			return 0, err
		}
		return 0, ErrPipeClosed
	}
	if err := e.pipe.life.err(); err != nil {
		return 0, err
	}
	n, err := e.w.Write(p)
	if err != nil {
		e.pipe.fail(err)
		return n, e.pipe.life.err()
	}
	return n, nil
}

// Close half-closes the end: the pending block and the frame end mark are
// flushed, so the peer reaches io.EOF after receiving every accepted byte. The
// other direction stays usable. Closing both ends, or any failure, releases
// the pipe's resources exactly once; repeated and concurrent calls are safe
// and return the shared terminal state.
func (e *PipeEnd) Close() error {
	return e.close(nil)
}

// CloseWithError fails this end and, through the shared lifecycle, the whole
// pipe, waking the peer with err. It is the explicit failure counterpart of
// Close.
func (e *PipeEnd) CloseWithError(err error) error {
	if err == nil || errors.Is(err, io.EOF) {
		// A successful end of stream is not a failure; Close reports io.EOF to
		// the peer on its own once the frame is finished.
		return e.Close()
	}
	return e.close(err)
}

func (e *PipeEnd) close(err error) error {
	e.clearDeadlines()

	e.closeMu.Lock()
	first := !e.closed
	e.closed = true
	e.closeMu.Unlock()
	if !first {
		return e.pipe.life.err()
	}

	if err != nil {
		e.pipe.fail(err)
		return e.pipe.life.err()
	}

	// Graceful half close. Reserve the finisher before recording the close, so
	// a concurrent failure that starts the teardown after seeing the first
	// closed end still waits for this end's frame to be finished. The flush
	// runs off this goroutine because the frame pipe may still be draining on
	// the peer.
	if !e.pipe.life.registerFinisher() {
		// A teardown already started after a failure; abort unwinds this end.
		return e.pipe.life.err()
	}
	e.pipe.life.markClosed(e.index)
	go e.finishFrame()
	e.pipe.requestTeardown()
	return e.pipe.life.err()
}

// finishFrame flushes the pending block and the frame end mark, then ends the
// frame pipe so the peer's decompression reaches io.EOF. A failure becomes the
// pipe's terminal state.
func (e *PipeEnd) finishFrame() {
	defer e.pipe.life.finisherDone()

	// Serialize with an in-flight Write, whose accepted bytes must be flushed.
	e.writeMu.Lock()
	err := e.w.Close()
	e.writeMu.Unlock()
	if err != nil && e.pipe.life.err() == nil {
		e.pipe.fail(err)
		return
	}
	_ = e.frame.Close()
	e.pipe.requestTeardown()
}

// SetReadDeadline sets the deadline for a blocked or future Read. A zero time
// clears it. Reaching the deadline aborts the pipe, after which every Read and
// Write on either end returns a timeout error.
func (e *PipeEnd) SetReadDeadline(t time.Time) error {
	return e.setDeadline(t, &e.readTimer, "read")
}

// SetWriteDeadline is SetReadDeadline for Write calls.
func (e *PipeEnd) SetWriteDeadline(t time.Time) error {
	return e.setDeadline(t, &e.writeTimer, "write")
}

// SetDeadline sets the same deadline for both Read and Write.
func (e *PipeEnd) SetDeadline(t time.Time) error {
	if err := e.SetReadDeadline(t); err != nil {
		return err
	}
	return e.SetWriteDeadline(t)
}

func (e *PipeEnd) setDeadline(t time.Time, slot **time.Timer, kind string) error {
	// Hold deadlineMu across the state check and the timer arming: the teardown
	// clears the timers under the same lock, so no timer can be created after
	// it ran and then fail a closed lifecycle.
	e.deadlineMu.Lock()
	defer e.deadlineMu.Unlock()

	if err := e.pipe.life.err(); err != nil {
		return err
	}
	if e.pipe.life.ended() {
		return ErrPipeClosed
	}
	if old := *slot; old != nil {
		old.Stop()
		*slot = nil
	}
	if t.IsZero() {
		return nil
	}
	fire := func() {
		e.pipe.fail(&pipeDeadlineError{kind: kind})
	}
	delay := time.Until(t)
	if delay <= 0 {
		fire()
		return nil
	}
	*slot = time.AfterFunc(delay, fire)
	return nil
}

func (e *PipeEnd) clearDeadlines() {
	e.deadlineMu.Lock()
	defer e.deadlineMu.Unlock()
	if e.readTimer != nil {
		e.readTimer.Stop()
		e.readTimer = nil
	}
	if e.writeTimer != nil {
		e.writeTimer.Stop()
		e.writeTimer = nil
	}
}

// pipeDeadlineError is the terminal state a reached deadline produces. It
// satisfies net.Error so the timeout contract of network-style streams holds.
type pipeDeadlineError struct {
	kind string
}

func (e *pipeDeadlineError) Error() string   { return "lz4: " + e.kind + " deadline exceeded" }
func (e *pipeDeadlineError) Timeout() bool   { return true }
func (e *pipeDeadlineError) Temporary() bool { return false }

// pipeLifecycle is the single state boundary shared by both ends: one
// cancellation signal, one first terminal error, one graceful-close count and
// one teardown. Every transition funnels through it, so a failure racing a
// Close cannot start two teardowns or double-release resources.
type pipeLifecycle struct {
	ctx    context.Context
	cancel context.CancelFunc

	mu       sync.Mutex
	terminal error
	closed   [2]bool
	finWG    sync.WaitGroup // pending graceful frame finishers
	started  bool           // the teardown goroutine has been launched
	done     chan struct{}  // closed when the single teardown finishes
}

func newPipeLifecycle() *pipeLifecycle {
	ctx, cancel := context.WithCancel(context.Background())
	return &pipeLifecycle{ctx: ctx, cancel: cancel, done: make(chan struct{})}
}

// fail records the first error as the terminal state and fires the shared
// cancellation signal, so both ends are woken.
func (l *pipeLifecycle) fail(err error) {
	if err == nil {
		return
	}
	l.mu.Lock()
	if l.terminal == nil {
		l.terminal = err
	}
	l.mu.Unlock()
	l.cancel()
}

func (l *pipeLifecycle) err() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.terminal
}

// ended reports whether the single teardown has been launched, which happens
// on a failure or once both ends closed gracefully.
func (l *pipeLifecycle) ended() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.started
}

func (l *pipeLifecycle) failed() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.terminal != nil
}

// terminalErr is the error the internal pipes are broken with: the recorded
// failure, or ErrPipeClosed if the lifecycle ended without one.
func (l *pipeLifecycle) terminalErr() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.terminal != nil {
		return l.terminal
	}
	return ErrPipeClosed
}

// markClosed records the graceful close of one end.
func (l *pipeLifecycle) markClosed(index int) {
	l.mu.Lock()
	l.closed[index] = true
	l.mu.Unlock()
}

// registerFinisher reserves a slot for a graceful frame finisher. It declines
// once the teardown has been scheduled: sharing the lifecycle lock with
// requestTeardown orders each Add before the teardown's Wait.
func (l *pipeLifecycle) registerFinisher() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.started {
		return false
	}
	l.finWG.Add(1)
	return true
}

func (l *pipeLifecycle) finisherDone() {
	l.finWG.Done()
}

func (l *pipeLifecycle) waitFinishers() {
	l.finWG.Wait()
}

// requestTeardown launches the single teardown goroutine the first time the
// lifecycle reaches an end state: a recorded failure, or both ends closed
// gracefully. The decision and the launch share one critical section, so
// concurrent Close and decompression failure cannot start two teardowns.
func (l *pipeLifecycle) requestTeardown(fn func()) {
	l.mu.Lock()
	should := !l.started && (l.terminal != nil || (l.closed[0] && l.closed[1]))
	if should {
		l.started = true
	}
	l.mu.Unlock()
	if should {
		go fn()
	}
}
