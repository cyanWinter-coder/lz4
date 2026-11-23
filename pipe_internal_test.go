package lz4

import (
	"bytes"
	"errors"
	"io"
	"runtime"
	"testing"
	"time"
)

// waitJoined runs fn (which must join the teardown) and fails if goroutines
// remain, mirroring the external test helper of the same purpose.
func waitJoined(t *testing.T, fn func()) {
	t.Helper()
	runtime.GC()
	before := runtime.NumGoroutine()
	fn()
	for deadline := time.Now().Add(5 * time.Second); runtime.NumGoroutine() > before; {
		if time.Now().After(deadline) {
			t.Fatalf("%d goroutines leaked", runtime.NumGoroutine()-before)
		}
		time.Sleep(time.Millisecond)
	}
}

// newPipeForInternalTest builds a StreamPipe with one end's internal frame pipe
// reachable, so a test can corrupt the compressed stream and drive the real
// decompression-failure path.
func newPipeForInternalTest(t *testing.T) (a, b *PipeEnd) {
	t.Helper()
	a, b, err := StreamPipe(BlockSizeOption(Block64Kb))
	if err != nil {
		t.Fatal(err)
	}
	return a, b
}

// buildFrame compresses data through a standalone Writer.
func buildFrame(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := NewWriter(&buf)
	if _, err := zw.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// TestStreamPipeDecompressionFailure feeds a truncated frame into one
// direction. The decompression goroutine's real error must become the terminal
// state of both ends, waking every blocked operation.
func TestStreamPipeDecompressionFailure(t *testing.T) {
	frame := buildFrame(t, bytes.Repeat([]byte("corrupt me "), 10000))
	truncated := frame[:len(frame)-7] // cut off the end mark and checksum

	a, b := newPipeForInternalTest(t)

	// Replace what a's compressor would write with the truncated frame, which
	// b's Reader consumes.
	go func() {
		_, _ = a.frame.Write(truncated)
		_ = a.frame.Close()
	}()

	start := time.Now()
	_, err := io.ReadAll(b)
	if err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("expected a decompression error, got %v", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("the decompression failure did not wake Read in time: %v", err)
	}

	if !errors.Is(a.Err(), err) || !errors.Is(b.Err(), err) {
		t.Fatalf("terminal state mismatch: a=%v b=%v read=%v", a.Err(), b.Err(), err)
	}
	if _, werr := a.Write([]byte("x")); !errors.Is(werr, err) {
		t.Fatalf("a.Write after corruption = %v, want %v", werr, err)
	}
	if rerr := b.Close(); !errors.Is(rerr, err) {
		t.Fatalf("b.Close after corruption = %v, want %v", rerr, err)
	}
	done := make(chan struct{})
	go func() { _ = a.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("a.Close blocked after a decompression failure")
	}
	waitJoined(t, func() { a.Wait() })
}

// TestStreamPipeWriteFailure: a broken frame sink makes the next compression
// write fail, and the same terminal error reaches the peer.
func TestStreamPipeWriteFailure(t *testing.T) {
	a, b := newPipeForInternalTest(t)
	sentinel := errors.New("frame sink broken")

	_ = a.frame.CloseWithError(sentinel)
	if _, werr := a.Write(bytes.Repeat([]byte("z"), 1<<16)); !errors.Is(werr, sentinel) {
		t.Fatalf("a.Write = %v, want %v", werr, sentinel)
	}
	if _, rerr := b.Read(make([]byte, 1)); !errors.Is(rerr, sentinel) {
		t.Fatalf("b.Read = %v, want %v", rerr, sentinel)
	}
	if err := b.Close(); !errors.Is(err, sentinel) {
		t.Fatalf("b.Close = %v, want %v", err, sentinel)
	}
	waitJoined(t, func() { a.Wait() })
}

// TestStreamPipeTerminalStateIsStable: after a failure every operation on both
// ends reports the identical value, and Close does not change it.
func TestStreamPipeTerminalStateIsStable(t *testing.T) {
	a, b := newPipeForInternalTest(t)
	sentinel := errors.New("terminal")
	if err := a.CloseWithError(sentinel); !errors.Is(err, sentinel) {
		t.Fatalf("CloseWithError = %v", err)
	}
	first := a.Err()
	if first == nil || !errors.Is(first, sentinel) {
		t.Fatalf("Err = %v", first)
	}
	for i := range 3 {
		if _, err := a.Write([]byte("x")); err != first && !errors.Is(err, sentinel) {
			t.Fatalf("a.Write %d = %v, want %v", i, err, first)
		}
		if _, err := b.Read(make([]byte, 1)); err != io.EOF && !errors.Is(err, sentinel) {
			t.Fatalf("b.Read %d = %v, want %v", i, err, first)
		}
		if err := b.Close(); !errors.Is(err, sentinel) {
			t.Fatalf("b.Close %d = %v, want %v", i, err, sentinel)
		}
		if got := b.Err(); got != first {
			t.Fatalf("ends disagree: %v vs %v", got, first)
		}
	}
	a.Wait()
}

// TestStreamPipeFailureIsImmediate: once the pipe fails, Read reports the
// terminal error without waiting for the peer to drain anything, and it never
// returns bytes together with a failure.
func TestStreamPipeFailureIsImmediate(t *testing.T) {
	a, b := newPipeForInternalTest(t)
	msg := bytes.Repeat([]byte("queued before failure "), 5000)
	if _, err := a.Write(msg); err != nil {
		t.Fatal(err)
	}

	sentinel := errors.New("late failure")
	if err := a.CloseWithError(sentinel); !errors.Is(err, sentinel) {
		t.Fatalf("CloseWithError = %v", err)
	}

	start := time.Now()
	for range 3 {
		n, err := b.Read(make([]byte, 4096))
		if !errors.Is(err, sentinel) {
			t.Fatalf("b.Read = %d, %v; want the terminal error", n, err)
		}
		if n != 0 {
			t.Fatalf("b.Read returned %d bytes with the terminal error", n)
		}
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("Read did not return promptly after the failure: %v", d)
	}
	_ = b.Close()
	a.Wait()
}

// TestStreamPipeLifecycleStates walks the explicit lifecycle boundary: alive,
// half-closed, fully closed and terminal, checking what each end reports.
func TestStreamPipeLifecycleStates(t *testing.T) {
	a, b := newPipeForInternalTest(t)
	if a.Err() != nil || b.Err() != nil {
		t.Fatal("a new pipe must have no terminal error")
	}
	select {
	case <-a.Done():
		t.Fatal("Done channel closed while the pipe is alive")
	default:
	}

	if err := a.Close(); err != nil {
		t.Fatalf("graceful half close: %v", err)
	}
	if a.Err() != nil {
		t.Fatalf("half close recorded an error: %v", a.Err())
	}
	if got, err := io.ReadAll(b); err != nil || len(got) != 0 {
		t.Fatalf("peer drain after half close = %d bytes, %v", len(got), err)
	}

	if err := b.Close(); err != nil {
		t.Fatalf("second end close: %v", err)
	}
	select {
	case <-a.Done():
	case <-time.After(time.Second):
		t.Fatal("Done channel still open after the pipe closed")
	}
	if err := a.Close(); err != nil {
		t.Fatalf("repeated close: %v", err)
	}
	a.Wait()
}

// TestBufferedPipeZeroValue: a pipe obtained without the constructor must be
// usable, so a Close on a zero value cannot panic on a nil condition.
func TestBufferedPipeZeroValue(t *testing.T) {
	var p bufferedPipe
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := p.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("Read after Close = %v, want io.EOF", err)
	}
	if _, err := p.Write([]byte("x")); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("Write after Close = %v, want io.ErrClosedPipe", err)
	}
}

// TestBufferedPipeBoundedBackPressure: a bounded buffer blocks a writer once
// it is full and unblocks it as a reader makes room; every byte drains before
// EOF.
func TestBufferedPipeBoundedBackPressure(t *testing.T) {
	p := newBufferedPipeCap(4)
	firstDone := make(chan struct{})
	allDone := make(chan struct{})
	go func() {
		_, _ = p.Write([]byte("abcd"))
		close(firstDone)
		_, _ = p.Write([]byte("efgh")) // blocks until a Read frees room
		_ = p.Close()
		close(allDone)
	}()

	<-firstDone
	select {
	case <-allDone:
		t.Fatal("the bounded writer was not blocked by a full buffer")
	case <-time.After(20 * time.Millisecond):
	}

	got, err := io.ReadAll(p)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "abcdefgh" {
		t.Fatalf("drained %q, want abcdefgh", got)
	}
	<-allDone
}

// TestBufferedPipeCloseWithError drains buffered data before reporting the
// terminal error, so accepted bytes are never skipped.
func TestBufferedPipeCloseWithError(t *testing.T) {
	p := newBufferedPipe()
	sentinel := errors.New("sentinel")
	if _, err := p.Write([]byte("kept")); err != nil {
		t.Fatal(err)
	}
	if err := p.CloseWithError(sentinel); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 8)
	n, _ := p.Read(got)
	if string(got[:n]) != "kept" {
		t.Fatalf("buffered Read = %q, want kept", got[:n])
	}
	if _, err := p.Read(got); !errors.Is(err, sentinel) {
		t.Fatalf("terminal Read = %v, want sentinel", err)
	}
}

// TestBufferedPipeUnbounded: an unbounded pipe never blocks a writer for lack
// of room, so a producer can finish its stream without a reader.
func TestBufferedPipeUnbounded(t *testing.T) {
	p := newBufferedPipe()
	p.setUnbounded()
	payload := make([]byte, 1<<20)
	done := make(chan struct{})
	go func() {
		_, _ = p.Write(payload)
		_ = p.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("an unbounded Write blocked")
	}
	got, err := io.ReadAll(p)
	if err != nil || len(got) != len(payload) {
		t.Fatalf("ReadAll = %d bytes, %v; want %d", len(got), err, len(payload))
	}
}

// TestBufferedPipeFullResume: a writer blocked on a full buffer resumes once
// the reader drains it, and the bytes keep their order across the capacity
// boundary even when the pipe compacts its buffer.
func TestBufferedPipeFullResume(t *testing.T) {
	p := newBufferedPipeCap(8)
	payload := []byte("0123456789abcdefghij") // larger than the capacity
	written := make(chan struct{})
	go func() {
		n, _ := p.Write(payload)
		if n != len(payload) {
			t.Errorf("Write = %d, want %d", n, len(payload))
		}
		_ = p.Close()
		close(written)
	}()

	var got []byte
	buf := make([]byte, 3)
	for {
		n, err := p.Read(buf)
		got = append(got, buf[:n]...)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
	}
	<-written
	if !bytes.Equal(got, payload) {
		t.Fatalf("drained %q, want %q", got, payload)
	}
}
