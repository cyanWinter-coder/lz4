package lz4_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/pierrec/lz4/v4"
)

// netTimeoutError is the part of net.Error a stream deadline satisfies; the
// pipe has no network dependency, so the tests do not import net.
type netTimeoutError interface {
	error
	Timeout() bool
}

// waitNoGoroutineLeak runs fn and fails if it leaves goroutines behind. A
// test whose teardown is asynchronous must have called Wait before returning.
func waitNoGoroutineLeak(t *testing.T, fn func()) {
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

// waitPipe joins the shared teardown; either end reaches it.
func waitPipe(ends ...*lz4.PipeEnd) {
	for _, e := range ends {
		e.Wait()
	}
}

// TestStreamPipeRoundTrip is the happy path: both ends exchange more than one
// block's worth of data and each receives exactly what the peer wrote.
func TestStreamPipeRoundTrip(t *testing.T) {
	waitNoGoroutineLeak(t, func() {
		a, b, err := lz4.StreamPipe(
			lz4.BlockSizeOption(lz4.Block64Kb),
			lz4.CompressionLevelOption(lz4.Level1),
			lz4.BlockChecksumOption(true),
		)
		if err != nil {
			t.Fatal(err)
		}

		payloadA := bytes.Repeat([]byte("alice->bob "), 20000) // several blocks
		payloadB := bytes.Repeat([]byte("bob->alice "), 15000)

		var wg sync.WaitGroup
		wg.Add(4)
		var gotA, gotB []byte
		go func() { defer wg.Done(); gotA, _ = io.ReadAll(a) }()
		go func() { defer wg.Done(); gotB, _ = io.ReadAll(b) }()
		go func() {
			defer wg.Done()
			if _, err := a.Write(payloadA); err != nil {
				t.Errorf("a.Write: %v", err)
			}
			if err := a.Close(); err != nil {
				t.Errorf("a.Close: %v", err)
			}
		}()
		go func() {
			defer wg.Done()
			if _, err := b.Write(payloadB); err != nil {
				t.Errorf("b.Write: %v", err)
			}
			if err := b.Close(); err != nil {
				t.Errorf("b.Close: %v", err)
			}
		}()
		wg.Wait()

		if !bytes.Equal(gotA, payloadB) {
			t.Fatalf("a read %d bytes, want %d", len(gotA), len(payloadB))
		}
		if !bytes.Equal(gotB, payloadA) {
			t.Fatalf("b read %d bytes, want %d", len(gotB), len(payloadA))
		}
		if err := a.Err(); err != nil {
			t.Fatalf("graceful round trip recorded %v", err)
		}
		waitPipe(a, b)
	})
}

// TestStreamPipeHalfCloseDrain: once an end closes, the peer receives every
// byte that Write accepted before io.EOF, and the reverse direction stays
// usable until its own end closes.
func TestStreamPipeHalfCloseDrain(t *testing.T) {
	waitNoGoroutineLeak(t, func() {
		a, b, err := lz4.StreamPipe(lz4.BlockSizeOption(lz4.Block64Kb))
		if err != nil {
			t.Fatal(err)
		}

		first := bytes.Repeat([]byte("first message "), 9000) // more than one block
		second := []byte("second message after the peer drained")

		if _, err := a.Write(first); err != nil {
			t.Fatal(err)
		}
		// Close a before b reads anything: the finished frame and the accepted
		// data must survive until b drains them.
		if err := a.Close(); err != nil {
			t.Fatal(err)
		}

		got, err := io.ReadAll(b)
		if err != nil {
			t.Fatalf("b drain: %v", err)
		}
		if !bytes.Equal(got, first) {
			t.Fatalf("b got %d bytes, want %d", len(got), len(first))
		}

		// The reverse direction is still open after a's half close.
		if _, err := b.Write(second); err != nil {
			t.Fatalf("b.Write after peer half close: %v", err)
		}
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
		got, err = io.ReadAll(a)
		if err != nil {
			t.Fatalf("a drain: %v", err)
		}
		if !bytes.Equal(got, second) {
			t.Fatalf("a got %q, want %q", got, second)
		}
		waitPipe(a, b)
	})
}

// TestStreamPipePartialReads checks data integrity across many small reads,
// including block boundaries.
func TestStreamPipePartialReads(t *testing.T) {
	waitNoGoroutineLeak(t, func() {
		a, b, err := lz4.StreamPipe(lz4.BlockSizeOption(lz4.Block64Kb))
		if err != nil {
			t.Fatal(err)
		}
		payload := bytes.Repeat([]byte("0123456789ABCDEF"), 5000)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			if _, err := a.Write(payload); err != nil {
				t.Errorf("Write: %v", err)
			}
			if err := a.Close(); err != nil {
				t.Errorf("a.Close: %v", err)
			}
		}()
		go func() {
			defer wg.Done()
			var got bytes.Buffer
			tmp := make([]byte, 37) // deliberately awkward size
			for {
				n, err := b.Read(tmp)
				got.Write(tmp[:n])
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Errorf("Read: %v", err)
					return
				}
			}
			if !bytes.Equal(got.Bytes(), payload) {
				t.Errorf("partial read mismatch: got %d bytes, want %d", got.Len(), len(payload))
			}
			if err := b.Close(); err != nil {
				t.Errorf("b.Close: %v", err)
			}
		}()
		wg.Wait()
		waitPipe(a, b)
	})
}

// TestStreamPipeFailureWakesBothEnds: an explicit failure on one end wakes a
// blocked reader and a blocked writer on either end, and every later operation
// returns the same terminal error.
func TestStreamPipeFailureWakesBothEnds(t *testing.T) {
	sentinel := errors.New("boom")

	waitNoGoroutineLeak(t, func() {
		a, b, err := lz4.StreamPipe()
		if err != nil {
			t.Fatal(err)
		}

		readerStarted := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		var readErr, writeErr error
		go func() {
			defer wg.Done()
			close(readerStarted)
			// A Read may return (0, nil) just before the abort breaks the pipe.
			buf := make([]byte, 1)
			for readErr == nil {
				_, readErr = b.Read(buf)
			}
		}()
		go func() {
			defer wg.Done()
			// Nobody drains a.
			for writeErr == nil {
				_, writeErr = a.Write(bytes.Repeat([]byte("x"), 4096))
			}
		}()
		<-readerStarted
		time.Sleep(10 * time.Millisecond)

		if err := a.CloseWithError(sentinel); !errors.Is(err, sentinel) {
			t.Fatalf("CloseWithError = %v, want %v", err, sentinel)
		}
		wg.Wait()

		if !errors.Is(readErr, sentinel) {
			t.Errorf("blocked reader got %v, want %v", readErr, sentinel)
		}
		if !errors.Is(writeErr, sentinel) {
			t.Errorf("blocked writer got %v, want %v", writeErr, sentinel)
		}
		for name, op := range map[string]func() error{
			"a.Read":  func() error { _, e := a.Read(nil); return e },
			"a.Write": func() error { _, e := a.Write(nil); return e },
			"a.Close": a.Close,
			"b.Read":  func() error { _, e := b.Read(nil); return e },
			"b.Write": func() error { _, e := b.Write(nil); return e },
			"b.Close": b.Close,
		} {
			if err := op(); !errors.Is(err, sentinel) {
				t.Errorf("%s after abort = %v, want %v", name, err, sentinel)
			}
		}
		if !errors.Is(a.Err(), sentinel) || !errors.Is(b.Err(), sentinel) {
			t.Errorf("Err() = %v, %v; both must report %v", a.Err(), b.Err(), sentinel)
		}
		waitPipe(a, b)
	})
}

// TestStreamPipeReadDeadline: a deadline reached while an end is blocked
// reading aborts the pipe and wakes both ends with the timeout error.
func TestStreamPipeReadDeadline(t *testing.T) {
	waitNoGoroutineLeak(t, func() {
		a, b, err := lz4.StreamPipe()
		if err != nil {
			t.Fatal(err)
		}

		if err := a.SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		_, err = a.Read(make([]byte, 1))
		if d := time.Since(start); d > 2*time.Second {
			t.Fatalf("Read did not wake on the deadline: %v", err)
		}
		var ne netTimeoutError
		if !errors.As(err, &ne) || !ne.Timeout() {
			t.Fatalf("Read error = %v, want a timeout net.Error", err)
		}

		// The shared cancellation signal fires on the peer too.
		select {
		case <-b.Done():
		case <-time.After(2 * time.Second):
			t.Fatal("peer Done channel was never closed")
		}
		if _, err := b.Write([]byte("late")); err == nil || !errors.Is(err, a.Err()) {
			t.Fatalf("peer Write after the deadline = %v", err)
		}
		if !errors.Is(b.Err(), a.Err()) {
			t.Fatalf("ends disagree on the terminal state: %v vs %v", a.Err(), b.Err())
		}
		waitPipe(a, b)
	})
}

// TestStreamPipeWriteDeadline: a reached write deadline fails the pipe, and a
// cleared deadline leaves it healthy.
func TestStreamPipeWriteDeadline(t *testing.T) {
	waitNoGoroutineLeak(t, func() {
		a, b, err := lz4.StreamPipe()
		if err != nil {
			t.Fatal(err)
		}
		if err := a.SetDeadline(time.Now().Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		if err := a.SetDeadline(time.Time{}); err != nil { // clear both
			t.Fatal(err)
		}

		// Still healthy after clearing.
		var wg sync.WaitGroup
		wg.Add(2)
		var got []byte
		go func() { defer wg.Done(); got, _ = io.ReadAll(b) }()
		go func() {
			defer wg.Done()
			if _, err := a.Write([]byte("alive")); err != nil {
				t.Errorf("Write: %v", err)
			}
			_ = a.Close()
		}()
		wg.Wait()
		if string(got) != "alive" {
			t.Fatalf("got %q after clearing the deadline", got)
		}

		if err := a.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		// Let the timer fire, then every write reports the terminal timeout.
		for deadline := time.Now().Add(time.Second); a.Err() == nil; {
			if time.Now().After(deadline) {
				t.Fatal("write deadline never fired")
			}
			time.Sleep(time.Millisecond)
		}
		_, err = a.Write([]byte("x"))
		var ne netTimeoutError
		if !errors.As(err, &ne) || !ne.Timeout() {
			t.Fatalf("Write after the deadline = %v, want a timeout net.Error", err)
		}
		if _, err := b.Read(make([]byte, 1)); !errors.Is(err, a.Err()) {
			t.Fatalf("peer Read = %v, want %v", err, a.Err())
		}
		_ = b.Close()
		waitPipe(a, b)
	})
}

// TestStreamPipeConcurrentClose: closing both ends concurrently and
// repeatedly releases the resources once, without a panic or a deadlock, and
// records no error.
func TestStreamPipeConcurrentClose(t *testing.T) {
	waitNoGoroutineLeak(t, func() {
		a, b, err := lz4.StreamPipe()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := a.Write([]byte("data")); err != nil {
			t.Fatal(err)
		}

		var wg sync.WaitGroup
		for range 20 {
			wg.Add(2)
			go func() { defer wg.Done(); _ = a.Close() }()
			go func() { defer wg.Done(); _ = b.Close() }()
		}
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent Close deadlocked")
		}
		if err := a.Err(); err != nil {
			t.Fatalf("graceful close recorded an error: %v", err)
		}
		waitPipe(a, b)
	})
}

// TestStreamPipeCloseWithoutReading: both ends write several blocks and no
// one reads, then both close. The teardown must still finish and free its
// goroutines instead of deadlocking on a full buffer.
func TestStreamPipeCloseWithoutReading(t *testing.T) {
	waitNoGoroutineLeak(t, func() {
		a, b, err := lz4.StreamPipe(lz4.BlockSizeOption(lz4.Block64Kb))
		if err != nil {
			t.Fatal(err)
		}
		payload := bytes.Repeat([]byte("unread payload "), 20000)
		if _, err := a.Write(payload); err != nil {
			t.Fatalf("a.Write: %v", err)
		}
		if _, err := b.Write(payload); err != nil {
			t.Fatalf("b.Write: %v", err)
		}

		done := make(chan struct{})
		go func() {
			_ = a.Close()
			_ = b.Close()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("Close deadlocked with full unread output buffers")
		}
		if err := a.Err(); err != nil {
			t.Fatalf("graceful close recorded an error: %v", err)
		}
		waitPipe(a, b)
	})
}

// TestStreamPipeDictionaryRoundTrip keeps DictionaryOption working through the
// pipe on both sides.
func TestStreamPipeDictionaryRoundTrip(t *testing.T) {
	dict := []byte("schema:id,name,email,timestamp,event,status")
	src := []byte("schema:id=1,name=a,event=login,status=ok\nschema:id=2,name=b,event=logout,status=ok\n")

	waitNoGoroutineLeak(t, func() {
		a, b, err := lz4.StreamPipe(lz4.DictionaryOption(dict))
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		wg.Add(2)
		var got []byte
		go func() { defer wg.Done(); got, _ = io.ReadAll(b) }()
		go func() {
			defer wg.Done()
			if _, err := a.Write(src); err != nil {
				t.Errorf("Write: %v", err)
			}
			if err := a.Close(); err != nil {
				t.Errorf("a.Close: %v", err)
			}
		}()
		wg.Wait()
		if !bytes.Equal(got, src) {
			t.Fatalf("dictionary round trip mismatch: got %q", got)
		}
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
		waitPipe(a, b)
	})
}

// TestStreamPipeOptions: option errors surface at construction, and neither a
// Writer nor a Reader concurrency above one is accepted: those types would own
// goroutines the pipe lifecycle does not join.
func TestStreamPipeOptions(t *testing.T) {
	if _, _, err := lz4.StreamPipe(lz4.BlockSizeOption(lz4.BlockSize(1000))); err == nil {
		t.Fatal("expected an invalid block size error")
	}
	if _, _, err := lz4.StreamPipe(lz4.ConcurrencyOption(1)); err != nil {
		t.Fatalf("ConcurrencyOption(1): %v", err)
	}
	if _, _, err := lz4.StreamPipe(lz4.ConcurrencyOption(4)); !errors.Is(err, lz4.ErrOptionNotApplicable) {
		t.Fatalf("ConcurrencyOption(4): got %v, want %v", err, lz4.ErrOptionNotApplicable)
	}
}

// TestStreamPipeStandardStreams puts the existing high-level types on top of a
// pipe end: a Writer compresses into the end and a Reader decompresses the
// peer's data, layer on layer. The pipe must stay an ordinary io.ReadWriter.
func TestStreamPipeStandardStreams(t *testing.T) {
	waitNoGoroutineLeak(t, func() {
		a, b, err := lz4.StreamPipe(lz4.BlockSizeOption(lz4.Block64Kb))
		if err != nil {
			t.Fatal(err)
		}
		payload := bytes.Repeat([]byte("layered stream "), 20000)

		var wg sync.WaitGroup
		wg.Add(2)
		var got []byte
		go func() {
			defer wg.Done()
			zw := lz4.NewWriter(a)
			if _, err := zw.Write(payload); err != nil {
				t.Errorf("Write: %v", err)
			}
			if err := zw.Close(); err != nil {
				t.Errorf("zw.Close: %v", err)
			}
			if err := a.Close(); err != nil {
				t.Errorf("a.Close: %v", err)
			}
		}()
		go func() {
			defer wg.Done()
			zr := lz4.NewReader(b)
			got, _ = io.ReadAll(zr)
			if err := b.Close(); err != nil {
				t.Errorf("b.Close: %v", err)
			}
		}()
		wg.Wait()
		if !bytes.Equal(got, payload) {
			t.Fatalf("layered round trip: got %d bytes, want %d", len(got), len(payload))
		}
		waitPipe(a, b)
	})
}

// TestStreamPipeCompressingReader: CompressingReader still accepts any
// io.ReadCloser, so a pipe end can feed it, and its compressed output is a
// normal LZ4 frame for a standalone Reader.
func TestStreamPipeCompressingReader(t *testing.T) {
	waitNoGoroutineLeak(t, func() {
		a, b, err := lz4.StreamPipe()
		if err != nil {
			t.Fatal(err)
		}
		payload := bytes.Repeat([]byte("compress me "), 10000)

		var wg sync.WaitGroup
		wg.Add(2)
		var frame []byte
		go func() {
			defer wg.Done()
			cr := lz4.NewCompressingReader(a)
			frame, _ = io.ReadAll(cr)
			if err := cr.Close(); err != nil {
				t.Errorf("cr.Close: %v", err)
			}
		}()
		go func() {
			defer wg.Done()
			if _, err := b.Write(payload); err != nil {
				t.Errorf("b.Write: %v", err)
			}
			if err := b.Close(); err != nil {
				t.Errorf("b.Close: %v", err)
			}
		}()
		wg.Wait()
		got, err := io.ReadAll(lz4.NewReader(bytes.NewReader(frame)))
		if err != nil {
			t.Fatalf("standalone reader: %v", err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("CompressingReader output: got %d bytes, want %d", len(got), len(payload))
		}
		_ = a.Close()
		waitPipe(a, b)
	})
}

// TestStreamPipeWriteAfterClose: a half-closed end rejects further writes with
// ErrPipeClosed instead of hanging or corrupting the frame, and the reverse
// direction still carries data.
func TestStreamPipeWriteAfterClose(t *testing.T) {
	waitNoGoroutineLeak(t, func() {
		a, b, err := lz4.StreamPipe()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := a.Write([]byte("before")); err != nil {
			t.Fatal(err)
		}
		if err := a.Close(); err != nil {
			t.Fatal(err)
		}
		if n, err := a.Write([]byte("after")); n != 0 || !errors.Is(err, lz4.ErrPipeClosed) {
			t.Fatalf("Write after Close = %d, %v; want 0, %v", n, err, lz4.ErrPipeClosed)
		}
		// The peer sees only the accepted bytes.
		got, err := io.ReadAll(b)
		if err != nil || string(got) != "before" {
			t.Fatalf("b drain = %q, %v", got, err)
		}
		// The reverse direction still works.
		if _, err := b.Write([]byte("reply")); err != nil {
			t.Fatalf("b.Write: %v", err)
		}
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
		got, err = io.ReadAll(a)
		if err != nil || string(got) != "reply" {
			t.Fatalf("a drain = %q, %v", got, err)
		}
		// A Write on an end whose half is closed stays rejected.
		if _, err := b.Write(nil); !errors.Is(err, lz4.ErrPipeClosed) {
			t.Fatalf("b.Write after Close = %v", err)
		}
		waitPipe(a, b)
	})
}

// TestStreamPipeWaitIsIdempotent: Wait can be called from either end, from
// both at once and repeatedly, and the teardown still runs once.
func TestStreamPipeWaitIsIdempotent(t *testing.T) {
	a, b, err := lz4.StreamPipe()
	if err != nil {
		t.Fatal(err)
	}
	_ = a.Close()
	_ = b.Close()

	var wg sync.WaitGroup
	for range 4 {
		wg.Add(2)
		go func() { defer wg.Done(); a.Wait() }()
		go func() { defer wg.Done(); b.Wait() }()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent Wait deadlocked")
	}
	waitPipe(a, b)
	if err := a.Close(); err != nil {
		t.Fatalf("Close after Wait = %v", err)
	}
}

// TestStreamPipeCloseWithEOFGraceful: CloseWithError(io.EOF) is a graceful end
// of stream, not a failure of the pipe.
func TestStreamPipeCloseWithEOFGraceful(t *testing.T) {
	waitNoGoroutineLeak(t, func() {
		a, b, err := lz4.StreamPipe()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := a.Write([]byte("eof")); err != nil {
			t.Fatal(err)
		}
		if err := a.CloseWithError(io.EOF); err != nil {
			t.Fatalf("CloseWithError(io.EOF) = %v", err)
		}
		got, err := io.ReadAll(b)
		if err != nil || string(got) != "eof" {
			t.Fatalf("b drain = %q, %v", got, err)
		}
		if err := a.Err(); err != nil {
			t.Fatalf("graceful end recorded %v", err)
		}
		_ = b.Close()
		waitPipe(a, b)
	})
}

// TestStreamPipeRepeatedVerification runs the full lifecycle many times, so an
// occasional data loss or a race cannot hide.
func TestStreamPipeRepeatedVerification(t *testing.T) {
	waitNoGoroutineLeak(t, func() {
		for i := range 100 {
			a, b, err := lz4.StreamPipe(lz4.BlockSizeOption(lz4.Block64Kb))
			if err != nil {
				t.Fatal(err)
			}
			msg := fmt.Appendf(nil, "iteration %d ", i)
			var wg sync.WaitGroup
			wg.Add(4)
			var ga, gb []byte
			go func() { defer wg.Done(); ga, _ = io.ReadAll(a) }()
			go func() { defer wg.Done(); gb, _ = io.ReadAll(b) }()
			go func() {
				defer wg.Done()
				for range 10 {
					if _, err := a.Write(msg); err != nil {
						t.Errorf("a.Write: %v", err)
						return
					}
				}
				_ = a.Close()
			}()
			go func() {
				defer wg.Done()
				for range 7 {
					if _, err := b.Write(msg); err != nil {
						t.Errorf("b.Write: %v", err)
						return
					}
				}
				_ = b.Close()
			}()
			wg.Wait()
			if want := bytes.Repeat(msg, 7); !bytes.Equal(ga, want) {
				t.Fatalf("iteration %d: a got %d bytes, want %d", i, len(ga), len(want))
			}
			if want := bytes.Repeat(msg, 10); !bytes.Equal(gb, want) {
				t.Fatalf("iteration %d: b got %d bytes, want %d", i, len(gb), len(want))
			}
			waitPipe(a, b)
		}
	})
}
