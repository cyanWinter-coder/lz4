package lz4_test

import (
	"fmt"
	"io"
	"sync"

	"github.com/pierrec/lz4/v4"
)

// ExampleStreamPipe wires two peers over one bidirectional LZ4 stream: each end
// compresses what it writes and decompresses what the other end wrote. Closing
// an end finishes its frame, so the peer reaches io.EOF after reading every
// accepted byte; the other direction is still usable until its own end closes.
func ExampleStreamPipe() {
	a, b, err := lz4.StreamPipe()
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { // b reads a's message and answers it
		defer wg.Done()
		msg, _ := io.ReadAll(b)
		fmt.Printf("b received: %s\n", msg)
		_, _ = b.Write([]byte("ack from b"))
		_ = b.Close()
	}()
	go func() { // a writes, closes its half, then reads the answer
		defer wg.Done()
		_, _ = a.Write([]byte("hello from a"))
		_ = a.Close()
		reply, _ := io.ReadAll(a)
		fmt.Printf("a received: %s\n", reply)
	}()
	wg.Wait()
	a.Wait()

	// Output:
	// b received: hello from a
	// a received: ack from b
}
