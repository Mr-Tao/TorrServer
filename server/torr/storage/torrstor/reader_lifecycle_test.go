package torrstor

import (
	"context"
	"testing"
)

func TestCloseReaderKeepsInFlightRegistered(t *testing.T) {
	c := NewCache(32<<20, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r := &Reader{
		cache:       c,
		readCtx:     ctx,
		cancelReads: cancel,
		inFlight:    2,
		activeReadRanges: map[uint64]byteInterval{
			1: {begin: 1024, end: 2048},
			2: {begin: 4096, end: 8192},
		},
	}

	c.registerReader(r)

	isRegistered := func() bool {
		c.muReaders.RLock()
		defer c.muReaders.RUnlock()

		_, ok := c.readers[r]
		return ok
	}

	if !isRegistered() {
		t.Fatal("reader was not registered")
	}

	if r.cancelReads == nil {
		t.Fatal("reader has no cancel function")
	}

	if r.readCtx != ctx {
		t.Fatal("reader has a different context")
	}

	if r.isClosed {
		t.Fatal("reader was already closed")
	}

	// Closing must not wait for active I/O.
	c.CloseReader(r)

	// Closing must cancel pending reads immediately,
	// even while the reader remains registered.
	select {
	case <-ctx.Done():
		// Expected.
	default:
		t.Fatal("CloseReader did not cancel pending reads")
	}

	if !isRegistered() {
		t.Fatal("reader removed while I/O is in flight")
	}

	// New operations must be rejected.
	if _, _, ok := r.beginRead(1024); ok {
		t.Fatal("closed reader accepted a new read")
	}

	// Completing only the first read is not enough.
	r.endRead(1)

	if !isRegistered() {
		t.Fatal("reader removed before last read finished")
	}

	// Completing the last read permits removal.
	r.endRead(2)

	if isRegistered() {
		t.Fatal("reader remained registered after last read")
	}

	// Repeated close must be harmless.
	c.CloseReader(r)
}
