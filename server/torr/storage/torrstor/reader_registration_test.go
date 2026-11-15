package torrstor

import (
	"testing"
	"time"
)

func TestEvictionBarrierBlocksReaderRegistration(t *testing.T) {
	c := NewCache(32<<20, nil)
	r := &Reader{cache: c}

	c.evictionMu.Lock()
	locked := true
	defer func() {
		if locked {
			c.evictionMu.Unlock()
		}
	}()

	started := make(chan struct{})
	finished := make(chan struct{})

	go func() {
		close(started)
		c.registerReader(r)
		close(finished)
	}()

	<-started

	// Registration must not complete while eviction
	// holds the exclusive barrier.
	select {
	case <-finished:
		t.Fatal("reader registered during eviction")
	case <-time.After(50 * time.Millisecond):
	}

	c.muReaders.RLock()
	_, exists := c.readers[r]
	c.muReaders.RUnlock()

	if exists {
		t.Fatal("reader became visible during eviction")
	}

	c.evictionMu.Unlock()
	locked = false

	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("reader registration did not resume")
	}

	c.muReaders.RLock()
	_, exists = c.readers[r]
	c.muReaders.RUnlock()

	if !exists {
		t.Fatal("reader was not registered")
	}
}
