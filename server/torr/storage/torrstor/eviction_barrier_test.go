package torrstor

import (
	"testing"
	"time"
)

func TestEvictionBarrierBlocksReaderStart(t *testing.T) {
	c := NewCache(32<<20, nil)

	// Active reader; resume() won't need the torrent client.
	r := &Reader{
		cache: c,
		isUse: true,
	}

	started := make(chan struct{})
	finished := make(chan bool, 1)

	c.evictionMu.Lock()

	go func() {
		close(started)

		ok := r.beginIO()
		if ok {
			r.endIO()
		}
		finished <- ok
	}()

	<-started

	// beginIO must not finish while eviction owns the lock.
	select {
	case <-finished:
		c.evictionMu.Unlock()
		t.Fatal("reader started during eviction")
	case <-time.After(25 * time.Millisecond):
	}

	c.evictionMu.Unlock()

	select {
	case ok := <-finished:
		if !ok {
			t.Fatal("reader unexpectedly refused to start")
		}
	case <-time.After(time.Second):
		t.Fatal("reader remained blocked after eviction")
	}
}
