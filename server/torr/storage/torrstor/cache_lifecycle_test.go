package torrstor

import (
	"sync"
	"testing"
)

func TestCacheActiveShutdown(t *testing.T) {
	c := &Cache{
		active: make(map[int]*Piece),
	}
	p := new(Piece)

	c.markActive(p)

	if got := len(c.activeSnapshot()); got != 1 {
		t.Fatalf("active pieces = %d, want 1", got)
	}

	c.isClosed.Store(true)

	if got := c.clearActive(); got != 1 {
		t.Fatalf("cleared pieces = %d, want 1", got)
	}

	if got := len(c.activeSnapshot()); got != 0 {
		t.Fatalf("active pieces after clear = %d, want 0", got)
	}

	c.markActive(p)

	if got := len(c.activeSnapshot()); got != 0 {
		t.Fatalf("late write repopulated active pieces: %d", got)
	}
}

func TestCacheConcurrentActiveShutdown(t *testing.T) {
	c := &Cache{
		active: make(map[int]*Piece),
	}

	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p := new(Piece)
			<-start

			for j := 0; j < 1000; j++ {
				c.markActive(p)
			}
		}()
	}

	close(start)

	c.isClosed.Store(true)
	c.clearActive()

	wg.Wait()

	if got := len(c.activeSnapshot()); got != 0 {
		t.Fatalf("concurrent writes repopulated active pieces: %d", got)
	}
}
