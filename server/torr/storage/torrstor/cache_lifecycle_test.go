package torrstor

import (
	"server/settings"
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

func TestCacheCloseClearsActive(t *testing.T) {
	oldSettings := settings.BTsets
	t.Cleanup(func() {
		settings.BTsets = oldSettings
	})
	settings.BTsets = &settings.BTSets{}

	s := NewStorage(32 << 20)
	c := NewCache(32<<20, s)
	s.caches[c.hash] = c

	p := &Piece{Id: 1, cache: c}
	c.pieces[p.Id] = p
	c.markActive(p)

	if got := len(c.activeSnapshot()); got != 1 {
		t.Fatalf("active pieces before close = %d, want 1", got)
	}

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	if !c.isClosed.Load() {
		t.Fatal("cache not marked as closed")
	}

	if s.GetCache(c.hash) != nil {
		t.Fatal("closed cache still registered in storage")
	}

	if c.getPieces() != nil {
		t.Fatal("piece map retained after close")
	}

	if got := len(c.activeSnapshot()); got != 0 {
		t.Fatalf("active pieces after close = %d, want 0", got)
	}

	c.markActive(p)

	if got := len(c.activeSnapshot()); got != 0 {
		t.Fatalf("late write repopulated closed cache: %d", got)
	}
}

func TestCacheClosePreservesReplacement(t *testing.T) {
	oldSettings := settings.BTsets
	t.Cleanup(func() {
		settings.BTsets = oldSettings
	})
	settings.BTsets = &settings.BTSets{}

	s := NewStorage(32 << 20)

	old := NewCache(32<<20, s)
	old.hash[0] = 0xab
	s.caches[old.hash] = old

	// Simulate the unregister phase of CloseHash.
	s.removeCache(old.hash, old)

	// A new instance is registered before the old Close runs.
	replacement := NewCache(32<<20, s)
	replacement.hash = old.hash
	s.caches[old.hash] = replacement

	if err := old.Close(); err != nil {
		t.Fatal(err)
	}

	if got := s.GetCache(old.hash); got != replacement {
		t.Fatal("closing old cache removed its replacement")
	}

	if replacement.isClosed.Load() {
		t.Fatal("replacement cache was closed")
	}

	if err := replacement.Close(); err != nil {
		t.Fatal(err)
	}

	if s.GetCache(old.hash) != nil {
		t.Fatal("replacement cache still registered after close")
	}
}
