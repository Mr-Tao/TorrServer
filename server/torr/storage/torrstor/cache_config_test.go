package torrstor

import (
	"testing"

	"server/settings"
)

func TestCacheConfigurationSnapshot(t *testing.T) {
	old := settings.BTsets
	t.Cleanup(func() {
		settings.BTsets = old
	})

	settings.BTsets = &settings.BTSets{
		ConnectionsLimit: 12,
		ReaderReadAHead:  80,
	}

	c := NewCache(32<<20, NewStorage(32<<20))

	if c.connectionsLimit != 12 {
		t.Fatalf("connectionsLimit = %d, want 12",
			c.connectionsLimit)
	}

	if c.readerReadAheadPercent != 80 {
		t.Fatalf("readerReadAheadPercent = %d, want 80",
			c.readerReadAheadPercent)
	}

	// Changes to global settings must not change an
	// already created cache.
	settings.BTsets = &settings.BTSets{
		ConnectionsLimit: 4,
		ReaderReadAHead:  95,
	}

	if c.connectionsLimit != 12 ||
		c.readerReadAheadPercent != 80 {
		t.Fatal("cache configuration changed after creation")
	}
}
