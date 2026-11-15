package torrstor

import "testing"

func TestReaderStateWithReadsSnapshot(t *testing.T) {
	r := &Reader{
		inFlight: 2,
		activeReadRanges: map[uint64]byteInterval{
			1: {begin: 1024, end: 2048},
			2: {begin: 4096, end: 8192},
		},
	}

	state, ranges, unknown := r.stateWithReadsSnapshot()

	if state.inFlight != 2 {
		t.Fatalf("inFlight=%d, want 2", state.inFlight)
	}

	if unknown {
		t.Fatal("unexpected unknownInFlight")
	}

	if len(ranges) != 2 {
		t.Fatalf("got %d ranges, want 2", len(ranges))
	}

	// Verify that the returned ranges are an independent
	// snapshot, not references to mutable reader state.
	r.mu.Lock()
	delete(r.activeReadRanges, 1)
	r.activeReadRanges[3] = byteInterval{
		begin: 16384,
		end:   32768,
	}
	r.mu.Unlock()

	foundFirst := false
	foundSecond := false

	for _, interval := range ranges {
		switch interval {
		case (byteInterval{begin: 1024, end: 2048}):
			foundFirst = true
		case (byteInterval{begin: 4096, end: 8192}):
			foundSecond = true
		}
	}

	if !foundFirst || !foundSecond {
		t.Fatalf("snapshot changed: %+v", ranges)
	}
}
