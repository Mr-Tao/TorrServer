package torrstor

import "testing"

func TestReaderPositionGeneration(t *testing.T) {
	r := &Reader{offset: 4096}

	// An old read starts at position 4096.
	oldGeneration := r.posGeneration

	// Simulate a Seek to position 8192.
	r.mu.Lock()
	r.posGeneration++
	r.offset = 8192
	r.posGeneration++
	r.mu.Unlock()

	// The old read finishes afterwards.
	r.finishReadPosition(oldGeneration, 1024)

	if got := r.Offset(); got != 8192 {
		t.Fatalf(
			"obsolete read changed position: got=%d want=8192",
			got,
		)
	}

	// A read in the current generation may advance it.
	currentGeneration := r.posGeneration
	r.finishReadPosition(currentGeneration, 512)

	if got := r.Offset(); got != 8704 {
		t.Fatalf(
			"current read did not advance position: got=%d want=8704",
			got,
		)
	}
}
