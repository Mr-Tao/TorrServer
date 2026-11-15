package torrstor

import (
	"testing"
	"time"
)

func TestPendingCleanAfterMaintenance(t *testing.T) {
	m := newSegmentTestPiece(memSegmentSize)
	c := m.piece.cache
	c.patchEnabled = true

	// Simulate an ongoing priority refresh or watchdog pass.
	c.muRemove.Lock()

	// Cleanup cannot acquire muRemove, so it must leave
	// a pending request for whoever releases the lock.
	c.cleanPieces()

	if !c.pendingClean.Load() {
		c.muRemove.Unlock()
		t.Fatal("cleanup request was not recorded")
	}

	// The maintenance operation finishes.
	c.unlockMaintenance()

	// A new cleanup should consume the pending request.
	deadline := time.Now().Add(3 * time.Second)
	for c.pendingClean.Load() {
		if time.Now().After(deadline) {
			t.Fatal("pending cleanup was never processed")
		}
		time.Sleep(time.Millisecond)
	}

	// Ensure the background cleanup has finished before
	// the test returns.
	c.muRemove.Lock()
	c.muRemove.Unlock()
}
