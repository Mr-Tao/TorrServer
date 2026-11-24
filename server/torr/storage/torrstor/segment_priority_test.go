package torrstor

import (
	"testing"

	"github.com/anacrolix/torrent"
)

func TestPieceOverlapsProtectedIntervals(t *testing.T) {
	tests := []struct {
		name      string
		start     int64
		length    int64
		protected []byteInterval
		want      bool
	}{
		{"no readers", 100, 200, nil, false},
		{"reader overlaps start", 100, 200, []byteInterval{{95, 101}}, true},
		{"reader overlaps end", 100, 200, []byteInterval{{299, 310}}, true},
		{"reader inside piece", 100, 200, []byteInterval{{150, 160}}, true},
		{"piece inside reader", 100, 200, []byteInterval{{90, 310}}, true},
		{"touches begin only", 100, 200, []byteInterval{{50, 100}}, false},
		{"touches end only", 100, 200, []byteInterval{{300, 350}}, false},
		{"disjoint windows", 100, 200, []byteInterval{{0, 90}, {320, 330}}, false},
		{"second reader overlaps", 100, 200, []byteInterval{{0, 90}, {200, 201}}, true},
		{"invalid window", 100, 200, []byteInterval{{200, 100}}, false},
		{"zero piece", 100, 0, []byteInterval{{90, 310}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := pieceOverlapsProtectedIntervals(tt.start, tt.length, tt.protected)
			if got != tt.want {
				t.Fatalf("overlap=%v, want %v", got, tt.want)
			}
		})
	}
}

// Reproduce the stale priority reported after the final HTTP reader
// closes: invalidating a segment used to leave the piece requested,
// which caused it to be re-downloaded and evicted again.
func TestRAMSegmentEvictionDropsStalePriorityAfterLastReader(t *testing.T) {
	c, torr := createFullEvictionTestCache(t)

	closed := c.NewReader(torr.Files()[0])
	c.CloseReader(closed)

	withMaintenanceLock(c, func() {
		originalCapacity := c.capacity
		defer func() { c.capacity = originalCapacity }()
		c.capacity = memSegmentSize
		c.getRemPiecesX(false, false)

		// Model a priority left behind by earlier preloading.
		// Complete pieces have no effective priority until invalidated.
		torr.Piece(0).SetPriority(torrent.PiecePriorityNow)

		freed, err := c.evictRAMSegments()
		if err != nil {
			t.Fatalf("eviction: %v", err)
		}
		if freed == 0 {
			t.Fatal("test did not discard any RAM segment")
		}
		if missing := torr.PieceBytesMissing(0); missing == 0 {
			t.Fatal("discarded chunks were not invalidated")
		}
		if got := torr.PieceState(0).Priority; got != torrent.PiecePriorityNone {
			t.Fatalf("evicted piece re-requested without readers: priority=%v", got)
		}
	})
}

// Eviction may remove a distant segment of a piece while a different
// HTTP reader still needs data from that same piece. That reader must
// keep the piece priority, even though some bytes were discarded.
func TestRAMSegmentEvictionPreservesAnotherReadersPriority(t *testing.T) {
	c, torr := createFullEvictionTestCache(t)

	closed := c.NewReader(torr.Files()[0])
	remaining := c.NewReader(torr.Files()[0])
	remaining.SetReadahead(0)
	c.CloseReader(closed)

	readID, _, ok := remaining.beginRead(16 << 10)
	if !ok {
		t.Fatal("failed to register an active read")
	}
	t.Cleanup(func() {
		remaining.endRead(readID)
		c.CloseReader(remaining)
	})

	withMaintenanceLock(c, func() {
		originalCapacity := c.capacity
		defer func() { c.capacity = originalCapacity }()
		c.capacity = memSegmentSize
		c.getRemPiecesX(false, false)

		torr.Piece(0).SetPriority(torrent.PiecePriorityNow)

		freed, err := c.evictRAMSegments()
		if err != nil {
			t.Fatalf("eviction with another reader: %v", err)
		}
		if freed == 0 {
			t.Fatal("test did not discard any unprotected RAM segment")
		}
		if missing := torr.PieceBytesMissing(0); missing == 0 {
			t.Fatal("discarded chunks were not invalidated")
		}
		if got := torr.PieceState(0).Priority; got != torrent.PiecePriorityNow {
			t.Fatalf("active reader lost its piece priority: got %v", got)
		}

		// The first 256 KiB segment contains the active read.
		segments := c.pieces[0].mPiece.allocatedSegmentOffsets()
		if len(segments) == 0 || segments[0] != 0 {
			t.Fatalf("active read's segment was evicted: %v", segments)
		}
	})
}
