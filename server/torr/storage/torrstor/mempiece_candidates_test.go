package torrstor

import (
	"reflect"
	"sync"
	"testing"
)

func TestMemPieceEvictableSegmentOffsets(t *testing.T) {
	const segmentSize int64 = 256 << 10
	const pieceStart int64 = 64 << 20

	mp := &MemPiece{
		segments: map[int64][]byte{
			0: make([]byte, memSegmentSize),
			1: make([]byte, memSegmentSize),
			3: make([]byte, memSegmentSize),
		},
	}

	protected := []byteInterval{
		{
			begin: pieceStart + segmentSize + 10,
			end:   pieceStart + 2*segmentSize,
		},
	}

	got := mp.evictableSegmentOffsets(
		pieceStart,
		4*segmentSize,
		segmentSize,
		protected,
	)

	want := []int64{0, 3 * memSegmentSize}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("candidates = %v, want %v", got, want)
	}

	// A snapshot must not change after the map changes.
	snapshot := mp.allocatedSegmentOffsets()

	mp.mu.Lock()
	delete(mp.segments, 0)
	mp.mu.Unlock()

	wantSnapshot := []int64{
		0,
		segmentSize,
		3 * segmentSize,
	}

	if !reflect.DeepEqual(snapshot, wantSnapshot) {
		t.Fatalf("snapshot changed: %v", snapshot)
	}
}

func TestAllocatedSegmentOffsetsConcurrent(t *testing.T) {
	const segmentSize int64 = 256 << 10

	mp := &MemPiece{
		segments: make(map[int64][]byte),
	}

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		for i := 0; i < 1000; i++ {
			off := int64(i%8) * segmentSize

			mp.mu.Lock()
			mp.segments[off] = []byte{1}

			if i%3 == 0 {
				delete(mp.segments, off)
			}
			mp.mu.Unlock()
		}
	}()

	for i := 0; i < 1000; i++ {
		_ = mp.allocatedSegmentOffsets()
	}

	wg.Wait()
}
