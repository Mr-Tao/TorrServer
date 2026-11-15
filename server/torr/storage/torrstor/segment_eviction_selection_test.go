package torrstor

import (
	"bytes"
	"io"
	"reflect"
	"testing"
)

func TestSegmentEvictionPreservesProtectedData(t *testing.T) {
	const S = memSegmentSize

	m := newSegmentTestPiece(3 * S)

	// This test exercises segment selection and removal,
	// not the asynchronous cache cleanup mechanism.
	// Mark the synthetic cache as closed so cleanPieces()
	// returns immediately without accessing global settings.
	m.piece.cache.isClosed.Store(true)

	data := bytes.Repeat([]byte{0x5a}, 16<<10)

	// Allocate three separate segments.
	for _, off := range []int64{0, S, 2 * S} {
		n, err := m.WriteAt(data, off)
		if err != nil || n != len(data) {
			t.Fatalf("write at %d: n=%d err=%v", off, n, err)
		}
	}

	if got := m.allocated(); got != 3*S {
		t.Fatalf("allocated=%d, want=%d", got, 3*S)
	}

	// A read is using part of the middle segment.
	protected := []byteInterval{
		{begin: S + 4096, end: S + 8192},
	}

	candidates := m.evictableSegmentOffsets(
		0, 3*S, S, protected,
	)

	want := []int64{0, 2 * S}
	if !reflect.DeepEqual(candidates, want) {
		t.Fatalf(
			"candidates=%v, want=%v",
			candidates, want,
		)
	}

	// Simulate successful torrent chunk invalidation.
	// The callback actually releases segment memory.
	invalidate := func(
		begin, length int64,
		discard func() error,
	) (int, error) {
		if length != S {
			t.Errorf("invalid segment length %d", length)
		}
		if err := discard(); err != nil {
			return 0, err
		}
		return int(length / (16 << 10)), nil
	}

	for _, off := range candidates {
		if _, err := m.discardSegmentWith(off, invalidate); err != nil {
			t.Fatalf("discard at %d: %v", off, err)
		}
	}

	// Only the protected segment remains allocated.
	if got := m.allocated(); got != S {
		t.Fatalf("allocated after eviction=%d, want=%d", got, S)
	}

	buf := make([]byte, len(data))

	// Protected data must be unchanged.
	n, err := m.ReadAt(buf, S)
	if err != nil || n != len(data) || !bytes.Equal(buf, data) {
		t.Fatalf("protected data corrupted: n=%d err=%v", n, err)
	}

	// Both evicted segments must be absent.
	for _, off := range want {
		n, err := m.ReadAt(buf, off)
		if n != 0 || err != io.EOF {
			t.Fatalf(
				"evicted segment %d still readable: n=%d err=%v",
				off, n, err,
			)
		}
	}
}
