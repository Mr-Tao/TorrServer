package torrstor

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestMemPieceDiscardSegment(t *testing.T) {
	m := newSegmentTestPiece(2 * memSegmentSize)

	// Prevent asynchronous cleanup during this test.
	m.piece.cache.muRemove.Lock()
	defer m.piece.cache.muRemove.Unlock()

	block := bytes.Repeat([]byte{0x5a}, 16<<10)

	for _, off := range []int64{0, memSegmentSize} {
		n, err := m.WriteAt(block, off)
		if err != nil || n != len(block) {
			t.Fatalf("write at %d: n=%d err=%v", off, n, err)
		}
	}

	if got := m.allocated(); got != 2*memSegmentSize {
		t.Fatalf("allocated %d", got)
	}

	invalidate := func(
		begin, length int64,
		discard func() error,
	) (int, error) {
		if length != memSegmentSize {
			t.Errorf("invalid length %d", length)
		}
		if err := discard(); err != nil {
			return 0, err
		}
		return 1, nil
	}

	n, err := m.discardSegmentWith(0, invalidate)
	if err != nil || n != 1 {
		t.Fatalf("discard: n=%d err=%v", n, err)
	}

	if got := m.allocated(); got != memSegmentSize {
		t.Fatalf("allocated after discard %d", got)
	}

	// Evicted data must no longer be readable.
	n, err = m.ReadAt(make([]byte, len(block)), 0)
	if n != 0 || err != io.EOF {
		t.Fatalf("evicted data: n=%d err=%v", n, err)
	}

	// The other segment must remain intact.
	got := make([]byte, len(block))
	n, err = m.ReadAt(got, memSegmentSize)
	if err != nil || n != len(block) || !bytes.Equal(got, block) {
		t.Fatalf("retained data: n=%d err=%v", n, err)
	}

	// Remove the remaining segment.
	_, err = m.discardSegmentWith(memSegmentSize, invalidate)
	if err != nil {
		t.Fatal(err)
	}

	if m.allocated() != 0 || m.piece.Size != 0 {
		t.Fatalf(
			"piece still occupied: allocated=%d size=%d",
			m.allocated(), m.piece.Size,
		)
	}
}

func TestMemPieceDiscardRejected(t *testing.T) {
	m := newSegmentTestPiece(memSegmentSize)

	m.piece.cache.muRemove.Lock()
	defer m.piece.cache.muRemove.Unlock()

	data := bytes.Repeat([]byte{0xa5}, 16<<10)
	if _, err := m.WriteAt(data, 0); err != nil {
		t.Fatal(err)
	}

	busy := errors.New("piece busy")

	_, err := m.discardSegmentWith(
		0,
		func(_, _ int64, discard func() error) (int, error) {
			// The underlying torrent API rejects the operation
			// without invoking discard.
			return 0, busy
		},
	)

	if !errors.Is(err, busy) {
		t.Fatalf("expected busy, got %v", err)
	}

	if m.allocated() != memSegmentSize {
		t.Fatal("rejected discard released memory")
	}

	got := make([]byte, len(data))
	n, err := m.ReadAt(got, 0)
	if err != nil || n != len(data) || !bytes.Equal(got, data) {
		t.Fatalf("data damaged: n=%d err=%v", n, err)
	}
}
