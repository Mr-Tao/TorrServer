package torrstor

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

func TestWrittenSpanUnion(t *testing.T) {
	var spans []writtenSpan
	steps := []struct {
		begin, end int64
		added      int64
		want       []writtenSpan
	}{
		{10, 20, 10, []writtenSpan{{10, 20}}},
		{10, 20, 0, []writtenSpan{{10, 20}}}, // retransmission
		{15, 25, 5, []writtenSpan{{10, 25}}}, // partial overlap
		{30, 40, 10, []writtenSpan{{10, 25}, {30, 40}}},
		{25, 30, 5, []writtenSpan{{10, 40}}}, // merge adjacent ranges
		{5, 45, 10, []writtenSpan{{5, 45}}}, // envelop old range
		{45, 45, 0, []writtenSpan{{5, 45}}}, // empty write
		{1, 3, 2, []writtenSpan{{1, 3}, {5, 45}}},
	}
	var total int64
	for i, step := range steps {
		var added int64
		spans, added = addWrittenSpan(spans, step.begin, step.end)
		total += added
		if added != step.added || !reflect.DeepEqual(spans, step.want) {
			t.Fatalf("step %d: added=%d spans=%v, want added=%d spans=%v",
				i, added, spans, step.added, step.want)
		}
		if got := writtenSpanBytes(spans); got != total {
			t.Fatalf("step %d: covered %d, tracked total %d", i, got, total)
		}
	}
}

func TestMemPieceLogicalSizeAfterPartialDiscard(t *testing.T) {
	m := newSegmentTestPiece(3 * memSegmentSize)
	c := m.piece.cache
	// Coordinate with WriteAt's asynchronous cleanup, like existing tests.
	c.muRemove.Lock()
	defer c.muRemove.Unlock()
	c.pieces[m.piece.Id] = m.piece

	write := func(off int64, length int) {
		t.Helper()
		n, err := m.WriteAt(bytes.Repeat([]byte{0x52}, length), off)
		if err != nil || n != length {
			t.Fatalf("write at %d: n=%d err=%v", off, n, err)
		}
	}
	discard := func(off int64) {
		t.Helper()
		_, err := m.discardSegmentWith(off, func(_, _ int64, cb func() error) (int, error) {
			return 1, cb()
		})
		if err != nil {
			t.Fatalf("discard at %d: %v", off, err)
		}
	}
	check := func(want int64) {
		t.Helper()
		if got := m.piece.meta().size; got != want {
			t.Fatalf("Piece.Size=%d, want %d", got, want)
		}
		st := c.GetState()
		if got := st.Filled; got != want {
			t.Fatalf("CacheState.Filled=%d, want %d", got, want)
		}
		if want > 0 {
			if got := st.Pieces[m.piece.Id].Size; got != want {
				t.Fatalf("CacheState piece size=%d, want %d", got, want)
			}
		}
	}

	const KiB = 1 << 10
	write(0, 16*KiB)
	write(0, 16*KiB)        // no double-count
	write(8*KiB, 16*KiB)    // only 8 KiB are new
	write(memSegmentSize, 8*KiB)
	write(2*memSegmentSize+1*KiB, 24*KiB)
	check(56 * KiB)

	discard(0)
	check(32 * KiB)
	write(0, 16*KiB)
	check(48 * KiB)
	write(8*KiB, 16*KiB)
	check(56 * KiB)

	discard(memSegmentSize)
	check(48 * KiB)
	discard(2 * memSegmentSize)
	check(24 * KiB)
	discard(0)
	check(0)
	if got := m.allocated(); got != 0 {
		t.Fatalf("allocated after all discards: %d", got)
	}
	// A fresh download must start its logical accounting from zero.
	write(0, 16*KiB)
	check(16 * KiB)
	m.Release()
	check(0)
}

func TestMemPieceCrossSegmentLogicalSize(t *testing.T) {
	m := newSegmentTestPiece(2 * memSegmentSize)
	m.piece.cache.muRemove.Lock()
	defer m.piece.cache.muRemove.Unlock()

	const KiB = 1 << 10
	off := memSegmentSize - 16*KiB
	b := bytes.Repeat([]byte{0x31}, 40*KiB)
	if n, err := m.WriteAt(b, off); err != nil || n != len(b) {
		t.Fatalf("cross-segment write: %d, %v", n, err)
	}
	if got := m.piece.meta().size; got != 40*KiB {
		t.Fatalf("initial Size=%d", got)
	}

	discard := func(offset int64) {
		t.Helper()
		_, err := m.discardSegmentWith(offset, func(_, _ int64, cb func() error) (int, error) {
			return 1, cb()
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	discard(0)
	if got := m.piece.meta().size; got != 24*KiB {
		t.Fatalf("after first discard Size=%d, want %d", got, 24*KiB)
	}
	discard(memSegmentSize)
	if got := m.piece.meta().size; got != 0 {
		t.Fatalf("after second discard Size=%d, want 0", got)
	}
}

func TestMemPieceRejectedDiscardKeepsLogicalSize(t *testing.T) {
	m := newSegmentTestPiece(memSegmentSize)
	m.piece.cache.muRemove.Lock()
	defer m.piece.cache.muRemove.Unlock()

	b := bytes.Repeat([]byte{0xa5}, 16<<10)
	if _, err := m.WriteAt(b, 0); err != nil {
		t.Fatal(err)
	}

	busy := errors.New("busy")
	_, err := m.discardSegmentWith(0,
		func(_, _ int64, _ func() error) (int, error) {
			return 0, busy // callback must not run
		})
	if !errors.Is(err, busy) {
		t.Fatalf("discard error=%v, want busy", err)
	}
	if got := m.piece.meta().size; got != int64(len(b)) {
		t.Fatalf("Size after refused discard=%d, want %d", got, len(b))
	}
	if got := m.allocated(); got != memSegmentSize {
		t.Fatalf("allocated after refused discard=%d", got)
	}
}
