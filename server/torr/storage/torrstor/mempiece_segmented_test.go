package torrstor

import (
	"bytes"
	"io"
	"sync"
	"testing"
)

func newSegmentTestPiece(length int64) *MemPiece {
	c := NewCache(1<<30, nil)
	c.pieceLength = length

	p := &Piece{Id: 1, cache: c}
	return NewMemPiece(p)
}

func TestMemPieceSparseAllocation(t *testing.T) {
	m := newSegmentTestPiece(64 << 20)
	data := bytes.Repeat([]byte{0xa5}, 16<<10)

	off := int64(48 << 20)
	n, err := m.WriteAt(data, off)
	if err != nil || n != len(data) {
		t.Fatalf("write: n=%d err=%v", n, err)
	}

	if got := m.allocated(); got != memSegmentSize {
		t.Fatalf("allocated %d, want %d", got, memSegmentSize)
	}

	got := make([]byte, len(data))
	n, err = m.ReadAt(got, off)
	if err != nil || n != len(got) || !bytes.Equal(got, data) {
		t.Fatalf("read: n=%d err=%v", n, err)
	}

	// A completely absent segment must not be read as zeros.
	n, err = m.ReadAt(make([]byte, 16<<10), 0)
	if n != 0 || err != io.EOF {
		t.Fatalf("missing segment: n=%d err=%v", n, err)
	}

	m.Release()
	if got := m.allocated(); got != 0 {
		t.Fatalf("after release: allocated %d", got)
	}
}

func TestMemPieceCrossSegmentBoundary(t *testing.T) {
	m := newSegmentTestPiece(1 << 20)
	data := []byte("crossing-segment-boundary")
	off := memSegmentSize - 8

	n, err := m.WriteAt(data, off)
	if err != nil || n != len(data) {
		t.Fatalf("write: n=%d err=%v", n, err)
	}

	if got := m.allocated(); got != 2*memSegmentSize {
		t.Fatalf("allocated %d, want %d", got, 2*memSegmentSize)
	}

	got := make([]byte, len(data))
	n, err = m.ReadAt(got, off)
	if err != nil || n != len(data) || !bytes.Equal(got, data) {
		t.Fatalf("read: n=%d err=%v", n, err)
	}
}

func TestMemPieceCompleteRead(t *testing.T) {
	const length = 1 << 20
	m := newSegmentTestPiece(length)
	data := bytes.Repeat([]byte{0x5c}, length)

	for off := 0; off < len(data); off += 16 << 10 {
		chunk := data[off : off+(16<<10)]
		n, err := m.WriteAt(chunk, int64(off))
		if err != nil || n != len(chunk) {
			t.Fatalf("write at %d: n=%d err=%v", off, n, err)
		}
	}

	got, err := io.ReadAll(io.NewSectionReader(m, 0, length))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("complete piece contents differ")
	}

	if m.allocated() != length {
		t.Fatalf("allocated %d, want %d", m.allocated(), length)
	}
}

func TestMemPieceConcurrentWrites(t *testing.T) {
	const (
		length    = 8 << 20
		blockSize = 16 << 10
		workers   = 8
	)

	m := newSegmentTestPiece(length)
	expected := make([]byte, length)

	for i := range expected {
		expected[i] = byte((i / blockSize) % 251)
	}

	var wg sync.WaitGroup

	for worker := 0; worker < workers; worker++ {
		wg.Add(1)

		go func(worker int) {
			defer wg.Done()

			for i := worker; i < length/blockSize; i += workers {
				// Write blocks in reverse order.
				block := length/blockSize - 1 - i
				off := block * blockSize

				n, err := m.WriteAt(
					expected[off:off+blockSize],
					int64(off),
				)

				if err != nil || n != blockSize {
					t.Errorf(
						"write block %d: n=%d err=%v",
						block, n, err,
					)
					return
				}
			}
		}(worker)
	}

	wg.Wait()

	got := make([]byte, length)
	n, err := m.ReadAt(got, 0)

	if err != nil || n != length {
		t.Fatalf("read: n=%d err=%v", n, err)
	}

	if !bytes.Equal(got, expected) {
		t.Fatal("concurrently written piece contents differ")
	}

	if allocated := m.allocated(); allocated != length {
		t.Fatalf("allocated %d bytes, want %d", allocated, length)
	}
}
