package torrstor

import (
	"fmt"
	"io"
	"sort"
	"sync"
)

const memSegmentSize int64 = 256 << 10
const memCleanupStep int64 = 4 << 20

type MemPiece struct {
	piece *Piece

	// Segments are allocated lazily. An absent segment
	// does not consume payload memory.
	segments map[int64][]byte
	bytes    int64

	lastCleanupBytes int64
	mu               sync.RWMutex
}

func NewMemPiece(p *Piece) *MemPiece {
	return &MemPiece{piece: p}
}

func (p *MemPiece) WriteAt(b []byte, off int64) (n int, err error) {
	length := p.piece.cache.pieceLength

	if off < 0 || off > length {
		return 0, io.ErrShortWrite
	}

	if int64(len(b)) > length-off {
		b = b[:int(length-off)]
		err = io.ErrShortWrite
	}
	if len(b) == 0 {
		return 0, err
	}

	p.mu.Lock()

	if p.segments == nil {
		p.segments = make(map[int64][]byte)
	}

	wasEmpty := p.bytes == 0

	for n < len(b) {
		idx := off / memSegmentSize
		seg := p.segments[idx]

		if seg == nil {
			segLength := memSegmentSize
			if remaining := length - idx*memSegmentSize; remaining < segLength {
				segLength = remaining
			}

			seg = make([]byte, int(segLength))
			p.segments[idx] = seg
			p.bytes += int64(cap(seg))
		}

		segOff := int(off % memSegmentSize)
		count := copy(seg[segOff:], b[n:])
		n += count
		off += int64(count)
	}

	p.piece.noteWrite(n, length)

	if wasEmpty {
		p.piece.cache.markActive(p.piece)
	}

	cleanup := wasEmpty ||
		p.bytes-p.lastCleanupBytes >= memCleanupStep

	if cleanup {
		p.lastCleanupBytes = p.bytes
	}

	p.mu.Unlock()

	if cleanup {
		go p.piece.cache.cleanPieces()
	}

	return n, err
}

func (p *MemPiece) ReadAt(b []byte, off int64) (n int, err error) {
	length := p.piece.cache.pieceLength

	if off < 0 {
		return 0, io.EOF
	}
	if len(b) == 0 {
		return 0, nil
	}
	if off >= length {
		return 0, io.EOF
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	wanted := len(b)
	if int64(wanted) > length-off {
		wanted = int(length - off)
	}

	for n < wanted {
		pos := off + int64(n)
		idx := pos / memSegmentSize
		seg := p.segments[idx]

		// Never silently manufacture an entire missing
		// segment out of zero bytes.
		if seg == nil {
			return n, io.EOF
		}

		segOff := int(pos % memSegmentSize)
		count := copy(b[n:wanted], seg[segOff:])
		n += count
	}

	p.piece.touch()

	if off+int64(len(b)) >= p.piece.meta().size {
		go p.piece.cache.cleanPieces()
	}

	if n < len(b) {
		return n, io.EOF
	}
	return n, nil
}

func (p *MemPiece) allocated() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.bytes
}

func (p *MemPiece) Release() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.segments = nil
	p.bytes = 0
	p.lastCleanupBytes = 0

	p.piece.resetMeta()
	p.piece.cache.unmarkActive(p.piece.Id)
}

type discardRangeFunc func(
	begin, length int64,
	discard func() error,
) (int, error)

// DiscardSegment removes one allocated 256 KiB segment and
// invalidates its torrent chunks as one coordinated operation.
//
// The offset must be relative to the piece and segment-aligned.
// This method does not decide whether the segment is safe to
// evict from the perspective of active HTTP readers.
func (p *MemPiece) DiscardSegment(offset int64) (int, error) {
	if p == nil || p.piece == nil || p.piece.cache == nil {
		return 0, fmt.Errorf("torrstor: invalid memory piece")
	}

	torr := p.piece.cache.getTorrent()
	if torr == nil {
		return 0, fmt.Errorf("torrstor: torrent not available")
	}

	return p.discardSegmentWith(
		offset,
		torr.Piece(p.piece.Id).DiscardChunkRange,
	)
}

// Separate the storage operation from the torrent API so that
// it can be tested without a running torrent client.
func (p *MemPiece) discardSegmentWith(
	offset int64,
	invalidate discardRangeFunc,
) (int, error) {
	if p == nil || p.piece == nil ||
		p.piece.cache == nil {
		return 0, fmt.Errorf("torrstor: invalid memory piece")
	}
	if invalidate == nil {
		return 0, fmt.Errorf("torrstor: missing invalidator")
	}

	length := p.piece.cache.pieceLength

	if torr := p.piece.cache.getTorrent(); torr != nil && torr.Info() != nil {
		length = torr.Info().Piece(p.piece.Id).Length()
	}

	if offset < 0 ||
		offset >= length ||
		offset%memSegmentSize != 0 {
		return 0, fmt.Errorf(
			"torrstor: invalid segment offset %d", offset,
		)
	}

	segmentLength := min(memSegmentSize, length-offset)
	index := offset / memSegmentSize

	return invalidate(offset, segmentLength, func() error {
		p.mu.Lock()
		defer p.mu.Unlock()

		if seg, ok := p.segments[index]; ok {
			delete(p.segments, index)
			p.bytes -= int64(cap(seg))
		}

		if p.bytes == 0 {
			p.segments = nil
			p.lastCleanupBytes = 0
			p.piece.clearSize()
			p.piece.cache.unmarkActive(p.piece.Id)
		} else if p.lastCleanupBytes > p.bytes {
			p.lastCleanupBytes = p.bytes
		}

		return nil
	})
}

// allocatedSegmentOffsets returns a stable snapshot of
// allocated segment offsets, relative to the piece.
//
// The returned slice is independent of the segments map.
func (p *MemPiece) allocatedSegmentOffsets() []int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	offsets := make([]int64, 0, len(p.segments))

	for idx := range p.segments {
		offsets = append(offsets, idx*memSegmentSize)
	}

	sort.Slice(offsets, func(i, j int) bool {
		return offsets[i] < offsets[j]
	})

	return offsets
}

// evictableSegmentOffsets returns allocated segments outside
// all protected byte intervals.
//
// This is a snapshot-based advisory selection.
// It does not modify the cache or torrent state.
func (p *MemPiece) evictableSegmentOffsets(
	pieceStart, pieceLength, segmentSize int64,
	protected []byteInterval,
) []int64 {
	return selectEvictableRAMSegments(
		pieceStart,
		pieceLength,
		segmentSize,
		p.allocatedSegmentOffsets(),
		protected,
	)
}
