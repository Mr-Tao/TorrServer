package torrstor

import (
	"io"
	"sync"
	"time"
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

	p.piece.Size += int64(n)
	if p.piece.Size > length {
		p.piece.Size = length
	}
	p.piece.Accessed = time.Now().Unix()

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

	p.piece.Accessed = time.Now().Unix()

	if off+int64(len(b)) >= p.piece.Size {
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

	p.piece.Size = 0
	p.piece.Complete = false
	p.piece.cache.unmarkActive(p.piece.Id)
}
