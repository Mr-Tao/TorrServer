package torrstor

import (
	"fmt"
	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/storage"
	"sync"
	"time"
)

type Piece struct {
	storage.PieceImpl `json:"-"`

	Id   int   `json:"-"`
	Size int64 `json:"size"`

	Complete bool  `json:"complete"`
	Accessed int64 `json:"accessed"`

	mPiece *MemPiece  `json:"-"`
	dPiece *DiskPiece `json:"-"`

	cache  *Cache `json:"-"`
	metaMu sync.RWMutex
}

type pieceMeta struct {
	size     int64
	complete bool
	accessed int64
}

func (p *Piece) meta() pieceMeta {
	p.metaMu.RLock()
	defer p.metaMu.RUnlock()

	return pieceMeta{
		size:     p.Size,
		complete: p.Complete,
		accessed: p.Accessed,
	}
}

func (p *Piece) noteWrite(n int, length int64) {
	p.metaMu.Lock()
	defer p.metaMu.Unlock()

	p.Size += int64(n)
	if p.Size > length {
		p.Size = length
	}
	p.Accessed = time.Now().Unix()
}

// subtractWritten adjusts the logical downloaded byte count when a RAM
// segment is discarded. The storage caller holds MemPiece.mu, so writes,
// discards and their accounting cannot interleave on the same piece.
func (p *Piece) subtractWritten(n int64) {
	if n <= 0 {
		return
	}
	p.metaMu.Lock()
	p.Size -= n
	if p.Size < 0 {
		p.Size = 0
	}
	p.metaMu.Unlock()
}

func (p *Piece) touch() {
	p.metaMu.Lock()
	p.Accessed = time.Now().Unix()
	p.metaMu.Unlock()
}

func (p *Piece) setComplete(v bool) {
	p.metaMu.Lock()
	p.Complete = v
	p.metaMu.Unlock()
}

func (p *Piece) resetMeta() {
	p.metaMu.Lock()
	p.Size = 0
	p.Complete = false
	p.metaMu.Unlock()
}

func (p *Piece) clearSize() {
	p.metaMu.Lock()
	p.Size = 0
	p.metaMu.Unlock()
}

func (p *Piece) initMeta(size int64, complete bool, accessed int64) {
	p.metaMu.Lock()
	p.Size = size
	p.Complete = complete
	p.Accessed = accessed
	p.metaMu.Unlock()
}

func NewPiece(id int, cache *Cache) *Piece {
	p := &Piece{
		Id:    id,
		cache: cache,
	}

	if !cache.useDisk {
		p.mPiece = NewMemPiece(p)
	} else {
		p.dPiece = NewDiskPiece(p)
	}
	return p
}

func (p *Piece) WriteAt(b []byte, off int64) (n int, err error) {
	if p.mPiece != nil {
		return p.mPiece.WriteAt(b, off)
	}
	return p.dPiece.WriteAt(b, off)
}

func (p *Piece) ReadAt(b []byte, off int64) (n int, err error) {
	if p.mPiece != nil {
		return p.mPiece.ReadAt(b, off)
	}
	return p.dPiece.ReadAt(b, off)
}

// memCost is what this piece costs the cache budget. In RAM mode that is the allocated buffer
// (a whole piece from the first written byte), not the bytes received; Size stays the logical
// downloaded size used for progress reporting. Disk pieces cost their file size.
func (p *Piece) memCost() int64 {
	if p.mPiece != nil {
		return p.mPiece.allocated()
	}
	return p.meta().size
}

func (p *Piece) MarkComplete() error {
	p.setComplete(true)
	return nil
}

func (p *Piece) MarkNotComplete() error {
	p.setComplete(false)
	return nil
}

func (p *Piece) Completion() storage.Completion {
	return storage.Completion{
		Complete: p.meta().complete,
		Ok:       true,
	}
}

func (p *Piece) Release() error {
	torr := p.cache.getTorrent()
	if torr == nil {
		return fmt.Errorf("torrstor: torrent not available")
	}

	tp := torr.Piece(p.Id)

	if p.mPiece != nil {
		// Atomically invalidate all torrent chunks and discard
		// RAM storage under the torrent client's lock.
		err := tp.DiscardAllChunks(func() error {
			p.mPiece.Release()
			return nil
		})
		if err != nil {
			return err
		}

		// DiscardAllChunks already updates completion,
		// chunk availability and reader notifications.
	} else {
		// Keep existing disk behavior for now.
		p.dPiece.Release()
		tp.UpdateCompletion()
	}

	tp.SetPriority(torrent.PiecePriorityNone)
	return nil
}
