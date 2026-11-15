package torrstor

import (
	"errors"
	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/storage"
	"server/settings"
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

	if !settings.BTsets.UseDisk {
		p.mPiece = NewMemPiece(p)
	} else {
		p.dPiece = NewDiskPiece(p)
	}
	return p
}

func (p *Piece) WriteAt(b []byte, off int64) (n int, err error) {
	if !settings.BTsets.UseDisk {
		return p.mPiece.WriteAt(b, off)
	} else {
		return p.dPiece.WriteAt(b, off)
	}
}

func (p *Piece) ReadAt(b []byte, off int64) (n int, err error) {
	if !settings.BTsets.UseDisk {
		return p.mPiece.ReadAt(b, off)
	} else {
		return p.dPiece.ReadAt(b, off)
	}
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
	if p.mPiece != nil {
		if p.cache == nil || p.cache.getTorrent() == nil {
			return errors.New("torrstor: torrent not available")
		}

		tp := p.cache.getTorrent().Piece(p.Id)

		return tp.DiscardAllChunks(func() error {
			p.mPiece.Release()
			return nil
		})
	}

	// Preserve the original disk-cache behavior.
	p.dPiece.Release()
	torr := p.cache.getTorrent()
	torr.Piece(p.Id).SetPriority(torrent.PiecePriorityNone)
	torr.Piece(p.Id).UpdateCompletion()

	return nil
}
