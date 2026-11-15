package torrstor

import (
	"io"
	"sync"
	"time"

	"github.com/anacrolix/torrent"

	"server/log"
	"server/settings"
)

type Reader struct {
	torrent.Reader
	offset    int64
	readahead int64
	file      *torrent.File

	cache    *Cache
	isClosed bool

	// inFlight counts reads and seeks inside the underlying reader; guarded by mu.
	inFlight int

	///Preload
	lastAccess int64
	isUse      bool
	mu         sync.Mutex
}

type readerState struct {
	offset     int64
	readahead  int64
	lastAccess int64
	active     bool
	closed     bool
	inFlight   int
}

func (r *Reader) stateSnapshot() readerState {
	r.mu.Lock()
	defer r.mu.Unlock()

	return readerState{
		offset:     r.offset,
		readahead:  r.readahead,
		lastAccess: r.lastAccess,
		active:     r.isUse,
		closed:     r.isClosed,
		inFlight:   r.inFlight,
	}
}

func newReader(file *torrent.File, cache *Cache) *Reader {
	r := new(Reader)
	r.file = file
	r.Reader = file.NewReader()

	r.SetReadahead(0)
	r.cache = cache
	r.isUse = true

	cache.muReaders.Lock()
	cache.readers[r] = struct{}{}
	cache.muReaders.Unlock()
	return r
}

func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	if !r.beginIO() {
		return 0, io.EOF
	}
	defer r.endIO()

	n, err := r.Reader.Seek(offset, whence)

	r.mu.Lock()
	if err == nil {
		r.offset = n
		r.lastAccess = time.Now().Unix()
	}
	r.mu.Unlock()

	return n, err
}

func (r *Reader) Read(p []byte) (n int, err error) {
	err = io.EOF
	if !r.beginIO() {
		return 0, io.EOF
	}
	defer r.endIO()
	if r.file.Torrent() != nil && r.file.Torrent().Info() != nil {
		r.beginIO()
		defer r.endIO()
		n, err = r.Reader.Read(p)

		// samsung tv fix xvid/divx
		//if r.offset == 0 && len(p) >= 192 {
		//	str := strings.ToLower(string(p[112:116]))
		//	if str == "xvid" || str == "divx" {
		//		p[112] = 0x4D // M
		//		p[113] = 0x50 // P
		//		p[114] = 0x34 // 4
		//		p[115] = 0x56 // V
		//	}
		//	str = strings.ToLower(string(p[188:192]))
		//	if str == "xvid" || str == "divx" {
		//		p[188] = 0x4D // M
		//		p[189] = 0x50 // P
		//		p[190] = 0x34 // 4
		//		p[191] = 0x56 // V
		//	}
		//}
		r.mu.Lock()
		r.offset += int64(n)
		r.lastAccess = time.Now().Unix()
		r.mu.Unlock()
	} else {
		log.TLogln("Torrent closed and readed")
	}
	return
}

func (r *Reader) SetReadahead(length int64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.cache != nil && length > r.cache.capacity {
		length = r.cache.capacity
	}

	r.readahead = length
	if r.isUse {
		r.Reader.SetReadahead(length)
	}
}

func (r *Reader) Offset() int64 {
	return r.stateSnapshot().offset
}

func (r *Reader) Readahead() int64 {
	return r.stateSnapshot().readahead
}

func (r *Reader) Close() {
	// file reader close in gotorrent
	// this struct close in cache
	r.mu.Lock()
	if r.isClosed {
		r.mu.Unlock()
		return
	}
	r.isClosed = true
	r.mu.Unlock()
	if len(r.file.Torrent().Files()) > 0 {
		r.Reader.Close()
	}
	if patchOn {
		go r.cache.getRemPiecesX(true, false)
	} else {
		go r.cache.getRemPieces()
	}
}

func (r *Reader) getPiecesRange() Range {
	startOff, endOff := r.getOffsetRange()
	return Range{r.getPieceNum(startOff), r.getPieceNum(endOff), r.file}
}

func (r *Reader) getReaderPiece() int {
	offset := r.Offset()
	return r.getPieceNum(offset)
}

func (r *Reader) getReaderRAHPiece() int {
	s := r.stateSnapshot()
	return r.getPieceNum(s.offset + s.readahead)
}

func (r *Reader) getPieceNum(offset int64) int {
	return int((offset + r.file.Offset()) / r.cache.pieceLength)
}

func (r *Reader) getOffsetRange() (int64, int64) {
	offset := r.Offset()
	prc := int64(settings.BTsets.ReaderReadAHead)
	readers := int64(r.getUseReaders())
	if readers == 0 {
		readers = 1
	}

	beginOffset := offset - (r.cache.capacity/readers)*(100-prc)/100
	endOffset := offset + (r.cache.capacity/readers)*prc/100

	if beginOffset < 0 {
		beginOffset = 0
	}

	if endOffset > r.file.Length() {
		endOffset = r.file.Length()
	}
	return beginOffset, endOffset
}

func (r *Reader) checkReader() {
	if r.cache != nil {
		r.cache.evictionMu.RLock()
		defer r.cache.evictionMu.RUnlock()
	}

	s := r.stateSnapshot()

	if time.Now().Unix() > s.lastAccess+60 &&
		r.cache.Readers() > 1 {
		r.readerOff()
	} else {
		r.readerOn()
	}
}

// beginIO claims the reader for one read or seek and resumes it if the sweep parked it.
// Registering under mu is what keeps readerOff from parking it until endIO.
func (r *Reader) beginIO() bool {
	if r.cache != nil {
		r.cache.evictionMu.RLock()
		defer r.cache.evictionMu.RUnlock()
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.isClosed {
		return false
	}

	r.inFlight++
	r.resume()
	return true
}

func (r *Reader) endIO() {
	r.mu.Lock()
	r.inFlight--
	r.mu.Unlock()
}

func (r *Reader) readerOn() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.resume()
}

// resume must be called with mu held.
func (r *Reader) resume() {
	if !r.isUse {
		if pos, err := r.Reader.Seek(0, io.SeekCurrent); err == nil && pos == 0 {
			r.Reader.Seek(r.offset, io.SeekStart)
		}
		r.Reader.SetReadahead(r.readahead)
		r.isUse = true
	}
}

func (r *Reader) readerOff() {
	r.mu.Lock()
	defer r.mu.Unlock()
	// lastAccess is stamped only when a read returns, so a read blocked on the swarm makes a
	// busy reader look idle; seeking the anacrolix reader under it moves that read.
	if r.inFlight > 0 {
		return
	}
	if r.isUse {
		r.Reader.SetReadahead(0)
		r.isUse = false
		if r.offset > 0 {
			r.Reader.Seek(0, io.SeekStart)
		}
	}
}

func (r *Reader) getUseReaders() int {
	return r.cache.GetUseReaders()
}
