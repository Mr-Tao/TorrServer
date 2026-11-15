package torrstor

import (
	"context"
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
	// All fields are protected by mu.
	activeReadRanges map[uint64]byteInterval
	nextReadID       uint64
	unknownInFlight  bool
	// Protected by mu. Invalidates position updates from old reads.
	posGeneration uint64
	// Created once in newReader. Canceled when the reader closes.
	readCtx     context.Context
	cancelReads context.CancelFunc
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

	return r.stateSnapshotLocked()
}

func (r *Reader) stateSnapshotLocked() readerState {
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

	r.readCtx, r.cancelReads = context.WithCancel(context.Background())

	cache.registerReader(r)
	return r
}

func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	if !r.beginIO() {
		return 0, io.EOF
	}
	defer r.endIO()

	// Invalidate reads started before this Seek.
	r.mu.Lock()
	r.posGeneration++
	r.mu.Unlock()

	// Do not hold r.mu while calling the underlying reader.
	pos, err := r.Reader.Seek(offset, whence)

	r.mu.Lock()

	// Also invalidate reads that started while Seek
	// was executing.
	r.posGeneration++

	if err == nil {
		r.offset = pos
	}

	r.lastAccess = time.Now().Unix()
	r.mu.Unlock()

	return pos, err
}

func (r *Reader) Read(p []byte) (n int, err error) {
	err = io.EOF
	readID, readGeneration, ok := r.beginRead(len(p))
	if !ok {
		return 0, io.EOF
	}
	defer r.endRead(readID)
	if r.file.Torrent() != nil && r.file.Torrent().Info() != nil {
		r.beginIO()
		defer r.endIO()
		n, err = r.Reader.ReadContext(r.readCtx, p)

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
		r.finishReadPosition(readGeneration, n)
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
	r.mu.Lock()

	if r.isClosed {
		r.mu.Unlock()
		return
	}

	r.isClosed = true
	cancel := r.cancelReads
	finish := r.inFlight == 0

	r.mu.Unlock()

	// Wake reads waiting for torrent data.
	// Do not hold r.mu while canceling.
	if cancel != nil {
		cancel()
	}

	if finish {
		r.finalizeClose()
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

	if r.inFlight != 0 {
		r.unknownInFlight = true
	}

	r.inFlight++
	r.resume()
	return true
}

func (r *Reader) endIO() {
	r.mu.Lock()

	r.inFlight--
	last := r.inFlight == 0

	if last {
		r.unknownInFlight = false
	}

	finish := last && r.isClosed
	r.mu.Unlock()

	if finish {
		r.finalizeClose()
	}
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

// beginRead registers the requested byte range before I/O begins.
func (r *Reader) beginRead(length int) (uint64, uint64, bool) {
	if r.cache != nil {
		r.cache.evictionMu.RLock()
		defer r.cache.evictionMu.RUnlock()
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.isClosed {
		return 0, 0, false
	}

	// Concurrent operations may change the underlying reader
	// position. Their ranges are not reliable.
	if r.inFlight != 0 {
		r.unknownInFlight = true
	}

	r.inFlight++
	r.nextReadID++
	id := r.nextReadID

	fileLength := r.file.Length()

	if length <= 0 || r.offset < 0 || r.offset >= fileLength {
		r.unknownInFlight = true
	} else {
		size := int64(length)

		if size > fileLength-r.offset {
			size = fileLength - r.offset
		}

		start := r.file.Offset() + r.offset
		end := start + size

		if start < r.file.Offset() || end < start {
			// Invalid or overflowing range.
			r.unknownInFlight = true
		} else {
			if r.activeReadRanges == nil {
				r.activeReadRanges = make(map[uint64]byteInterval)
			}

			r.activeReadRanges[id] = byteInterval{
				begin: start,
				end:   end,
			}
		}
	}

	r.resume()
	return id, r.posGeneration, true
}

func (r *Reader) endRead(id uint64) {
	r.mu.Lock()

	delete(r.activeReadRanges, id)

	r.inFlight--
	last := r.inFlight == 0

	if last {
		r.unknownInFlight = false
	}

	finish := last && r.isClosed
	r.mu.Unlock()

	if finish {
		r.finalizeClose()
	}
}

// finishReadPosition updates the published position only
// if no Seek has invalidated the read.
func (r *Reader) finishReadPosition(generation uint64, n int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.posGeneration == generation {
		r.offset += int64(n)
	}

	r.lastAccess = time.Now().Unix()
}

func (r *Reader) stateWithReadsSnapshot() (
	readerState,
	[]byteInterval,
	bool,
) {
	r.mu.Lock()
	defer r.mu.Unlock()

	state := r.stateSnapshotLocked()

	ranges := make([]byteInterval, 0, len(r.activeReadRanges))
	for _, interval := range r.activeReadRanges {
		ranges = append(ranges, interval)
	}

	return state, ranges, r.unknownInFlight
}

// finalizeClose is called exactly once, after the last
// in-flight operation has completed.
func (r *Reader) finalizeClose() {
	c := r.cache

	if c != nil {
		// Unregistration must not overlap an eviction snapshot.
		c.evictionMu.RLock()

		c.muReaders.Lock()
		delete(c.readers, r)
		c.muReaders.Unlock()

		c.evictionMu.RUnlock()
	}

	// Do not close the underlying torrent reader while
	// an operation is still using it.
	if r.Reader != nil && r.file != nil {
		if t := r.file.Torrent(); t != nil && len(t.Files()) > 0 {
			_ = r.Reader.Close()
		}
	}

	if c != nil {
		go c.getRemPieces()
		go c.clearPriority()
	}
}
