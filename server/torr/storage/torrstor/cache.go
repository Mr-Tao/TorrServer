package torrstor

import (
	"errors"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anacrolix/torrent"

	"server/log"
	"server/settings"
	"server/torr/storage/state"
	"server/torr/utils"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
)

func init() {
	if addr := os.Getenv("TS_PPROF"); addr != "" {
		go func() { log.TLogln("pprof listening on", addr, http.ListenAndServe(addr, nil)) }()
	}
}

const ramEvictionBatchBytes int64 = 2 << 20

// ---- instrumentation / patch switch (TS_PATCH=1 enables the fast paths)
var patchOn = os.Getenv("TS_PATCH") != "0"

var inflightLimit = func() int {
	n := 20
	fmt.Sscanf(os.Getenv("TS_INFLIGHT"), "%d", &n)
	return n
}()

type cacheStats struct {
	cleanCalls, cleanSkipped, cleanRuns, getRemCalls, getRemFull  atomic.Int64
	clrCalls, clrFull, pieceStateCalls, removed, scanned, gcCalls atomic.Int64
	getRemNs, clrNs, gcNs, cleanNs                                atomic.Int64
}

var stats cacheStats

func (c *Cache) statsLoop() {
	for !c.isClosed.Load() {
		time.Sleep(2 * time.Second)
		cc, sk, cr := stats.cleanCalls.Swap(0), stats.cleanSkipped.Swap(0), stats.cleanRuns.Swap(0)
		gn, gf, gms := stats.getRemCalls.Swap(0), stats.getRemFull.Swap(0), stats.getRemNs.Swap(0)/1e6
		sc := stats.scanned.Swap(0)
		cn, cf, cms, ps := stats.clrCalls.Swap(0), stats.clrFull.Swap(0), stats.clrNs.Swap(0)/1e6, stats.pieceStateCalls.Swap(0)
		gcn, gcms := stats.gcCalls.Swap(0), stats.gcNs.Swap(0)/1e6
		rm, clms := stats.removed.Swap(0), stats.cleanNs.Swap(0)/1e6
		if cc+gn+cn == 0 {
			continue
		}
		c.muActive.Lock()
		na := len(c.active)
		c.muActive.Unlock()
		log.TLogln(fmt.Sprintf("[CSTAT patch=%v] clean calls=%d skipped=%d runs=%d busy=%dms | getRem n=%d full=%d %dms scanned=%d | clearPrio n=%d full=%d %dms pstate=%d | gc n=%d %dms | removed=%d | active=%d filled=%dMB cap=%dMB readers=%d",
			c.patchEnabled, cc, sk, cr, clms, gn, gf, gms, sc, cn, cf, cms, ps, gcn, gcms, rm, na, c.statFilled.Load()>>20, c.capacity>>20, len(c.readersSnapshot())))
	}
}

func (c *Cache) markActive(p *Piece) {
	c.muActive.Lock()
	c.active[p.Id] = p
	c.muActive.Unlock()
}

func (c *Cache) unmarkActive(id int) {
	c.muActive.Lock()
	delete(c.active, id)
	c.muActive.Unlock()
}

func (c *Cache) activeSnapshot() map[int]*Piece {
	c.muActive.Lock()
	defer c.muActive.Unlock()
	m := make(map[int]*Piece, len(c.active))
	for k, v := range c.active {
		m[k] = v
	}
	return m
}

type Cache struct {
	storage.TorrentImpl
	storage *Storage

	// filled is the eviction input: memory the pieces occupy (allocated buffers in RAM mode),
	// recomputed by getRemPieces. It is not the downloaded byte count (see GetState).
	capacity int64
	filled   int64
	hash     metainfo.Hash

	pieceLength int64
	pieceCount  int

	// pieces content is immutable after Init; muPieces guards the map
	// reference itself (nilled in Close), so holders of a snapshot may
	// safely iterate it without the lock
	pieces   map[int]*Piece
	muPieces sync.RWMutex

	readers   map[*Reader]struct{}
	muReaders sync.RWMutex

	isRemove atomic.Bool
	isClosed atomic.Bool
	muRemove sync.Mutex
	// muPrio serializes clearPriority and setLoadPriority so that the priority
	// reset of a reader that has just closed cannot wipe the priorities a
	// freshly created reader has already set.
	muPrio  sync.Mutex
	torrent atomic.Pointer[torrent.Torrent]

	muActive     sync.Mutex
	active       map[int]*Piece
	prioSet      map[int]struct{} // guarded by muPrio
	pendingClean atomic.Bool
	lastFull     time.Time
	lastPrio     atomic.Int64
	statFilled   atomic.Int64
	// evictionMu coordinates reader position changes with cache eviction.
	//
	// Readers acquire RLock when starting an operation.
	// Eviction acquires Lock across window selection and discard.
	evictionMu sync.RWMutex
	// Configuration captured at cache creation.
	patchEnabled           bool
	connectionsLimit       int
	readerReadAheadPercent int64
	useDisk                bool
	savePath               string
	removeCacheOnDrop      bool
	autoCacheSize          bool
}

func NewCache(capacity int64, storage *Storage) *Cache {
	connectionsLimit := 0
	readerReadAheadPercent := int64(95)

	if bt := settings.BTsets; bt != nil {
		connectionsLimit = bt.ConnectionsLimit
		readerReadAheadPercent = int64(bt.ReaderReadAHead)
	}

	ret := &Cache{
		capacity:               capacity,
		filled:                 0,
		pieces:                 make(map[int]*Piece),
		storage:                storage,
		readers:                make(map[*Reader]struct{}),
		active:                 make(map[int]*Piece),
		prioSet:                make(map[int]struct{}),
		patchEnabled:           patchOn,
		connectionsLimit:       connectionsLimit,
		readerReadAheadPercent: readerReadAheadPercent,
	}

	if bt := settings.BTsets; bt != nil {
		ret.useDisk = bt.UseDisk
		ret.savePath = bt.TorrentsSavePath
		ret.removeCacheOnDrop = bt.RemoveCacheOnDrop
		ret.autoCacheSize = bt.CacheSize == 0
	}

	return ret
}

func (c *Cache) Init(info *metainfo.Info, hash metainfo.Hash) {
	log.TLogln("Create cache for:", info.Name, hash.HexString())
	if c.capacity == 0 {
		c.capacity = info.PieceLength * 4
	}

	c.pieceLength = info.PieceLength
	c.pieceCount = info.NumPieces()
	c.hash = hash

	if settings.BTsets.UseDisk {
		name := filepath.Join(settings.BTsets.TorrentsSavePath, hash.HexString())
		err := os.MkdirAll(name, 0o777)
		if err != nil {
			log.TLogln("Error create dir:", err)
		}
	}

	for i := 0; i < c.pieceCount; i++ {
		c.pieces[i] = NewPiece(i, c)
	}

	go c.priorityWatchdog()
	if os.Getenv("TS_STATS") == "1" {
		go c.statsLoop()
	}
}

// priorityWatchdog re-arms piece priorities while readers are active.
//
// setLoadPriority is only reached through the cache cleanup path, which is
// driven by piece reads and writes (see mempiece.go and diskpiece.go). Should
// priorities ever end up cleared while a reader still needs data, nothing is
// downloaded, so no piece I/O happens, so cleanup never runs and the
// priorities are never restored - the torrent stalls indefinitely with peers
// connected. Re-arming them periodically breaks that cycle regardless of how
// the priorities were lost.
func (c *Cache) priorityWatchdog() {
	for {
		time.Sleep(5 * time.Second)
		if c.isClosed.Load() {
			return
		}
		if c.getTorrent() == nil {
			continue
		}
		if c.GetUseReaders() > 0 {
			c.muRemove.Lock()

			// The cache may have closed while acquiring the lock.
			if c.isClosed.Load() {
				c.muRemove.Unlock()
				return
			}

			if c.patchEnabled {
				full := time.Since(c.lastFull) > 60*time.Second
				if full {
					c.lastFull = time.Now()
				}
				c.getRemPiecesX(true, full)
			} else {
				c.getRemPiecesX(true, true)
			}

			c.unlockMaintenance()
		}
	}
}

func (c *Cache) getTorrent() *torrent.Torrent {
	if c == nil {
		return nil
	}
	return c.torrent.Load()
}

func (c *Cache) SetTorrent(torr *torrent.Torrent) {
	if c == nil {
		return
	}
	c.torrent.Store(torr)
}

func (c *Cache) getPieces() map[int]*Piece {
	c.muPieces.RLock()
	defer c.muPieces.RUnlock()
	return c.pieces
}

func (c *Cache) readersSnapshot() []*Reader {
	c.muReaders.RLock()
	defer c.muReaders.RUnlock()
	list := make([]*Reader, 0, len(c.readers))
	for r := range c.readers {
		list = append(list, r)
	}
	return list
}

func (c *Cache) Piece(m metainfo.Piece) storage.PieceImpl {
	if val, ok := c.getPieces()[m.Index()]; ok {
		return val
	}
	return &PieceFake{}
}

func (c *Cache) Close() error {
	started := time.Now()
	log.TLogln("Cache close begin:", c.hash)
	defer func() {
		log.TLogln("Cache close done:", c.hash, time.Since(started))
	}()
	if torr := c.getTorrent(); torr != nil {
		log.TLogln("Close cache for:", torr.Name(), c.hash)
	} else {
		log.TLogln("Close cache for:", c.hash)
	}
	c.isClosed.Store(true)

	phase := time.Now()
	c.storage.removeCache(c.hash)
	log.TLogln("Cache unregister done:", c.hash, time.Since(phase))
	phase = time.Now()

	if settings.BTsets.RemoveCacheOnDrop {
		name := filepath.Join(settings.BTsets.TorrentsSavePath, c.hash.HexString())
		if name != "" && name != "/" {
			for _, v := range c.getPieces() {
				if v.dPiece != nil {
					os.Remove(v.dPiece.name)
				}
			}
			os.Remove(name)
		}
	}

	log.TLogln("Cache disk cleanup done:", c.hash, time.Since(phase))
	phase = time.Now()
	c.muReaders.Lock()
	c.readers = nil
	c.muReaders.Unlock()
	log.TLogln("Cache readers cleared:", c.hash, time.Since(phase))

	phase = time.Now()
	c.muPieces.Lock()
	c.pieces = nil
	c.muPieces.Unlock()
	log.TLogln("Cache pieces cleared:", c.hash, time.Since(phase))

	phase = time.Now()
	c.muActive.Lock()
	activeCount := len(c.active)
	clear(c.active)
	c.muActive.Unlock()
	log.TLogln("Cache active cleared:", c.hash, activeCount,
		"pieces", time.Since(phase))

	log.TLogln("Cache GC begin:", c.hash)
	phase = time.Now()
	utils.FreeOSMemGC()
	log.TLogln("Cache GC done:", c.hash, time.Since(phase))
	return nil
}

func (c *Cache) removePiece(piece *Piece) bool {
	if c.isClosed.Load() {
		return false
	}
	return piece.Release() == nil
}

func (c *Cache) AdjustRA(readahead int64) {
	if c == nil {
		return
	}
	if settings.BTsets.CacheSize == 0 {
		c.capacity = readahead * 3
	}
	for _, r := range c.readersSnapshot() {
		r.SetReadahead(readahead)
	}
}

func (c *Cache) GetState() *state.CacheState {
	cState := new(state.CacheState)

	piecesState := make(map[int]state.ItemState, 0)
	var fill int64 = 0

	scan := c.getPieces()
	if c.patchEnabled && c.active != nil {
		scan = c.activeSnapshot()
	}

	torr := c.getTorrent()

	for _, p := range scan {
		m := p.meta()
		if m.size <= 0 {
			continue
		}

		priority := torrent.PiecePriorityNone
		if torr != nil {
			priority = torr.PieceState(p.Id).Priority
		}

		fill += m.size
		piecesState[p.Id] = state.ItemState{
			Id:        p.Id,
			Size:      m.size,
			Length:    c.pieceLength,
			Completed: m.complete,
			Priority:  int(priority),
		}
	}

	readersState := make([]*state.ReaderState, 0)

	for _, r := range c.readersSnapshot() {
		rng := r.getPiecesRange()
		pc := r.getReaderPiece()
		readersState = append(readersState, &state.ReaderState{
			Start:  rng.Start,
			End:    rng.End,
			Reader: pc,
		})
	}

	cState.Capacity = c.capacity
	cState.PiecesLength = c.pieceLength
	cState.PiecesCount = c.pieceCount
	cState.Hash = c.hash.HexString()
	cState.Filled = fill
	cState.Pieces = piecesState
	cState.Readers = readersState
	return cState
}

func (c *Cache) cleanPieces() {
	if c.isClosed.Load() {
		return
	}
	stats.cleanCalls.Add(1)
	if c.patchEnabled {
		c.pendingClean.Store(true)
	}

	// Protection against concurrent deletion
	if !c.muRemove.TryLock() {
		stats.cleanSkipped.Add(1)
		return // Cleanup is already in progress in another goroutine
	}
	c.isRemove.Store(true)
	defer func() {
		c.isRemove.Store(false)
		c.unlockMaintenance()
	}()
	// The cache may have been closed between the initial
	// isClosed check and acquiring muRemove.
	if c.isClosed.Load() {
		return
	}

	for iter := 0; iter < 4; iter++ {
		if c.patchEnabled {
			c.pendingClean.Store(false)
		}
		t0 := time.Now()
		apply := true
		if c.patchEnabled {
			now := time.Now().UnixNano()
			apply = now-c.lastPrio.Load() > 400e6 // priorities at most every 400 ms
			if apply {
				c.lastPrio.Store(now)
			}
		}
		remPieces := c.getRemPiecesX(apply, false)
		stats.cleanRuns.Add(1)
		if c.filled > c.capacity {
			released := false

			// Segment-level eviction for the patched RAM cache.
			if c.patchEnabled {
				freed, err := c.evictRAMSegments()
				if err != nil {
					log.TLogln("RAM segment eviction:", err)
				}

				if freed > 0 {
					released = true

					// Continue reclaiming in another bounded pass,
					// but only if this pass made progress.
					if c.filled > c.capacity {
						c.pendingClean.Store(true)
					}
				}
			}

			// Preserve legacy whole-piece eviction for disk storage
			// and for the unpatched cache.
			for _, p := range remPieces {
				if c.filled <= c.capacity {
					break
				}

				// Never discard an entire RAM piece through this
				// fallback: it may contain protected segments.
				if c.patchEnabled && p.mPiece != nil {
					continue
				}

				before := p.memCost()
				if before == 0 {
					continue
				}

				if !c.removePiece(p) {
					continue
				}

				after := p.memCost()
				if before > after {
					c.filled -= before - after
					released = true
				}
			}

			if released {
				utils.FreeOSMemGCThrottled()
			}
		}
		stats.cleanNs.Add(int64(time.Since(t0)))
		if !(c.patchEnabled && c.pendingClean.Load()) {
			break
		}
	}
}

func (c *Cache) getRemPieces() []*Piece { return c.getRemPiecesX(true, true) }

// getRemPiecesX: applyPrio re-arms priorities, full also does the O(all pieces) priority reset.
func (c *Cache) getRemPiecesX(applyPrio, full bool) []*Piece {
	t0 := time.Now()
	stats.getRemCalls.Add(1)
	if full {
		stats.getRemFull.Add(1)
	}
	defer func() { stats.getRemNs.Add(int64(time.Since(t0))) }()
	readers := c.readersSnapshot()

	// Collect read ranges from active readers
	ranges := make([]Range, 0)
	for _, r := range readers {
		r.checkReader()
		if r.stateSnapshot().active {
			ranges = append(ranges, r.getPiecesRange())
		}
	}
	ranges = mergeRange(ranges)

	piecesRemove := make([]*Piece, 0)
	fill := int64(0)

	// Determine which chunks can be deleted
	scan := c.getPieces()
	if c.patchEnabled && c.active != nil {
		scan = c.activeSnapshot()
	}
	stats.scanned.Add(int64(len(scan)))
	for id, p := range scan {
		m := p.meta()
		if m.size > 0 {
			fill += p.memCost()
		}
		if len(ranges) > 0 {
			if !inRanges(ranges, id) {
				if m.size > 0 && !c.isIdInFileBE(ranges, id) {
					piecesRemove = append(piecesRemove, p)
				}
			}
		} else {
			// When preloading, clear everything except the beginning and end of the file
			if m.size > 0 && !c.isIdInFileBE(ranges, id) {
				piecesRemove = append(piecesRemove, p)
			}
		}
	}

	if applyPrio {
		if c.patchEnabled {
			c.clearPriorityX(full)
		} else {
			c.clearPriority()
		}
		c.setLoadPriority(ranges)
	}

	// Complete pieces first, oldest first; a piece still downloading goes last. The torrent
	// client never re-requests chunks it has received, so evicting their bytes makes a
	// responsive reader that comes back to them read zeros.
	sort.Slice(piecesRemove, func(i, j int) bool {
		a := piecesRemove[i].meta()
		b := piecesRemove[j].meta()

		if a.complete != b.complete {
			return a.complete
		}
		return a.accessed < b.accessed
	})

	c.filled = fill
	c.statFilled.Store(fill)
	return piecesRemove
}

func (c *Cache) setLoadPriority(ranges []Range) {
	torr := c.getTorrent()
	if torr == nil {
		return
	}
	readers := c.readersSnapshot()
	pieces := c.getPieces()
	if len(readers) == 0 || pieces == nil {
		return
	}
	c.muPrio.Lock()
	defer c.muPrio.Unlock()
	for _, r := range readers {
		if !r.stateSnapshot().active {
			continue
		}
		if c.isIdInFileBE(ranges, r.getReaderPiece()) {
			continue
		}
		readerPos := r.getReaderPiece()
		readerRAHPos := r.getReaderRAHPiece()
		end := r.getPiecesRange().End
		count := c.connectionsLimit / len(readers) // max concurrent loading blocks
		if c.patchEnabled && inflightLimit > 0 {
			count = inflightLimit / len(readers)
		}
		limit := 0
		for i := readerPos; i < end && limit < count; i++ {
			if !pieces[i].Complete {
				c.prioSet[i] = struct{}{}
				if i == readerPos {
					torr.Piece(i).SetPriority(torrent.PiecePriorityNow)
				} else if i == readerPos+1 {
					torr.Piece(i).SetPriority(torrent.PiecePriorityNext)
				} else if i > readerPos && i <= readerRAHPos {
					torr.Piece(i).SetPriority(torrent.PiecePriorityReadahead)
				} else if i > readerRAHPos && i <= readerRAHPos+5 && torr.PieceState(i).Priority != torrent.PiecePriorityHigh {
					torr.Piece(i).SetPriority(torrent.PiecePriorityHigh)
				} else if i > readerRAHPos+5 && torr.PieceState(i).Priority != torrent.PiecePriorityNormal {
					torr.Piece(i).SetPriority(torrent.PiecePriorityNormal)
				}
				limit++
			}
		}
	}
}

func (c *Cache) isIdInFileBE(ranges []Range, id int) bool {
	// keep 8/16 MB
	FileRangeNotDelete := int64(c.pieceLength)
	if FileRangeNotDelete < 8<<20 {
		FileRangeNotDelete = 8 << 20
	}

	for _, rng := range ranges {
		ss := int(rng.File.Offset() / c.pieceLength)
		se := int((rng.File.Offset() + FileRangeNotDelete) / c.pieceLength)

		es := int((rng.File.Offset() + rng.File.Length() - FileRangeNotDelete) / c.pieceLength)
		ee := int((rng.File.Offset() + rng.File.Length()) / c.pieceLength)

		if id >= ss && id < se || id > es && id <= ee {
			return true
		}
	}
	return false
}

//////////////////
// Reader section
////////

func (c *Cache) NewReader(file *torrent.File) *Reader {
	return newReader(file, c)
}

func (c *Cache) GetUseReaders() int {
	count := 0
	for _, r := range c.readersSnapshot() {
		if r.stateSnapshot().active {
			count++
		}
	}
	return count
}

func (c *Cache) Readers() int {
	if c == nil {
		return 0
	}
	c.muReaders.RLock()
	defer c.muReaders.RUnlock()
	return len(c.readers)
}

func (c *Cache) CloseReader(r *Reader) {
	if r != nil {
		r.Close()
	}
}

func (c *Cache) clearPriorityX(full bool) {
	if full {
		c.clearPriority()
		return
	}
	torr := c.getTorrent()
	if torr == nil {
		return
	}
	t0 := time.Now()
	stats.clrCalls.Add(1)
	c.muPrio.Lock()
	defer c.muPrio.Unlock()
	defer func() { stats.clrNs.Add(int64(time.Since(t0))) }()
	ranges := make([]Range, 0)
	for _, r := range c.readersSnapshot() {
		r.checkReader()
		if r.stateSnapshot().active {
			ranges = append(ranges, r.getPiecesRange())
		}
	}
	ranges = mergeRange(ranges)
	for id := range c.prioSet {
		if len(ranges) == 0 || !inRanges(ranges, id) {
			stats.pieceStateCalls.Add(1)
			if torr.PieceState(id).Priority != torrent.PiecePriorityNone {
				torr.Piece(id).SetPriority(torrent.PiecePriorityNone)
			}
			delete(c.prioSet, id)
		}
	}
}

func (c *Cache) clearPriority() {
	torr := c.getTorrent()
	if torr == nil {
		return
	}
	t0 := time.Now()
	stats.clrCalls.Add(1)
	stats.clrFull.Add(1)
	defer func() { stats.clrNs.Add(int64(time.Since(t0))) }()
	// This used to sleep for a second before clearing priorities. A reader
	// created during that window could have its PiecePriorityNow/Next/Readahead
	// reset to None right after setLoadPriority had assigned them, starving the
	// player. A mutex provides the same ordering without the race window.
	c.muPrio.Lock()
	defer c.muPrio.Unlock()
	ranges := make([]Range, 0)
	for _, r := range c.readersSnapshot() {
		r.checkReader()
		if r.stateSnapshot().active {
			ranges = append(ranges, r.getPiecesRange())
		}
	}
	ranges = mergeRange(ranges)

	for id := range c.getPieces() {
		if len(ranges) > 0 {
			if !inRanges(ranges, id) {
				stats.pieceStateCalls.Add(1)
				if torr.PieceState(id).Priority != torrent.PiecePriorityNone {
					torr.Piece(id).SetPriority(torrent.PiecePriorityNone)
				}
			}
		} else {
			stats.pieceStateCalls.Add(1)
			if torr.PieceState(id).Priority != torrent.PiecePriorityNone {
				torr.Piece(id).SetPriority(torrent.PiecePriorityNone)
			}
		}
	}
}

func (c *Cache) GetCapacity() int64 {
	if c == nil {
		return 0
	}
	return c.capacity
}

func (c *Cache) withStableReaderWindows(
	fn func([]byteInterval) error,
) error {
	c.evictionMu.Lock()
	defer c.evictionMu.Unlock()

	return fn(c.protectedByteIntervals())
}

// registerReader makes registration atomic with respect to eviction.
// A newly returned Reader must be visible in the cache's reader map.
func (c *Cache) registerReader(r *Reader) {
	c.evictionMu.RLock()
	defer c.evictionMu.RUnlock()

	c.muReaders.Lock()
	defer c.muReaders.Unlock()

	c.readers[r] = struct{}{}
}

// refreshReaderPriorities updates priorities after a reader closes.
// Serialize it with cache cleanup so test teardown and other
// maintenance operations can wait for its completion.
func (c *Cache) refreshReaderPriorities() {
	if c.isClosed.Load() {
		return
	}

	c.muRemove.Lock()
	defer c.unlockMaintenance()

	if c.isClosed.Load() {
		return
	}

	c.getRemPieces()
}

// unlockMaintenance releases muRemove and schedules any cleanup
// request that could not acquire the lock.
//
// The pending check must happen after unlocking:
// a new cleanPieces call can otherwise set pendingClean
// between the check and Unlock, then fail TryLock.
func (c *Cache) unlockMaintenance() {
	c.muRemove.Unlock()

	if c.patchEnabled &&
		c.pendingClean.Load() &&
		!c.isClosed.Load() {
		go c.cleanPieces()
	}
}

// evictRAMSegments reclaims unprotected RAM segments until the
// cache fits its capacity, or no eligible segments remain.
//
// Caller must hold muRemove.
func (c *Cache) evictRAMSegments() (freed int64, err error) {
	torr := c.getTorrent()
	if torr == nil || torr.Info() == nil {
		return 0, nil
	}

	err = c.withStableReaderWindows(func(protected []byteInterval) error {
		active := c.activeSnapshot()

		// Deterministic order for now. A later version can
		// prioritize cold segments instead of piece indices.
		ids := make([]int, 0, len(active))
		for id, p := range active {
			if p != nil && p.mPiece != nil {
				ids = append(ids, id)
			}
		}
		sort.Ints(ids)

		for _, id := range ids {
			if c.filled-freed <= c.capacity ||
				freed >= ramEvictionBatchBytes {
				break
			}

			p := active[id]
			pieceLength := torr.Info().Piece(id).Length()
			if pieceLength <= 0 {
				continue
			}

			pieceStart := int64(id) * c.pieceLength

			offsets := p.mPiece.evictableSegmentOffsets(
				pieceStart,
				pieceLength,
				memSegmentSize,
				protected,
			)

			for _, offset := range offsets {
				// Essential: check the batch limit for EVERY segment.
				if c.filled-freed <= c.capacity ||
					freed >= ramEvictionBatchBytes {
					break
				}

				before := p.mPiece.allocated()

				_, discardErr := p.mPiece.DiscardSegment(offset)
				if errors.Is(discardErr, torrent.ErrChunkDiscardBusy) {
					continue
				}
				if discardErr != nil {
					return fmt.Errorf(
						"evict piece %d segment %d: %w",
						id, offset, discardErr,
					)
				}

				after := p.mPiece.allocated()
				if after < before {
					freed += before - after
				}
			}
		}

		return nil
	})

	// Account for successfully discarded segments even if a
	// later discard returned an error.
	c.filled -= freed
	return freed, err
}
