package torrstor

import (
	"crypto/sha1"
	"testing"

	"server/settings"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
)

const evictionTestPieceSize int64 = 4 << 20

// createFullEvictionTestCache creates a verified 4 MiB piece
// backed by the real TorrServer RAM storage.
func createFullEvictionTestCache(t *testing.T) (
	*Cache,
	*torrent.Torrent,
) {
	t.Helper()

	oldSettings := settings.BTsets
	settings.BTsets = &settings.BTSets{
		UseDisk:          false,
		CacheSize:        16 << 20,
		ConnectionsLimit: 8,
		ReaderReadAHead:  95,
	}
	t.Cleanup(func() {
		settings.BTsets = oldSettings
	})

	data := make([]byte, evictionTestPieceSize)
	sum := sha1.Sum(data)

	info := metainfo.Info{
		Name:        "eviction-limits.bin",
		Length:      evictionTestPieceSize,
		PieceLength: evictionTestPieceSize,
		Pieces:      sum[:],
	}

	infoBytes, err := bencode.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}

	mi := &metainfo.MetaInfo{InfoBytes: infoBytes}
	stor := NewStorage(16 << 20)

	cfg := localTorrentConfig()
	cfg.DefaultStorage = stor
	cfg.Seed = true

	client, err := torrent.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}

	var c *Cache
	t.Cleanup(func() {
		client.Close()

		// Wait for maintenance started before shutdown.
		if c != nil {
			c.muRemove.Lock()
			c.muRemove.Unlock()
		}
	})

	torr, _, err := client.AddTorrentSpec(
		torrent.TorrentSpecFromMetaInfo(mi),
	)
	if err != nil {
		t.Fatal(err)
	}

	c = stor.GetCache(torr.InfoHash())
	if c == nil {
		t.Fatal("RAM cache was not initialized")
	}
	if !c.patchEnabled {
		t.Fatal("segment eviction requires TS_PATCH enabled")
	}

	c.SetTorrent(torr)

	p := c.pieces[0]
	if p == nil || p.mPiece == nil {
		t.Fatal("expected a RAM-backed piece")
	}

	n, err := p.mPiece.WriteAt(data, 0)
	if err != nil || n != len(data) {
		t.Fatalf("populate piece: n=%d err=%v", n, err)
	}

	torr.Piece(0).VerifyData()
	if !torr.PieceState(0).Completion.Complete {
		t.Fatal("test piece failed SHA-1 verification")
	}

	return c, torr
}

// withMaintenanceLock ensures the mutex is released
// even if an assertion calls t.Fatal().
func withMaintenanceLock(c *Cache, fn func()) {
	c.muRemove.Lock()
	defer c.muRemove.Unlock()
	fn()
}

func TestRAMSegmentEvictionBatchLimit(t *testing.T) {
	c, torr := createFullEvictionTestCache(t)
	m := c.pieces[0].mPiece

	withMaintenanceLock(c, func() {
		originalCapacity := c.capacity
		defer func() { c.capacity = originalCapacity }()

		c.capacity = memSegmentSize

		// Recompute the current physical RAM usage.
		c.getRemPiecesX(false, false)

		before := c.filled
		if before != evictionTestPieceSize {
			t.Fatalf("filled = %d, want %d",
				before, evictionTestPieceSize)
		}

		segmentsBefore := len(m.allocatedSegmentOffsets())

		freed, err := c.evictRAMSegments()
		if err != nil {
			t.Fatalf("evictRAMSegments: %v", err)
		}

		if freed == 0 {
			allocated := m.allocatedSegmentOffsets()

			var candidates []int64
			var probeN int
			var probeErr error

			err := c.withStableReaderWindows(
				func(protected []byteInterval) error {
					candidates = m.evictableSegmentOffsets(
						0,
						evictionTestPieceSize,
						memSegmentSize,
						protected,
					)

					// Probe one candidate through the real torrent
					// invalidation API to expose the underlying error.
					if len(candidates) > 0 {
						probeN, probeErr = m.DiscardSegment(candidates[0])
					}

					return nil
				},
			)
			if err != nil {
				t.Fatal(err)
			}

			t.Fatalf(
				"no RAM reclaimed: allocated=%d, segments=%d, "+
					"candidates=%d, complete=%v, missing=%d, "+
					"probe=(chunks=%d, err=%v)",
				m.allocated(),
				len(allocated),
				len(candidates),
				torr.PieceState(0).Completion.Complete,
				torr.PieceBytesMissing(0),
				probeN,
				probeErr,
			)
		}

		if freed != ramEvictionBatchBytes {
			t.Fatalf("freed = %d, want %d",
				freed, ramEvictionBatchBytes)
		}

		segmentsAfter := len(m.allocatedSegmentOffsets())
		wantRemoved := int(
			ramEvictionBatchBytes / memSegmentSize,
		)

		if got := segmentsBefore - segmentsAfter; got != wantRemoved {
			t.Fatalf("removed %d segments, want %d",
				got, wantRemoved)
		}

		if want := before - freed; c.filled != want {
			t.Fatalf("filled = %d, want %d",
				c.filled, want)
		}

		if c.filled <= c.capacity {
			t.Fatal("test did not leave cache over capacity")
		}

		// Verify that torrent metadata was invalidated too.
		if missing := torr.PieceBytesMissing(0); missing != freed {
			t.Fatalf("missing torrent bytes = %d, want %d",
				missing, freed)
		}

		if torr.PieceState(0).Completion.Complete {
			t.Fatal("partially evicted piece is still marked complete")
		}
	})
}

func TestRAMSegmentEvictionAllProtected(t *testing.T) {
	c, torr := createFullEvictionTestCache(t)
	m := c.pieces[0].mPiece

	// Register a real cache-managed reader whose in-flight
	// interval covers the entire 4 MiB file.
	r := c.NewReader(torr.Files()[0])
	r.SetReadahead(0)

	readID, _, ok := r.beginRead(int(evictionTestPieceSize))
	if !ok {
		t.Fatal("could not register protected read")
	}

	t.Cleanup(func() {
		r.endRead(readID)
		c.CloseReader(r)
	})

	originalCapacity := c.capacity
	defer withMaintenanceLock(c, func() {
		c.capacity = originalCapacity
	})

	var filledBefore int64
	var allocatedBefore int64

	withMaintenanceLock(c, func() {
		c.capacity = memSegmentSize
		c.getRemPiecesX(false, false)

		filledBefore = c.filled
		allocatedBefore = m.allocated()

		if filledBefore <= c.capacity {
			t.Fatal("cache must be over capacity")
		}

		freed, err := c.evictRAMSegments()
		if err != nil {
			t.Fatalf("evictRAMSegments: %v", err)
		}
		if freed != 0 {
			t.Fatalf("evicted %d protected bytes", freed)
		}

		if got := m.allocated(); got != allocatedBefore {
			t.Fatalf("allocated = %d, want %d",
				got, allocatedBefore)
		}
	})

	// Exercise the automatic cleanup with no eligible RAM
	// segments. It must return without rescheduling itself
	// when there is no new cleanup request.
	c.cleanPieces()

	withMaintenanceLock(c, func() {
		if got := m.allocated(); got != allocatedBefore {
			t.Fatalf("protected RAM changed: %d -> %d",
				allocatedBefore, got)
		}

		if c.filled != filledBefore {
			t.Fatalf("filled changed: %d -> %d",
				filledBefore, c.filled)
		}

		if c.pendingClean.Load() {
			t.Fatal("cleanup remains pending without new writes")
		}

		if missing := torr.PieceBytesMissing(0); missing != 0 {
			t.Fatalf("protected piece has %d missing bytes", missing)
		}
	})
}
