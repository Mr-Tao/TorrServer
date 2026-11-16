package torrstor

import (
	"bytes"
	"context"
	"crypto/sha1"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"golang.org/x/time/rate"

	"server/settings"
)

func localTorrentConfig() *torrent.ClientConfig {
	cfg := torrent.NewDefaultClientConfig()

	cfg.ListenPort = 0
	cfg.ListenHost = func(string) string { return "127.0.0.1" }

	cfg.DisableIPv6 = true
	cfg.DisableUTP = true
	cfg.NoDHT = true
	cfg.DisableTrackers = true
	cfg.NoDefaultPortForwarding = true
	cfg.DisablePEX = true

	return cfg
}

func TestStreamingSegmentRedownload(t *testing.T) {
	runStreamingRedownload(t, false, false, false, false, false)
}

func TestStreamingWholePieceRelease(t *testing.T) {
	runStreamingRedownload(t, true, false, false, false, false)
}

func TestStreamingCompletedPieceRelease(t *testing.T) {
	runStreamingRedownload(t, false, true, false, false, false)
}

func TestStreamingProtectedSegmentRedownload(t *testing.T) {
	runStreamingRedownload(t, false, false, true, false, false)
}

func TestStreamingAutomaticSegmentEviction(t *testing.T) {
	runStreamingRedownload(t, false, false, true, true, false)
}

func TestStreamingCompletedSegmentRedownload(t *testing.T) {
	runStreamingRedownload(t, false, false, false, false, true)
}

func runStreamingRedownload(
	t *testing.T,
	wholePiece, completedPiece, protectedEviction,
	automaticEviction, completedRedownload bool,
) {
	// Configure an isolated RAM-cache test.
	// Restore global settings after the test finishes.
	previousSets := settings.BTsets
	// Retain cache pointers even after Client.Close()
	// removes them from their Storage instances.
	var cachesToDrain []*Cache

	settings.BTsets = &settings.BTSets{
		UseDisk:          false,
		CacheSize:        16 << 20,
		ConnectionsLimit: 8,
		ReaderReadAHead:  95,
		ResponsiveMode:   true,
	}

	t.Cleanup(func() {
		// All deferred Client.Close() calls have finished
		// before testing invokes this cleanup.
		//
		// Wait for any cleanup that was already running
		// when the caches were closed.
		for _, c := range cachesToDrain {
			if c == nil {
				continue
			}

			c.muRemove.Lock()
			c.muRemove.Unlock()
		}

		// No cache cleanup may still be reading BTsets.
		settings.BTsets = previousSets
	})

	const (
		pieceLength = 4 << 20
		blockSize   = 16 << 10
	)

	// Deterministic, nonuniform test data.
	data := make([]byte, pieceLength)
	for i := range data {
		data[i] = byte((i + i/4096) % 251)
	}

	sum := sha1.Sum(data)
	info := metainfo.Info{
		Name:        "streaming-test.bin",
		Length:      int64(len(data)),
		PieceLength: pieceLength,
		Pieces:      sum[:],
	}

	infoBytes, err := bencode.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	mi := &metainfo.MetaInfo{InfoBytes: infoBytes}

	// Seeder: fully populated RAM-backed torrent.
	seedStor := NewStorage(16 << 20)

	seedCfg := localTorrentConfig()
	seedCfg.DefaultStorage = seedStor
	seedCfg.Seed = true
	seedCfg.UploadRateLimiter = rate.NewLimiter(
		rate.Limit(256<<10), 64<<10,
	)

	seeder, err := torrent.NewClient(seedCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer seeder.Close()

	seedTorrent, _, err := seeder.AddTorrentSpec(
		torrent.TorrentSpecFromMetaInfo(mi),
	)
	if err != nil {
		t.Fatal(err)
	}

	seedCache := seedStor.GetCache(seedTorrent.InfoHash())
	if seedCache == nil {
		t.Fatal("seeder cache not initialized")
	}

	cachesToDrain = append(cachesToDrain, seedCache)

	seedCache.SetTorrent(seedTorrent)

	// Populate the complete piece before any peer connects.
	n, err := seedCache.pieces[0].mPiece.WriteAt(data, 0)
	if err != nil || n != len(data) {
		t.Fatalf("seed write: n=%d err=%v", n, err)
	}

	// Verify its SHA-1 and mark the piece complete.
	seedTorrent.Piece(0).VerifyData()
	if !seedTorrent.PieceState(0).Completion.Complete {
		t.Fatal("seeder piece failed verification")
	}

	if completedPiece {
		p := seedCache.pieces[0]

		if missing := seedTorrent.PieceBytesMissing(0); missing != 0 {
			t.Fatalf("verified piece has %d missing bytes", missing)
		}

		if err := p.Release(); err != nil {
			t.Fatalf("completed piece release: %v", err)
		}

		if seedTorrent.PieceState(0).Completion.Complete {
			t.Fatal("released piece still marked complete")
		}

		if missing := seedTorrent.PieceBytesMissing(0); missing != pieceLength {
			t.Fatalf(
				"missing after release: got %d, want %d",
				missing, pieceLength,
			)
		}

		if allocated := p.mPiece.allocated(); allocated != 0 {
			t.Fatalf("released piece retains %d bytes", allocated)
		}

		return
	}

	// Leecher: our modified TorrServer RAM cache.
	leechCapacity := int64(16 << 20)

	if protectedEviction || completedRedownload {
		leechCapacity = 128 << 10
	}

	leechStor := NewStorage(leechCapacity)
	leechCfg := localTorrentConfig()
	leechCfg.DefaultStorage = leechStor
	leechCfg.Seed = false

	leecher, err := torrent.NewClient(leechCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer leecher.Close()

	leechTorrent, _, err := leecher.AddTorrentSpec(
		torrent.TorrentSpecFromMetaInfo(mi),
	)
	if err != nil {
		t.Fatal(err)
	}

	leechCache := leechStor.GetCache(leechTorrent.InfoHash())
	if leechCache == nil {
		t.Fatal("leecher cache not initialized")
	}

	cachesToDrain = append(cachesToDrain, leechCache)

	leechCache.SetTorrent(leechTorrent)

	preparationLockHeld := false

	if protectedEviction || completedRedownload {
		// Keep downloaded segments in RAM until the test has
		// established its protected reader.
		leechCache.muRemove.Lock()
		preparationLockHeld = true

		defer func() {
			if preparationLockHeld {
				leechCache.muRemove.Unlock()
			}
		}()
	}

	leechTorrent.AddClientPeer(seeder)

	reader := leechTorrent.NewReader()
	defer reader.Close()
	reader.SetResponsive()
	reader.SetReadahead(256 << 10)

	readTimeout := 30 * time.Second
	if completedRedownload {
		readTimeout = 75 * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), readTimeout)
	defer cancel()

	// Receive the first 16 KiB from the real seeder.
	first := make([]byte, blockSize)
	n, err = reader.ReadContext(ctx, first)
	if err != nil || n != blockSize {
		t.Fatalf("initial read: n=%d err=%v", n, err)
	}
	if !bytes.Equal(first, data[:blockSize]) {
		t.Fatal("initial data mismatch")
	}

	if leechTorrent.PieceState(0).Completion.Complete {
		t.Fatal("piece completed before discard")
	}
	if completedRedownload {
		m := leechCache.pieces[0].mPiece

		// Read and verify an arbitrary remaining range using
		// the real peer-to-peer torrent reader.
		readAndCheck := func(label string, start int64) {
			if _, err := reader.Seek(start, io.SeekStart); err != nil {
				t.Fatal(err)
			}

			buf := make([]byte, 64<<10)

			for pos := start; pos < pieceLength; {
				want := int(min(int64(len(buf)), pieceLength-pos))
				n, err := reader.ReadContext(ctx, buf[:want])

				if n > 0 && !bytes.Equal(
					buf[:n], data[int(pos):int(pos)+n],
				) {
					t.Fatalf("%s: data mismatch at %d", label, pos)
				}

				pos += int64(n)

				if err != nil &&
					!(errors.Is(err, io.EOF) && pos == pieceLength) {
					t.Fatalf("%s: read at %d: %v", label, pos, err)
				}

				if n == 0 {
					t.Fatalf("%s: no progress at %d", label, pos)
				}
			}
		}

		waitComplete := func(label string) {
			deadline := time.Now().Add(8 * time.Second)

			for !leechTorrent.PieceState(0).Completion.Complete {
				if time.Now().After(deadline) {
					t.Fatalf(
						"%s: piece not complete, missing=%d",
						label,
						leechTorrent.PieceBytesMissing(0),
					)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}

		// Finish downloading the entire 4 MiB piece.
		readAndCheck("initial download", blockSize)
		waitComplete("initial download")

		if missing := leechTorrent.PieceBytesMissing(0); missing != 0 {
			t.Fatalf("complete piece missing %d bytes", missing)
		}

		if allocated := m.allocated(); allocated != pieceLength {
			t.Fatalf("allocated %d, want %d", allocated, pieceLength)
		}

		// Recompute physical RAM usage before explicit eviction.
		// muRemove is still held by the test.
		leechCache.getRemPiecesX(false, false)

		freed, err := leechCache.evictRAMSegments()
		if err != nil {
			t.Fatalf("completed piece eviction: %v", err)
		}

		if freed != ramEvictionBatchBytes {
			t.Fatalf(
				"freed %d bytes, want %d",
				freed, ramEvictionBatchBytes,
			)
		}

		if leechTorrent.PieceState(0).Completion.Complete {
			t.Fatal("partially evicted piece remains complete")
		}

		if missing := leechTorrent.PieceBytesMissing(0); missing != freed {
			t.Fatalf(
				"missing %d bytes after eviction, want %d",
				missing, freed,
			)
		}

		// Fetch the missing chunks from the real seeder.
		// Preserved chunks should remain available locally.
		readAndCheck("redownload", 0)
		waitComplete("redownload")

		if missing := leechTorrent.PieceBytesMissing(0); missing != 0 {
			t.Fatalf("redownload left %d missing bytes", missing)
		}

		// This is the critical end-to-end assertion.
		if !leechTorrent.PieceState(0).Completion.Complete {
			t.Fatal("piece did not become complete again")
		}

		t.Log("completed piece successfully evicted and reverified")
		return
	}
	if wholePiece {
		before := leechTorrent.PieceBytesMissing(0)

		if before >= pieceLength {
			t.Fatal("no torrent chunks received before release")
		}

		// This is the legacy, uncoordinated eviction path.
		deadline := time.Now().Add(3 * time.Second)

		for {
			err = leechCache.pieces[0].Release()

			if !errors.Is(err, torrent.ErrChunkDiscardBusy) {
				break
			}

			if time.Now().After(deadline) {
				t.Fatal("whole-piece release stayed busy")
			}

			time.Sleep(10 * time.Millisecond)
		}

		if err != nil {
			t.Fatalf("whole-piece release: %v", err)
		}

// Release invalidates the cached piece, but active readers may
// immediately trigger new downloads. Neither the allocated byte
// count nor PieceBytesMissing is stable after Release returns.
t.Logf(
	"whole-piece release: allocated=%d missing=%d/%d",
	leechCache.pieces[0].mPiece.allocated(),
	leechTorrent.PieceBytesMissing(0),
	pieceLength,
)

		// The entire piece was discarded. Its first block must
		// be fetched again from the seeder.
		if _, err := reader.Seek(0, io.SeekStart); err != nil {
			t.Fatal(err)
		}

		again := make([]byte, blockSize)
		n, err = reader.ReadContext(ctx, again)

		if err != nil || n != blockSize {
			t.Fatalf("read after release: n=%d err=%v", n, err)
		}

if !bytes.Equal(again, data[:blockSize]) {
	firstBad := 0
	for firstBad < blockSize && again[firstBad] == data[firstBad] {
		firstBad++
	}

	end := firstBad + 16
	if end > blockSize {
		end = blockSize
	}

	t.Fatalf(
		"re-downloaded data mismatch: "+
			"firstBad=%d, got=% x, want=% x, "+
			"allZero=%t, allocated=%d, missing=%d/%d",
		firstBad,
		again[firstBad:end],
		data[firstBad:end],
		bytes.Equal(again, make([]byte, blockSize)),
		leechCache.pieces[0].mPiece.allocated(),
		leechTorrent.PieceBytesMissing(0),
		pieceLength,
	)
}

		return
	}

	if protectedEviction {
		const farOffset int64 = 2 << 20

		// Download a real torrent block in a distant segment.
		if _, err := reader.Seek(farOffset, io.SeekStart); err != nil {
			t.Fatal(err)
		}

		far := make([]byte, blockSize)
		n, err := reader.ReadContext(ctx, far)
		if err != nil || n != blockSize {
			t.Fatalf("far read: n=%d err=%v", n, err)
		}
		if !bytes.Equal(far, data[int(farOffset):int(farOffset)+blockSize]) {
			t.Fatal("far segment data mismatch")
		}

		m := leechCache.pieces[0].mPiece

		// Both segments must physically exist in RAM.
		allocated := m.allocatedSegmentOffsets()
		hasOffset := func(offsets []int64, wanted int64) bool {
			for _, off := range offsets {
				if off == wanted {
					return true
				}
			}
			return false
		}

		if !hasOffset(allocated, 0) ||
			!hasOffset(allocated, farOffset) {
			t.Fatalf("missing test segments: %v", allocated)
		}

		// Register a cache-managed reader with an in-flight
		// range in the first segment.
		tracked := leechCache.NewReader(leechTorrent.Files()[0])
		tracked.SetReadahead(0)

		readID, _, ok := tracked.beginRead(blockSize)
		if !ok {
			t.Fatal("could not register in-flight read")
		}

		defer func() {
			tracked.endRead(readID)
			leechCache.CloseReader(tracked)
		}()

		missingBefore := leechTorrent.PieceBytesMissing(0)

		// cleanPieces normally calls getRemPiecesX before eviction.
		// Reproduce that accounting step while holding muRemove.
		leechCache.getRemPiecesX(false, false)

		filledBefore := leechCache.filled
		if filledBefore <= leechCache.capacity {
			t.Fatalf(
				"test cache is not over capacity: filled=%d capacity=%d",
				filledBefore, leechCache.capacity,
			)
		}
		if automaticEviction {
			// The direct torrent reader was last positioned at farOffset.
			// Move it away so its readahead does not immediately request
			// the segment we want to evict.
			if _, err := reader.Seek(0, io.SeekStart); err != nil {
				t.Fatal(err)
			}
			reader.SetReadahead(0)

			// Release the preparation lock. Any cleanup requested
			// by MemPiece.WriteAt may now execute.
			preparationLockHeld = false
			leechCache.unlockMaintenance()

			deadline := time.NewTimer(5 * time.Second)
			defer deadline.Stop()

			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()

			for {
				// Synchronize with cleanPieces and inspect the result.
				leechCache.muRemove.Lock()
				allocated = m.allocatedSegmentOffsets()
				evicted := !hasOffset(allocated, farOffset)
				leechCache.muRemove.Unlock()

				if evicted {
					break
				}

				select {
				case <-ticker.C:
				case <-deadline.C:
					t.Fatalf(
						"automatic eviction did not release segment %d; "+
							"allocated=%v",
						farOffset, allocated,
					)
				}
			}
		} else {
			// Try eviction until the distant segment is removed.
			// DiscardChunkRange can temporarily report ErrChunkDiscardBusy;
			// evictRAMSegments deliberately skips such segments.
			var totalFreed int64
			deadline := time.Now().Add(3 * time.Second)

			for {
				freed, evictErr := leechCache.evictRAMSegments()
				if evictErr != nil {
					t.Fatalf("RAM segment eviction: %v", evictErr)
				}
				totalFreed += freed

				if !hasOffset(m.allocatedSegmentOffsets(), farOffset) {
					break
				}

				if time.Now().After(deadline) {
					t.Fatalf(
						"distant segment was not evicted: allocated=%v",
						m.allocatedSegmentOffsets(),
					)
				}

				time.Sleep(10 * time.Millisecond)
			}

			if totalFreed < memSegmentSize {
				t.Fatalf(
					"freed %d bytes, want at least %d",
					totalFreed, memSegmentSize,
				)
			}

			if want := filledBefore - totalFreed; leechCache.filled != want {
				t.Fatalf(
					"cache accounting: filled=%d, want=%d",
					leechCache.filled, want,
				)
			}
		}

		allocated = m.allocatedSegmentOffsets()

		if !hasOffset(allocated, 0) {
			t.Fatal("protected segment was evicted")
		}
		if hasOffset(allocated, farOffset) {
			t.Fatal("distant segment remains allocated")
		}

		// Torrent metadata must also reflect the invalidation.
		missingAfter := leechTorrent.PieceBytesMissing(0)
		if missingAfter <= missingBefore {
			t.Fatalf(
				"torrent chunks not invalidated: before=%d after=%d",
				missingBefore,
				missingAfter,
			)
		}

		// Check that the protected data is still intact.
		kept := make([]byte, blockSize)
		n, err = m.ReadAt(kept, 0)
		if err != nil || n != blockSize ||
			!bytes.Equal(kept, data[:blockSize]) {
			t.Fatalf("protected data changed: n=%d err=%v", n, err)
		}

		// Finally, re-download the segment that was evicted.
		if _, err := reader.Seek(farOffset, io.SeekStart); err != nil {
			t.Fatal(err)
		}

		again := make([]byte, blockSize)
		n, err = reader.ReadContext(ctx, again)
		if err != nil || n != blockSize {
			t.Fatalf("redownload: n=%d err=%v", n, err)
		}

		if !bytes.Equal(
			again,
			data[int(farOffset):int(farOffset)+blockSize],
		) {
			t.Fatal("re-downloaded segment data mismatch")
		}

		return
	}

	// Discard the 256 KiB segment containing the received block.
	var discarded int
	deadline := time.Now().Add(3 * time.Second)

	for {
		discarded, err = leechCache.pieces[0].mPiece.DiscardSegment(0)
		if !errors.Is(err, torrent.ErrChunkDiscardBusy) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("discard stayed busy")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err != nil || discarded == 0 {
		t.Fatalf("discard: chunks=%d err=%v", discarded, err)
	}

	t.Logf("invalidated %d torrent chunks", discarded)

	// Seek back and request the evicted bytes again.
	if _, err := reader.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}

	second := make([]byte, blockSize)
	n, err = reader.ReadContext(ctx, second)
	if err != nil || n != blockSize {
		t.Fatalf("read after discard: n=%d err=%v", n, err)
	}
	if !bytes.Equal(second, data[:blockSize]) {
		t.Fatal("re-downloaded data mismatch")
	}
}
