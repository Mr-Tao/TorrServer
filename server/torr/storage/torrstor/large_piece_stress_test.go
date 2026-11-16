package torrstor

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"runtime"
	"testing"
	"time"

	"server/settings"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"golang.org/x/time/rate"
)

func TestStreamingLargePieceMemoryBound(t *testing.T) {
	const (
		pieceLength int64 = 64 << 20
		cacheSize   int64 = 32 << 20
		readSize          = 64 << 10
		replaySize  int64 = 1 << 20
	)

	previousSettings := settings.BTsets
	settings.BTsets = &settings.BTSets{
		UseDisk:          false,
		CacheSize:        cacheSize,
		ConnectionsLimit: 8,
		ReaderReadAHead:  95,
		ResponsiveMode:   true,
	}

	var seeder, leecher *torrent.Client
	var seedCache, leechCache *Cache
	var reader *Reader

	t.Cleanup(func() {
		if reader != nil {
			reader.Close()
		}
		if leecher != nil {
			leecher.Close()
		}
		if seeder != nil {
			seeder.Close()
		}

		// Drain maintenance before restoring global settings.
		for _, c := range []*Cache{leechCache, seedCache} {
			if c != nil {
				c.muRemove.Lock()
				c.muRemove.Unlock()
			}
		}

		settings.BTsets = previousSettings
	})

	data := make([]byte, pieceLength)
	for i := range data {
		data[i] = byte((i + i/4096) % 251)
	}

	sum := sha1.Sum(data)
	info := metainfo.Info{
		Name:        "large-piece-stress.bin",
		Length:      pieceLength,
		PieceLength: pieceLength,
		Pieces:      sum[:],
	}

	infoBytes, err := bencode.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}

	mi := &metainfo.MetaInfo{InfoBytes: infoBytes}

	// Seeder: enough RAM to retain the complete verified piece.
	seedStor := NewStorage(128 << 20)
	seedCfg := localTorrentConfig()
	seedCfg.DefaultStorage = seedStor
	seedCfg.Seed = true
	seedCfg.UploadRateLimiter = rate.NewLimiter(
		rate.Limit(8<<20), 1<<20,
	)

	seeder, err = torrent.NewClient(seedCfg)
	if err != nil {
		t.Fatal(err)
	}

	seedTorrent, _, err := seeder.AddTorrentSpec(
		torrent.TorrentSpecFromMetaInfo(mi),
	)
	if err != nil {
		t.Fatal(err)
	}

	seedCache = seedStor.GetCache(seedTorrent.InfoHash())
	if seedCache == nil {
		t.Fatal("seeder cache unavailable")
	}

	seedCache.SetTorrent(seedTorrent)

	n, err := seedCache.pieces[0].mPiece.WriteAt(data, 0)
	if err != nil || n != len(data) {
		t.Fatalf("seed write: n=%d err=%v", n, err)
	}

	seedTorrent.Piece(0).VerifyData()
	if !seedTorrent.PieceState(0).Completion.Complete {
		t.Fatal("seeder piece verification failed")
	}

	// Leecher: the cache being tested.
	leechStor := NewStorage(cacheSize)
	leechCfg := localTorrentConfig()
	leechCfg.DefaultStorage = leechStor
	leechCfg.Seed = false

	leecher, err = torrent.NewClient(leechCfg)
	if err != nil {
		t.Fatal(err)
	}

	leechTorrent, _, err := leecher.AddTorrentSpec(
		torrent.TorrentSpecFromMetaInfo(mi),
	)
	if err != nil {
		t.Fatal(err)
	}

	leechCache = leechStor.GetCache(leechTorrent.InfoHash())
	if leechCache == nil {
		t.Fatal("leecher cache unavailable")
	}
	if !leechCache.patchEnabled {
		t.Fatal("segment eviction is disabled")
	}

	leechCache.SetTorrent(leechTorrent)
	leechTorrent.AddClientPeer(seeder)

	files := leechTorrent.Files()
	if len(files) != 1 {
		t.Fatalf("got %d files, want 1", len(files))
	}

	// Use the TorrServer wrapper, not Torrent.NewReader().
	// Its registered windows are essential to this test.
	reader = leechCache.NewReader(files[0])
	reader.SetResponsive()
	reader.SetReadahead(2 << 20)

	m := leechCache.pieces[0].mPiece

	type result struct {
		err     error
		forward time.Duration
		replay  time.Duration
		maxRead time.Duration
	}

	done := make(chan result, 1)
	progress := make(chan int64, 1)

	go func() {
		var out result

		readRange := func(label string, start, length int64) error {
			if _, err := reader.Seek(start, io.SeekStart); err != nil {
				return fmt.Errorf("%s: seek: %w", label, err)
			}

			buf := make([]byte, readSize)
			end := start + length
			nextProgress := int64(1 << 20)

			for pos := start; pos < end; {
				want := int(min(int64(len(buf)), end-pos))

				t0 := time.Now()
				n, err := reader.Read(buf[:want])
				elapsed := time.Since(t0)

				if elapsed > out.maxRead {
					out.maxRead = elapsed
				}

				if n > 0 {
					if !bytes.Equal(
						buf[:n],
						data[int(pos):int(pos)+n],
					) {
						return fmt.Errorf(
							"%s: data mismatch at %d",
							label, pos,
						)
					}
					pos += int64(n)
				}

				if err != nil &&
					!(errors.Is(err, io.EOF) && pos == end) {
					return fmt.Errorf(
						"%s: read at %d: %w",
						label, pos, err,
					)
				}

				if n == 0 {
					return fmt.Errorf(
						"%s: no progress at %d", label, pos,
					)
				}

				if label == "forward" && pos >= nextProgress {
					select {
					case progress <- pos:
					default:
					}
					nextProgress += 1 << 20
				}
			}

			return nil
		}

		start := time.Now()
		out.err = readRange("forward", 0, pieceLength)
		out.forward = time.Since(start)

		if out.err == nil {
			// The playback window has advanced to the end.
			// Wait for automatic eviction of the first segment.
			deadline := time.Now().Add(5 * time.Second)

			for {
				found := false
				for _, off := range m.allocatedSegmentOffsets() {
					if off == 0 {
						found = true
						break
					}
				}
				if !found {
					break
				}
				if time.Now().After(deadline) {
					out.err = fmt.Errorf(
						"first segment was not automatically evicted",
					)
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
		}

		if out.err == nil {
			start = time.Now()
			out.err = readRange("replay", 0, replaySize)
			out.replay = time.Since(start)
		}

		done <- out
	}()

	// Sample actual RAM segments while reading proceeds.
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	//	timeout := time.NewTimer(120 * time.Second)
	timeout := time.NewTimer(90 * time.Second)
	defer timeout.Stop()

	diagnostics := time.NewTicker(3 * time.Second)
	defer diagnostics.Stop()

	var previousReceived int64

	var peakAllocated int64
	var got result
	finished := false

	for !finished {
		select {
		case <-ticker.C:
			if allocated := m.allocated(); allocated > peakAllocated {
				peakAllocated = allocated
			}

		case pos := <-progress:
			t.Logf("forward progress: %d / %d MiB",
				pos>>20, pieceLength>>20)

		case got = <-done:
			finished = true

		case <-timeout.C:
			reader.Close()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
			}
			t.Fatal("large-piece streaming timed out")

		case <-diagnostics.C:
			stats := leechTorrent.Stats()
			received := stats.BytesReadData.Int64()
			allocated := m.allocated()
			segments := m.allocatedSegmentOffsets()

			t.Logf(
				"diagnostics: pos=%d MiB, RAM=%d MiB, "+
					"segments=%d, missing=%d MiB, "+
					"RX=%d MiB (+%d KiB/3s), "+
					"usefulRX=%d MiB, wastedChunks=%d, peers=%d",
				reader.Offset()>>20,
				allocated>>20,
				len(segments),
				leechTorrent.PieceBytesMissing(0)>>20,
				received>>20,
				(received-previousReceived)>>10,
				stats.BytesReadUsefulData.Int64()>>20,
				stats.ChunksReadWasted.Int64(),
				stats.ActivePeers,
			)

			previousReceived = received
		}
	}

	if got.err != nil {
		t.Fatal(got.err)
	}

	// Automatic cleanup is asynchronous. Give pending work
	// time to bring physical allocation back near capacity.
	const tolerance int64 = 4 << 20
	settleDeadline := time.Now().Add(5 * time.Second)

	for m.allocated() > cacheSize+tolerance &&
		time.Now().Before(settleDeadline) {
		time.Sleep(20 * time.Millisecond)
	}

	finalAllocated := m.allocated()
	if finalAllocated > cacheSize+tolerance {
		t.Fatalf(
			"RAM remains over limit: allocated=%d capacity=%d",
			finalAllocated, cacheSize,
		)
	}

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	t.Logf(
		"forward=%s replay=%s max_read=%s",
		got.forward, got.replay, got.maxRead,
	)
	t.Logf(
		"leecher RAM: peak=%d MiB final=%d MiB capacity=%d MiB",
		peakAllocated>>20,
		finalAllocated>>20,
		cacheSize>>20,
	)
	t.Logf(
		"process heap: Alloc=%d MiB HeapSys=%d MiB",
		mem.Alloc>>20, mem.HeapSys>>20,
	)

	// Network statistics after forward reading and replay.
	stats := leechTorrent.Stats()

	t.Logf(
		"network: received=%d MiB useful=%d MiB wasted=%d chunks",
		stats.BytesReadData.Int64()>>20,
		stats.BytesReadUsefulData.Int64()>>20,
		stats.ChunksReadWasted.Int64(),
	)
}
