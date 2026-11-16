package torrstor

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"server/settings"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"golang.org/x/time/rate"
)

func TestStreamingConcurrentReaders(t *testing.T) {
	runConcurrentReaderScenario(t, false)
}

func TestStreamingConcurrentSeek(t *testing.T) {
	runConcurrentReaderScenario(t, true)
}

func runConcurrentReaderScenario(t *testing.T, seekScenario bool) {
	const (
		pieceLength int64 = 64 << 20
		cacheSize   int64 = 32 << 20
		readLength  int64 = 24 << 20
		readSize          = 64 << 10

		stallTimeout = 12 * time.Second
		testTimeout  = 60 * time.Second
	)

	startOffsets := [2]int64{0, 32 << 20}

	type readPhase struct {
		offset int64
		length int64
	}

	plans := [2][]readPhase{
		{{offset: 0, length: readLength}},
		{{offset: 32 << 20, length: readLength}},
	}

	if seekScenario {
		plans[1] = []readPhase{
			{offset: 32 << 20, length: 4 << 20},
			{offset: 48 << 20, length: 8 << 20},
			{offset: 32 << 20, length: 4 << 20},
		}
	}

	var expectedBytes [2]int64
	for id, phases := range plans {
		for _, phase := range phases {
			expectedBytes[id] += phase.length
		}
	}

	oldSettings := settings.BTsets
	settings.BTsets = &settings.BTSets{
		UseDisk:          false,
		CacheSize:        cacheSize,
		ConnectionsLimit: 8,
		ReaderReadAHead:  95,
		ResponsiveMode:   true,
	}

	var seeder, leecher *torrent.Client
	var seedCache, leechCache *Cache
	var readers [2]*Reader
	var workers sync.WaitGroup
	stop := make(chan struct{})
	aPaused := make(chan struct{})
	resumeA := make(chan struct{})

	t.Cleanup(func() {
		close(stop)
		// Stop networking before waiting for reader goroutines.
		if leecher != nil {
			leecher.Close()
		}
		if seeder != nil {
			seeder.Close()
		}

		workers.Wait()

		for _, r := range readers {
			if r != nil {
				r.Close()
			}
		}

		for _, c := range []*Cache{leechCache, seedCache} {
			if c != nil {
				c.muRemove.Lock()
				c.muRemove.Unlock()
			}
		}

		settings.BTsets = oldSettings
	})

	// One 64 MiB piece with deterministic contents.
	data := make([]byte, pieceLength)
	for i := range data {
		data[i] = byte((i + i/4096) % 251)
	}

	sum := sha1.Sum(data)
	info := metainfo.Info{
		Name:        "concurrent-readers.bin",
		Length:      pieceLength,
		PieceLength: pieceLength,
		Pieces:      sum[:],
	}

	infoBytes, err := bencode.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	mi := &metainfo.MetaInfo{InfoBytes: infoBytes}

	// Seeder.
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

	// Leecher.
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

	files := leechTorrent.Files()
	if len(files) != 1 {
		t.Fatalf("got %d files, want 1", len(files))
	}

	for i := range readers {
		r := leechCache.NewReader(files[0])
		readers[i] = r

		r.SetResponsive()
		r.SetReadahead(2 << 20)

		if _, err := r.Seek(startOffsets[i], io.SeekStart); err != nil {
			t.Fatal(err)
		}
	}

	leechTorrent.AddClientPeer(seeder)

	type progressEvent struct {
		id    int
		bytes int64
	}
	type readResult struct {
		id    int
		bytes int64
		err   error
	}

	progress := make(chan progressEvent, 64)
	done := make(chan readResult, 2)

	ready := make(chan struct{}, 2)
	start := make(chan struct{})

	for id, r := range readers {
		workers.Add(1)

		go func(id int, r *Reader) {
			defer workers.Done()

			ready <- struct{}{}
			<-start

			buf := make([]byte, readSize)
			var received int64
			nextProgress := int64(1 << 20)
			paused := false

			finish := func(err error) {
				done <- readResult{id, received, err}
			}

			for phaseIndex, phase := range plans[id] {
				// Reader B waits for A to reach 8 MiB
				// before performing the first seek.
				if seekScenario && id == 1 && phaseIndex == 1 {
					select {
					case <-aPaused:
					case <-stop:
						finish(fmt.Errorf("reader B canceled"))
						return
					}
				}

				if _, err := r.Seek(phase.offset, io.SeekStart); err != nil {
					finish(fmt.Errorf(
						"reader %d: seek to %d: %w",
						id, phase.offset, err,
					))
					return
				}

				// Resume reader A after the new position
				// has been registered with the scheduler.
				if seekScenario && id == 1 && phaseIndex == 1 {
					close(resumeA)
				}

				var phaseRead int64

				for phaseRead < phase.length {
					want := int(min(
						int64(len(buf)),
						phase.length-phaseRead,
					))

					n, err := r.Read(buf[:want])

					if n > 0 {
						offset := phase.offset + phaseRead
						expected := data[int(offset) : int(offset)+n]

						if !bytes.Equal(buf[:n], expected) {
							finish(fmt.Errorf(
								"reader %d phase %d: "+
									"data mismatch at offset %d",
								id, phaseIndex, offset,
							))
							return
						}

						phaseRead += int64(n)
						received += int64(n)
					}

					if err != nil {
						finish(fmt.Errorf(
							"reader %d phase %d: "+
								"read at %d: %w",
							id, phaseIndex,
							phase.offset+phaseRead, err,
						))
						return
					}

					if n == 0 {
						finish(fmt.Errorf(
							"reader %d phase %d: no progress",
							id, phaseIndex,
						))
						return
					}

					if received >= nextProgress {
						progress <- progressEvent{id, received}
						nextProgress += 1 << 20
					}

					// Pause A at 8 MiB. The second reader
					// will seek to 48 MiB and resume us.
					if seekScenario && id == 0 &&
						!paused && received >= 8<<20 {

						paused = true
						close(aPaused)

						select {
						case <-resumeA:
						case <-stop:
							finish(fmt.Errorf("reader A canceled"))
							return
						}
					}
				}
			}

			finish(nil)
		}(id, r)
	}

	// Both goroutines are ready before either begins reading.
	<-ready
	<-ready
	close(start)

	var positions [2]int64
	var lastProgress [2]time.Time
	var finished [2]bool

	now := time.Now()
	lastProgress[0], lastProgress[1] = now, now

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	timeout := time.NewTimer(testTimeout)
	defer timeout.Stop()

	peakRAM := int64(0)
	completed := 0

	for completed < 2 {
		select {
		case p := <-progress:
			positions[p.id] = p.bytes
			lastProgress[p.id] = time.Now()

			other := 1 - p.id

			// A coarse fairness requirement: one reader must
			// not run 16 MiB ahead while the other has yet
			// to receive its first MiB.
			if p.bytes >= 16<<20 &&
				positions[other] < 1<<20 {
				t.Fatalf(
					"reader starvation: A=%d MiB B=%d MiB",
					positions[0]>>20,
					positions[1]>>20,
				)
			}

			if p.bytes%(4<<20) == 0 {
				t.Logf(
					"progress: A=%d B=%d MiB",
					positions[0]>>20,
					positions[1]>>20,
				)
			}

		case result := <-done:
			if result.err != nil {
				t.Fatal(result.err)
			}
			if result.bytes != expectedBytes[result.id] {
				t.Fatalf(
					"reader %d read %d bytes, want %d",
					result.id, result.bytes, expectedBytes[result.id],
				)
			}
			finished[result.id] = true
			completed++

		case <-ticker.C:
			allocated := leechCache.pieces[0].mPiece.allocated()
			if allocated > peakRAM {
				peakRAM = allocated
			}

			for id := range readers {
				if finished[id] {
					continue
				}
				if time.Since(lastProgress[id]) > stallTimeout {
					t.Fatalf(
						"reader %d stalled: A=%d B=%d MiB, RAM=%d MiB",
						id,
						positions[0]>>20,
						positions[1]>>20,
						allocated>>20,
					)
				}
			}

		case <-timeout.C:
			t.Fatalf(
				"concurrent streaming timed out: A=%d B=%d MiB",
				positions[0]>>20,
				positions[1]>>20,
			)
		}
	}

	t.Logf(
		"both readers completed: A=%d B=%d MiB, peak RAM=%d MiB",
		expectedBytes[0]>>20,
		expectedBytes[1]>>20,
		peakRAM>>20,
	)
}
