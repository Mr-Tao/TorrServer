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
	runStreamingRedownload(t, false, false)
}

func TestStreamingWholePieceRelease(t *testing.T) {
	runStreamingRedownload(t, true, false)
}

func TestStreamingCompletedPieceRelease(t *testing.T) {
	runStreamingRedownload(t, false, true)
}

func runStreamingRedownload(t *testing.T, wholePiece bool, completedPiece bool) {
	// Configure an isolated RAM-cache test.
	// Restore global settings after the test finishes.
	previousSets := settings.BTsets

	settings.BTsets = &settings.BTSets{
		UseDisk:          false,
		CacheSize:        16 << 20,
		ConnectionsLimit: 8,
		ReaderReadAHead:  95,
		ResponsiveMode:   true,
	}

	t.Cleanup(func() {
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

	seedCache := seedStor.GetCache(mi.HashInfoBytes())
	if seedCache == nil {
		t.Fatal("seeder cache not initialized")
	}
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
	stor := NewStorage(16 << 20)
	leechCfg := localTorrentConfig()
	leechCfg.DefaultStorage = stor
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

	cache := stor.GetCache(mi.HashInfoBytes())
	if cache == nil {
		t.Fatal("TorrServer cache not initialized")
	}
	cache.SetTorrent(leechTorrent)

	leechTorrent.AddClientPeer(seeder)

	reader := leechTorrent.NewReader()
	defer reader.Close()
	reader.SetResponsive()
	reader.SetReadahead(256 << 10)

	ctx, cancel := context.WithTimeout(
		context.Background(), 30*time.Second,
	)
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
	if wholePiece {
		before := leechTorrent.PieceBytesMissing(0)

		if before >= pieceLength {
			t.Fatal("no torrent chunks received before release")
		}

		// This is the legacy, uncoordinated eviction path.
		deadline := time.Now().Add(3 * time.Second)

		for {
			err = cache.pieces[0].Release()

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

		if allocated := cache.pieces[0].mPiece.allocated(); allocated != 0 {
			t.Fatalf("release retained %d bytes", allocated)
		}

		after := leechTorrent.PieceBytesMissing(0)

		if after != pieceLength {
			t.Fatalf(
				"ghost chunks after release: missing=%d, want=%d",
				after, pieceLength,
			)
		}

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
			t.Fatal("re-downloaded data mismatch")
		}

		return
	}

	// Discard the 256 KiB segment containing the received block.
	var discarded int
	deadline := time.Now().Add(3 * time.Second)

	for {
		discarded, err = cache.pieces[0].mPiece.DiscardSegment(0)
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
