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
