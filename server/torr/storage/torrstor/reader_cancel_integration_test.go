package torrstor

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
)

// A storage backend with no downloaded pieces.
type cancelWaitStorage struct{}

func (*cancelWaitStorage) OpenTorrent(
	*metainfo.Info, metainfo.Hash,
) (storage.TorrentImpl, error) {
	return &cancelWaitTorrentStorage{}, nil
}

func (*cancelWaitStorage) Close() error {
	return nil
}

type cancelWaitTorrentStorage struct{}

func (*cancelWaitTorrentStorage) Piece(
	metainfo.Piece,
) storage.PieceImpl {
	return cancelWaitPiece{}
}

func (*cancelWaitTorrentStorage) Close() error {
	return nil
}

type cancelWaitPiece struct{}

func (cancelWaitPiece) ReadAt([]byte, int64) (int, error) {
	return 0, io.EOF
}

func (cancelWaitPiece) WriteAt(b []byte, _ int64) (int, error) {
	return len(b), nil
}

func (cancelWaitPiece) MarkComplete() error {
	return nil
}

func (cancelWaitPiece) MarkNotComplete() error {
	return nil
}

func (cancelWaitPiece) Completion() storage.Completion {
	return storage.Completion{
		Complete: false,
		Ok:       true,
	}
}

func TestCloseReaderCancelsBlockedTorrentRead(t *testing.T) {
	const (
		pieceSize int64 = 16 << 10
		fileSize  int64 = 1 << 20
	)

	info := metainfo.Info{
		Name:        "unavailable.bin",
		Length:      fileSize,
		PieceLength: pieceSize,
		Pieces:      make([]byte, int(fileSize/pieceSize)*20),
	}

	infoBytes, err := bencode.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}

	mi := &metainfo.MetaInfo{InfoBytes: infoBytes}

	cfg := torrent.NewDefaultClientConfig()
	cfg.ListenHost = func(string) string {
		return "127.0.0.1"
	}
	cfg.ListenPort = 0
	cfg.NoDHT = true
	cfg.DisableUTP = true
	cfg.DisableIPv6 = true
	cfg.DisablePEX = true
	cfg.DisableTrackers = true
	cfg.NoDefaultPortForwarding = true
	cfg.DefaultStorage = &cancelWaitStorage{}

	client, err := torrent.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	torr, _, err := client.AddTorrentSpec(
		torrent.TorrentSpecFromMetaInfo(mi),
	)
	if err != nil {
		t.Fatal(err)
	}

	files := torr.Files()
	if len(files) != 1 {
		t.Fatalf("got %d files, want 1", len(files))
	}

	// This cache manages the wrapper reader only.
	// The torrent itself uses the empty test storage.
	c := NewCache(128<<10, nil)
	c.pieceLength = pieceSize
	c.readerReadAheadPercent = 95

	r := c.NewReader(files[0])
	defer c.CloseReader(r)

	type result struct {
		n   int
		err error
	}

	done := make(chan result, 1)

	go func() {
		buf := make([]byte, 1024)
		n, err := r.Read(buf)
		done <- result{n, err}
	}()

	// Wait until the wrapper has registered its read.
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()

	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

waitForRead:
	for {
		if r.stateSnapshot().inFlight > 0 {
			break waitForRead
		}

		select {
		case got := <-done:
			t.Fatalf("read finished unexpectedly: %+v", got)

		case <-ticker.C:

		case <-deadline.C:
			t.Fatal("read did not start")
		}
	}

	// No piece is available, so the read should remain blocked.
	select {
	case got := <-done:
		t.Fatalf("read finished before close: %+v", got)

	case <-time.After(50 * time.Millisecond):
	}

	// Verify that a real blocked Read() protects only its
	// registered byte range, not the entire file.
	state, ranges, unknown := r.stateWithReadsSnapshot()

	if state.inFlight != 1 {
		t.Fatalf("inFlight = %d, want 1", state.inFlight)
	}

	if unknown {
		t.Fatal("ordinary blocked Read protects the entire file")
	}

	wantRange := byteInterval{
		begin: files[0].Offset(),
		end:   files[0].Offset() + 1024,
	}

	if len(ranges) != 1 || ranges[0] != wantRange {
		t.Fatalf(
			"in-flight ranges = %v, want [%v]",
			ranges, wantRange,
		)
	}

	// The reader is blocked in the first 256 KiB segment.
	// A distant segment at offset 512 KiB must remain
	// eligible for eviction.
	farOffset := 2 * memSegmentSize

	var candidates []int64

	err = c.withStableReaderWindows(
		func(protected []byteInterval) error {
			candidates = selectEvictableRAMSegments(
				0,
				fileSize,
				memSegmentSize,
				[]int64{0, farOffset},
				protected,
			)
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if len(candidates) != 1 || candidates[0] != farOffset {
		t.Fatalf(
			"evictable segments = %v, want [%d]",
			candidates, farOffset,
		)
	}

	// This synthetic cache has no torrent storage of its own.
	// Suppress background priority maintenance during teardown.
	c.isClosed.Store(true)

	// This must cancel the underlying ReadContext().
	c.CloseReader(r)

	select {
	case got := <-done:
		if got.n != 0 || !errors.Is(got.err, context.Canceled) {
			t.Fatalf(
				"read after close: n=%d err=%v",
				got.n, got.err,
			)
		}

	case <-time.After(3 * time.Second):
		t.Fatal("torrent read remained blocked after close")
	}

	// The final in-flight operation has finished.
	// The wrapper must now be unregistered.
	c.muReaders.RLock()
	_, registered := c.readers[r]
	c.muReaders.RUnlock()

	if registered {
		t.Fatal("closed reader remained registered")
	}
}
