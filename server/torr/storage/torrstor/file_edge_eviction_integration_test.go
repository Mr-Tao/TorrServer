package torrstor

import (
	"crypto/sha1"
	"io"
	"slices"
	"testing"

	"server/settings"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
)

// Test the full eviction path with real torrent chunk invalidation and a
// reader of an 80 MiB file. The much smaller cache forces the metadata guard
// to share its 1 MiB reservation between both ends of the file.
func TestRAMSegmentEvictionRetainsFileEdgesUntilFinalReader(t *testing.T) {
	const (
		MiB         int64 = 1 << 20
		pieceLength int64 = 4 * MiB
		pieceCount        = 20
	)
	oldSettings := settings.BTsets
	settings.BTsets = &settings.BTSets{
		UseDisk: false, CacheSize: 64 << 20,
		ConnectionsLimit: 8, ReaderReadAHead: 95,
	}
	t.Cleanup(func() { settings.BTsets = oldSettings })

	data := make([]byte, pieceLength)
	digest := sha1.Sum(data)
	hashes := make([]byte, 0, pieceCount*len(digest))
	for i := 0; i < pieceCount; i++ {
		hashes = append(hashes, digest[:]...)
	}
	info := metainfo.Info{
		Name: "large-video.mkv", Length: int64(pieceCount)*pieceLength,
		PieceLength: pieceLength, Pieces: hashes,
	}
	infoBytes, err := bencode.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	mi := &metainfo.MetaInfo{InfoBytes: infoBytes}
	stor := NewStorage(64 << 20)
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
		if c != nil {
			c.muRemove.Lock()
			c.muRemove.Unlock()
		}
	})

	torr, _, err := client.AddTorrentSpec(torrent.TorrentSpecFromMetaInfo(mi))
	if err != nil {
		t.Fatal(err)
	}
	c = stor.GetCache(torr.InfoHash())
	if c == nil || !c.patchEnabled {
		t.Fatal("patched RAM cache unavailable")
	}
	c.SetTorrent(torr)

	// Cache the first and last pieces, plus a middle piece available for
	// eviction. Only existing data can be pinned by the new policy.
	for _, id := range []int{0, 9, 19} {
		p := c.pieces[id]
		if p == nil || p.mPiece == nil {
			t.Fatalf("missing RAM piece %d", id)
		}
		n, err := p.mPiece.WriteAt(data, 0)
		if err != nil || n != len(data) {
			t.Fatalf("write piece %d: n=%d err=%v", id, n, err)
		}
		torr.Piece(id).VerifyData()
		if !torr.PieceState(id).Completion.Complete {
			t.Fatalf("piece %d failed verification", id)
		}
	}

	reader := c.NewReader(torr.Files()[0])
	reader.SetReadahead(0)
	t.Cleanup(func() { c.CloseReader(reader) })
	if _, err := reader.Seek(40*MiB, io.SeekStart); err != nil {
		t.Fatal(err)
	}

	withMaintenanceLock(c, func() {
		orig := c.capacity
		defer func() { c.capacity = orig }()
		c.capacity = 4 * MiB

		c.getRemPiecesX(false, false)
		freed, err := c.evictRAMSegments()
		if err != nil || freed == 0 {
			t.Fatalf("eviction freed=%d err=%v", freed, err)
		}

		head := c.pieces[0].mPiece.allocatedSegmentOffsets()
		tail := c.pieces[19].mPiece.allocatedSegmentOffsets()
		if !slices.Contains(head, 0) ||
			!slices.Contains(head, memSegmentSize) {
			t.Fatalf("first two head segments not preserved: %v", head)
		}
		if !slices.Contains(tail, pieceLength-memSegmentSize) ||
			!slices.Contains(tail, pieceLength-2*memSegmentSize) {
			t.Fatalf("last two tail segments not preserved: %v", tail)
		}
	})

	// Closing the final reader must immediately remove the file-edge guard.
	c.CloseReader(reader)
	withMaintenanceLock(c, func() {
		orig := c.capacity
		defer func() { c.capacity = orig }()
		c.capacity = memSegmentSize
		c.getRemPiecesX(false, false)
		freed, err := c.evictRAMSegments()
		if err != nil || freed == 0 {
			t.Fatalf("eviction after last reader freed=%d err=%v", freed, err)
		}
		head := c.pieces[0].mPiece.allocatedSegmentOffsets()
		if slices.Contains(head, 0) || slices.Contains(head, memSegmentSize) {
			t.Fatalf("file-edge protection survived final reader: %v", head)
		}
	})
}
