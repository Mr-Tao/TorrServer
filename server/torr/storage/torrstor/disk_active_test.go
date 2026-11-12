package torrstor

import (
	"os"
	"path/filepath"
	"testing"

	"server/settings"
)

func TestDiskActiveIndex(t *testing.T) {
	oldSettings := settings.BTsets
	oldPatch := patchOn
	t.Cleanup(func() {
		settings.BTsets = oldSettings
		patchOn = oldPatch
	})

	patchOn = true
	settings.BTsets = &settings.BTSets{
		UseDisk:          true,
		TorrentsSavePath: t.TempDir(),
	}

	c := NewCache(256<<20, nil)
	c.pieceLength = 64 << 20

	dir := filepath.Join(
		settings.BTsets.TorrentsSavePath,
		c.hash.HexString(),
	)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}

	// A piece left behind by an earlier server instance.
	if err := os.WriteFile(
		filepath.Join(dir, "5"),
		[]byte("old"),
		0600,
	); err != nil {
		t.Fatal(err)
	}

	existing := NewPiece(5, c)
	c.pieces[5] = existing

	if got := len(c.activeSnapshot()); got != 1 {
		t.Fatalf("restored index: got %d pieces, want 1", got)
	}

	fresh := NewPiece(7, c)
	c.pieces[7] = fresh

	if _, err := fresh.WriteAt([]byte("new"), 0); err != nil {
		t.Fatal(err)
	}

	if got := len(c.activeSnapshot()); got != 2 {
		t.Fatalf("after write: got %d pieces, want 2", got)
	}

	// Empty entries must not be included in eviction scans.
	for id := 10; id < 100; id++ {
		c.pieces[id] = &Piece{Id: id, cache: c}
	}

	rem := c.getRemPiecesX(false, false)

	if len(rem) != 2 {
		t.Fatalf("eviction candidates: got %d, want 2", len(rem))
	}
	if c.filled != 6 {
		t.Fatalf("filled: got %d bytes, want 6", c.filled)
	}

	fresh.dPiece.Release()

	if got := len(c.activeSnapshot()); got != 1 {
		t.Fatalf("after release: got %d pieces, want 1", got)
	}
	if _, err := os.Stat(fresh.dPiece.name); !os.IsNotExist(err) {
		t.Fatalf("released file still exists or stat failed: %v", err)
	}
}
