package torr

import (
	"sync"
	"testing"

	"github.com/anacrolix/torrent"
)

func TestCacheStateConcurrentTorrentPointer(t *testing.T) {
	tr := new(Torrent)
	raw := new(torrent.Torrent)

	var wg sync.WaitGroup
	start := make(chan struct{})

	wg.Add(2)

	go func() {
		defer wg.Done()
		<-start

		for i := 0; i < 1000; i++ {
			tr.muTorrent.Lock()
			tr.Torrent = raw
			tr.muTorrent.Unlock()

			tr.muTorrent.Lock()
			tr.Torrent = nil
			tr.muTorrent.Unlock()
		}
	}()

	go func() {
		defer wg.Done()
		<-start

		for i := 0; i < 1000; i++ {
			if tr.CacheState() != nil {
				t.Error("unexpected state for torrent without cache")
				return
			}
		}
	}()

	close(start)
	wg.Wait()
}
