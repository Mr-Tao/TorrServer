package torrstor

import "sort"

// byteInterval is a half-open [begin, end) byte range,
// relative to the beginning of the entire torrent.
type byteInterval struct {
	begin int64
	end   int64
}

// selectEvictableRAMSegments returns piece-relative offsets of
// allocated segments that do not overlap any protected interval.
//
// pieceStart:       absolute byte offset of this piece in the torrent
// pieceLength:      actual length of this piece (including a short final piece)
// segmentSize:      RAM segment size
// allocatedOffsets: piece-relative offsets of allocated segments
// protected:        torrent-relative byte ranges required by readers
//
// This function only selects candidates. It never frees memory.
func selectEvictableRAMSegments(
	pieceStart, pieceLength, segmentSize int64,
	allocatedOffsets []int64,
	protected []byteInterval,
) []int64 {
	if pieceStart < 0 || pieceLength <= 0 || segmentSize <= 0 {
		return nil
	}

	var candidates []int64
	seen := make(map[int64]struct{}, len(allocatedOffsets))

	for _, off := range allocatedOffsets {
		if off < 0 || off >= pieceLength || off%segmentSize != 0 {
			continue
		}

		if _, exists := seen[off]; exists {
			continue
		}
		seen[off] = struct{}{}

		length := segmentSize
		if remaining := pieceLength - off; remaining < length {
			length = remaining
		}

		begin := pieceStart + off
		end := begin + length

		keep := false
		for _, r := range protected {
			if r.begin >= r.end {
				continue
			}

			// Overlap of two half-open intervals.
			if begin < r.end && r.begin < end {
				keep = true
				break
			}
		}

		if !keep {
			candidates = append(candidates, off)
		}
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i] < candidates[j]
	})

	return candidates
}
