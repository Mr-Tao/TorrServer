package torrstor

import "sort"

const fileEdgeProtectionBytes int64 = 8 << 20

// fileEdgeProtectedIntervals selects RAM segments near the beginning and end
// of large files that currently have an active cache reader. Container metadata
// (such as MKV cues or MP4 indexes) can live at either end. No data is loaded
// here: this only keeps already-cached segments from being evicted.
//
// The budget is at most one quarter of the cache, charged in whole RAM
// segments. Alternating head/tail segments across distinct active files shares
// the budget without pinning the whole cache when several files are played.
// When the final reader closes, no file-edge intervals remain protected.
func fileEdgeProtectedIntervals(
	readers []readerWindowSnapshot,
	capacity, pieceLength, segmentSize int64,
) []byteInterval {
	if capacity <= 0 || pieceLength <= 0 || segmentSize <= 0 {
		return nil
	}

	// Charge a full segment even when the last torrent piece is shorter.
	// This conservatively bounds physical RAM pinned by the file-edge guard.
	budgetSegments := (capacity / 4) / segmentSize
	if budgetSegments <= 0 {
		return nil
	}

	type fileExtent struct{ start, end int64 }
	seenFiles := make(map[fileExtent]struct{})
	var files []fileExtent
	for _, r := range readers {
		if !readerParticipates(r.state) || r.fileStart < 0 ||
			r.fileLength <= 2*fileEdgeProtectionBytes ||
			r.fileLength > int64(^uint64(0)>>1)-r.fileStart {
			continue
		}
		f := fileExtent{r.fileStart, r.fileStart + r.fileLength}
		if _, seen := seenFiles[f]; !seen {
			seenFiles[f] = struct{}{}
			files = append(files, f)
		}
	}
	if len(files) == 0 {
		return nil
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].start != files[j].start {
			return files[i].start < files[j].start
		}
		return files[i].end < files[j].end
	})

	type edgeSegments struct{ head, tail []byteInterval }
	edges := make([]edgeSegments, 0, len(files))
	maxDepth := 0
	for _, f := range files {
		e := edgeSegments{
			head: segmentsFromFileEdge(f.start, f.start+fileEdgeProtectionBytes,
				pieceLength, segmentSize, true),
			tail: segmentsFromFileEdge(f.end-fileEdgeProtectionBytes, f.end,
				pieceLength, segmentSize, false),
		}
		maxDepth = max(maxDepth, len(e.head), len(e.tail))
		edges = append(edges, e)
	}

	protected := make([]byteInterval, 0, min(int64(len(files)*64), budgetSegments))
	seenSegments := make(map[byteInterval]struct{})
	for depth := 0; depth < maxDepth; depth++ {
		for _, e := range edges {
			for _, candidates := range [2][]byteInterval{e.head, e.tail} {
				if depth >= len(candidates) {
					continue
				}
				seg := candidates[depth]
				if _, seen := seenSegments[seg]; seen {
					continue
				}
				seenSegments[seg] = struct{}{}
				protected = append(protected, seg)
				if int64(len(protected)) >= budgetSegments {
					return protected
				}
			}
		}
	}
	return protected
}

// Return whole piece-relative RAM segments intersecting the requested file
// edge, ordered from the file boundary inwards. The intervals use absolute
// torrent offsets and are valid even when files start mid-piece.
func segmentsFromFileEdge(
	begin, end, pieceLength, segmentSize int64,
	fromStart bool,
) []byteInterval {
	if begin >= end {
		return nil
	}
	var result []byteInterval
	if fromStart {
		for pos := begin; pos < end; {
			seg := torrentSegmentAt(pos, pieceLength, segmentSize)
			result = append(result, seg)
			pos = seg.end
		}
	} else {
		for pos := end - 1; pos >= begin; {
			seg := torrentSegmentAt(pos, pieceLength, segmentSize)
			result = append(result, seg)
			if seg.begin == 0 {
				break
			}
			pos = seg.begin - 1
		}
	}
	return result
}

func torrentSegmentAt(pos, pieceLength, segmentSize int64) byteInterval {
	pieceStart := (pos / pieceLength) * pieceLength
	segStart := pieceStart + ((pos-pieceStart)/segmentSize)*segmentSize
	segEnd := min(segStart+segmentSize, pieceStart+pieceLength)
	return byteInterval{begin: segStart, end: segEnd}
}
