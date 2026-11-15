package torrstor

import "server/settings"

// readerWindowSnapshot contains immutable information needed
// to calculate the reader's protected range.
type readerWindowSnapshot struct {
	state      readerState
	fileStart  int64 // Absolute torrent offset
	fileLength int64
}

func readerParticipates(s readerState) bool {
	// An in-flight operation remains protected even if
	// Close() has already marked the reader closed.
	return s.inFlight > 0 || (s.active && !s.closed)
}

// protectedIntervalsFromSnapshots calculates torrent-relative,
// half-open protected byte ranges.
//
// It is pure: no locks, no torrent access, no cache modifications.
func protectedIntervalsFromSnapshots(
	readers []readerWindowSnapshot,
	capacity int64,
	percent int64,
) []byteInterval {
	active := 0
	for _, r := range readers {
		if readerParticipates(r.state) {
			active++
		}
	}
	if active == 0 {
		return nil
	}

	if percent < 0 {
		percent = 0
	} else if percent > 100 {
		percent = 100
	}

	var protected []byteInterval

	for _, r := range readers {
		if !readerParticipates(r.state) ||
			r.fileStart < 0 || r.fileLength <= 0 {
			continue
		}

		fileEnd := r.fileStart + r.fileLength

		// During an in-flight read or seek, the published
		// position may be stale. Protect the entire file.
		if r.state.inFlight > 0 || capacity <= 0 {
			protected = append(protected, byteInterval{
				begin: r.fileStart,
				end:   fileEnd,
			})
			continue
		}

		share := capacity / int64(active)
		if share <= 0 {
			protected = append(protected, byteInterval{
				begin: r.fileStart,
				end:   fileEnd,
			})
			continue
		}

		// Match the existing getOffsetRange() policy.
		behind := share * (100 - percent) / 100
		ahead := share * percent / 100

		pos := r.state.offset
		if pos < 0 {
			pos = 0
		} else if pos > r.fileLength {
			pos = r.fileLength
		}

		begin := pos
		if behind > begin {
			begin = 0
		} else {
			begin -= behind
		}

		end := r.fileLength
		if ahead < r.fileLength-pos {
			end = pos + ahead
		}

		// Avoid an empty interval for an active reader.
		if begin == end {
			if end < r.fileLength {
				end++
			} else if begin > 0 {
				begin--
			}
		}

		protected = append(protected, byteInterval{
			begin: r.fileStart + begin,
			end:   r.fileStart + end,
		})
	}

	return protected
}

// protectedByteIntervals collects reader snapshots without
// holding muReaders while acquiring individual reader locks.
func (c *Cache) protectedByteIntervals() []byteInterval {
	readers := c.readersSnapshot()
	windows := make([]readerWindowSnapshot, 0, len(readers))

	for _, r := range readers {
		if r.file == nil {
			continue
		}

		windows = append(windows, readerWindowSnapshot{
			state:      r.stateSnapshot(),
			fileStart:  r.file.Offset(),
			fileLength: r.file.Length(),
		})
	}

	percent := int64(95)
	if settings.BTsets != nil {
		percent = int64(settings.BTsets.ReaderReadAHead)
	}

	return protectedIntervalsFromSnapshots(
		windows,
		c.capacity,
		percent,
	)
}
