package torrstor

// readerWindowSnapshot contains immutable information needed
// to calculate the reader's protected range.
type readerWindowSnapshot struct {
	state      readerState
	fileStart  int64 // Absolute torrent offset
	fileLength int64
	// Byte ranges of individual reads currently in progress.
	// Intervals use absolute torrent offsets, not file-relative offsets.
	inFlightReads []byteInterval
	// An overlapping Seek or another operation made the
	// recorded read ranges unreliable.
	unknownInFlight bool
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

		// If the cache window has no capacity, protect the entire file.
		if capacity <= 0 {
			protected = append(protected, byteInterval{
				begin: r.fileStart,
				end:   fileEnd,
			})
			continue
		}

		// Protect the exact ranges of in-flight reads.
		// If their ranges are unknown, protect the entire file.
		if r.state.inFlight > 0 {
			protected = append(protected, readerInFlightIntervals(r)...)

			// When an operation has no reliable byte range,
			// the entire file is already protected.
			// No additional playback window is necessary.
			if r.unknownInFlight ||
				len(r.inFlightReads) != r.state.inFlight {
				continue
			}
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

		state, ranges, unknown := r.stateWithReadsSnapshot()

		windows = append(windows, readerWindowSnapshot{
			state:           state,
			fileStart:       r.file.Offset(),
			fileLength:      r.file.Length(),
			inFlightReads:   ranges,
			unknownInFlight: unknown,
		})

	}

	percent := c.readerReadAheadPercent

	return protectedIntervalsFromSnapshots(
		windows,
		c.capacity,
		percent,
	)
}

// readerInFlightIntervals returns the byte ranges that must remain
// allocated while operations on this reader are in progress.
//
// If any operation lacks a recorded range (including a Seek),
// conservatively protect the entire file.
func readerInFlightIntervals(r readerWindowSnapshot) []byteInterval {
	if r.state.inFlight <= 0 {
		return nil
	}

	if r.unknownInFlight ||
		len(r.inFlightReads) != r.state.inFlight {
		return []byteInterval{
			{
				begin: r.fileStart,
				end:   r.fileStart + r.fileLength,
			},
		}
	}

	return r.inFlightReads
}
