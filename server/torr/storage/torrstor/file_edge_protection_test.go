package torrstor

import (
	"io"
	"reflect"
	"testing"
)

func activeFileWindow(start, length, position int64) readerWindowSnapshot {
	return readerWindowSnapshot{
		state: readerState{active: true, offset: position},
		fileStart: start, fileLength: length,
	}
}

func TestFileEdgeProtectedIntervalsSingleFile(t *testing.T) {
	const MiB int64 = 1 << 20
	readers := []readerWindowSnapshot{activeFileWindow(0, 80*MiB, 40*MiB)}
	got := fileEdgeProtectedIntervals(readers, 64*MiB, 16*MiB, memSegmentSize)

	// 8 MiB at the head and 8 MiB at the tail, each in 256 KiB segments.
	if len(got) != 64 {
		t.Fatalf("protected %d segments, want 64", len(got))
	}
	countHead, countTail := 0, 0
	for _, s := range got {
		if s.end-s.begin != memSegmentSize {
			t.Fatalf("invalid segment interval %+v", s)
		}
		switch {
		case s.begin >= 0 && s.end <= 8*MiB:
			countHead++
		case s.begin >= 72*MiB && s.end <= 80*MiB:
			countTail++
		default:
			t.Fatalf("guarded ordinary playback data: %+v", s)
		}
	}
	if countHead != 32 || countTail != 32 {
		t.Fatalf("head/tail segments: %d/%d, want 32/32", countHead, countTail)
	}
}

func TestFileEdgeProtectedIntervalsMultiFileBudget(t *testing.T) {
	const MiB int64 = 1 << 20
	readers := []readerWindowSnapshot{
		activeFileWindow(0, 80*MiB, 40*MiB),
		activeFileWindow(96*MiB, 80*MiB, 30*MiB),
		// A second reader of the same file must not double its reservation.
		activeFileWindow(0, 80*MiB, 60*MiB),
	}
	got := fileEdgeProtectedIntervals(readers, 64*MiB, 16*MiB, memSegmentSize)
	if len(got) != 64 {
		t.Fatalf("protected %d segments, want 64", len(got))
	}
	counts := [4]int{}
	seen := make(map[byteInterval]bool)
	for _, s := range got {
		if seen[s] {
			t.Fatalf("duplicate protected segment %+v", s)
		}
		seen[s] = true
		switch {
		case s.begin >= 0 && s.end <= 8*MiB:
			counts[0]++
		case s.begin >= 72*MiB && s.end <= 80*MiB:
			counts[1]++
		case s.begin >= 96*MiB && s.end <= 104*MiB:
			counts[2]++
		case s.begin >= 168*MiB && s.end <= 176*MiB:
			counts[3]++
		default:
			t.Fatalf("unexpected segment %+v", s)
		}
	}
	if want := [4]int{16, 16, 16, 16}; counts != want {
		t.Fatalf("per-file head/tail protection %v, want %v", counts, want)
	}
}

func TestFileEdgeProtectionReaderLifecycleAndLimits(t *testing.T) {
	const MiB int64 = 1 << 20
	base := activeFileWindow(0, 80*MiB, 40*MiB)
	test := func(name string, readers []readerWindowSnapshot, capacity int64, want int) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			got := fileEdgeProtectedIntervals(readers, capacity, 16*MiB, memSegmentSize)
			if len(got) != want {
				t.Fatalf("protected %d segments, want %d", len(got), want)
			}
		})
	}
	test("no reader", nil, 64*MiB, 0)
	parked := base
	parked.state.active = false
	test("parked reader", []readerWindowSnapshot{parked}, 64*MiB, 0)
	closed := base
	closed.state.closed = true
	test("closed reader", []readerWindowSnapshot{closed}, 64*MiB, 0)
	closed.state.inFlight = 1
	test("closing reader with in-flight read", []readerWindowSnapshot{closed}, 64*MiB, 64)
	test("smaller budget", []readerWindowSnapshot{base}, 8*MiB, 8)
	test("insufficient budget", []readerWindowSnapshot{base}, 0, 0)
	short := base
	short.fileLength = 16 * MiB
	test("short file", []readerWindowSnapshot{short}, 64*MiB, 0)
}

func TestFileEdgeProtectionUnalignedFileAndPiece(t *testing.T) {
	const MiB int64 = 1 << 20
	const pieceLength int64 = 3*memSegmentSize + 4096
	fileStart := int64(257 << 10)
	fileEnd := fileStart + 80*MiB
	windows := []readerWindowSnapshot{activeFileWindow(fileStart, 80*MiB, 40*MiB)}
	got := fileEdgeProtectedIntervals(windows, 64*MiB, pieceLength, memSegmentSize)
	if len(got) == 0 || int64(len(got))*memSegmentSize > 16*MiB {
		t.Fatalf("budget exceeded: %d segments", len(got))
	}

	foundStart, foundEnd := false, false
	for _, interval := range got {
		if interval.begin <= fileStart && fileStart < interval.end {
			foundStart = true
		}
		if interval.begin < fileEnd && fileEnd <= interval.end {
			foundEnd = true
		}
		if interval.begin < 0 || interval.end <= interval.begin ||
			interval.end-interval.begin > memSegmentSize {
			t.Fatalf("invalid piece-relative segment %+v", interval)
		}
	}
	if !foundStart || !foundEnd {
		t.Fatalf("file boundaries not protected: start=%v end=%v", foundStart, foundEnd)
	}
}

func TestFileEdgeProtectionSelectsActualRAMSegments(t *testing.T) {
	const MiB int64 = 1 << 20
	const pieceLength int64 = 16 * MiB
	windows := []readerWindowSnapshot{activeFileWindow(0, 160*MiB, 48*MiB)}
	reads := protectedIntervalsFromSnapshots(windows, 64*MiB, 95)
	edges := fileEdgeProtectedIntervals(windows, 64*MiB, pieceLength, memSegmentSize)
	all := append(append([]byteInterval(nil), reads...), edges...)

	allocated := []int64{0, 4*MiB, 8*MiB, 12*MiB}
	got := selectEvictableRAMSegments(0, pieceLength, memSegmentSize, allocated, all)
	want := []int64{8 * MiB, 12 * MiB}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("head eviction candidates: got %v, want %v", got, want)
	}
	last := 9 * pieceLength
	got = selectEvictableRAMSegments(last, pieceLength, memSegmentSize, allocated, all)
	want = []int64{0, 4 * MiB}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("tail eviction candidates: got %v, want %v", got, want)
	}

	// With all readers gone, the same segments become evictable.
	none := fileEdgeProtectedIntervals(nil, 64*MiB, pieceLength, memSegmentSize)
	if len(none) != 0 {
		t.Fatalf("file edges survived the last reader: %v", none)
	}
	allOffsets := selectEvictableRAMSegments(0, pieceLength, memSegmentSize, allocated, none)
	if !reflect.DeepEqual(allOffsets, allocated) {
		t.Fatalf("head still protected without readers: %v", allOffsets)
	}
	_ = io.EOF // This test never reads data; it exercises candidate selection.
}
