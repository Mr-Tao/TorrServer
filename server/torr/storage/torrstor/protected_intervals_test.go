package torrstor

import (
	"reflect"
	"slices"
	"testing"
)

func TestProtectedIntervalsFromSnapshots(t *testing.T) {
	const MiB int64 = 1 << 20

	tests := []struct {
		name     string
		readers  []readerWindowSnapshot
		capacity int64
		percent  int64
		want     []byteInterval
	}{
		{
			name:     "single reader with file offset",
			capacity: 32 * MiB,
			percent:  75,
			readers: []readerWindowSnapshot{
				{
					state: readerState{
						offset: 20 * MiB,
						active: true,
					},
					fileStart:  64 * MiB,
					fileLength: 128 * MiB,
				},
			},
			want: []byteInterval{
				{76 * MiB, 108 * MiB},
			},
		},
		{
			name:     "two active readers",
			capacity: 32 * MiB,
			percent:  75,
			readers: []readerWindowSnapshot{
				{
					state: readerState{
						offset: 20 * MiB,
						active: true,
					},
					fileLength: 128 * MiB,
				},
				{
					state: readerState{
						offset: 40 * MiB,
						active: true,
					},
					fileLength: 128 * MiB,
				},
			},
			want: []byteInterval{
				{16 * MiB, 32 * MiB},
				{36 * MiB, 52 * MiB},
			},
		},
		{
			name:     "inflight read protects whole file",
			capacity: 32 * MiB,
			percent:  75,
			readers: []readerWindowSnapshot{
				{
					state: readerState{
						offset:   20 * MiB,
						inFlight: 1,
						closed:   true,
					},
					fileStart:  64 * MiB,
					fileLength: 128 * MiB,
				},
			},
			want: []byteInterval{
				{64 * MiB, 192 * MiB},
			},
		},
		{
			name:     "inactive reader",
			capacity: 32 * MiB,
			percent:  75,
			readers: []readerWindowSnapshot{
				{
					state:      readerState{},
					fileLength: 128 * MiB,
				},
			},
			want: nil,
		},
		{
			name:     "window clamped at end of file",
			capacity: 32 * MiB,
			percent:  75,
			readers: []readerWindowSnapshot{
				{
					state: readerState{
						offset: 9 * MiB,
						active: true,
					},
					fileLength: 10 * MiB,
				},
			},
			want: []byteInterval{
				{1 * MiB, 10 * MiB},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := protectedIntervalsFromSnapshots(
				tt.readers,
				tt.capacity,
				tt.percent,
			)

			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf(
					"protected = %v, want %v",
					got, tt.want,
				)
			}
		})
	}
}

func TestProtectedIntervalsInFlightReads(t *testing.T) {
	const MiB int64 = 1 << 20
	const segmentSize int64 = 256 << 10

	// Four allocated segments in an 8 MiB piece.
	allocated := []int64{
		0,
		2 * MiB,
		4 * MiB,
		6 * MiB,
	}

	tests := []struct {
		name     string
		inFlight int
		reads    []byteInterval
		want     []int64
		unknown  bool
	}{
		{
			name:     "one active read",
			inFlight: 1,
			reads: []byteInterval{
				{4*MiB + 4096, 4*MiB + 8192},
			},
			want: []int64{0, 2 * MiB, 6 * MiB},
		},
		{
			name:     "two active reads",
			inFlight: 2,
			reads: []byteInterval{
				{2*MiB + 4096, 2*MiB + 8192},
				{4*MiB + 4096, 4*MiB + 8192},
			},
			want: []int64{0, 6 * MiB},
		},
		{
			name:     "untracked operation protects whole file",
			inFlight: 1,
			reads:    nil,
			want:     nil,
		},
		{
			name:     "seek invalidates recorded read range",
			inFlight: 1,
			reads: []byteInterval{
				{4*MiB + 4096, 4*MiB + 8192},
			},
			unknown: true,
			want:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			readers := []readerWindowSnapshot{
				{
					state: readerState{
						offset:   16 * MiB,
						inFlight: tt.inFlight,
					},
					fileStart:       0,
					fileLength:      64 * MiB,
					inFlightReads:   tt.reads,
					unknownInFlight: tt.unknown,
				},
			}

			// The ordinary playback window is around 16 MiB,
			// outside the 8 MiB piece being examined.
			protected := protectedIntervalsFromSnapshots(
				readers,
				1*MiB,
				50,
			)

			got := selectEvictableRAMSegments(
				0,
				8*MiB,
				segmentSize,
				allocated,
				protected,
			)

			if !slices.Equal(got, tt.want) {
				t.Fatalf(
					"evictable segments: got=%v want=%v",
					got, tt.want,
				)
			}
		})
	}
}
