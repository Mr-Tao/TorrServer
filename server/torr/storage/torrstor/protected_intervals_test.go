package torrstor

import (
	"reflect"
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
