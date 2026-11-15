package torrstor

import (
	"reflect"
	"testing"
)

func TestSelectEvictableRAMSegments(t *testing.T) {
	const segment int64 = 256 << 10
	const pieceLength int64 = 4 * segment

	all := []int64{0, segment, 2 * segment, 3 * segment}

	tests := []struct {
		name       string
		pieceStart int64
		length     int64
		allocated  []int64
		protected  []byteInterval
		want       []int64
	}{
		{
			name:      "no readers",
			length:    pieceLength,
			allocated: all,
			want:      all,
		},
		{
			name:      "exact segment boundaries",
			length:    pieceLength,
			allocated: all,
			protected: []byteInterval{
				{segment, 2 * segment},
			},
			want: []int64{0, 2 * segment, 3 * segment},
		},
		{
			name:      "window crosses segment boundary",
			length:    pieceLength,
			allocated: all,
			protected: []byteInterval{
				{segment - 1, segment + 1},
			},
			want: []int64{2 * segment, 3 * segment},
		},
		{
			name:      "two independent readers",
			length:    pieceLength,
			allocated: all,
			protected: []byteInterval{
				{1, 2},
				{2*segment + 13, 2*segment + 14},
			},
			want: []int64{segment, 3 * segment},
		},
		{
			name:      "seek near beginning",
			length:    pieceLength,
			allocated: all,
			protected: []byteInterval{
				{0, segment},
			},
			want: []int64{segment, 2 * segment, 3 * segment},
		},
		{
			name:      "seek near end",
			length:    pieceLength,
			allocated: all,
			protected: []byteInterval{
				{3 * segment, 4 * segment},
			},
			want: []int64{0, segment, 2 * segment},
		},
		{
			name:       "short final piece",
			pieceStart: 64 << 20,
			length:     2*segment + 37,
			allocated:  []int64{0, segment, 2 * segment},
			protected: []byteInterval{
				{(64 << 20) + 2*segment + 36,
					(64 << 20) + 3*segment},
			},
			want: []int64{0, segment},
		},
		{
			name:      "duplicate and invalid offsets",
			length:    pieceLength,
			allocated: []int64{3 * segment, segment, segment, -segment, segment + 1, 4 * segment, 0},
			want:      []int64{0, segment, 3 * segment},
		},
		{
			name:      "all segments protected",
			length:    pieceLength,
			allocated: all,
			protected: []byteInterval{
				{0, pieceLength},
			},
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := selectEvictableRAMSegments(
				tt.pieceStart,
				tt.length,
				segment,
				tt.allocated,
				tt.protected,
			)

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}
