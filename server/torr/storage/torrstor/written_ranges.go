package torrstor

// writtenSpan is a half-open range of bytes received within a single RAM
// segment. Spans are stored sorted and non-overlapping (adjacent spans are
// merged). Unlike the segment allocation size, these ranges track actual
// writes, including short, overlapping or repeated writes.
type writtenSpan struct {
	begin int64
	end   int64
}

// addWrittenSpan unions [begin, end) with the existing spans and returns
// the number of newly covered bytes. The input must already be sorted and
// non-overlapping; the result preserves that invariant.
func addWrittenSpan(spans []writtenSpan, begin, end int64) ([]writtenSpan, int64) {
	if begin >= end {
		return spans, 0
	}

	added := end - begin
	for _, s := range spans {
		if overlap := min(end, s.end) - max(begin, s.begin); overlap > 0 {
			added -= overlap
		}
	}

	merged := writtenSpan{begin: begin, end: end}
	out := make([]writtenSpan, 0, len(spans)+1)
	inserted := false

	for _, s := range spans {
		if inserted {
			out = append(out, s)
			continue
		}
		if s.end < merged.begin {
			out = append(out, s)
			continue
		}
		if merged.end < s.begin {
			out = append(out, merged, s)
			inserted = true
			continue
		}
		merged.begin = min(merged.begin, s.begin)
		merged.end = max(merged.end, s.end)
	}
	if !inserted {
		out = append(out, merged)
	}
	return out, added
}

func writtenSpanBytes(spans []writtenSpan) int64 {
	var n int64
	for _, s := range spans {
		n += s.end - s.begin
	}
	return n
}
