package segmentlog

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/committeddb/committed/pkg/segmentlog/internal/format"
)

func TestTailRewriteCapacityBoundary(t *testing.T) {
	for _, compression := range []Compression{NoCompression, ZstdDefault} {
		for _, extra := range []int{0, 1} {
			t.Run(fmt.Sprintf("compression=%d/extra=%d", compression, extra), func(t *testing.T) {
				// An odd, tiny block target exercises rounding and oversized records.
				opts := Options{BlockSize: 33, Compression: compression}
				l, err := CreateLog(t.TempDir(), 0, LogOptions{SegmentBytes: 64, Encoding: opts})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = l.Close() })
				if err := l.Append([]Record{{0, nil}}); err != nil {
					t.Fatal(err)
				}
				limit := (opts.BlockSize / 2) * (format.MaxBlocks - 1)
				const remaining = 64 - format.FrameOverhead
				payload := bytes.Repeat([]byte("x"), limit-remaining-format.FrameOverhead+extra)
				result, err := l.Rewrite(t.Context(), 1, func(Record) ([]byte, bool, error) { return payload, true, nil })
				if extra != 0 {
					if !errors.Is(err, ErrInvalid) || !errors.Is(err, ErrLogPoisoned) || result.Published {
						t.Fatal(result, err)
					}
					l = reopenLog(t, l)
					c, err := l.InspectCatalog()
					if err != nil || c.Generation != 0 {
						t.Fatal(c, err)
					}
					r, err := l.Read(0)
					if err != nil || len(r.Payload) != 0 {
						t.Fatal("failed growth changed original", err)
					}
					return
				}
				if err != nil || !result.Published || !result.TailChanged {
					t.Fatal(result, err)
				}
				l = reopenLog(t, l)
				// Consume the entire reserved append budget in the same tail.
				if err := l.Append([]Record{{1, nil}, {2, nil}, {3, nil}}); err != nil {
					t.Fatal(err)
				}
				state, err := l.tail.State()
				if err != nil || state.Framed != 64 || state.Start != 0 {
					t.Fatal(state, err)
				}
				// Check actual format encoding at the bound, after the reserved appends.
				if err := WriteSegment(io.Discard, Coverage{0, 4}, tailRecords(l.file, state.End), opts); err != nil {
					t.Fatal(err)
				}
				if err := l.Append([]Record{{4, nil}}); err != nil {
					t.Fatal(err)
				}
				// Force the frozen append file through the real immutable rewrite path.
				resultSealed, err := l.RewriteSealed(t.Context(), 2, func(r Record) ([]byte, bool, error) {
					if r.ID == 0 {
						r.Payload[0] = 'y'
					}
					return r.Payload, true, nil
				})
				if err != nil || !resultSealed.Published || resultSealed.ChangedSegments != 1 {
					t.Fatal(resultSealed, err)
				}
				l = reopenLog(t, l)
				if err := l.Verify(t.Context()); err != nil {
					t.Fatal(err)
				}
				payload[0] = 'y'
				r, err := l.Read(0)
				if err != nil || !bytes.Equal(r.Payload, payload) {
					t.Fatal("lost grown payload", err)
				}
				for id := uint64(1); id <= 4; id++ {
					r, err := l.Read(id)
					if err != nil || len(r.Payload) != 0 {
						t.Fatal(id, err)
					}
				}
			})
		}
	}
}

func TestTailCapacityAdverseBlockPacking(t *testing.T) {
	// Each 16-byte frame forces the preceding 18-byte frame into its own
	// block, and vice versa. Some non-final blocks are less than half full.
	const target = 33
	const limit = (target / 2) * (format.MaxBlocks - 1)
	var count uint64
	var framed int
	records := func(yield func(Record, error) bool) {
		for {
			size := format.FrameOverhead
			if count%2 != 0 {
				size += 2
			}
			if framed+size > limit {
				return
			}
			r := Record{count, make([]byte, size-format.FrameOverhead)}
			count++
			framed += size
			if !yield(r, nil) {
				return
			}
		}
	}
	var encoded bytes.Buffer
	if err := WriteSegment(&encoded, Coverage{0, ^uint64(0)}, records, Options{BlockSize: target}); err != nil {
		t.Fatal(err)
	}
	s, err := OpenSegment(bytes.NewReader(encoded.Bytes()), int64(encoded.Len()))
	if err != nil {
		t.Fatal(err)
	}
	if framed != limit || s.Count() != count || len(s.blocks) != int(count) {
		t.Fatal(framed, s.Count(), count, len(s.blocks))
	}
	if err := s.Verify(); err != nil {
		t.Fatal(err)
	}
}
