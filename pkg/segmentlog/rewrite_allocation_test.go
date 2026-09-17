package segmentlog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"testing"
)

func TestRewriteComparisonBufferOwnership(t *testing.T) {
	for _, first := range []int{-1, 1, 4} {
		t.Run(fmt.Sprint(first), func(t *testing.T) {
			source := []Record{{0, nil}, {3, []byte("short")}, {6, bytes.Repeat([]byte("x"), 512)}, {9, []byte("a")}, {12, []byte("later")}, {15, nil}}
			segment := open(t, encode(t, source...))
			calls := make([]int, len(source))
			var output bytes.Buffer
			changed, err := segment.Rewrite(t.Context(), func() (io.Writer, error) {
				if first < 0 {
					t.Fatal("no-op created output")
				}
				return &output, nil
			}, func(r Record) ([]byte, bool, error) {
				index := int(r.ID / 3)
				calls[index]++
				if first >= 0 && index >= first {
					if len(r.Payload) == 0 {
						return nil, false, nil
					}
					r.Payload[0] ^= 0xff
				}
				// Equal bytes in a different allocation must still count as unchanged.
				if index == 2 {
					return bytes.Clone(r.Payload), true, nil
				}
				return r.Payload, true, nil
			}, Options{})
			if err != nil || changed != (first >= 0) {
				t.Fatal(changed, err)
			}
			expected := make([]Record, 0, len(source))
			for i, r := range source {
				if calls[i] != 1 {
					t.Fatal("callback repeated or omitted", i, calls[i])
				}
				p := bytes.Clone(r.Payload)
				if first >= 0 && i >= first {
					if len(p) == 0 {
						continue
					}
					p[0] ^= 0xff
				}
				expected = append(expected, Record{r.ID, p})
			}
			if first >= 0 {
				segment = open(t, output.Bytes())
			}
			got := collect(t, segment)
			if len(got) != len(expected) {
				t.Fatal("wrong survivor count", len(got), len(expected))
			}
			for i, r := range got {
				if r.ID != expected[i].ID || !bytes.Equal(r.Payload, expected[i].Payload) {
					t.Fatal("incorrect rewritten history", i)
				}
			}
		})
	}
}

// BenchmarkRewritePreparation isolates change detection and stream preparation
// over a 20 MiB append-format range. Output encoding and publication are excluded.
func BenchmarkRewritePreparation(b *testing.B) {
	const count = 5120
	const payloadSize = 4080
	var data bytes.Buffer
	if err := WriteTailHeader(&data, 0); err != nil {
		b.Fatal(err)
	}
	for start := 0; start < count; start += 64 {
		records := make([]Record, 64)
		for i := range records {
			records[i] = Record{uint64(start + i), bytes.Repeat([]byte("x"), payloadSize)}
		}
		data.Write(encodeTailGroup(records, 64*(payloadSize+16)))
	}
	for _, mode := range []string{"noop", "first", "boundary", "last"} {
		b.Run(mode, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(count * payloadSize)
			for b.Loop() {
				calls, delivered := 0, 0
				changed, err := prepareRewrite(b.Context(), tailRecords(bytes.NewReader(data.Bytes()), int64(data.Len())), func(r Record) ([]byte, bool, error) {
					calls++
					if (mode == "first" && r.ID == 0) || (mode == "boundary" && r.ID == 64) || (mode == "last" && r.ID == count-1) {
						r.Payload[0] = 'y'
					}
					return r.Payload, true, nil
				}, func(records iter.Seq2[Record, error]) error {
					for r, err := range records {
						if err != nil {
							return err
						}
						want := byte('x')
						if (mode == "first" && delivered == 0) || (mode == "boundary" && delivered == 64) || (mode == "last" && delivered == count-1) {
							want = 'y'
						}
						if r.ID != uint64(delivered) || len(r.Payload) != payloadSize || r.Payload[0] != want {
							return fmt.Errorf("incorrect output at %d", delivered)
						}
						delivered++
					}
					return nil
				})
				wantDelivered := count
				if mode == "noop" {
					wantDelivered = 0
				}
				if err != nil || changed != (mode != "noop") || calls != count || delivered != wantDelivered {
					b.Fatal(changed, calls, delivered, err)
				}
			}
		})
	}
}

func TestRewriteReplaysOnlyUnchangedPrefix(t *testing.T) {
	for _, first := range []int{0, 1, 64, 127} {
		for _, erase := range []bool{false, true} {
			t.Run(fmt.Sprintf("first=%d/erase=%t", first, erase), func(t *testing.T) {
				// Sparse IDs with a nonzero start: prefix length is a record count.
				source := make([]Record, 128)
				for i := range source {
					source[i] = Record{1000 + uint64(i*3), []byte("original")}
				}
				starts, fetched := 0, 0
				input := func(yield func(Record, error) bool) {
					starts++
					for _, r := range source {
						fetched++
						if !yield(Record{r.ID, bytes.Clone(r.Payload)}, nil) {
							return
						}
					}
				}
				calls, delivered := 0, 0
				changed, err := prepareRewrite(t.Context(), input, func(r Record) ([]byte, bool, error) {
					calls++
					if r.ID == source[first].ID {
						return []byte("changed"), !erase, nil
					}
					return r.Payload, true, nil
				}, func(records iter.Seq2[Record, error]) error {
					for r, err := range records {
						if err != nil {
							return err
						}
						index := delivered
						if erase && index >= first {
							index++
						}
						want := "original"
						if !erase && index == first {
							want = "changed"
						}
						if r.ID != source[index].ID || string(r.Payload) != want {
							t.Fatal("incorrect output", r, index)
						}
						delivered++
					}
					return nil
				})
				wantStarts, wantDelivered := 2, len(source)
				if first == 0 {
					wantStarts = 1
				}
				if erase {
					wantDelivered--
				}
				if err != nil || !changed || starts != wantStarts || fetched != len(source)+first || calls != len(source) || delivered != wantDelivered {
					t.Fatal(changed, starts, fetched, calls, delivered, err)
				}
			})
		}
	}
}

func TestRewriteFirstChangeCanceledBeforeEmission(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	changed, err := prepareRewrite(ctx, sequence(Record{10, []byte("original")}), func(Record) ([]byte, bool, error) {
		return []byte("changed"), true, nil
	}, func(records iter.Seq2[Record, error]) error {
		cancel()
		for _, err := range records {
			if err == nil {
				t.Fatal("delivered replacement after cancellation")
			}
			return err
		}
		return nil
	})
	if !changed || !errors.Is(err, context.Canceled) {
		t.Fatal(changed, err)
	}
}
