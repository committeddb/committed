# Frame capacity reservation experiment

The production frame encoder appends its length, ID, payload, and checksum in
sequence. A test-only candidate, `appendFrameReserved`, reserves capacity for the
whole frame first and writes directly into that space. It is retained in
`internal/format/frame_test.go`; the production encoder is unchanged.

Capacity-boundary tests compare candidate bytes with production encoding for
empty, small, and larger payloads, with an existing destination prefix and
varying spare capacity. Both encodings round-trip through the frame parser.

## Measurements

September 17, 2026, macOS/arm64, Apple M4 Max, Go 1.26.6. Measurements ran after
validation, without concurrent tests or lint. Host load was uncontrolled. These
are allocation totals and synthetic encoding times, not filesystem throughput
or peak memory.

`BenchmarkFrameGrowth` compares standalone frames starting from a nil destination,
10,000 iterations per case in one run:

| Payload bytes | Production B/op | Candidate B/op | Production allocations | Candidate allocations |
| ---: | ---: | ---: | ---: | ---: |
| 0 | 24 | 16 | 2 | 1 |
| 49 | 216 | 80 | 4 | 1 |
| 4,080 | 4,120 | 4,096 | 3 | 1 |

A separate before/after experiment substituted the candidate into the production
call site and ran `BenchmarkMultiBlockEncoding`: 80 blocks containing 20 MiB of
framed data, written to `io.Discard`. Baseline `8bf7c6a` and candidate ran once
each, sequentially, with 20 iterations per case. Fixture creation was untimed.

| Input / encoding | Production B/op | Candidate B/op | Production allocs/op | Candidate allocs/op | Production ms/op | Candidate ms/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Repeated / plain | 1,175,946 | 1,170,280 | 37 | 34 | 8.66 | 9.97 |
| Repeated / ZstdDefault | 5,184,573 | 5,179,174 | 79 | 76 | 11.36 | 13.62 |
| Random / plain | 1,175,680 | 1,170,280 | 37 | 34 | 8.58 | 10.24 |
| Random / ZstdDefault | 5,670,320 | 5,664,927 | 57 | 54 | 13.91 | 17.17 |

Full-segment savings were only three allocations and roughly 5 KiB; segment
writing already reuses its block buffer. Timings were worse in this comparison.
One uncontrolled pair does not establish a general regression, but these results
do not justify adding reservation work to every record encoding. The candidate
was removed from production and retained only as an experiment. It changed no
persisted format bytes.

The candidate passed the storage race suites, Linux segmentlog suite, lint, and
gosec before removal from production. The final test-only version passed the
format race suite and lint.

```sh
go test ./pkg/segmentlog/internal/format -run '^TestAppendFrameCapacityBoundaries$' -bench '^BenchmarkFrameGrowth$' -benchtime=10000x
```
