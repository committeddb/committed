# Sparse segment lookup allocations

Local run: macOS arm64, Apple M4 Max, Go 1.26.6. Before and after measurements each
use 100 iterations, run sequentially without concurrent test runs. These are
synthetic in-memory measurements, not database throughput or physical disk I/O.

## Workload

`BenchmarkSegmentSeek` opens one segment containing a single block of either 16
or 4,096 records. Every record has a 32-byte repeated payload and an even ID. The
query is an absent odd ID near the middle; Seek must return the following even ID.
Plain and ZstdDefault encodings are separate cases. Segment construction and
metadata opening are outside the timer. Block reads, decompression where used,
and complete block validation are included.

The baseline collected all record descriptors in the selected block before
searching that slice. The current lookup uses the shared block validator and
retains only the first matching descriptor. It still validates every subsequent
frame and checks the block's last ID and record count before returning success.
A corrupt suffix cannot be hidden by a valid matching prefix.

## Measured allocations

| Encoding | Records | Before B/op | Current B/op | Before allocs/op | Current allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| Plain | 16 | 1,760 | 768 | 6 | 1 |
| Plain | 4,096 | 619,158 | 196,611 | 16 | 1 |
| ZstdDefault | 16 | 33,656 | 32,664 | 30 | 25 |
| ZstdDefault | 4,096 | 801,083 | 378,265 | 30 | 15 |

For the larger block, allocated bytes fell about 68% for plain and 53% for zstd.
The block buffers and decoder allocations remain. A returned payload can retain
its entire decoded block; the optimization removes the record-descriptor slice,
not that backing buffer. Payload capacity is limited to its length, and separate
reads return independent mutable payloads.

Seek still searches block metadata and validates a full selected block. These
measurements do not establish cold-read latency or end-to-end application-reader
performance. Range scans continue to collect records after validating a block.

## Correctness coverage

Tests exercise sparse lookup, exact lookup of a gap, ID zero with an empty payload,
lookup beyond the last record, and payload ownership. Plain and compressed cases
include a matching early record followed by a bad final frame with valid outer
block checksums. Lookup returns an error instead of exposing the early match.
Existing corruption and repeated-history suites exercise the shared validator.

## Reproduce

```sh
go test ./pkg/segmentlog -run '^$' -bench '^BenchmarkSegmentSeek$' -benchtime=100x -count=1
```
