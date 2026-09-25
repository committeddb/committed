# Segment scan allocations

Local run: macOS arm64, Apple M4 Max, Go 1.26.6. Before and after measurements each
use 100 iterations, run sequentially without concurrent test runs. These are
synthetic in-memory measurements, not database throughput or physical disk I/O.

## Workload

`BenchmarkSegmentScan` scans one segment containing a single block of either 16
or 4,096 records. Every record has a 32-byte repeated payload and an even ID.
Plain and ZstdDefault encodings are separate cases. Segment construction and
metadata opening are outside the timer. Block reads, decompression where used,
complete block validation, and iteration over all records are included.

The baseline grew the record-descriptor slice through repeated appends. The
current scan allocates its capacity once using the block's record count, bounded
by the declared decoded size and maximum block size. Allocation occurs after
decoding and the first successful frame check. The entire block must still pass
validation before the scan delivers any records.

## Measured allocations

| Encoding | Records | Before B/op | Current B/op | Before allocs/op | Current allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| Plain | 16 | 1,760 | 1,280 | 6 | 2 |
| Plain | 4,096 | 619,224 | 327,683 | 16 | 2 |
| ZstdDefault | 16 | 33,658 | 33,176 | 30 | 26 |
| ZstdDefault | 4,096 | 801,114 | 509,451 | 30 | 16 |

For the larger block, allocated bytes fell about 47% for plain and 36% for zstd.
Scans still collect descriptors for every record in each selected block, including
records outside a requested ID interval. Block buffers and decoder allocations
remain, and retaining a payload can retain the decoded block buffer. These
measurements do not establish end-to-end application scan performance.

## Correctness coverage

Plain and compressed cases exercise a bounded scan whose valid requested prefix
precedes a corrupt final frame with a valid outer block checksum. The scan returns
an error without delivering the prefix. Existing scan, corruption, and repeated
history tests exercise block selection, ordering, cancellation, and reopening.

## Reproduce

```sh
go test ./pkg/segmentlog -run '^$' -bench '^BenchmarkSegmentScan$' -benchtime=100x -count=1
```
