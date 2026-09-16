# Sealed-segment verification allocations

Local run: macOS arm64, Apple M4 Max, Go 1.26.6. Each measurement uses 100
iterations of `BenchmarkSegmentVerification`, with fixture construction outside
the timer. The before and after runs were sequential, without concurrent test
runs. These are synthetic in-memory measurements, not storage throughput or
peak resident memory.

## Workload and implementation

Each segment contains one block of either 16 or 4,096 sparse-indexed records,
each with a 32-byte repeated payload. Plain and ZstdDefault encodings are separate
cases. The benchmark verifies the segment against its expected coverage, count,
and SHA-256 using a `bytes.Reader`. It includes header/index parsing, payload
hashing, and block/frame validation, but no filesystem reads, file syncs, directory
publication, or application decoding. Reported MB/s uses logical payload bytes.

The baseline used `decodeBlock` during verification, building a slice of every
record before discarding it. The current `walkBlock` validates the same bytes and
metadata without collecting that slice. `Segment.Verify` and catalog digest
verification use this path. Reads use the same validator with an internal record
collector, and expose no records if any part of the block fails validation.

## Measured allocations

| Encoding | Records | Before B/op | Current B/op | Before allocs/op | Current allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| Plain | 16 | 34,960 | 34,000 | 15 | 11 |
| Plain | 4,096 | 652,364 | 229,841 | 25 | 11 |
| ZstdDefault | 16 | 66,907 | 65,996 | 39 | 35 |
| ZstdDefault | 4,096 | 834,375 | 411,541 | 39 | 25 |

For the larger block, allocated bytes fell about 65% for plain and 51% for zstd.
Stored/decoded buffers, metadata, hashing, and zstd decoder allocations remain.
Verification still scans every frame and all referenced history. These results
show reduced allocation in verification, not constant-cost startup or rotation.

## Correctness coverage

Tests exercise verification without collection and reads with collection against
explicit expected outcomes for duplicate/decreasing IDs, incorrect first/last IDs,
incorrect record counts, and a corrupt final frame with a valid enclosing block
checksum. Failed reads return no partially validated record slice. Existing
single-byte corruption, truncation, format-0, compressed-block, catalog recovery,
and shared backend history tests exercise the same validator.

## Reproduce

```sh
go test ./pkg/segmentlog -run '^$' -bench '^BenchmarkSegmentVerification$' -benchtime=100x -count=1
```
