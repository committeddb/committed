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

## Frozen append-file verification

Frozen append files now share one complete record/reference validator between
ordinary opening and explicit digest verification. When verification requests a
SHA-256 digest, the scanner hashes the physical header and append groups during
that validation pass. It no longer rereads the complete file afterward. Normal
opening still validates without computing the whole-file hash.

`BenchmarkClosedTailVerification` uses a 20 MiB original-frame fixture with 5,120
records, 4,080-byte payloads, and groups of 64. Including headers/trailers, the
file contains 20,975,392 bytes. The fixture and expected digest are constructed
outside the timer. A reader wrapper counts bytes actually requested through
ReadAt while verification checks both semantic validity and the expected hash.

Recorded September 17, 2026 with Go 1.26.6, Linux/arm64, Alpine 3.20 in the local
OrbStack VM. Baseline `39c81c4` and current binaries ran three sequential pairs,
30 verifications per case, reversing order in the second pair, after tests and
lint completed. Values below are medians across three runs. All runs passed.

| Implementation | Reader bytes/op | Allocated bytes/op | Allocations/op | Mean ms/op |
| --- | ---: | ---: | ---: | ---: |
| Separate validation and digest passes | 41,950,784 | 303,488 | 10 | 11.947 |
| Combined validation and digest pass | 20,975,392 | 270,624 | 7 | 11.586 |

Reader bytes halved, but elapsed time improved only about 3% in this in-memory
fixture. SHA-256 and record/group checksum work remain. This is not a physical
disk-read measurement, filesystem benchmark, or claim that full verification is
twice as fast. The host and VM were not load-controlled.

Tests assert that each physical byte is read exactly once, check I/O error
propagation at headers and payloads, and reject corrupt framing even when its
whole-file hash matches. Unordered records with valid frame/group/file checksums
are also rejected. Existing reference-mismatch tests retain size, coverage,
count, truncation, extension, and digest checks. Storage/backend race tests, the
Linux storage suite, lint, and gosec pass.

This changes explicit verification of frozen append files. It does not remove
the prevalidation before rewrite callbacks, alter rewrite locking, or eliminate
the full-history scrub selection scan.

```sh
go test ./pkg/segmentlog -run '^$' -bench '^BenchmarkClosedTailVerification$' -benchtime=30x -count=3
```
