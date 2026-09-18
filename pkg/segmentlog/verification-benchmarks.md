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

## Reusing stored-block buffers

Both `Segment.Verify` and catalog digest verification reuse one stored-block
buffer within each pass. The buffer grows only when a larger block requires it;
its size is bounded by the validated maximum stored block size. Verification
retains no records, so overwriting the buffer after a block validates is safe.
Payload-returning reads retain their independent buffers. The measurements in this section precede decoder/output reuse, described below;
at that point compressed blocks still allocated decoded output and decoder state
separately.

`BenchmarkMultiBlockVerification` encodes 5,120 records with repeated 4,080-byte
payloads into 80 blocks of 256 KiB each before compression: 20 MiB of framed data.
The `frames` case runs `Segment.Verify` on an already-open segment. The `digest`
case includes opening/index validation and checking the full SHA-256 against the
catalog reference. Both use in-memory readers; setup is outside the timer. This
measures allocated bytes and CPU work, not filesystem syncs or peak memory.

Recorded September 17, 2026, Go 1.26.6, Linux/arm64, Alpine 3.20 on the local
OrbStack VM. Baseline `6097873` and changed binaries ran three sequential pairs
of 20 iterations per case, explicitly waiting for each process to finish before
starting the next and reversing order in the second pair. Tests and lint were
finished before measurement. All runs passed; host and VM load were uncontrolled.
An earlier batch without explicit process-completion waits is excluded below.
Values are medians across the three sequential runs.

| Encoding / check | Before B/op | After B/op | Before allocs/op | After allocs/op | Before time | After time |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Plain / frames | 20,971,882 | 262,144 | 81 | 1 | 25.78 ms | 14.68 ms |
| Plain / digest | 21,021,067 | 311,616 | 97 | 18 | 49.35 ms | 36.90 ms |
| ZstdDefault / frames | 33,147,271 | 33,097,992 | 1,130 | 1,054 | 56.99 ms | 53.29 ms |
| ZstdDefault / digest | 33,196,910 | 33,146,620 | 1,148 | 1,069 | 88.86 ms | 55.51 ms |

The plain cases avoid allocating another buffer for each of the remaining 79
blocks. Compression makes stored buffers small in this deliberately repetitive
fixture, so reusing them barely changes total allocated bytes. Timings varied
substantially; these measurements establish no production latency guarantee.
Variable-block-size tests cover growth, shrinking, and later read failures in
both verification paths. Existing corruption, checksum, truncation, and legacy
format tests remain in place.

## Reusing the verification decoder

Verification now owns one lazy zstd decoder and reusable decoded-output buffer
per pass, in addition to the stored-block buffer. Plain blocks do not initialize
zstd. Each compressed block receives an output slice whose capacity is limited
to that block's declared decoded size, even when the retained backing allocation
is larger. Exact decoded-length, checksum, frame, index, maximum-memory, and
window checks remain in place. The decoder closes on success or failure.
Point reads use a fresh decoder per block. Multi-block scans reuse decoder state
with independent output storage per block, so returned payloads remain independent. There is no global pool or decoder shared across verification calls.

The same 80-block, 20 MiB in-memory benchmark ran on September 17, 2026 with Go
1.26.6, Linux/arm64, Alpine 3.20 on the local OrbStack VM. Baseline `7ba814a` and
changed binaries ran three sequential pairs of 20 iterations per case after
validation completed, reversing order in the second pair. Each process finished
before the next started. All six runs passed. Host and VM load were uncontrolled.
Medians across the three runs follow.

| Encoding / check | Before B/op | After B/op | Before allocs/op | After allocs/op | Before time | After time |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Plain / frames | 262,144 | 262,144 | 1 | 1 | 4.431 ms | 6.071 ms |
| Plain / digest | 311,616 | 311,616 | 18 | 18 | 11.437 ms | 15.403 ms |
| ZstdDefault / frames | 33,097,834 | 416,019 | 1,051 | 16 | 10.309 ms | 4.980 ms |
| ZstdDefault / digest | 33,147,659 | 465,487 | 1,069 | 33 | 12.631 ms | 5.018 ms |

Compressed verification allocated about 98.7% fewer bytes. Plain allocation was
unchanged, but plain timing was worse in these samples; the cause is unresolved.
These small, uncontrolled measurements establish neither production latency nor
an across-the-board timing improvement. Allocation is cumulative per pass, not
peak resident memory. Decoded-buffer growth and decoder internals still allocate.

Tests exercise large/small block transitions, undersized declarations after a
larger allocation, concatenated compressed frames, corrupt frames, intervening
plain blocks, successful reuse after errors, and standalone output ownership.
The complete storage race suites, Linux segmentlog suite, lint, and gosec passed.

## Reusing storage for metadata hashing

After indexed payload validation, digest verification reuses its stored-block
buffer to hash the index and footer. If that buffer is too small, it allocates
the remaining metadata size, capped at 32 KiB. Reads remain bounded to that
size. The footer guarantees a nonempty copy buffer, including for an empty
segment. This replaces the unconditional 32 KiB buffer allocated by `io.Copy`;
payload validation, metadata parsing, read coverage, and SHA-256 comparison
remain unchanged. Closed append-file verification is unaffected.

Tests cover empty segments, one-block indexes, and indexes larger than a copy
chunk. They check that every metadata byte is read twice (parsing and hashing),
each payload byte is read once, and a wrong digest is rejected. Existing tests
cover format-0 files, corruption, varying block sizes, and read failures. The
segmentlog race suite, Linux segmentlog suite, lint, and gosec passed.

Recorded September 17, 2026 with the same Go/Linux/OrbStack setup above. Baseline
`8203c03` and changed binaries ran three sequential pairs, 20 iterations per
case, reversing order in the second pair. Validation finished before measurement
and each process finished before the next began. All runs passed; host and VM
load were uncontrolled. Values below are medians across three runs.

| Digest fixture | Before B/op | After B/op | Before allocs/op | After allocs/op | Before time | After time |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Plain / 16 small records | 34,000 | 1,232 | 11 | 10 | 2.906 µs | 1.110 µs |
| Plain / 4,096 small records | 229,845 | 197,077 | 11 | 10 | 137.850 µs | 186.009 µs |
| ZstdDefault / 16 small records | 66,275 | 33,271 | 35 | 34 | 23.230 µs | 8.615 µs |
| ZstdDefault / 4,096 small records | 411,592 | 378,708 | 25 | 24 | 297.159 µs | 146.531 µs |
| Plain / 80 blocks, 20 MiB | 303,472 | 270,704 | 11 | 10 | 10.960 ms | 11.016 ms |
| ZstdDefault / 80 blocks, 20 MiB | 457,227 | 428,665 | 26 | 26 | 3.762 ms | 3.791 ms |

Small records have 32-byte payloads. The 80-block fixture uses 4,080-byte
payloads. The change saves 32 KiB per plain digest pass and about 28 KiB for the
compressed 80-block fixture, whose stored blocks are too small to hold its
metadata copy buffer. Decoder allocation varies slightly between runs.
Frame-only verification is unaffected. Timing is mixed, including a slower
4,096-record plain median; these samples establish no general latency gain.
The figures measure cumulative allocation, not peak memory, and exclude
filesystem I/O and catalog publication.
