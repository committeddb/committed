# Indexed scan decoder reuse

Each indexed `Records` or bounded `recordsIn` iteration owns one lazy zstd
decoder. It closes that decoder on completion, error, or early termination.
Compressed blocks receive fresh decoded-output buffers, while plain blocks keep
their independently read stored buffers. Advancing or closing a scan does not
invalidate previously delivered payloads. There is no shared decoder on Segment
and no global pool. Point reads retain their separate decoder lifetime.

The decoder's owned-output method retains decompression state but discards its
reference to each returned output buffer. Verification continues using borrowed,
reusable output because it exposes no records. Both paths retain the same
checksum, length, window, frame, and index validation. Selected blocks validate
completely before any records are delivered.

Tests interleave two scans, retain records across multiple differently sized
blocks, mutate one scan's payloads, and check that the other scan's retained bytes
stay unchanged. They also check payload ownership after early iterator closure
and subsequent scans. Existing corruption and rewrite tests exercise the same
block validator. Storage race suites, Linux segmentlog tests, lint, and gosec pass.

## Measurements

`BenchmarkIndexedScan` creates an indexed segment containing 5,120 sparse-ID
records with repeated 4,080-byte payloads: 20 MiB of framed data in 80 blocks.
Each timed scan checks every ID and payload and the final record count. Fixture
encoding and opening are outside the timer. The source is an in-memory reader;
there are no filesystem syncs or publication operations in this benchmark.

September 17, 2026, Go 1.26.6, Linux/arm64, Alpine 3.20 on the local OrbStack VM.
Baseline `e9a766b` and changed binaries ran three sequential pairs with 20
iterations per case, reversing order in the second pair. Each process finished
before the next started, after validation completed. All runs passed. Host and
VM load were uncontrolled. Values below are medians across three runs.

| Encoding | Before B/op | After B/op | Before allocations/op | After allocations/op | Before time | After time |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Plain | 21,155,863 | 21,156,108 | 160 | 160 | 5.761 ms | 5.514 ms |
| ZstdDefault | 33,332,513 | 21,366,536 | 1,209 | 269 | 12.444 ms | 10.148 ms |

Compressed scans allocated about 36% fewer bytes. Plain allocation was effectively
unchanged. Decoded payloads and record descriptors still allocate per block;
retaining returned records can retain these buffers beyond the scan. These are
cumulative allocations, not peak memory. The small timing sample does not
establish production throughput or whole-log scrub latency. Managed scans and
rewrites still hold the log mutex; this change affects indexed segments, not
frozen append-format ranges.

```sh
go test ./pkg/segmentlog -run '^$' -bench '^BenchmarkIndexedScan$' -benchtime=20x
```
