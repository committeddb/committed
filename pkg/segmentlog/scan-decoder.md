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
unchanged. These decoder measurements predate descriptor reuse below. Decoded payloads
still allocate per block;
retaining returned records can retain these buffers beyond the scan. These are
cumulative allocations, not peak memory. The small timing sample does not
establish production throughput or whole-log scrub latency. Managed scans and
rewrites still hold the log mutex; this change affects indexed segments, not
frozen append-format ranges.

```sh
go test ./pkg/segmentlog -run '^$' -bench '^BenchmarkIndexedScan$' -benchtime=20x
```

## Reusing the internal record list

A scan now retains one record-descriptor list and reuses it between blocks. Each
record is yielded by value, so replacing a descriptor does not change a record
already held by a caller. Payload storage remains independent per block. Before
reuse, the list is cleared so unused entries after a smaller block do not retain
old payload buffers. The list grows only when a block contains more records than
its current capacity. It retains its largest allocation until the scan finishes;
this reduces cumulative allocation, not necessarily the peak working set.

The complete next block still validates before delivery. Tests cover growing and
shrinking record counts, retained records after descriptor reuse, cleared unused
entries, and rejection of a corrupt final frame even when a reusable list exists.
The existing interleaved-scan and early-stop ownership tests remain in place.

`BenchmarkIndexedScan` now includes 32-byte payloads as well as 4,080-byte payloads.
The small-record fixture contains 436,906 records and 20,971,488 framed bytes in
81 blocks; the larger-record fixture remains 5,120 records in 80 blocks. Both use
sparse IDs and repeated payload bytes. Every timed scan checks all IDs, payloads,
and the record count. Fixture creation stays outside the timer.

Recorded September 17, 2026 with the same Go/Linux/OrbStack setup described above.
Baseline `116e2fc` and changed binaries ran three sequential pairs, 20 iterations
per case, reversing order in the second pair. Each process finished before the
next began, after tests and lint completed. All runs passed. Host and VM load
were uncontrolled. The following values are medians across three runs.

| Encoding / payload bytes | Before B/op | After B/op | Before allocs/op | After allocs/op | Before time | After time |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Plain / 32 | 35,391,918 | 21,153,026 | 162 | 82 | 8.411 ms | 8.176 ms |
| Plain / 4,080 | 21,155,880 | 20,973,842 | 160 | 81 | 5.227 ms | 5.140 ms |
| ZstdDefault / 32 | 38,824,810 | 24,582,813 | 281 | 193 | 19.012 ms | 16.401 ms |
| ZstdDefault / 4,080 | 21,364,616 | 21,182,005 | 266 | 186 | 8.927 ms | 8.261 ms |

Descriptor reuse saves about 13.6 MiB of cumulative allocation per small-record
scan, compared with about 178 KiB for the larger records. Timing remains a small
local sample, not a production guarantee. Storage race suites, Linux segmentlog
tests, lint, and gosec passed.
