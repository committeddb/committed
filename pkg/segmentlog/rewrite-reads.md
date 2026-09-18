# Rewrite source read amplification

`BenchmarkRewriteSourceReads` measures opening a range and preparing its
transformed record stream. It counts successful bytes and calls through
`io.ReaderAt`, with an in-memory backing reader. These are logical source reads,
not filesystem syscalls or physical device reads. The storage implementation is
unchanged.

The fixture contains 5,120 sparse-ID records with repeated 4,080-byte payloads:
20 MiB of framed records in 80 append groups or indexed blocks. It compares
append format, plain indexed format, and default zstd indexed format. Each case
either makes no change, changes the first record, or changes the last record.
The callback runs once per record; output checks IDs, lengths, the changed byte,
and survivor count. A no-op must not emit a replacement stream.

Fixture construction is outside the timer. Opening, validation, change detection,
prefix replay, and output consumption are timed. Replacement encoding, filesystem
I/O, durable publication, and reclamation are excluded. The callback mutates its
input to exercise the same change-detection contract as the managed experiment.

## Measurements

September 17, 2026, implementation `303db56`, Go 1.26.6, Linux/arm64, Alpine 3.20
on OrbStack. Three sequential processes ran 20 iterations per case after the
benchmark passed under the race detector and lint passed. Every Linux case
passed. Read counts and byte totals were identical in all runs. Times are
medians across three runs on an uncontrolled host and VM.

| Source / change | Source bytes | Read bytes/op | Read calls/op | Median time |
| --- | ---: | ---: | ---: | ---: |
| Append / none | 20,975,392 | 41,950,784 | 322 | 9.286 ms |
| Append / first | 20,975,392 | 41,950,784 | 322 | 8.545 ms |
| Append / last | 20,975,392 | 62,926,176 | 483 | 13.083 ms |
| Plain indexed / none | 20,975,424 | 20,975,424 | 83 | 5.084 ms |
| Plain indexed / first | 20,975,424 | 20,975,424 | 83 | 4.676 ms |
| Plain indexed / last | 20,975,424 | 41,946,944 | 163 | 9.349 ms |
| Zstd indexed / none | 51,039 | 51,039 | 83 | 4.889 ms |
| Zstd indexed / first | 51,039 | 51,039 | 83 | 4.353 ms |
| Zstd indexed / last | 51,039 | 98,174 | 163 | 9.004 ms |

## Why the counts differ

Opening a frozen append file validates the complete file and its catalog
coverage/count before any transform callbacks. Preparing a rewrite then scans
its records again. Each pass reads one file header and, for each group, one
group header and one body: 161 reads per pass in this fixture. Consequently,
even an unchanged range reads the file twice. A first-record change requires no
prefix replay. A last-record change replays almost the entire stream; group
validation reads the complete final group, making three full file reads here.
The transformation itself still runs only once per record.

Indexed open reads the header, footer, and index without reading payload blocks.
The record iterator reads and validates one complete selected block before
delivering any of its records. That gives three metadata reads plus 80 block
reads. Last-record prefix replay adds 80 block reads, reusing the already opened
metadata. It reads the complete final block even though replay stops before the
changed record.

These are different validation boundaries: frozen append-file opening establishes
whole-file validity before callbacks, whereas indexed iteration establishes
selected-block validity before delivery. The timing comparison does not compare
identical corruption guarantees. Removing the initial append scan would change
the current contract; these measurements do not establish that doing so is safe.

## Interpretation

The earlier [CPU profile](rewrite-profile.md) attributes substantial rewrite CPU
to CRC32C and source reads. The local Go 1.26.6 ARM64 implementation dispatches
Castagnoli checksums to hardware CRC instructions when available, and the profile
shows that assembly path in use. The benchmark quantifies repeated source reads;
it does not measure checksum bytes separately or identify a faster checksum
implementation.

Zstd's small source size reflects deliberately repetitive payloads. Both indexed
encodings still decode or parse all 20 MiB of framed records, explaining why
compressed-byte reduction does not imply an equally large CPU-time reduction.
These in-memory timings do not predict managed rewrite latency, backup costs,
or cold storage performance. The full-history rewrite still holds the log mutex.

```sh
go test -race ./pkg/segmentlog -run '^$' -bench '^BenchmarkRewriteSourceReads$' -benchtime=1x
go test ./pkg/segmentlog -run '^$' -bench '^BenchmarkRewriteSourceReads$' -benchtime=20x -count=3
```
