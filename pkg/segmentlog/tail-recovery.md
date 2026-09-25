# Tail recovery buffer reuse

Recovery validates every frame and reconstructs the physical SHA-256 digest.
Validation-only scans now reuse their group header and body buffers. The body
buffer grows when a larger validated group requires it; its size is bounded by
the existing 32 MiB group limit plus trailer. The scanner creates no record list
when there is no visitor. Scans that deliver records keep independent group bodies
so callers can retain payloads across callbacks.

This changes neither the file format nor recovery decisions. Checksums, record
ordering, group boundaries, rewrite checkpoints, and synchronization remain in
place. The buffer belongs to one scan; there is no pool or shared mutable cache.

## Measurement method

September 17, 2026; Go 1.26.6, Linux/arm64 in Alpine 3.20 on the local OrbStack VM.
Baseline `91d2e1f` already includes incremental tail digests. Both binaries contain
the same new benchmark. Three pairs run sequentially after tests and lint, with
the second pair reversing execution order. The host and VM are not load-controlled.

`BenchmarkTailRecoveryScan` validates an in-memory 20 MiB tail of 4,080-byte
payloads and 16-byte record frames, with either one or 64 records per append
group. Each run performs 20 scans, reconstructing and checking the digest and
record progress. Fixture construction is outside the timer. Allocation numbers
are bytes allocated per scan, not peak resident memory. This benchmark excludes
filesystem reads and synchronization.

The separate `BenchmarkRolloverSize/MiB=20` comparison opens real files on the
container's disposable overlay filesystem. Each run measures five full-tail
reopens and verifies all records and published digests. These are warm-cache,
small-history measurements, not production latency or cold recovery results.

## Results

All scan and full-log benchmark runs passed. The table reports medians across
three runs; scan timings are per-scan means over 20 iterations.

| Records per group | Before bytes/scan | After bytes/scan | Before allocations/scan | After allocations/scan | Before scan (ms) | After scan (ms) |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 25,067,845 | 5,136 | 10,244 | 6 | 50.54 | 47.28 |
| 64 | 21,629,736 | 270,608 | 164 | 6 | 48.26 | 35.17 |

Allocated bytes fell by more than 98% in both cases. These fixed-group fixtures
need one body allocation; a scan encountering progressively larger groups may
allocate multiple times as its buffer grows. Payload-delivering scans still
allocate separate bodies and are not covered by this allocation claim.

For actual full-tail reopen, median five-cycle means fell from 118.8 ms to
54.66 ms. Baseline run means ranged from 92.74–142.0 ms; buffer-reuse means ranged
from 34.85–59.11 ms. All three pairs improved, but the large timing variation and
much slower baseline than the earlier incremental-digest session prevent treating
these absolute values or their percentage difference as a production guarantee.
The allocation reduction is the more direct evidence of this change. No append,
rollover publication, or sync ordering changed.

## Correctness

The regression fixture grows and shrinks group sizes, retains every callback
payload, and compares validation-only state and SHA-256 against the physical
bytes. Existing corruption, truncation, checkpoint, uncertain-write, and
process-crash tests exercise the same scanner. The segmentlog and EventLog suites
pass under the macOS race detector; the segmentlog suite also passes on Linux.

## Reproduction

```sh
go test ./pkg/segmentlog -run '^$' -bench '^BenchmarkTailRecoveryScan$' -benchtime=20x -count=3
go test ./pkg/segmentlog -run '^$' -bench '^BenchmarkRolloverSize/MiB=20$' -benchtime=1x -count=3
```
