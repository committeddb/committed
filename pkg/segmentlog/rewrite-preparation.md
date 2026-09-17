# Rewrite comparison-buffer reuse

Rewrite preparation must detect whether each range changes so it can preserve
unchanged files. Transforms may modify their input payload in place, so comparing
the returned payload with that same input slice is insufficient.

Preparation now keeps one private, reusable copy of the original payload while
looking for the first change. A no-op still copies and compares bytes, but no
longer allocates a fresh payload copy for every record. Once a change is found,
the range needs a replacement regardless of later results: preparation releases
its comparison buffer and applies the remaining transforms without copying or
comparing their original payloads. Every examined record still invokes the
transform once. Cancellation and callback errors retain their existing behavior.

The buffer is local to one preparation call and is never passed to a callback.
Growth follows encountered payload sizes. It is not a pool, shared cache, or
whole-history buffer. Decoded blocks/groups, record descriptors, changed output,
and the unchanged-prefix reread still have their existing costs. Managed rewrites
still scan selected history and hold the log mutex throughout.

## Focused benchmark

`BenchmarkRewritePreparation` prepares a 20 MiB append-format range containing
5,120 records with 4,080-byte payloads, in groups of 64. Cases leave all records
unchanged, modify the first record in place, or modify the last record in place.
Each iteration checks callback counts and, when changed, all output IDs, payload
lengths, and expected first bytes. Fixture construction is outside the timer.
Output is consumed but not encoded, synced, or published. The benchmark isolates
preparation allocations; it does not measure an entire managed scrub.

Baseline `d8c5473` clones every original payload. Both binaries contain the same
benchmark. Linux/arm64 measurements use Go 1.26.6 in Alpine 3.20 on the local
OrbStack VM, with 20 iterations per case and sequential baseline/current runs
after tests and lint. Allocation is total allocated bytes per preparation, not
peak memory. The host and VM are not load-controlled.

## Measured preparation allocations

Recorded September 17, 2026. All six cases passed their checks. Times are per-
preparation means over 20 iterations in one run per case; they are not latency
percentiles or filesystem throughput.

| First change | Before bytes/op | After bytes/op | Before allocations/op | After allocations/op | Before ms/op | After ms/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| None | 42,783,494 | 21,815,830 | 5,293 | 175 | 7.654 | 6.456 |
| First record | 43,056,244 | 22,088,831 | 5,303 | 185 | 7.573 | 6.213 |
| Last record | 64,594,837 | 43,627,714 | 5,461 | 343 | 13.458 | 12.447 |

Allocated bytes fell about 49% for no-op and early-change preparation and 32%
for a late change. The late-change case still rereads the unchanged prefix.
The no-op case still copies each payload into the reusable buffer to detect
in-place mutation; it saves allocations, not that copy work. After the first
change, both copying and comparing original bytes are skipped.

## Full-size plain scrub comparison

The same binaries ran the [1,300 MiB workload](full-size-churn.md), baseline then
current, once each after the focused benchmarks. Each run performed two scrub
rounds and passed all survivor, recovery, reclamation, stable-file, and final
verification checks.

| Implementation | Scrub seconds/round | Replacement bytes/round | Retired bytes/round |
| --- | ---: | ---: | ---: |
| Clone every original payload | 15.650 | 105,098,480 | 104,987,720 |
| Reuse comparison bytes, stop comparing after first change | 9.803 | 105,098,480 | 104,987,720 |

Both runs preserved the 60 unaffected closed files and produced the same byte
counts. Scrub time improved in this single pair, but the baseline itself was much
slower than the earlier 6.680-second session. Timing therefore remains sensitive
to this uncontrolled VM. The allocation reduction is stronger evidence than a
latency percentage from one pair. Rewrites still block reads and appends while
scanning history; this change does not remove that pause or establish 100 TB
scrub performance.

## Correctness

The ownership regression covers empty, growing, and shrinking payloads; an equal
replacement in a different allocation; early and late in-place edits; and edits
and deletions after the first change. It verifies exact callback counts, complete
survivor bytes, and no output creation for a no-op. Existing rewrite cancellation,
failure, corruption, concurrency, and crash tests cover the same preparation path.

## Reproduction

```sh
go test -race ./pkg/segmentlog/... ./internal/cluster/db/eventlog/...
go test ./pkg/segmentlog -run '^$' -bench '^BenchmarkRewritePreparation$' -benchtime=20x -count=1
go test ./pkg/segmentlog -run '^$' -bench '^BenchmarkLiveSegmentChurnFullSize/codec=0$' -benchtime=1x -count=1 -timeout=15m
```
