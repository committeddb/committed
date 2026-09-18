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
and replay of the unchanged prefix still allocate. Replay stops at the known
unchanged record count; a first-record change needs no prefix replay. Managed rewrites
still scan selected history and hold the log mutex throughout.

## Focused benchmark

`BenchmarkRewritePreparation` prepares a 20 MiB append-format range containing
5,120 records with 4,080-byte payloads, in groups of 64. Cases leave all records
unchanged, modify the first record in place, modify the first record of the second
group, or modify the last record in place. The second-group case was added for
the prefix-replay measurements below.
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

## Stopping prefix replay before the changed record

Preparation records how many records were unchanged before the first change.
Output replays exactly that many records, stopping immediately after the last
unchanged one. Previously, it fetched the first changed record again to discover
where to stop. A change to the first record therefore now skips prefix replay
entirely; a change at a block/group boundary avoids decoding that next block or
group during replay. Later transformations still run once per record. An explicit
context check before output preserves cancellation when replay is skipped.

Tests use sparse IDs starting above zero and verify exact input traversal counts,
callback counts, survivor IDs, and payloads for both replacement and erasure.
A separate test cancels after detecting the first change but before output and
requires cancellation without delivering a replacement record.

Recorded September 17, 2026, Go 1.26.6, Linux/arm64, Alpine 3.20 on the local
OrbStack VM. Baseline `ef8ae68` and changed binaries ran sequentially, one run per
version with 20 iterations per case, after validation finished. Both runs passed.
The fixture is the 20 MiB append-format range described above; the boundary case
changes ID 64, the first record of the second group. Host/VM load was uncontrolled.

| First change | Before B/op | After B/op | Before ms/op | After ms/op |
| --- | ---: | ---: | ---: | ---: |
| None | 21,815,904 | 21,816,389 | 27.04 | 27.12 |
| First record | 22,088,954 | 21,816,080 | 22.89 | 27.38 |
| First record of second group | 22,361,544 | 22,088,850 | 30.95 | 19.09 |
| Last record | 43,628,046 | 43,627,795 | 46.78 | 49.21 |

First-record and group-boundary changes avoid roughly 266 KiB of allocation in
this fixture by skipping an unnecessary group decode and associated descriptors.
No-op and last-record cases allocate about the same amount. Timing moved in both
directions; this single pair establishes no general latency improvement. The
unchanged prefix still needs replay when it exists, and managed rewriting still
holds the log mutex and scans its requested scope.
