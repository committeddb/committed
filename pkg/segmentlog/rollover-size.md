# Full-size rollover and recovery measurements

`BenchmarkRolloverSize` measures 1, 20, and 32 MiB original-frame targets, including
the managed log's 20 MiB default. It uses 4,080-byte payloads plus 16-byte frames,
appended in batches of up to 64 records. Append-group framing is additional.

Each run measures five cycles:

1. Fill the active tail exactly to its original-frame target.
2. Close and time reopening the full active tail.
3. Time an append of one record that must cross the segment boundary.
4. Close and time reopening with one active record and growing closed history.

The fixture checks coverage, counts, frozen size, retained predecessor inode and
size, original append progress, and the newly appended record after recovery.
It scans every record against its expected payload and runs full verification at
the end. A 64 KiB version runs as `TestRolloverSizeWorkload` in the normal suite.

The benchmark adds test-only timers around ordinary append calls while filling
the tail, the existing file installer, and the bbolt commit function. Production code and synchronization behavior are unchanged.

## Linux baseline: separate empty-tail installation

Recorded September 17, 2026 with Go 1.26.6, Linux/arm64, and Alpine 3.20 in the
local OrbStack VM. Data resides on the container's disposable `overlay` writable
layer; only the executable is bind-mounted read-only. Tests and lint completed
before the benchmark. The VM and host were not reserved or load-controlled.

All nine runs passed their checks. Each value below is the median of three runs'
five-cycle means, in milliseconds. The range shows the smallest and largest
run means, not individual append extremes or tail-latency percentiles.

| Original-frame target | Boundary append | Range of boundary means | Full-tail reopen | One-record-tail reopen |
| ---: | ---: | ---: | ---: | ---: |
| 1 MiB | 66.73 | 63.06–70.48 | 10.84 | 5.11 |
| 20 MiB | 77.09 | 57.71–81.83 | 34.40 | 10.17 |
| 32 MiB | 67.13 | 59.42–71.79 | 50.90 | 10.55 |

To keep phase sums meaningful, the breakdown below uses the same run that supplied
each size's median boundary mean:

| Target | New-tail installation | Metadata commit | Other boundary work | Total |
| ---: | ---: | ---: | ---: | ---: |
| 1 MiB | 24.51 | 30.69 | 11.53 | 66.73 |
| 20 MiB | 19.43 | 28.92 | 28.74 | 77.09 |
| 32 MiB | 20.94 | 21.07 | 25.12 | 67.13 |

In this baseline, installation includes writing/syncing the empty new tail and publishing its name
durably. Metadata commit includes the bbolt transaction and its synchronization.
Other work includes hashing the old tail, changing handles, and writing/syncing
the new record. It is **not** a measurement of hashing alone.

Durable installation and commit account for much of the measured boundary time.
The much slower 1 MiB result compared with the earlier tiny-segment Linux run,
and the non-monotonic boundary results, prevent attributing the whole difference
to segment size. Full-tail recovery grows with the amount of active data in these
samples. One-record-tail recovery stays much smaller despite accumulating up to
160 MiB of closed history, consistent with avoiding a historical payload scan.

These results do not establish production latency. They use warm filesystem
state, a VM-backed filesystem, only five closed ranges, and no concurrent log
users. Closing/reopening does not evict the operating-system cache. The workload
does not establish cold recovery, 100 TB scale, physical device flush behavior,
or power-loss durability. The earlier small-segment timings should not be used
as estimates for default-size rollover.

## Populated-tail installation comparison

Rollover now installs the header and first append group together before committing
metadata. The file installer syncs those bytes and their directory entry; a
successful metadata commit needs no separate append write/sync for that group.
The on-disk format and normal synchronization settings are unchanged.

A fresh comparison on September 17 used the same Linux setup, with committed
baseline `cdb84f1` and the populated-tail implementation. Three sequential pairs
ran baseline then populated, each measuring five cycles at the default 20 MiB
target. All six runs passed payload, coverage, recovery, and verification checks.
No other test or lint job from this experiment ran during the comparison.

| Implementation | Median boundary mean (ms) | Range of boundary means (ms) | Median full-tail reopen (ms) | Median one-record-tail reopen (ms) |
| --- | ---: | ---: | ---: | ---: |
| Separate empty-tail installation | 19.57 | 16.97–20.03 | 18.14 | 1.13 |
| Header and first group installed together | 15.88 | 15.86–16.49 | 14.70 | 1.26 |

The median boundary mean decreased by about 19%; each pair improved. This is a
small, fixed-order sample on an uncontrolled VM, not a latency guarantee. The
fresh baseline is much faster than the earlier baseline above, demonstrating
substantial environment variability. Recovery code is unchanged; differences in
reopen timings do not establish a recovery improvement.

For the run supplying each median boundary mean, the baseline spent 5.10 ms in
installation, 2.57 ms in metadata commit, and 11.91 ms in other work. The populated
version spent 3.89, 2.50, and 9.48 ms respectively. Installation now includes the
first group; other work no longer includes its separate append sync. At that revision, hashing the predecessor and durable installation/metadata
commit remained synchronous.

Failure and subprocess-exit tests check both sides of publication with a
multi-record first group. Before commit, recovery ignores the populated orphan.
After commit, recovery retains the complete group even if the append was never
acknowledged. Callers already reconcile durable record IDs after uncertain errors.
The segmentlog and EventLog suites passed under the macOS race detector; the full
segmentlog suite also passed on Linux, including subprocess recovery tests.

## Incremental digest comparison

The appender now maintains SHA-256 as groups are written. Recovery reconstructs
it in the existing validation pass, reading each byte once. Rollover captures
the digest with the synchronized tail state instead of rereading the predecessor.
New-tail installation hashes the header and first group as they are written.
The digest is process-local; no format or synchronization setting changes.

A fresh September 17 comparison used baseline `e7c8e0c` (populated-tail
installation) and incremental hashing on the same Linux setup. Each version ran
three times at 20 MiB, five cycles per run. The second pair reversed execution
order; all runs were sequential, after tests and lint completed. Both binaries
included the same new `tail-fill-ms` timer. All six runs passed their checks.

| Implementation | Median boundary mean (ms) | Range of boundary means (ms) | Median full-tail reopen (ms) | Median tail fill (ms) |
| --- | ---: | ---: | ---: | ---: |
| Hash predecessor at rollover | 16.22 | 15.62–16.24 | 18.09 | 160.6 |
| Incremental digest | 5.28 | 4.25–10.52 | 27.05 | 149.5 |

Boundary means improved in all three pairs, with a median reduction of about 67%.
Work outside installation and metadata commit fell from 9.75–10.66 ms to
0.055–0.069 ms. Installation and commit still depend on filesystem latency; their
variation explains most of the remaining spread.

This moves work rather than eliminating hashing. Full-tail recovery became slower
in every pair, adding roughly 8–11 ms to reconstruct the 20 MiB digest. The tail
fill metric totals ordinary batched Append calls while filling one segment,
excluding payload construction. Its run means varied from 129–216 ms before and
133–217 ms afterward; these samples do not isolate the extra append CPU cost or
establish a throughput improvement. Recovery and ordinary appends now pay the
hashing cost instead of concentrating it in boundary append. The benchmark
reopens a full tail immediately before every boundary, so it also exercises the
reconstructed digest.

Tests independently hash physical file bytes and compare them with appender
state after append, recovery, populated-tail installation, and partial or complete
erasure of the active tail. They check that recovery reads the file only once,
invalid input leaves the digest unchanged, and failed write/sync prevents digest
publication. Existing full verification and process-crash tests also pass. The
VM, warm-cache, small-sample, and production-scale limitations above still apply.

## Reproduction

```sh
go test -race ./pkg/segmentlog -run '^TestRolloverSizeWorkload$' -count=1
go test ./pkg/segmentlog -run '^$' -bench '^BenchmarkRolloverSize$' -benchtime=1x -count=3
```

The overall `ns/op` includes fixture construction and verification. Use the named
phase metrics for comparison. The workload passed on Linux without the race
detector and separately under the macOS race detector.
