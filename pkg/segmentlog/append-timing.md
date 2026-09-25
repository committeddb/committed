# Ordinary append timing breakdown

`BenchmarkRolloverSize` has test-only timers around the tail file's `WriteAt` and
`Sync` calls during ordinary fill batches. Each batch is checked to perform
exactly one write and one sync. No production synchronization, allocation, or
encoding behavior changes.

The workload fills five active tails to a 20 MiB original-frame target, appending
up to 64 records per batch with 4,080-byte payloads. That produces 400 ordinary
fill batches per run. Boundary appends are timed separately. Payload generation
and timer-wrapper setup occur outside the append timer. Unlike the shared-backend
sustained workload, this fixture reopens each full tail before rollover and does
not walk the directory after every append. It exercises `pkg/segmentlog` directly,
without the EventLog adapter.

## Metrics

- `tail-fill-ms`: total ordinary append time, divided by five segment fills.
- `fill-write-ms`, `fill-sync-ms`: time spent inside file write and sync calls,
  divided by five fills.
- `fill-other-ms`: the remainder, including metadata lookup, validation,
  allocation, encoding, checksumming, incremental hashing, and scheduling outside
  the timed file calls. It is not a measurement of allocation or GC alone.
- `slowest-fill-ms` and its write/sync/other components: the slowest single
  ordinary batch across the entire run, with all components from that same batch.
  These values are maxima, not latency percentiles.

All measurements are elapsed wall time. File-call timers include kernel,
filesystem, VM, and scheduling delays; they do not isolate physical device time.
The wrappers add small timing and allocation overhead. A CPU profile measures
sampled CPU work, not time blocked waiting for sync, and covers fixture setup and
verification unless filtered to append call stacks.

## Recorded Linux timing

September 17, 2026, Go 1.26.6, Linux/arm64 in Alpine 3.20 on the local OrbStack VM.
Data resides on a disposable container overlay filesystem. Three unprofiled runs
were sequential, after tests and lint completed. The host and VM were not
load-controlled. All runs passed their recovery, full-record, and digest checks.

Each row below reports mean elapsed milliseconds per segment fill, comprising
80 ordinary append batches. Small discrepancies in sums reflect rounding.

| Run | Total fill | Write calls | Sync calls | Other work |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 673.8 | 64.53 | 489.6 | 119.7 |
| 2 | 552.4 | 50.63 | 377.1 | 124.7 |
| 3 | 658.5 | 53.97 | 494.5 | 110.1 |

Sync accounts for 68–75% of total ordinary append elapsed time in these runs.
That does not explain every outlier. The slowest individual batch from each run
had this breakdown, also in milliseconds:

| Run | Batch total | Write call | Sync call | Other work |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 79.18 | 1.047 | 77.72 | 0.4131 |
| 2 | 48.57 | 0.2220 | 2.463 | 45.88 |
| 3 | 92.34 | 0.1458 | 39.79 | 52.40 |

These measurements locate waits both inside and outside file calls. They do not
prove that outside-call stalls are GC pauses or that inside-call stalls are disk
flush latency. Scheduling and the VM can affect both. This workload also differs
from the earlier shared-backend comparison, so it does not identify the cause of
that comparison's particular outliers. No production optimization or durability
change was made on the strength of these measurements.

## Separate CPU profile

Three additional runs captured a CPU profile separately from the timing table.
All passed their correctness checks. The profile contains 10.21 seconds of CPU
samples over 18.26 seconds elapsed, including fixture construction, recovery,
and full verification. Filtering to stacks containing `Tail.Append` retains
3.65 seconds of samples; 2.55 seconds of those are attributed directly to the
Linux syscall function. The append stacks include 1.92 seconds under file sync
and 0.66 seconds under positioned writes (inclusive values, not additive to the
syscall total).

The whole-workload profile also shows CRC32C and SHA-256 work, as expected from
append and verification. CPU samples do not correlate an individual long append
with GC or descheduling, nor do they measure time blocked on storage. These
results therefore support keeping I/O and outside-call work separately visible;
they do not establish a single cause for the observed stalls.

## Reproduction

```sh
go test -race ./pkg/segmentlog -run '^TestRolloverSizeWorkload$' -count=1
go test ./pkg/segmentlog -run '^$' -bench '^BenchmarkRolloverSize/MiB=20$' -benchtime=1x -count=3
```

For a separate CPU profile, write the compiled test binary and profile under
`.claude-scratch/`, then run the binary with `-test.cpuprofile` alongside the
benchmark flags. Keep profiling separate from the unprofiled timing runs.
