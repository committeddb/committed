# Concurrent event-log workload comparison

## Method

Run the benchmark separately from other tests or lint jobs:

```sh
go test ./internal/cluster/db/wal -run '^$' -bench '^BenchmarkStreamingWorkload$' -benchtime=1x -count=3
```

Both cases use public `wal.Open` backend selection, durable
`Save`/`ApplyCommittedBatch`, and the same production `Storage.ReaderAt` path.
They reuse the deterministic JSON fixture from the
[backup workload](backup-workload-results.md).

- Preload 8,192 records, then append another 8,192 in 32 batches of 256.
  The complete history contains about 65 MiB of protobuf input.
- Start 128 readers at the existing head and four at the first user record.
  All readers are ready before writes start. The writer never waits for readers.
- Each reader independently follows the applied watermark, retrying EOF after
  one millisecond. It checks exact index, key, and payload for every record.
- Each sample verifies 1,114,112 delivered Actuals: 8,192 for each live reader
  and 16,384 for each historical reader.
- Default 20 MiB segments; tidwall's default 16-segment cache; segmented
  ZstdDefault with 160 MiB recent and 160 MiB historical cache budgets.
- The production background sealer runs. Its idle check is shortened from 30
  seconds to one millisecond so this short workload exercises compression.
  Its normal 100 ms pacing between successful compressions is unchanged.
- A counting wrapper records successful compression completions during the
  measured interval. The benchmark requires at least one, and checks for
  compression errors after joining the worker at shutdown.
- Fixture generation, expected-payload decoding, preloading, and shutdown are
  outside the timer. Reader creation, writing, decoding, exact-payload checks,
  EOF waits, and waiting for every reader to finish are inside it.
- This runs storage readers, not full syncable workers or destination writes.
  It includes no scrub, backup capture, cluster replication, or network traffic.

Batch latency includes Raft Save and application apply. Completion times start
at the shared reader/writer release; reader completion reports the slowest reader
in each group. Per-sample percentiles use the nearest rank among only 32 batches.
Compression completions can include work on preloaded history that finishes
during the measured interval.

## Local results

Three independent samples per backend on Apple M4 Max, darwin/arm64, Go 1.26.6,
GOMAXPROCS 16, without overlapping test or lint runs. Ranges below are the
minimum and maximum across those samples, not confidence intervals.

| Measurement | tidwall | segmented |
| --- | ---: | ---: |
| Writer completion | 1.225–1.337 s | 1.164–1.198 s |
| Slowest live reader completion | 1.235–1.344 s | 1.495–1.517 s |
| Slowest historical reader completion | 1.233–1.343 s | 1.526–1.546 s |
| Batch p50 | 35.79–39.96 ms | 33.73–33.99 ms |
| Batch p95 | 56.36–58.40 ms | 56.93–61.07 ms |
| Largest batch latency | 57.03–59.98 ms | 57.02–63.08 ms |
| Compression completions | 2 each run | 2 each run |
| Cumulative allocations | 18.29–18.44 GB | 18.20 GB |

The segmented writer finished earlier in these samples, but its readers took
longer to finish: roughly 0.33–0.36 seconds remained after writes completed.
Tidwall's readers finished within roughly 6–10 ms of writer completion.
These observations do not identify the cause of the difference.

Allocation figures sum all measured allocations, including decoded Actuals for
more than a million deliveries. They are **not peak memory or resident RAM**.
Background activity overlaps measurement boundaries, so the small allocation
difference is not a precise per-record backend cost.

The history fits within both configured caches. This experiment does not test
cache pressure, cold historical reads at large scale, realistic destination
backpressure, or production latency distributions. It establishes correctness
and comparative runtime for this specific concurrent workload.

## Automated coverage

`TestStreamingWorkload` runs the same harness with 1,024 records, eight live
readers, and two historical readers against both backends. It is untagged, so
normal CI test and race jobs include it. It checks record contents and counts,
without timing thresholds. Its smaller history does not roll over default
20 MiB segments.

The full benchmark exercises concurrent compression and can also be run with
`-race`. Benchmark measurements are opt-in; normal CI does not run them.
