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

## Initial local results

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

## Copying cached payloads outside the log mutex

The segmented cursor previously copied each cached record while holding the
log-wide mutex. A CPU/mutex profile of three segmented workload samples attributed
about 85% of sampled mutex delay to cursor read releases. Profiles include setup
and perturb timings; delay aggregates time across waiting goroutines.

Cached cursor hits now select the record under that mutex and copy its payload
after unlocking. The selected bytes remain alive through the local slice:
sealed payloads are immutable, and resident-tail appends never change an existing
payload prefix. Rewrite publication still uses the same mutex and replaces
arrays. Callers still receive private payloads. Initial acquisition and uncached
reads retain their existing path.

Three fresh, unprofiled samples per engine produced:

| Measurement | tidwall | segmented |
| --- | ---: | ---: |
| Writer completion | 1.225–1.302 s | 1.252–1.291 s |
| Slowest live reader completion | 1.235–1.311 s | 1.262–1.300 s |
| Slowest historical reader completion | 1.234–1.311 s | 1.261–1.298 s |
| Batch p50 | 35.85–38.93 ms | 36.70–37.39 ms |
| Batch p95 | 52.85–60.98 ms | 58.49–62.25 ms |
| Largest batch latency | 52.97–63.00 ms | 63.25–64.18 ms |
| Compression completions | 2 each run | 1 each run |
| Cumulative allocations | 18.25–18.47 GB | 17.94 GB |

The earlier segmented reader backlog is largely absent in these samples:
readers finish about 8–10 ms after the writer. Writer completion is later than
in the initial segmented samples, reflecting different interleaving rather than
an improvement to every metric.

Only one segmented compression completed inside each shorter measured interval,
versus two initially. Shutdown joins any in-flight step outside the timer.
Consequently this comparison does not establish a reduction in total compression
work or allocations; it measures writer/reader completion while compression runs.
It also does not establish performance under cache pressure or at production scale.

## Automated coverage

`TestStreamingWorkload` runs the same harness with 1,024 records, eight live
readers, and two historical readers against both backends. It is untagged, so
normal CI test and race jobs include it. It checks record contents and counts,
without timing thresholds. Its smaller history does not roll over default
20 MiB segments.

The full benchmark exercises concurrent compression and can also be run with
`-race`. Benchmark measurements are opt-in; normal CI does not run them.
