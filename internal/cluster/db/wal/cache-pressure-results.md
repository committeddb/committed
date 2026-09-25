# Concurrent workload with constrained caches

## Method

```sh
go test ./internal/cluster/db/wal -run '^$' -bench '^BenchmarkStreamingCachePressure$' -benchtime=1x -count=3
```

This uses the [shared streaming harness](streaming-workload-results.md), including
durable writes, exact record validation, 128 live readers, four historical
readers, and the production background sealer with its idle check shortened to
one millisecond. The sealer's 100 ms pacing is unchanged.

The history has 32,768 records (about 130 MiB of protobuf input). Preloading
writes the first 24,576 records outside the timer; the measured writer adds 8,192
records in 32 batches. Historical readers start at the first user record and
after 8,192, 16,384, and 20,480 user records. All continue through the final index.
Each sample checks 1,134,592 delivered Actuals, including exact index, key, and
payload, and requires background compression to complete during measurement.

The same workload runs with two cache configurations:

| Configuration | Tidwall | Segmented |
| --- | --- | --- |
| Default | 16 segments | 160 MiB recent + 160 MiB historical |
| Constrained | 2 segments | 32 MiB recent + 32 MiB historical |

These are retention settings, **not equal total-memory limits**. Tidwall counts
segments; segmented charges allocated payload and index capacity. Active tails,
reader-retained arrays, returned payload copies, fixture data, and the OS page
cache are outside these limits. In particular, eviction never invalidates a
segmented reader's retained immutable bytes.

The history exceeds either constrained cache configuration. Default-cache
samples provide a control for the same history size and staggered reader starts;
the earlier streaming benchmark used a smaller history and common starts.

## Local results

Three independent samples per configuration/backend on Apple M4 Max,
darwin/arm64, Go 1.26.6, GOMAXPROCS 16, without overlapping tests or lint.
Ranges are minimum–maximum observations, not confidence intervals.
Reader completion is the slowest reader across both groups.

| Cache | Engine | Writer completion | Reader completion | Batch p95 | Largest batch | Cumulative allocations |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Default | tidwall | 1.241–1.306 s | 1.253–1.310 s | 51.66–56.63 ms | 51.94–59.52 ms | 18.57–18.68 GB |
| Default | segmented | 1.342–1.393 s | 1.352–1.404 s | 65.28–66.22 ms | 79.89–99.11 ms | 18.63–18.64 GB |
| Constrained | tidwall | 1.292–1.368 s | 1.304–1.378 s | 55.92–58.58 ms | 63.94–72.65 ms | 18.82–18.89 GB |
| Constrained | segmented | 1.336–1.369 s | 1.348–1.386 s | 64.18–75.11 ms | 76.60–100.10 ms | 19.16–19.29 GB |

All samples completed two compression steps during measurement, with no
compression errors. Shutdown joins any remaining in-flight work outside the
timer. Allocation figures are cumulative GB, **not peak or resident memory**.

The smaller caches did not produce a large reader backlog in these samples.
Segmented readers finished about 12–21 ms after writes, compared with roughly
10–16 ms for tidwall. Segmented's total completion ranges overlapped its
default-cache control, while its cumulative allocations and some batch latency
spikes were higher. This small sample does not establish a general performance
ranking or identify the cause of those spikes.

## Limits and automated checks

This deliberately creates limited engine-cache retention on a modest history.
The OS page cache is not flushed, and the fixture was just written locally.
No cache-miss, disk-I/O, or resident-memory counters are collected. It does not
measure cold-disk throughput, bound total process memory, or test a large catalog
or 100 TB log. Four finite catch-up readers also do not represent an indefinitely
sustained collection of independently paced historical readers.

The untagged `TestStreamingWorkload` includes same-start and staggered-start
cases for both backends, so normal CI verifies the harness's per-reader contents
and counts without timing thresholds. Its small history does not exceed the
default caches. The full constrained benchmark runs through `make bench/workloads`
(`-benchtime=1x -count=1`) in the CI `make bench` job on pull requests,
pushes to `main`, and manual workflow dispatches. CI records results in its job
log and downloadable benchmark artifact without performance regression thresholds. The three-sample
measurements above were separate local runs; `-race` benchmark runs are also
invoked separately.
