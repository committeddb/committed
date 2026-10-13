# Performance envelope

What write, apply, and catch-up rates to plan around, where the numbers come
from, and how to re-measure them on your own hardware. Every number below is
**fsync-honest**: committed never acknowledges a write before the raft log is
durable, so the write path's floor is your disk's fsync latency, not CPU.

## The envelope

Conservative planning figures — deliberately below what the reference
hardware measures (see the table in the next section), because production
disks are usually slower than a laptop NVMe and quorum fsyncs land on three
machines:

| Path | Plan around | Governs |
|---|---|---|
| Single-caller write latency | 10–50 ms per proposal | One synchronous `POST /v1/proposal` — fsync-bound |
| Aggregate write throughput | ≥ 1,000 proposals/sec | Many concurrent proposers; raft coalesces their fsyncs |
| Apply rate (batched) | ≥ 1,000 entries/sec | How fast committed entries become readable/syncable |
| Syncable catch-up read rate | ≥ 500,000 entries/sec | Rebuild, re-materialization, new consumer of an old topic |
| Ingest snapshot rate | ≥ 5,000 rows/sec | Initial snapshot of a source table (pipelined) |
| Ingest streaming rate | 10–20 source transactions/sec per ingestable | Change capture after the snapshot; one source transaction at a time |

Two shapes matter more than any single number:

- **Latency and throughput diverge by well over an order of magnitude.** A
  serial caller pays the whole write path per proposal, several fsyncs (~30
  ms on the reference machine → ~30/sec). Concurrent callers share fsyncs
  through raft's Ready batching (~300 µs/op at 8-way → ~3,400/sec). If a load
  generator sees double-digit throughput, it is measuring its own serial
  round-trips, not the cluster's capacity — add concurrency or batch entities
  into fewer proposals.
- **An ingestable streams one source transaction at a time.** It keeps the
  source's commit order, so it waits for each transaction to be durable
  before sending the next: its rate is one transaction per write round-trip
  (~50 ms on the reference machine → ~20/sec). The limit is per transaction,
  not per row: rows committed in one source transaction travel together. A
  source that sustains more transactions per second than this falls behind
  and stays behind; watch `lag` on `GET /v1/ingestable/{id}/status`.
  Separate ingestables stream in parallel.
- **Reads are not the bottleneck.** Catch-up drains the log ~500× faster
  than the write path fills it, so a rebuilt destination converges quickly; sync
  throughput in practice is bounded by the destination database, not the
  log.

## Reference measurements

Measured 2026-08-27 on a 16-core Apple-silicon laptop with local NVMe (all
three nodes of the multi-node run share that one disk, which understates a
real cluster's aggregate fsync capacity). Re-measure on your hardware with
the commands shown.

| Benchmark | Result | Derived rate |
|---|---|---|
| `BenchmarkProposeApplyRoundTrip/serial` | 14.1 ms/op | ~70 proposals/sec |
| `BenchmarkProposeApplyRoundTrip/parallel-8` | 294 µs/op | ~3,400 proposals/sec |
| `BenchmarkApply/per-entry` | 12.5 ms/entry | ~80 entries/sec |
| `BenchmarkApply/batched` (64) | 0.29 ms/entry | ~3,500 entries/sec |
| `BenchmarkReaderCatchUp` | 0.57 µs/entry | ~1.7M entries/sec |
| `BenchmarkIngestShape/batch` | 62 µs/row | ~16,000 rows/sec |
| 3-node HTTP burst (`TestMultiNodeThroughput_Report`, 8 workers) | p50 204 ms, p99 345 ms | ~37 proposals/sec |

`BenchmarkProposeApplyRoundTrip` relaxes bbolt's fsync (the raft log is still
fsynced), so it isolates raft from the metadata store; a running node fsyncs
both. Measured 2026-10-13 on the same laptop: the
`BenchmarkAggregateLargeGroup` rows with `make bench`, the rest against a
running single node with default settings:

| Measurement | Result | Derived rate |
|---|---|---|
| Serial `POST /v1/proposal`, one entity each | median 30 ms | ~31 proposals/sec |
| PostgreSQL ingestable streaming one-row transactions | ~50 ms per transaction | ~20 transactions/sec |
| `BenchmarkAggregateLargeGroup`, five emits, 1,000-input group | 3.1 ms per input | ~320 inputs/sec into that group |
| `BenchmarkAggregateLargeGroup`, five emits, 10,000-input group | 32 ms per input | ~31 inputs/sec into that group |
| Source commit to read-model row, aggregate stage, 40-event group | median 69 ms | — |
| Source commit to read-model row, aggregate stage, 10,000-event group | median 108 ms | — |

The streaming figure commits one-row transactions to PostgreSQL 16 faster
than the ingestable applies them and reads the rate from `appliedIndex` on
`GET /v1/node/status`. The commit-to-row figures run the
[event-store recipe](../recipes/event-store-read-models.md) against
PostgreSQL 16, appending one event at a time to one stream and timing
until its row updates. A 10,000-event stream appended at ~80 events/sec
caught up ~6.5 minutes after the last append, at the streaming rate: the
refold was not the limit.

The 3-node HTTP number is the most conservative of all: real processes, real
HTTP, follower→leader forwarding, quorum replication, and three nodes
contending for one laptop disk at only 8-way concurrency. Treat it as a
floor, not a capacity estimate.

## Re-measuring

Prerequisites: Go 1.27+ and `make` — nothing else. Unlike the Docker-backed
e2e suites, none of this needs Docker or an external database; the 3-node
report builds the real binary and runs it on loopback ports.

```bash
make bench             # micro-benchmarks and storage workload comparisons
make bench/workloads   # backup, concurrent streaming, and constrained caches
make bench/multinode   # the 3-node throughput report (~15s), prints the
                       # THROUGHPUT REPORT line with proposals/sec + p50/p99
```

(`make test/multinode` runs the full crash-recovery suite the report lives
in; the dedicated target exists because `go test` hides the report line on a
passing run.)

Compare before/after a change with
[benchstat](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat):

```bash
go install golang.org/x/perf/cmd/benchstat@latest

make bench > before.txt   # on main
make bench > after.txt    # on your branch
benchstat before.txt after.txt
```

CI runs `make bench` and the 3-node report on pull requests, pushes to `main`,
and manual workflow dispatches. Branch pushes without a PR do not trigger this
workflow.

`make bench` runs the micro-benchmarks with a one-second time target, then
`make bench/workloads`. The workload target runs each backup, streaming, and
constrained-cache case exactly once (`-benchtime=1x -count=1`). These are complete
durable workloads, so time-based calibration is unnecessary. For repeated local
samples:

```bash
make bench/workloads BENCH_WORKLOAD_COUNT=3
```

The CI benchmark job uploads a `bench-<commit SHA>-<run attempt>` artifact using
[GitHub's artifact action](https://github.com/actions/upload-artifact).
It contains `bench-results.txt` (combined benchmark output) and
`bench-environment.txt` (checked-out commit, Go version, target OS/architecture,
and kernel information). Output remains in the job log as well. Partial output
is uploaded after benchmark failure; saving output through `tee` preserves the
benchmark command's failure status.

Download artifacts from two CI runs and compare their `bench-results.txt` files
with `benchstat`. A single CI sample is useful for inspection, not a statistical
performance verdict; use repeated runs on consistent hardware for close comparisons.
The artifact uses the repository's default retention period.

## Why regressions are not gated by thresholds

Percent-level performance assertions on shared CI runners flake: the same
code has measured 2.9× on one run and 1.49× on the next under runner load.
The posture instead:

- **Ratio floors inside tests** where a ratio is the semantic claim (the
  MySQL parallel-snapshot test asserts speedup ≥ 1.25× — a regression
  *floor*, not a performance target).
- **Order-of-magnitude floors** in the throughput report (single-digit
  proposals/sec would mean the write path lost its fsync coalescing).
- **Human comparison via `benchstat`** and the CI bench logs for
  percent-level drift, where a person can judge noise against change.

## What the numbers imply operationally

- **Size proposals, not requests.** One proposal carrying N entities costs
  one round-trip and shares one fsync; N proposals cost N round-trips.
- **Disk fsync latency is the write knob.** Faster fsync (NVMe, non-shared
  disks) moves every write number; CPU rarely does.
- **Rebuilds are cheap on the log side.** At catch-up rates the log replays
  millions of entries per minute; budget rebuild time against the
  destination database's ingest rate instead.
- **Interpretation adds nothing measurable** on the restatement-free path
  (~3 ns/entity), so leaving the interpretation layer on costs nothing.
