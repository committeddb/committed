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
| Ingest streaming rate | ≥ 5,000 source transactions/sec per ingestable (PostgreSQL, MySQL) | Change capture after the snapshot, in source commit order |
| Source commit to read-model row | ≤ 1 s at p99 | An event committed at the source until a projection's row shows it |
| Projection rate, one aggregate stage into PostgreSQL | ≥ 7,500 events/sec | How fast one projection consumes the log; past this rate its read model falls behind |

Two shapes matter more than any single number:

- **Latency and throughput diverge by well over an order of magnitude.** A
  serial caller pays the whole write path per proposal, several fsyncs (~30
  ms on the reference machine → ~30/sec). Concurrent callers share fsyncs
  through raft's Ready batching (~300 µs/op at 8-way → ~3,400/sec). If a load
  generator sees double-digit throughput, it is measuring its own serial
  round-trips, not the cluster's capacity — add concurrency or batch entities
  into fewer proposals.
- **An ingestable keeps many source transactions in flight.** Each source
  transaction becomes one proposal, and the ingestable sends the next
  without waiting for the last to be durable; the log still applies them in
  source commit order. So its rate is set by raft's batched fsyncs, not by
  one write round-trip per transaction. A source that sustains more
  transactions per second than the envelope falls behind; watch `lag` on
  `GET /v1/ingestable/{id}/status`. Separate ingestables stream in parallel.
- **Reading the log is not the bottleneck.** Catch-up reads the log ~500×
  faster than the write path fills it, so a syncable's rate is set by what
  it does with each entry: the destination database's writes for a mirror,
  and the stage's own work as well for a projection (see the projection rate
  above). A rebuilt projection converges at that rate, not at the read rate.

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
both. Measured on the same laptop: the `BenchmarkAggregateLargeGroup`
rows with `make bench` (2026-10-14), the rest against a running single node
with default settings (2026-10-13):

| Measurement | Result | Derived rate |
|---|---|---|
| Serial `POST /v1/proposal`, one entity each | median 30 ms | ~31 proposals/sec |
| `BenchmarkAggregateLargeGroup`, five emits, append into a 1,000-input group | 0.13 ms per input | ~7,500 inputs/sec into that group |
| `BenchmarkAggregateLargeGroup`, five emits, append into a 10,000-input group | 0.06 ms per input | ~16,500 inputs/sec into that group |
| Source commit to read-model row, aggregate stage, 40-event group | median 69 ms | — |
| Source commit to read-model row, aggregate stage, 10,000-event group | median 108 ms | — |

The commit-to-row figures run the
[event-store recipe](../recipes/event-store-read-models.md) against
PostgreSQL 16, appending one event at a time to one stream and timing
until its row updates.

The [event-store recipe](../recipes/event-store-read-models.md) with
PostgreSQL 16 as source and destination, one event per source transaction,
on the same laptop (2026-10-14). A backlog drain commits the events to
PostgreSQL with every node stopped, then starts the nodes and times the
ingestable to `lag` 0 and the read model to its last row:

| Workload | Nodes | Ingest drained | Read model complete |
|---|---|---|---|
| 10,000 events, one per stream | 3 | 3.6 s (~2,800 transactions/sec) | 4.0 s |
| 10,000 events into one stream | 3 | 3.2 s (~3,100 transactions/sec) | 3.9 s |
| 50,000 events, one per stream | 3 | 10.5 s (~4,800 transactions/sec) | 15.2 s |
| 10,000 events, one per stream | 1 | 1.6 s (~6,100 transactions/sec) | 2.7 s |
| 10,000 events into one stream | 1 | 1.4 s (~6,900 transactions/sec) | 2.8 s |

The live load runs `eventstore-load` (see [Re-measuring](#re-measuring))
on three local nodes, with eight writers appending to PostgreSQL:

| Offered load | Source commit to read-model row | Catch-up after the last append | Second read model from the log |
|---|---|---|---|
| 50 events/sec for 2 min | p50 355 ms, p99 1.03 s | 0.2 s | 2.1 s (~2,800 events/sec) |
| 2,000 events/sec for 1 min (the writers reached 436/sec) | p50 378 ms, p99 1.05 s | 0.3 s | 5.2 s (~5,100 events/sec) |

Latency holds as the rate rises, and the read model catches up within a
second of the last append: the cluster keeps up, and the latency is the
path's depth. A single append into a quiet cluster reaches its row in about
70 ms. Under load each entry waits on fsyncs that queue behind the other two
nodes' and PostgreSQL's on the one shared disk; on one node the same stretch,
from proposal to the syncable reading the entry, takes a third as long.
Separate disks per node shorten it.

The 3-node HTTP number is the most conservative of all: real processes, real
HTTP, follower→leader forwarding, quorum replication, and three nodes
contending for one laptop disk at only 8-way concurrency. Treat it as a
floor, not a capacity estimate.

### Three Linux nodes

Measured 2026-10-15 on three AWS i8ge.large machines (2 vCPUs, local NVMe),
one per availability zone, with the source and read-model databases in
Docker on a fourth machine in the first node's zone. Same recipe, one event
per source transaction, 1,000 events/sec for 10 minutes from 32 writers,
with four streams growing to 10,000 events:

| Source | Commit to row p50 | p99 | Idle append to row | Catch-up after the last append | Second read model from the log |
|---|---|---|---|---|---|
| PostgreSQL 16 | 71 ms | 176 ms | 6 ms | 0.4 s | 2,211 events/sec |
| MySQL 9 | 62 ms | 187 ms | 6 ms | 0.3 s | 2,078 events/sec |
| SQL Server 2022 | 42 ms | 240 ms | 99 ms | 0.3 s | 2,866 events/sec |

Each read model matched the source exactly. SQL Server's idle latency is its
change-tracking poll (`pollInterval`, 250 ms by default).

At 10,000 events/sec — measured 2026-10-16 on the same three nodes, with the
databases on an 8-vCPU machine, one event per source transaction from 128
writers for 3 minutes (1.8 million events into ~278,000 streams, four of them
growing to 10,000 events):

| Source | Commit to row p50 | p99 | Catch-up after the last append | Second read model from the log |
|---|---|---|---|---|
| PostgreSQL 16 | 120 ms | 263 ms | 1.2 s | 14,423 events/sec |
| MySQL 9 | 138 ms | 375 ms | 1.2 s | 14,547 events/sec |

Both kept pace for the whole run: the p99 in each minute stayed flat, the
ingestable's lag stayed near zero, and the read model matched the source
exactly. The leader, which runs the ingestable and the projection, used
about 60% of its two vCPUs.

SQL Server is not in this table: its change-tracking source accepted only
about 7,000 inserts/sec at this load, even with twice the CPU, so the run
could not offer committed 10,000/sec. At the rate it did accept, committed
kept up (catch-up 1.2 s) with a p99 of 2.0 s, most of it waiting on the
change-tracking queries.

Building a read model over history — measured 2026-10-29 on the same three
nodes: the event-store recipe's projection built from an existing log of
100 million events (15.7 million streams) in 65 minutes, 25,800 events/sec
on average, and every one of its 15,742,438 rows matched the source. The
build runs on one worker, so its rate is one node's. The projection's stage
state took about 42 bytes per event, against 36 for the log itself; 0.8.2
needed about 610 bytes per event for the same projection, and slowed to
15,500 events/sec by 81 million events.

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

The event-store workload needs Docker for its PostgreSQL databases. Build
its bundle for this machine, then run it against three local nodes:

```bash
make bench/eventstore-bundle BUNDLE_ARGS="--local -o $PWD/bench-bundle"
bench-bundle/run.sh -- -rate 50 -duration 2m -writers 8 -hot-streams 2 -hot-events 1500
```

It prints the commit-to-row percentiles, catch-up, and second-read-model
rate, and writes the results to `bench-bundle/results`. `run.sh` without
`BENCH_NODES` runs the nodes locally; set it to run them on other machines
over SSH.

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
