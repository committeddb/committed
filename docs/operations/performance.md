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

Two shapes matter more than any single number:

- **Latency and throughput diverge by ~50×.** A serial caller pays a full
  fsync per proposal (~14 ms on the reference machine → ~70/sec). Concurrent
  callers share fsyncs through raft's Ready batching (~300 µs/op at 8-way →
  ~3,400/sec). If a load generator sees double-digit throughput, it is
  measuring its own serial round-trips, not the cluster's capacity — add
  concurrency or batch entities into fewer proposals.
- **Reads are not the bottleneck.** Catch-up drains the log ~500× faster
  than the write path fills it, so a rebuilt sink converges quickly; sync
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

The 3-node HTTP number is the most conservative of all: real processes, real
HTTP, follower→leader forwarding, quorum replication, and three nodes
contending for one laptop disk at only 8-way concurrency. Treat it as a
floor, not a capacity estimate.

## Re-measuring

Prerequisites: Go 1.26+ and `make` — nothing else. Unlike the Docker-backed
e2e suites, none of this needs Docker or an external database; the 3-node
report builds the real binary and runs it on loopback ports.

```bash
make bench             # the micro-benchmarks above (real fsyncs, ~30s)
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

CI runs `make bench` and the 3-node report on every push and pull request,
so each run's log holds that commit's numbers — release-to-release
comparison is reading two logs side by side.

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
