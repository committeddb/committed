# Production and segmented Actual-reader comparison

`BenchmarkActualReaderEngines` consumes both engines through the existing
`db.ActualReader` interface. One workload constructs the same protobuf history,
creates a reader after a specified Raft index, and checks every returned Actual's
index and entity count. Only backend setup and fixture append differ.

## Implementations

- **production-tidwall:** the repository's tidwall fork, production checksum
  framing, and the real `wal.Reader`, including its persistent sequence cursor,
  generation checks, applied watermark, type resolution, and protobuf decoding.
  Its cache uses `DefaultEventCacheSegments` (16), with a 20 MiB segment target.
- **segmented:** `eventLogAdapter` over the segmented EventLog, using its Actual
  reader, applied watermark, and the same type resolver. Cache budgets are
  160 MiB recent plus 160 MiB historical, with a 20 MiB segment target. Each
  Actual read currently performs a new `Seek` rather than retaining a physical
  cursor across reads.

Both implement the small test-only `readerBenchmarkStore` fixture interface for
append, reader construction, and close. Timed consumption uses `db.ActualReader`.
The benchmark does not use the experimental tidwall EventLog wrapper and its
per-record binary search. It does not change production storage activation.

## Scope

The fixture contains 16,384 records, sparse Raft indexes increasing by ten, one
entity per proposal, and 4,096 bytes of zero-filled entity data (64 MiB of entity
data plus protobuf and storage framing). Records are appended in batches of 64.
Each backend uses the same explicit-version type cache. A minimal `Storage`
provides that resolver and, for tidwall, the production reader's event-log state.
Raft, BoltDB application updates, and background workers are excluded.

Each case warms up by consuming its window once. Each measured iteration creates
a fresh reader and reads either the complete history or 128 Actuals from an older
window or near the head. Reader construction and initial cursor resolution are
included. Fixture construction and cleanup are excluded. Results are validated;
allocated bytes measure cumulative allocation, not resident memory.

The nominal cache capacities are similar, but not identical memory limits:
tidwall counts segments; segmented storage charges decoded array capacities and
indexes. Active-tail memory is additional. This history fits comfortably in both
caches. Production zstd sealing is configured on tidwall, but no background sealer
runs; both fixtures contain plain append files. This benchmark does not measure
compression, cold loads, eviction pressure, scrub, concurrent readers/appends,
append durability cost, or full database throughput.

## Reproduction

Apple M4 Max, darwin/arm64, Go 1.26.6, GOMAXPROCS=16, 2026-09-19. Three one-second
samples per case, with no concurrent agent-started build or lint job.

```sh
GOCACHE="$PWD/.claude-scratch/go-build" go test ./internal/cluster/db/wal \
  -run '^$' -bench '^BenchmarkActualReaderEngines$' -benchtime=1s -count=3
```

## Results

Medians of three samples; each operation consumes the entire named window.

| Backend | Window | Time/op | Allocated bytes/op | Allocations/op |
| --- | --- | ---: | ---: | ---: |
| production-tidwall | catch-up | 23.899 ms | 237,240,436 | 294,913 |
| production-tidwall | historical-window | 0.198 ms | 1,991,376 | 2,389 |
| production-tidwall | near-head-window | 0.190 ms | 1,991,376 | 2,389 |
| segmented | catch-up | 197.876 ms | 439,512,026 | 1,440,826 |
| segmented | historical-window | 1.602 ms | 3,479,848 | 11,650 |
| segmented | near-head-window | 0.681 ms | 2,863,367 | 6,402 |

The production tidwall reader is faster in every measured window. Its sequential
cursor avoids repeating the initial physical-position lookup for every Actual.
The segmented reader still enters managed seek and catalog lookup per Actual.
These are known differences in the measured paths; the benchmark does not isolate
how much time each accounts for. Both paths include payload copying and decoding.
The results establish a shared application-level baseline rather than storage
format superiority or a production-readiness claim.
