# On-disk lifecycle scale baseline

Local run: macOS arm64, Apple M4 Max, Go 1.26.6. One iteration per case, run
sequentially without concurrent test processes. These are warm-filesystem,
synthetic measurements with no latency distribution or cold-cache eviction.

## Workload and checks

`BenchmarkEventLogScale` uses 1 MiB segment targets and batches of 255 records.
Each record contains 4 KiB of deterministic varying text; IDs are spaced by ten.
Eight batches contain 2,040 records (7.96875 MiB of payload); 64 batches contain
16,320 records (63.75 MiB). Both backends receive the same records and batches.
Encoding labels select their existing plain or zstd configuration; active data
and incompressible blocks need not be compressed.

Each iteration builds a new history, reclaims setup artifacts, and closes it.
Timed phases reopen it, append one record, erase one middle record, reclaim
obsolete files, and scan every survivor. The append forces segmented rotation:
255 original frames consume 1,048,560 bytes, just below the 1 MiB target.
Tidwall framing differs, so the same input does not imply the same rotation work.
The comparison uses the experimental tidwall generation container, not production
Storage's append path.

The scan checks every surviving ID and payload length. An additional untimed
reopen checks that the erased ID remains absent, the appended payload remains
byte-identical, and original append progress is preserved. The benchmark fails
if those checks fail.

## Measured phase times

All times below are milliseconds. Each row is a single sample.

| Backend | Encoding | Batches | Build | Reopen | Append | Scrub | Reclaim | Scan |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| segmented | plain | 8 | 421.8 | 10.34 | 55.77 | 52.64 | 31.07 | 3.72 |
| segmented | plain | 64 | 5,444 | 58.13 | 96.77 | 149.6 | 66.34 | 20.08 |
| tidwall | plain | 8 | 143.4 | 7.41 | 5.19 | 70.44 | 49.43 | 5.66 |
| tidwall | plain | 64 | 1,295 | 28.77 | 4.37 | 499.1 | 687.9 | 39.90 |
| segmented | zstd | 8 | 412.2 | 10.56 | 58.51 | 71.12 | 34.23 | 4.30 |
| segmented | zstd | 64 | 5,099 | 47.20 | 87.00 | 204.2 | 86.21 | 20.76 |
| tidwall | zstd | 8 | 129.7 | 9.34 | 5.05 | 133.5 | 41.02 | 4.07 |
| tidwall | zstd | 64 | 1,123 | 31.00 | 5.22 | 1,042 | 311.6 | 26.62 |

Build includes payload generation and initial appends. It is reported separately
and excluded from Go's timed lifecycle and allocation metrics. Initial cleanup,
closing, and final recovery checks are also excluded. Phase timers include each
operation's local durability work; the scan includes its correctness assertions.

At 64 batches, reclamation removed about 2.13 MB for segmented storage versus
67.08 MB for tidwall. This includes obsolete artifacts from the measured append
and scrub together, including catalog files. It is not a measurement of backup
uploads, live storage footprint, or scrub-only output bytes.

Cumulative allocations across the timed lifecycle were about 628 MB (segmented
plain), 640 MB (segmented zstd), 1,585 MB (tidwall plain), and 2,273 MB (tidwall
zstd) at 64 batches. These are bytes allocated over several operations, not peak
resident memory or simultaneous retained data.

## Interpretation and limits

The selective rewrite reduced scrub time and obsolete-file volume in this run.
The segmented boundary append was substantially slower than tidwall's append.
Segmented catalog publication verifies the complete referenced history, so
rotation work grows with history even though it adds one segment. Recovery and
reclamation also verify history. This experiment does not isolate verification,
codec, allocation, or sync costs from one another.

The larger histories are still only tens of MiB and at most dozens of segmented
ranges. These results do not establish TB/PB behavior, catalog-limit behavior,
peak memory, sustained application throughput, production compression ratios,
or recovery after a crash. The [concurrency tests](../../../../pkg/segmentlog/concurrency-testing.md)
exercise scheduling separately.

```sh
go test ./internal/cluster/db/eventlog -run '^$' -bench '^BenchmarkEventLogScale$' -benchtime=1x -count=1 -timeout=10m
```

Use a fixed iteration count: every iteration creates and validates an independent
on-disk history, so wall time exceeds the reported timed lifecycle.
