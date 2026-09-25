# Event-log backup workload comparison

## Method

Run:

```sh
go test ./internal/cluster/db/wal -run '^$' -bench '^BenchmarkBackupWorkload$' -benchtime=1x -count=1
```

This is a synthetic, single-process storage workload using the public `wal.Open`
selection and the same `Save`/`ApplyCommittedBatch` paths for both engines. It is
not a cluster throughput benchmark or a customer-data compression estimate.

- 32,768 JSON records, about 130 MiB of protobuf input, in batches of 256.
- Default 20 MiB segment target; deterministic JSON combines unique pseudorandom
  trace text with repeated order-description text.
- 128 deletes followed by a real scrub command. Localized deletes target the first
  128 records; distributed deletes target every 256th record throughout history.
- Tidwall uses its default 16-segment cache. Segmented uses 160 MiB each for recent
  and historical caches and ZstdDefault encoding for indexed outputs, including
  background compression of untouched closed tails.
- Durability remains enabled. Background sealing is stopped and the existing
  compressor, where available, is drained explicitly before both backup captures.
  Scrubbing is driven explicitly after applying its command.
- Backup capture hashes selected **event-log files only**, including the segmented
  catalog. It excludes application metadata, Raft logs, archive framing, upload,
  concurrent writers, and concurrent syncable readers.
- Reuse requires the same relative filename, byte count, and SHA-256 in both
  captures. Changed bytes include new files, changed files, and the active tail.
  This measures potential file reuse, not an implemented hosting deduplication
  policy. Full independent archives still store the entire backup each time.
- The benchmark verifies that all requested upserts were erased.

## Local measurement with sealed-segment compression

One iteration per case on Apple M4 Max, darwin/arm64, Go benchmark GOMAXPROCS 16,
without overlapping test or lint runs. Times are observations from a single run,
not statistically established rankings. Sizes below are MiB (1,048,576 bytes).

| Deletes | Engine | Before backup | After backup | Changed bytes | Reused bytes / files | Append µs/record | Scrub ms | Compression ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Localized | tidwall | 42.93 | 42.40 | 42.40 | 0 / 0 | 147.6 | 774.0 | 397.7 |
| Localized | segmented | 42.92 | 42.79 | 15.89 | 26.91 / 5 | 130.6 | 456.6 | 833.4 |
| Distributed | tidwall | 42.93 | 42.42 | 42.42 | 0 / 0 | 143.6 | 773.7 | 399.6 |
| Distributed | segmented | 42.92 | 42.76 | 42.76 | 0 / 0 | 146.9 | 1092.0 | 866.9 |

Append time includes Raft Save and application apply, excluding fixture generation
and subsequent explicit compression. Compression time combines the explicit
draining steps before both backups, including segmented retirement reclamation.
Scrub time includes encoding the changed ranges. The active tail stays plain.

Total measured workflow time was 8.30–8.37 seconds per case for tidwall and
7.80–9.15 seconds for segmented. Allocated bytes over the entire measured workflow
were 6.43–6.50 GB for tidwall and 5.59–6.37 GB for segmented. These are cumulative
allocations, **not peak or resident RAM**.

## What this establishes

The initial measurement before background compression produced a 130.62 MiB
segmented baseline: untouched closed append-format files were still plain.
Compressing those files brings the initial event-log backup to 42.92 MiB, close
to tidwall's 42.93 MiB for the same input.

Localized erasure preserves five untouched compressed segmented files. Its
changed backup bytes remain about 62.5% lower than tidwall's. Total post-scrub
backup size is about 0.9% larger. Distributed erasure touches every closed range,
eliminating the reuse advantage; total backup size is about 0.8% larger.

The segmented explicit compression step took roughly twice as long in this run,
adding about 0.44–0.47 seconds for this workload. Localized scrub was faster and
distributed scrub was slower. These observations do not establish general
runtime rankings or the latency impact of compression running alongside writers:
the benchmark deliberately drains compression separately.

Backup-cost reduction still depends on which ranges each scrub touches and
whether the hosting backup system reuses unchanged files. Independent full
archives store the entire backup each time. This workload supplies no evidence
about 100 TB scale, long retention histories, network transfer costs, or
performance with many concurrent streaming readers.

[The concurrent reader workload](streaming-workload-results.md) separately
measures durable appends with live readers, historical readers, and background
compression.

## Archive restore coverage

`TestCompressedBackupRestoreBackends` separately checks complete node archives,
including metadata and Raft state, using public `wal.Open` backend selection.
Its smaller fixture uses 256 records and 64 KiB segments. It explicitly drains
the production compression capability and requires at least one sealed segment
to be compressed before capturing the archive.

Eight cases cover both engines, live/offline backups, and histories before/after
scrub. Each verifies exact surviving keys and payloads, absence of erased
original records, the scrub generation, and restored event/applied/Raft progress. The restored node
accepts another committed write, closes, reopens, and verifies both that write
and the original history again. It then deletes another archived row, completes
a new scrub and compression pass, and reopens again. The test checks the new
generation, all surviving archived payloads, all erased original records, and the
post-restore write. This untagged test is included in normal CI
and race jobs, without performance thresholds. The live cases keep the source
open but do not append concurrently with capture.
