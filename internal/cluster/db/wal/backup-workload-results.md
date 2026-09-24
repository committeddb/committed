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
  and historical caches and ZstdDefault encoding for rewrite outputs.
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

## Initial local measurement

One iteration per case on Apple M4 Max, darwin/arm64, Go benchmark GOMAXPROCS 16.
Times are observations from a single run, not statistically established rankings.
Sizes below are MiB (1,048,576 bytes).

| Deletes | Engine | Before backup | After backup | Changed bytes | Reused bytes / files | Append µs/record | Scrub ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Localized | tidwall | 42.93 | 42.40 | 42.40 | 0 / 0 | 151.9 | 788.9 |
| Localized | segmented | 130.62 | 115.88 | 15.89 | 99.99 / 5 | 156.4 | 353.7 |
| Distributed | tidwall | 42.93 | 42.42 | 42.42 | 0 / 0 | 148.0 | 809.7 |
| Distributed | segmented | 130.62 | 42.76 | 42.76 | 0 / 0 | 151.0 | 977.7 |

Append time includes Raft Save and application apply, excluding fixture generation
and subsequent explicit compression. Tidwall's combined explicit compression
steps took 398–409 ms per case. Segmented exposes no background sealed-file
compressor; its rewrite encoding time is included in scrub time.

Total measured workflow time was 8.41–8.51 seconds per case. Allocated bytes over
the entire measured workflow were 6.43–6.50 GB for tidwall and 4.61–5.38 GB for
segmented. These are cumulative allocations, **not peak or resident RAM**.

## What this establishes

Localized erasure preserves untouched segmented files. In this run, its changed
backup bytes were about 62.5% lower than tidwall's. Distributed erasure touched
every closed range and eliminated that reuse advantage.

Compression is a separate issue: segmented rollover retains closed append-format
files without compressing them. Only rewritten ranges become compressed indexed
segments. Consequently its initial backup was about 3.04 times tidwall's, and its
localized post-scrub backup remained about 2.73 times larger. Distributed scrub
rewrote all ranges, bringing total backup size close to tidwall's.

The workload therefore demonstrates stable-file reuse, but does **not** establish
a general backup-cost reduction. That depends on compression, which ranges each
scrub touches, and whether the hosting backup system reuses unchanged files.
It supplies no evidence about 100 TB scale, long retention histories, network
transfer costs, or performance with many concurrent streaming readers.
