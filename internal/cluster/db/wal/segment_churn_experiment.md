# Initial event-log rewrite churn experiment

Local exploratory run: macOS arm64, Go 1.26.6. This is a deterministic filesystem
and content-equivalence experiment, not a throughput benchmark or a production
backup-cost estimate. Production Storage remains unchanged.

## Workload and comparison

The fixture has 512 sparse-indexed Committed proposals. Each contains one subject
entity with 1 KiB of deterministic varying text inside a JSON-shaped value, plus
one small audit entity that must survive. The three workloads are:

- **No-op:** no tombstones or selected subjects.
- **Isolated:** a tombstone selects the first subject; its audit entity survives.
- **Scattered:** tombstones select every sixteenth subject (32 subjects), touching
  every sealed segment in this fixture.

Both backends use a 32 KiB segment target. Their framing and rotation policies
still differ, producing 18 initial tidwall files versus 19 segmentlog files.
Each workload starts from a fresh source containing its tombstones. This is one
scrub per source, not a sequence of daily backups.

The legacy side uses the repository's tidwall fork and reproduces the existing
scrub's copy operation: verify source frames, apply `scrubFilterEntry`, write every
survivor into a new dense log with NoSync, then Sync and drain sealed compression.
It does not invoke Storage's live scrub coordinator, concurrent catch-up, directory
swap, or BoltDB reconciliation. The segmentlog side runs the same filter through
the experimental adapter, publishes a whole-log rewrite, then reclaims obsolete
files. Tests compare every surviving protobuf byte and Raft index.

Plain and zstd cases are separate. Zstd uses tidwall's sealed compression and
segmentlog's ZstdDefault independently; these are different encodings, not a
controlled comparison of encoder levels. Active tails remain uncompressed.

## Results

Bytes below describe completed payload files; catalog metadata is separate.

| Encoding | Workload | Tidwall new file bytes | Segmentlog new file bytes | Tidwall new-hash bytes | Segmentlog new-hash bytes |
| --- | --- | ---: | ---: | ---: | ---: |
| Plain | No-op | 595,956 | 0 | 0 | 0 |
| Plain | Isolated | 594,903 | 31,759 | 594,903 | 31,759 |
| Plain | Scattered | 562,260 | 556,452 | 562,260 | 556,452 |
| Zstd | No-op | 369,140 | 0 | 0 | 0 |
| Zstd | Isolated | 368,432 | 19,118 | 368,432 | 19,118 |
| Zstd | Scattered | 348,998 | 337,353 | 348,998 | 337,353 |

- **New file bytes** count the sizes of newly created completed payload files.
  The tidwall replacement directory contains all-new files. Segmentlog counts
  only replacements; unchanged names and hashes are retained. This is not total
  physical write I/O: temporary uncompressed files, syncs, and compression passes
  are excluded.
- **New-hash bytes** count completed files whose SHA-256 is absent from the source
  inventory, independently of filenames. A no-op tidwall rewrite creates files
  but reproduces the same hashes in this fixture. A content-addressed backup can
  potentially reuse them.
- Segmentlog retained all 19 payload files for a no-op, 18 for the isolated edit,
  and just the active tail for scattered edits. Current catalog plus CURRENT
  occupied about 4.8 KiB, including for the no-op generation update. This is the
  final metadata footprint, not measured metadata write traffic.
- For the isolated zstd edit, new payload bytes fell by about **94.8%**. For the
  scattered zstd workload, the reduction was only **3.3%**. Segment-local rewriting
  cannot avoid replacing a segment whose contents actually change.

Final zstd payload footprints were similar: isolated tidwall 368,432 bytes versus
segmentlog 367,937 bytes; scattered 348,998 versus 348,585. The main isolated-edit
benefit here is stable historical files, not a smaller complete snapshot.

## What this establishes—and what remains

This fixture confirms byte-equivalent scrub output and demonstrates that isolated
changes preserve unrelated files. It also shows why reduced local rewriting and
reduced content-addressed backup churn must be measured separately. These results
do not imply the existing backup implementation automatically reuses those files.
Complete independent backups still store complete snapshots; hosted reuse remains
outside the storage engine's responsibility.

Remaining experiments need production-shaped payload distributions, larger ranges
and histories, whole-record and whole-range deletion, metadata-heavy churn, active-
tail edits, repeated generations, CPU/memory/latency measurements, and complete
backup/restore and peer-transfer integration. Small synthetic segment targets and
one local run cannot predict TB-scale costs or justify a production compression
policy.

Reproduce the inventory and correctness checks:

```sh
go test ./internal/cluster/db/wal -run '^TestSegmentRewriteChurnExperiment$' -v
go test -race ./internal/cluster/db/wal -run '^TestSegment'
```
