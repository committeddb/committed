# Event-log rewrite churn experiments

Local exploratory run: macOS arm64, Go 1.26.6. This is a deterministic filesystem
and content-equivalence experiment, not a throughput benchmark or a production
backup-cost estimate. Production Storage remains unchanged.

The measurements below were recorded before rollover retained append files.
That implementation converted and compressed closed tails during rollover;
the current implementation retains their original uncompressed bytes. The
recorded sizes do not describe the current initial storage footprint.

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

## Repeated scrub sequence

`TestSegmentRepeatedRewriteChurn` follows one history through six rewrite
generations, reclaiming and reopening the segmented backend after each rewrite.
The fixture starts with 64 sparse-indexed records and an 8 KiB segment target.
Each subject has 1 KiB of deterministic varying text; the first record also has
an audit entity. Precomputed subject selections are supplied directly to the
existing scrub filter. This tests storage and filter behavior, not authorization
or the production scrub coordinator.

The sequence performs a no-op, removes the first subject while retaining its
audit entity, erases all seven records in the second sealed range, erases the
single active-tail record, repeats the selections, and finally appends 16 records
before repeating the selections again. Both backends receive the same appends
and cumulative selections. The reference remains the legacy tidwall copy
primitive, not the experimental tidwall generation container.

Measured completed replacement payload bytes from the local run:

| Step | Changed records | Legacy plain | Segmented plain | Legacy zstd | Segmented zstd |
| --- | ---: | ---: | ---: | ---: | ---: |
| No-op | 0 | 69,138 | 0 | 45,552 | 0 |
| Partial record | 1 | 68,078 | 6,678 | 47,475 | 4,476 |
| Whole sealed range | 7 | 60,523 | 0 | 39,885 | 0 |
| Active tail | 1 | 59,443 | 32 | 41,757 | 32 |
| Repeat | 0 | 59,443 | 0 | 41,757 | 0 |
| Append and repeat | 0 | 76,723 | 0 | 53,123 | 0 |
| **Rewrite-only total** | | **393,348** | **6,710** | **269,549** | **4,508** |

Append bytes are excluded: inventories are captured after any appends and before
each rewrite. Metadata is also separate; the current segmented catalog/CURRENT
footprint was about 2.4–2.9 KiB. A zero in the table means no newly created payload
bytes, not that a rewrite performed no reads, metadata writes, or file removals.
The 32-byte replacement is an empty active-tail header; its catalog checkpoint
retains the erased append progress.

The test verifies unchanged sealed descriptors and retained file bytes, removal
of obsolete erased-range/tail files after reclamation, and byte-equivalent
survivors against the legacy rewrite. It checks original append progress after
reopen, including after tail erasure and subsequent appends. Repeating selections
produces no new segmented payload files. Legacy no-op rewrites reproduce existing
hashes despite creating replacement files, so replacement-byte totals are not
unique backup-byte totals. Per-step new-hash measurements are included in test
output.

## Evidence and limits

This fixture confirms byte-equivalent scrub output and demonstrates that isolated
changes preserve unrelated files. It also shows why reduced local rewriting and
reduced content-addressed backup churn must be measured separately. These results
do not imply the existing backup implementation automatically reuses those files.
Complete independent backups still store complete snapshots; hosted reuse remains
outside the storage engine's responsibility.

The fixture uses small synthetic segments and does not measure CPU, memory,
latency, or complete backup/restore and peer-transfer behavior. One local run
cannot predict TB-scale costs or justify a production compression policy.

Reproduce the inventory and correctness checks:

```sh
go test ./internal/cluster/db/wal -run '^TestSegment(RewriteChurnExperiment|RepeatedRewriteChurn)$' -v
go test -race ./internal/cluster/db/wal -run '^TestSegment'
```
