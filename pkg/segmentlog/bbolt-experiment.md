# Integrated bbolt catalog experiment

`CreateLog` and `OpenLog` use a dedicated `metadata.db` through the repository's
bbolt dependency. They require exclusive ownership of the directory. There is no
format autodetection, conversion, or production database activation. The segmented
EventLog adapter uses the same catalog through `Create`/`Open`.

## Metadata boundary

The private `layout` interface separates the managed log from metadata storage:

- `head` returns log identity, revision, generation, options, and active-tail
  state, with no historical range list.
- `ranges` iterates references overlapping a requested ID interval.
- `publishRollover` consumes the existing private prepared-file handle.
- `publishRewrite` atomically publishes changed references and tail state.
- `reclaim` applies each backend's retirement policy.

`Log.InspectCatalog` exposes a detached diagnostic snapshot under the log mutex.
It materializes every range, so its time and memory grow with history size; it
does not pin files after returning. The private `Current` operation builds it.
Managed bbolt append,
read, progress, rewrite, and reclamation do not request it. Rewrite streams all
selected ranges and retains only changed references for publication; transforms
still inspect the record stream because there is no subject-selection index.

The bbolt schema has three buckets:

| Bucket | Contents |
| --- | --- |
| `state` | Versioned header, Catalog fields excluding Segments, and closed-range count |
| `ranges` | Big-endian uint64 start ID -> complete SegmentRef |
| `retired` | Obsolete filename -> filename and original start ID |

Values use canonical JSON plus CRC32C. Range keys must match their encoded
coverage start; selected references are validated before use. Empty ranges keep
their original coverage. This is an experimental format. Per-value checksums do
not protect bbolt's entire tree structure or provide repair/redundancy.

Unlike a full catalog, this format has no 65,536-range or 16 MiB total-catalog
limit. Existing record, segment encoding, and segment-size limits remain.
The bbolt database uses the hashmap freelist with normal synchronization and
persisted freelist enabled. File growth, fragmentation, and long-lived readers
still affect bbolt memory, write cost, and recovery cost.

## Publication and durability

Rollover retains the synchronized predecessor, hashes its bytes, and durably
installs the new empty tail. One bbolt transaction then inserts the closed range,
updates active-tail state and revision, and queues the predecessor if the range
was entirely erased. The log changes appenders only after the transaction
succeeds. New record writes and their sync follow. There is no per-rollover
catalog file or CURRENT replacement in this backend.

Rewrite prepares replacement files with the existing segment and tail code.
Publication verifies/syncs changed files and the active tail, confirms directory
durability, then uses one transaction to change all selected references,
revision/generation, the tail checkpoint, and retirement records. No-op ranges
keep their existing references and bytes.

Any error from the metadata commit boundary poisons the managed handle. A commit
error can mean an uncertain outcome; files are retained and recovery determines
the selected state. There is no rollback by deleting files. Initial metadata
creation syncs the directory after the database has been initialized.

## Recovery, reads, and verification

Bbolt open loads the persisted header and checks the first and last range
boundaries. Managed recovery then validates/syncs the active tail and restores
original append accounting. It does not traverse every range or verify historical
payloads during open. Missing metadata is an error and is never recreated on open.
Missing/corrupt metadata boundary entries or an invalid active tail fail open.

Range lookups seek in the B+tree, and iteration checks coverage continuity as it
advances. Selected files must be regular files. Normal reads retain segment/group
checksum checks; they do not recompute whole-file SHA-256. Missing files and
corruption in unvisited historical ranges are detected when accessed, not
necessarily at open.

`Log.Verify(ctx)` first validates the complete range index, its coverage and
count, generated filename/start relationships, and every retirement record.
Retirements must match their keys and filename starts and cannot select a live
file. Already-removed retired files are valid pending queue acknowledgement.
Verification then streams every selected reference and checks payload
integrity and whole-file digests, then checks the active tail. These are logical
metadata and payload checks, not a structural audit of bbolt's underlying pages.
It currently uses
the existing verifier, including its file syncs. Cancellation is checked between
files. Verification and managed operations hold the Log mutex. This operation is
full-history work and is not part of the fast open path.

## Reclamation

Reclaim drains the committed retirement queue in batches of up to 128 entries.
Before removal, entries are checked against current active/range selection.
Regular files are removed and directory-synced before their queue entries are
removed in a metadata transaction. If queue acknowledgement fails, reopen/retry
repeats removal safely, including directory synchronization for an absent name.
The Log mutex excludes readers and mutations throughout the operation.

This backend does not list the directory, scan all live payloads, or collect a
full live-name map to reclaim committed retirements.

`Log.ReclaimOrphans(ctx)` provides separate, explicit full-directory maintenance.
Before any deletion, it streams and validates every range entry, coverage
continuity, the range count, and the generated filename/start relationship. It
then reads directory entries in batches of 128 and checks each managed data
filename against the live range index and active-tail name. Unselected regular
data files and reserved installation temporary files are durably removed;
unknown names (including complete-catalog manifests), directories, and symlinks
are preserved. It does not read live payloads or collect all live names in memory.
Metadata validation errors stop cleanup before any deletion. `Log.Verify` remains
the separate payload integrity check.

The sweep holds the log mutex for the entire operation. Its work grows with the
number of ranges and directory entries; it is never automatic during open,
append, or routine retirement. Cancellation leaves durable partial progress.
Other failures poison the handle; reopen and retry. Successful completion syncs
the directory even if a previous interrupted removal left no remaining filename.
The sweep can also remove queued obsolete files, but leaves their retirement
records intact for the idempotent `Reclaim` acknowledgement path.

A bbolt read transaction protects metadata pages, not external segment files;
managed serialization currently supplies file lifetime protection.

## Validation

The integrated tests cover:

- Common EventLog conformance, sparse IDs, both encoding policies, original
  append progress, no-op transformations, and close/reopen.
- Closed-range and active-tail erasure, byte identity of untouched ranges,
  exact/range reads, and retirement after rewrite.
- Errors before a metadata transaction, after its mutations but before commit,
  and after a successful commit is reported as an error.
- Atomic replacement of multiple closed ranges and the active tail, preserving
  either the original or replacement selection and retirement queue on reopen.
- Retrying reclamation when unlink succeeds and queue acknowledgement fails.
- Subprocess termination without deferred cleanup before metadata transactions,
  after their in-memory mutations, and after successful commits during rollover,
  multi-range/tail rewrite, and retirement acknowledgement. A separate boundary
  terminates after the first durable retirement removal. Recovery checks the
  selected layout, survivor IDs, append progress, idempotent reclamation, full
  verification, and subsequent append/reopen.
- Metadata checksum damage, missing historical files, invalid active-tail start,
  symlink references, and refusing to initialize missing metadata on open.
- Explicit verification rejects incorrect range counts, extra range entries,
  malformed retirement records, and queued live files. Retirement validation is
  shared with reclamation, including rejecting a live filename paired with an
  incorrect start. Verification checks beyond a single retirement batch and
  accepts already-removed files awaiting acknowledgement.
- A controlled scan/rewrite overlap and shared backend race tests.
- Orphan cleanup after subprocess-interrupted publication, multiple directory
  batches, preserved unknown/nonregular entries, corrupt metadata refusal, and
  retries after cancellation, removal errors, and subprocess exit during a sweep.
- A 100,000-range erased-history fixture, beyond the flat catalog limit. A test
  wrapper rejects any request for a complete catalog snapshot while exercising
  managed operations. Full verification succeeds after reopen.

Fault injection and subprocess exits operate at transaction and filesystem
boundaries. Subprocess tests leave the operating system running; they do not
simulate arbitrary torn bbolt writes, hardware power loss, or every filesystem
failure. Full scans, large replacement sets, cold-cache behavior, metadata repair,
backup capture, physical file-count scaling, and long-running fragmented workloads
are not established by these tests.

## Benchmarks

The comparison measurements below were captured before consolidation, when both
managed catalog implementations were available. The complete-catalog benchmark
variant is no longer present in the managed engine.

`BenchmarkEventLogScale` includes `segmented/plain` and
`segmented/zstd`, both using bbolt. It creates actual segment files and measures managed
reopen, boundary append, rewrite, reclaim, and scan. Each iteration checks all
survivor IDs/payload lengths and verifies erasure and progress after another open.
The tidwall row still does not force tidwall rollover and is not a boundary-to-
boundary comparison.

`BenchmarkBoltCatalogScale` measures 20 actual managed rollovers after histories
with 50,000, 500,000, or 5,000,000 entirely erased ranges. Fixtures have metadata
for those original ranges, not millions of physical files or TBs of payloads.
Fixture building is outside the timer. After measurement it reopens and verifies
every newly appended record and the original frontier. Metadata page allocation
is a bbolt statistic, not physical disk-write volume.

```sh
go test ./internal/cluster/db/eventlog -run '^$' -bench '^BenchmarkEventLogScale/segmented' -benchtime=1x -count=1 -timeout=10m
go test ./pkg/segmentlog -run '^$' -bench '^BenchmarkBoltCatalogScale$' -benchtime=1x -count=1 -timeout=10m
```

## Recorded local measurements

macOS arm64, Apple M4 Max, Go 1.26.6, bbolt 1.5.0, 16 KiB bbolt pages.
The lifecycle benchmark ran each case three times with one iteration per run.
Other test/lint jobs had completed. Filesystem caches were warm; these small
samples do not establish latency tails, Linux behavior, or production throughput.
All 24 lifecycle runs passed their correctness and post-reopen checks.

Median times below are milliseconds:

| Catalog | Encoding | Batches | Boundary append | Reopen | Rewrite | Reclaim | Scan |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| bbolt | plain | 8 | 22.99 | 4.65 | 53.06 | 13.11 | 5.69 |
| bbolt | plain | 64 | 24.26 | 4.31 | 109.90 | 15.17 | 38.62 |
| complete | plain | 8 | 33.71 | 46.26 | 89.00 | 62.21 | 5.03 |
| complete | plain | 64 | 33.92 | 283.50 | 384.20 | 317.10 | 38.42 |
| bbolt | zstd | 8 | 23.06 | 4.36 | 38.02 | 13.17 | 4.81 |
| bbolt | zstd | 64 | 24.09 | 4.86 | 83.52 | 12.51 | 40.63 |
| complete | zstd | 8 | 33.26 | 39.63 | 94.20 | 60.01 | 4.73 |
| complete | zstd | 64 | 36.99 | 298.40 | 404.00 | 301.10 | 38.47 |

These boundary appends perform the same segment rollover and new-record sync.
They compare the two metadata publication mechanisms. Their latency is still
tens of milliseconds; this is not a solved low-latency append path.

Reopen and reclamation perform different work across the implementations. Bbolt
does not verify historical payloads on open, and it reclaims committed retirements
without full live-history verification or directory enumeration. Rewrite also
skips the complete-catalog backend's separate full-history preflight; its
transform still reads/validates the selected record stream. Those semantics are
part of the experiment, not evidence of equivalent full verification becoming
cheaper. Scanning remains sequential within closed append files. The plain/zstd
labels affect indexed rewrite outputs; ordinary rollover does not compress files.

Large erased-history fixtures produced the following single-run results. Each
append number is the mean of 20 actual boundary appends, with real new-tail
installation and record synchronization:

| Erased historical ranges | Mean boundary append (ms) | Metadata page allocation per append (bytes) | Metadata file bytes | Warm managed reopen (ms) |
| ---: | ---: | ---: | ---: | ---: |
| 50,000 | 25.85 | 82,739 | 16,777,216 | 4.03 |
| 500,000 | 25.85 | 82,739 | 177,225,728 | 5.24 |
| 5,000,000 | 23.86 | 81,920 | 1,707,261,952 | 4.97 |

All new records and original progress were verified after reopen. The similar
append cost across these fixtures supports bounded metadata work for rollover;
it does not establish every cost as constant under fragmentation, long readers,
or tree growth. Empty references occupy less space than references to actual
files. These fixtures measure metadata cardinality, not a 100 TB payload set or
millions of directory entries, and their new records have empty payloads.
