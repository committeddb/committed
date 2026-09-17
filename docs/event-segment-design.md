# Experimental event storage: current implementation

The repository contains an experimental segmented event log and a shared storage
interface with two implementations. Production `wal.Storage` still uses the
legacy tidwall layout. Tests create the experimental backends explicitly in
isolated directories; there is no production backend setting or automatic format
activation.

## Packages and responsibilities

| Layer | Implementation and responsibility |
| --- | --- |
| Shared contract | `internal/cluster/db/eventlog/eventlog.go`: `EventLog`, opaque records, sparse IDs, common errors and operation results |
| Segmented backend | `internal/cluster/db/eventlog/segmented`: delegates to `pkg/segmentlog` |
| Tidwall backend | `internal/cluster/db/eventlog/tidwall`: experimental generation container over the tidwall fork |
| Application adapter | `internal/cluster/db/wal/eventlog_*.go`: protobuf identity checks, Actual readers, applied visibility, committed replay, metadata selection and protected readers |
| Generic engine | `pkg/segmentlog`: immutable segments, active tail, catalogs, synchronous managed operations and reclamation |
| Filesystem primitives | `internal/durablefs`: directory ownership, durable immutable installation and atomic pointer replacement |

The generic engine stores sparse `uint64` IDs and opaque bytes. It has no
application, protobuf, Raft, or BoltDB dependencies. The application adapter uses
the embedded Raft index as the record ID and validates the outer/inner identity.
It preserves original protobuf bytes, including unknown fields.

The experimental tidwall backend stores dense physical sequences internally and
translates stable IDs through its own record envelope. Its CURRENT/generation
container differs from production `events/`; it does not open or convert that
layout automatically. The [shared contract documentation](../internal/cluster/db/eventlog/README.md)
describes both backends and their conformance tests.

## Segmented storage behavior

Sealed segments own fixed half-open ID ranges. Rewriting changes surviving
records without shifting those ranges or renumbering IDs. Unchanged sealed files
retain their filenames and bytes. Completely erased sealed ranges retain a
catalog descriptor with no payload file.

```text
before:
  range [100, 200): file A, records 100, 110, 150, 199
  range [200, 300): file B, records 200, 230, 299

after erasing 110:
  range [100, 200): replacement file, records 100, 150, 199
  range [200, 300): file B unchanged
```

Managed append syncs before returning success. Rotation uses persisted original
framed-byte accounting, independent of append batch boundaries. A completed append
file becomes an immutable range without copying or changing its bytes. Its exact
size is recorded in the catalog; subsequent writes use a new active file. Tail rewrite
checkpoints preserve original append progress and rotation accounting even when
records are erased. `LastAppended` reports original progress rather than the last
surviving record.

`Read` performs exact lookup, `Seek` finds the next survivor at or after an ID,
and `Scan` visits a half-open range under one managed view. Closed append files
use sequential validation and reads; indexed files use block indexes. Missing IDs are
reported separately from corrupt data. Segment blocks support plain encoding and
independent zstd compression; a block is stored plain when compression would not
reduce its size.

The implemented binary layouts and limits are documented in:

- [Segment format and compression](../pkg/segmentlog/README.md).
- [Active-tail format and recovery](../pkg/segmentlog/tail-format.md).
- [Catalog metadata and publication](../pkg/segmentlog/bbolt-experiment.md).
- [Managed lifecycle](../pkg/segmentlog/log-lifecycle.md).
- [Sealed-range rewriting](../pkg/segmentlog/sealed-rewriting.md).
- [Whole-log rewriting](../pkg/segmentlog/whole-log-rewriting.md).
- [Reclamation](../pkg/segmentlog/reclamation.md).

## Publication, recovery, and current limits

The segmented engine uses bbolt transactions to select its layout. The tidwall
adapter uses CURRENT to select a generation. Replacement files are synced and
durably installed before selection changes. Unreferenced newer files do not
select themselves. Segmented recovery checks metadata boundaries and the active
tail; historical files are checked on access or by explicit `Log.Verify`.
Publication errors can leave an uncertain outcome; affected handles require
close/reopen rather than continuing mutation or attempting a destructive rollback.

Publication and physical cleanup are separate operations. Explicit `Reclaim`
removes obsolete files under exclusive ownership. Bbolt reclamation drains
committed retirement records. `Log.ReclaimOrphans` separately scans for unpublished
files. Old payload files can remain until reclamation succeeds.

The segmented managed log serializes reads, append, sealing, rewrite, and
reclamation. Rollover and catalog publication are synchronous, but rollover does
not convert or compress the old append file. It has no background preparation, pinned
backup views, or decoded-block cache. Complete-catalog startup verifies all
referenced payloads. The bbolt catalog uses bounded metadata queries and active-
tail recovery; see [the experiment](../pkg/segmentlog/bbolt-experiment.md).
Standalone and rewrite publication verify new or changed sealed references and
the active tail, reusing prior verification for exact unchanged immutable references.
Managed rollover separates physical preparation from metadata publication through
a private, single-use handle; it does not repeat preparation's file checks or syncs. Both formats select a complete logical layout; bbolt stores range entries
individually rather than serializing their full list at each publication. The tests do not establish TB- or PB-scale operation.

Incomplete tail suffixes are reported without automatic truncation. Storage alone
does not prove that discarding a suffix preserves previously acknowledged data.
Fault-injection tests exercise publication and I/O failure paths; they do not
establish every filesystem power-loss outcome.

## Application experiments

The package-private adapter validates and deduplicates replay of the same
committed history against original append progress. Its streaming Actual reader
filters internal entities and gates delivery on an explicitly supplied applied
watermark. Exact Actual lookup retains metadata. Deadline-bound protected readers
defer rewrite publication across calls while allowing appends between reads.

Metadata supersession selection uses the accumulator shared with production
Storage. Experimental `rewriteMetadata` holds one adapter lock across bound
validation, selection and publication. It does not supply RTBF authorization,
update BoltDB, or declare application-level erasure complete. Production scrub,
backup, peer transfer, and Raft/BoltDB recovery use their existing paths.

`Storage.copyEventLog` copies a stable legacy event log into a fresh experimental
backend in bounded batches. It checks legacy checksums, protobuf ordering and
agreement with source append progress, and preserves exact entry bytes. The caller
must discard a failed copy, which can contain a durable prefix. The helper has no
persisted completion record, digest comparison, activation step, or conversion CLI.
Tests compare copied bytes, reopen both backends, and continue committed replay.

See the [adapter documentation](../internal/cluster/db/wal/segment_eventlog_experiment.md)
for operation contracts and focused tests.

## Measurements and validation

The [rewrite churn experiment](../internal/cluster/db/wal/segment_churn_experiment.md)
compares the legacy tidwall copy primitive with segmented rewriting for synthetic
no-op, isolated, and scattered scrub workloads in plain and zstd modes. It checks
survivor-byte equivalence and measures retained files, replacement bytes, and new
content hashes. These measurements are not production backup-cost estimates.

[Compression benchmarks](../pkg/segmentlog/compression-benchmarks.md) report
synthetic size, CPU, and allocation measurements. Shared backend conformance tests
cover sparse IDs, strict append batches, rewrite, erased append progress,
reopen/reclaim, ownership, and failure recovery. Adapter tests cover visibility,
exact lookup, replay, protected reads, and metadata rewriting with both backends.
