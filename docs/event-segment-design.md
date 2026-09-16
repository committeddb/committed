# Owned event storage: requirements and segment format

Status: **draft for review**, targeting an experiment during 0.8.1 development.
This describes proposed behavior, not the shipped storage format or a release
commitment. Constants and binary layouts below must be validated before freezing
a format version.

## Decision to explore

Own the permanent event-log implementation. Preserve unchanged sealed segments
when RTBF, delete-key erasure, or metadata cleanup changes other history. Reuse
the current record payloads, scrub-selection rules, and application reader
contract; replace the physical assumption that records occupy a dense global
WAL sequence.

The first implementation concerns `events/`. Raft's entry/state logs and BoltDB
retain their current roles. This bounds the experiment without committing the
owned format to Raft's different prefix/suffix-truncation semantics.

Related contracts: [event semantics](event-log-architecture.md),
[current storage architecture](storage-architecture.md),
[compatibility](api-compatibility.md), [backups](operations/backup.md), and
[RTBF](operations/rtbf.md).

## 1. Requirements

| ID | Requirement | Evidence required |
| --- | --- | --- |
| R1 | Store the ordered committed record stream, including internal metadata and Raft control entries. Never renumber surviving Raft indexes. | Compare raw surviving entries and application-visible Actuals with the current implementation. |
| R2 | Durably append before advancing BoltDB's applied index. Preserve `EventIndex >= AppliedIndex` and replay idempotence. | Crash injection around writes, syncs, rotation, and metadata application. |
| R3 | Seek at/after an index, read the next survivor, and perform exact lookup with bounded memory. Gaps are legitimate; corruption is not. | Deleted checkpoints, empty ranges, cold seeks, and checksum failures. |
| R4 | Rewrite or remove affected segments without renaming, rewriting, recompressing, or changing the hashes of unaffected sealed files. | Before/after inventory for every scrub workload. |
| R5 | Remove selected entities within a record as well as whole records. Retain every other entity and its original ordering. | Mixed-entity Actuals; all current scrub conformance cases. |
| R6 | Publish one coherent scrub generation. No reader, backup, or peer transfer observes a mixture of old and new scrub results. | Concurrent reads, backup, catch-up, and restart during publication. |
| R7 | Reclaim obsolete payload files and auxiliary copies; expose delays caused by pinned readers and cleanup failures. | Inspect decoded managed files, temporary files, retained handles, and cleanup progress. |
| R8 | Compress sealed data off the append path; choose codec and level without rewriting unrelated segments. | Append latency under sealing; compression size/CPU/memory measurements. |
| R9 | Transfer whole compatible sealed files, stream boundary records, and restore consistent backups. | Catch-up across different local layouts and restore after concurrent appends. |
| R10 | Detect unsupported formats, missing referenced files, invalid lengths, and corrupt indexes explicitly. | Malformed-file tests, fuzzing, feature gates, and old-binary refusal. |
| R11 | Normal restart reads catalog and bounded tail/index metadata, not all historical payloads. | Startup work vs segment count and total bytes; full verification remains separately available. |
| R12 | Survive supported filesystem crashes without acknowledging a transition whose files or directory entries were not synced. | File and directory sync fault matrix on the supported production platform. |

Physical erasure here means removal from Committed-managed filesystem content.
It does not promise overwriting SSD cells or external filesystem snapshots. Old
backups and transient Raft copies retain the existing documented responsibilities.

Non-goals for the first cut: S3 integration, arbitrary query indexes, latest-value
compaction of ordinary revision history, per-subject encryption, automatic
cross-segment repacking, and replacing consensus. Segment selection may initially
scan the whole log; reducing scan I/O is separate from eliminating rewrite I/O.

## 2. Record identity and storage boundary

### Package ownership and abstraction layers

The reusable engine lives in `pkg/segmentlog/` within this repository and its
existing Go module. Its API accepts ordered, sparse record IDs and opaque bytes.
It must not import Committed application packages or understand Actuals, Raft,
RTBF, protobuf, or bbolt. The Committed adapter stays in `internal/cluster/db/`.

| Layer | Owns |
| --- | --- |
| Committed adapter | Serialization, record-ID validation against payloads, scrub selection/transformation, AppliedIndex visibility, metadata reconciliation, backup/peer protocol semantics |
| Ordered segmented log | Append, sparse seek, range ownership, catalog publication, rotation, recovery, bounded captured views, rewrite transactions and retirement |
| Segment operations | Active/sealed readers and writers, block lookup, replacement encoding for a fixed range |
| Internal encoding | Frames, checksums, block indexes, bounded codec implementations |
| Internal durable filesystem operations | File/directory sync, durable replacement primitives, platform behavior and fault injection |

These are conceptual boundaries, not five public interfaces. Keep segment
lifecycle and catalog coordination together initially. Extract internal helpers
where they have concrete responsibilities. The ordered log controls the sequence
of durability operations; filesystem helpers do not choose recovery policy.

The generic rewrite operation accepts a caller-supplied keep/replace/remove
transformation. Replacements retain IDs. It preserves unaffected sealed files,
while changed ranges receive new immutable revisions. Publication and physical
retirement are distinct outcomes. Consistent captured views support backup and
transfer without making either protocol part of the engine.

The [initial package implementation](../pkg/segmentlog/README.md) covers sealed
segments and replacement preparation only. Its experimental format 1 supports plain and zstd
blocks, with backward reads of format 0. It is not the complete proposed format
below. Internal file-publication primitives now implement synced immutable
installation and atomic pointer replacement, including explicit uncertainty after
a failed publication sync. A single-file active-tail appender and non-mutating
scanner now validate bounded groups and poison handles after I/O failures.
Incomplete suffixes are reported but never automatically discarded. Atomic local catalogs now validate references and publish through CURRENT; their
initial verifier scans full payloads and retains old revisions. Lifecycle/recovery
coordination, bounded-metadata startup, rotation, complete log durability, and
Committed integration remain subsequent slices.

### Committed record identity

The Committed adapter uses the committed **Raft index** as record identity. A data record produces an
Actual with this index; some stored records contain only internal/control data
and never produce an application-visible Actual. There is no second persistent
dense WAL index.

Retain the current serialized `raftpb.Entry` payload initially, including index,
term, entry type, and proposal bytes. Preserve unchanged payload bytes verbatim.
This avoids combining a storage rewrite with a domain serialization migration.

`db.ActualReader.Read()` and syncable checkpoints remain unchanged. Inside the
storage package, provide raw seek, exact lookup, sequential iteration, append,
and layout capture. These operations retain control/metadata records and perform
integrity verification. They are not the application reader, which filters them.
The existing reader can track a segment/block/record position and re-seek from
its last examined Raft index after a layout revision; no new public cursor API
is required.

Exact lookup of an erased index returns not-found. Sequential lookup returns the
next surviving index. An absent catalog-referenced file or a broken checksum
returns corruption, never a gap to skip. Reads continue to honor AppliedIndex;
durable-but-not-yet-applied events remain invisible to syncables.

## 3. Segment identity, ranges, and boundaries

Separate three concepts:

- **Logical range:** the original interval of Raft indexes owned by a segment,
  `[start, end)`. Scrubbing does not shrink or shift this interval.
- **Surviving records:** zero or more sorted records inside that interval.
- **Physical revision:** one immutable encoding of those survivors, identified
  by a digest. A scrub can replace the physical revision for the same range.

Example:

```text
range [100, 200): A  records 100, 110, 150, 199
range [200, 300): B  records 200, 230, 299

erase index 110:
range [100, 200): A' records 100,      150, 199
range [200, 300): B  same filename, bytes, and digest
```

An entirely erased range has a catalog descriptor with zero records and no
payload file. This distinguishes known empty history from a missing file. Empty
adjacent descriptors may later coalesce as catalog-only metadata. Descriptors
contain indexes/counts, never subject keys. Keep a separate captured coverage
high-water mark so an empty range cannot make progress appear to regress.

Initial rotation proposal: target roughly the current 20 MiB of **original,
uncompressed framed input**, cutting only between records. A large record gets
its own oversize segment, subject to the existing record-size limits. Record the
original range and rotation accounting; a scrub never packs later records into
space it reclaimed. Do not split or join sealed ranges during ordinary cleaning.

Boundary accounting must not depend on Ready batch sizes, compression ratios,
or background worker timing. Count original input even if a concurrent scrub
removes its payload. Persist that accounting across restart. Prefer deterministic
boundaries for new writes, but do not require identical physical encodings across
all nodes for correctness (section 8).

## 4. Proposed file formats

```text
events/
  CURRENT                         # pointer to a committed catalog revision
  catalog-<revision>.manifest      # immutable catalog snapshot
  <range-start>-<sha256>.seg       # immutable sealed segment
  <tail-id>.active                # append-only, uncompressed active tail
  staging/                        # unpublished replacements and seal output
```

Filenames help inspection; validated catalog/header fields establish identity.
Use integer little-endian encoding and versioned magic for new binary files.
Never include wall-clock time, host paths, node IDs, or global scrub generation
in immutable segment bytes. Catalogs carry operational provenance and generation.

### Sealed segment

| Region | Proposed contents |
| --- | --- |
| Fixed header | Magic, format version, required-feature bits, header length, logical range bounds, header CRC32C. |
| Independent data blocks | Block header followed by plain or compressed record frames. |
| Block index | First/last surviving Raft index, file offset, stored length, decoded length, record count for each block. |
| Footer/trailer | Index location/length, total record count, first/last surviving index, index checksum, footer checksum and versioned end magic. |

Each block header carries its version, codec ID, stored/decoded lengths, record
count, first/last index, a header checksum, and a checksum of stored bytes.
Validate lengths and header integrity before allocation/decompression. Verify
stored bytes before decoding; bound decoded output. Checksums use CRC32C; the
catalog additionally records SHA-256 over the complete sealed file for transfer
and backup verification. Neither checksum is an authenticity mechanism.

Record frame proposal: `frame length | Raft index | payload length | payload |
CRC32C`. The checksum covers lengths, index, and payload. The Committed adapter
must verify that the outer index equals the payload's index. This duplicates a small field to permit indexing and
validation without decoding every protobuf. Records are strictly increasing
within and across blocks; no duplicates. Frames never cross block boundaries.

Start experiments with 256 KiB and 1 MiB decoded block targets. Oversize records
occupy a block alone. Smaller blocks bound cold-read decompression and memory;
larger blocks may compress better. Select zstd as the initial sealing codec with
a plain-codec option. Codec IDs and required decoder features belong on disk;
encoder tuning belongs in policy. Pin encoder settings for reproducibility
tests, but permit readers to consume different supported encodings.

The index is part of its immutable segment, so it cannot become stale relative
to rewritten payloads. Seek binary-searches segment descriptors, then block
bounds, then scans the selected decoded block for the requested index. It never
binary-searches hypothetical dense index positions. Cache decoded blocks with a
byte budget; do not require one in-memory entry per historical record.

All field widths, maximum lengths/counts, reserved values, and exact CRC coverage
must become a byte-level format specification before implementation is adopted.
Unknown required features fail closed; no magic-based fallback to legacy data.

### Active tail and durable append

The active tail is recoverable before it is sealed; it has no mandatory final
index/footer. Use a versioned header and framed append groups containing records
plus an end marker with group length, record count, last index, and group CRC.
Groups express local write completion, not Raft consensus or deterministic
segment boundaries. Sealing reconstructs canonical blocks independent of groups.

An append call writes complete groups and syncs every affected tail file before
returning durable success. Only then may EventIndex advance and metadata apply
proceed. A group marker alone does not prove an fsync occurred. New files and
catalog references must also be directory-durable before successful publication.
If a write/sync fails, poison or recover the append handle before retrying;
never continue from an uncertain partial suffix.

Recover complete groups, including groups that reached disk before the caller
observed success; replay deduplication handles them. Only a demonstrably incomplete
final group may be discarded, and only when doing so is consistent with durable
metadata and Raft recovery bounds. A complete group's checksum failure or any
mid-file corruption fails loudly. Do not reinterpret corruption as a successful
shorter log. The fault tests must settle ambiguous torn-tail cases before this
rule is finalized; availability must not be purchased by silently losing history.

Rotation first finalizes/syncs the old tail and creates/syncs the next tail, then
publishes the new catalog state. Closed plain tails are valid immutable inputs
while the sealer works. Sealing creates a compressed revision off-path and
publishes it before reclaiming the plain predecessor. Thus restart and backup
must support sealed compressed files, closed plain tails, and one active tail.

## 5. Catalog and atomic publication

The catalog is a **local storage catalog**, recording which files constitute the
event log. It is unrelated to hosted backup deduplication. Initially use a full,
checksummed, versioned catalog snapshot per structural change; no catalog write
per normal append. If catalog size becomes a bottleneck, benchmark journaled
edits later rather than begin with two catalog representations.

Catalog fields: format/required features, history identity, local revision,
logical scrub generation, ordered range descriptors and their physical digests,
closed/active tail descriptors, and any pending retirement/completion state.
Tail end/index are recovered from validated groups, not a stale catalog value.
Keep **local layout revision** distinct from **logical scrub generation**: sealing
changes layout, while erasure changes logical history. Unchanged files can be
shared by successive generations without rewriting their headers.

Publication protocol under a short structural lock:

1. Validate expected input revisions; reconcile concurrent appends/rotations.
2. Finish replacement files, fsync them, and durably install their final names.
3. Write/fsync a new catalog and fsync its directory entry.
4. Write/fsync a temporary CURRENT pointer containing revision and catalog hash;
   atomically replace CURRENT and fsync its parent directory.
5. Publish the corresponding in-memory view; retire old files once pins allow.

After a crash, CURRENT selects the catalog. Unreferenced higher revisions are
unpublished work, not evidence to advance state. A malformed CURRENT or corrupt
referenced catalog fails closed; never fall back silently to pre-erasure history.
If a sync fails after pointer replacement, stop structural mutation and recover
from disk; do not assert either success or rollback from uncertain durability.

Scrub generation and its complete file set become authoritative together in this
catalog. BoltDB's existing pending/completed scrub bookkeeping is reconciled from
the published catalog after restart. Publication cannot depend on atomically
committing two different stores. Older catalog files and retired segments must
not remain an unbounded archive of erased state.

## 6. Selective scrub algorithm

1. Capture the replicated scrub bound, authorization, and current input view.
   Reuse `tombstoneSelections`, `metadataSupersessions`, delete-key erasure gates,
   and `scrubFilterEntry` semantics. Storage does not invent retention policy.
2. Scan to identify records that change. The first experiment may retain the
   existing full-log scans. A segment is unaffected only if **all** rules leave
   every record unchanged, including metadata and delete-key transformations.
3. Produce new files only for affected ranges, preserving surviving indexes,
   record ordering, range ownership, and unaffected payload bytes. Empty ranges
   become empty descriptors. No placeholders or cross-range repacking.
4. Validate replacements and prepare one catalog update for the entire scrub.
5. Publish the whole scrub generation using section 5, then perform existing
   post-scrub metadata reconciliation and physical cleanup with resumable state.

The active tail needs special handling when it contains records below the bound.
Build a replacement prefix while appends continue, chase the appended suffix,
and finish a bounded catch-up under the append/publication lock, as today's
scrubber does. Preserve original rotation accounting. A concurrent seal/rotation
requires rebasing against validated input revisions, or retrying preparation;
never install a prefix that drops a concurrently acknowledged append. Pin input
files against retirement during preparation, while allowing append-only growth.

No-op scrub: publish generation/progress if required, but preserve all payload
files. Completion distinguishes logical publication from physical retirement.
An existing generation mismatch remains a reason to refuse mixed-history
catch-up even if many individual segments have unchanged bytes.

Initial resource expectation: full scan cost can remain O(history), but output
bytes and temporary payload space should be O(affected segments + tail catch-up).
This is not an unconditional bound if old pinned generations accumulate. Permit
only one scrub transition with outstanding erasure retirement at a time, bound
capture lifetimes, and report blockers. Low disk space must defer safely before
publication rather than remove the source first.

## 7. Reader and backup lifetime

Ordinary readers pin an immutable view only for a bounded read operation, and
re-seek at the next operation when generation changes. Preserve the existing
from-zero read protection needed for delete-key pair consistency. Long protected
reads need cancellation and observable pin duration; they cannot silently retain
erased revisions indefinitely.

A live backup captures a catalog view plus a complete-group tail byte limit and
pins those files. It uses the existing metadata -> Raft state -> events -> Raft
entries capture ordering and bounds checks. It need not prevent unrelated new
appends. For the first implementation, it delays scrub publication like today's
layout freeze, with a bounded duration/cancellation mechanism so erasure can
proceed. Reference-counted old-generation backups are a later policy choice,
not an implicit extension of the RTBF window.

Archived CURRENT/catalog must describe **only captured files and the captured
tail limit**, not later live state. Restore validates every referenced digest
and range before its atomic publish. Backup still includes the other stores and
retains the existing staged-projection rebuild requirements.

## 8. Peer transfer and determinism

Keep deterministic logical scrub results: the same committed input and bound
produce the same survivors. Prefer reproducible physical segments, but explicitly
replace the current assumption that equal scrub generation implies identical
file boundaries and bytes. Migration, encoding upgrades, and different prior
rewrite histories can produce different valid representations.

Negotiate format capability and transfer a pinned source view identified by
history identity, scrub generation, and coverage bounds. Never mix generations.
Keep the current conservative reset/full-fetch behavior for a newer generation
in the initial experiment; targeted generation repair is future work.

Adopt a whole file only when its logical range fits the receiver's missing
coverage without overlap, its digest validates, and its format is understood.
Transfer partial ranges as validated raw records and reconstruct local segments.
Require explicit empty-range descriptors so a missing file cannot masquerade as
erasure. Verify ordered, complete range coverage against the source capture;
gaps in record indexes alone cannot establish coverage. This protocol needs
versioning: old `FirstSeq` adjacency is not valid for the new format.

## 9. Compatibility and rollout

Keep the tidwall reader as a legacy adapter during experimentation. Use explicit
format detection and golden fixtures; a new reader must read existing 0.8.0
data/backups without requiring an immediate conversion of live history.

Start with an **offline, opt-in conversion** into a separate target directory:
read and validate the old log, retain original committed payloads/indexes and
scrub state, write the new format, verify equivalence, and atomically publish a
complete converted data-directory set. Never alter the source on failure. An
operator-retained source is another PII-bearing copy and must have an explicit
disposal policy. Do not silently activate on restart or leave a pre-conversion
copy inside the canonical data directory.

Before rollout, gate binary feature level, backup format, and peer protocol.
Old binaries must refuse the new directory **before** discovering a partial or
empty event log; a new magic value alone is insufficient because legacy file
discovery may ignore unfamiliar filenames. Prove a preexisting old-binary guard
works or ship a preparatory guard release before conversion. Mixed-version
clusters either negotiate a validated legacy transfer path or defer activation
until all members support the format. Reader compatibility is not permission for
an older writer to open new storage. Downgrade requires an explicit export;
otherwise document it as unsupported.

## 10. Experiment and adoption gates

Compare the current fork and the proposed implementation using identical event
streams, sync frequency, cache budgets, compression settings, and hardware.
Separate logical input bytes, on-disk bytes, and bytes rewritten.

Workloads: append-only; repeated no-op scrub; isolated old-record deletion;
scattered deletions; partial-Actual rewrite; complete range erasure; metadata-only
cleanup; delete-key erasure; append while scrubbing; slow/pinned readers; and
catch-up/backup during maintenance. Include realistic large records and both
compressible and incompressible payloads.

Measure durable append throughput and p50/p95/p99 latency; replay throughput;
cold seek latency; compression ratio and CPU; peak RAM; startup work; read/write
I/O; temporary space; unchanged-file fraction; newly produced compressed bytes;
and time to logical and physical scrub completion. Measure catalog overhead
separately from payload churn. No speedup or regression threshold is claimed yet;
agree an acceptable envelope against the baseline before adoption.

Correctness gates:

- Byte-identical unaffected sealed files across every selective/no-op scrub.
- Raw-record and Actual-reader equivalence to existing scrub semantics.
- All surviving indexes unchanged, including resume after an erased checkpoint.
- Corruption cannot be interpreted as deleted history or a supported old format.
- Crash matrix at append, rotation, seal, catalog, CURRENT, metadata reconciliation,
  retirement, and conversion boundaries; include short writes, sync failures,
  disk full, missing files, and restart while an obsolete generation is pinned.
- Read/backup/catch-up races and mismatched generation/layout/codec tests.
- Decode all managed retired/temp/current files after physical completion to
  verify erased entities and raw keys are absent; raw byte search alone is
  insufficient for compressed data. Preserve the current Raft-copy caveat.
- Existing race, backup, upgrade, multinode, and adversarial coverage remains
  applicable, supplemented by parser and publication-state-machine fuzzing.

## 11. Decisions to settle before implementation

1. Final record/header/footer encoding and allocation bounds; whether duplicated
   outer indexes justify their overhead.
2. Segment and block targets and encoder levels, chosen from measurements.
3. Active-tail rotation during a long scrub and the exact incomplete-suffix
   recovery proof. These are correctness blockers, not tuning details.
4. Catalog size at TB scale and platform-specific durable replace behavior.
5. Maximum protected-read/backup lifetime and how callers observe cancellation.
6. History identity source and the versioned coverage-based peer protocol.
7. Old-binary refusal and migration sequencing before any format activation.

## Source notes

The implementation references for this draft are
`internal/cluster/db/wal/{reader,wal_eventlog,scrub,event_fetch,live_backup,checksum}.go`,
`internal/cluster/db/actual_reader.go`, and
`third_party/forked/tidwall-wal/{wal,compress,layout}.go`.
Some older architecture prose predates the fork's directory-fsync and live-backup
changes. Implementation and current tests must be reconciled with those documents
as part of any adopted design; this draft does not silently redefine the shipped
durability contract.
