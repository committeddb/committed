# Local catalog: experimental format 0

The catalog names one complete local layout: a contiguous sequence of sealed or
empty ranges and an optional active tail. It is not a hosted backup manifest.
This slice supplies validation, publication, and recovery selection. It does not
yet coordinate readers, rotation, scrub policy, or physical file retirement.

## Ownership and state

`Catalog` records a caller-provided nonzero 16-byte History identity, local
Revision, logical Generation, starting record ID, ordered SegmentRefs, and an
optional TailRef. An optional SegmentBytes field persists the managed log rotation
target; zero omits it for standalone catalogs. Once published, it cannot change.
Revision begins at 1. Publication requires an expected revision
and exactly its successor; History and Start cannot change, Generation cannot
regress, and sealed coverage cannot shrink. A physical seal can advance Revision
without changing Generation; a logical scrub advances Generation separately.

Each nonempty SegmentRef has its original half-open Coverage, filename, SHA-256,
and surviving Count. Empty ranges retain Coverage with zero Count, empty filename,
and zero digest. Ranges are contiguous from Start, cannot overlap, and cannot
reference the same file twice. An active tail begins immediately after sealed
coverage (or at Start when no sealed ranges exist). Its end is read from validated
append groups, not copied from a stale catalog.

An immutable SegmentRef may have a nonzero `TailBytes` field. It identifies a
closed append-format file, retaining its `.active` basename and exactly that byte
size. Its Count describes physical survivors and Coverage preserves the original
range, including any erased high IDs. No footer or conversion is written into the
old file. Zero/omitted TailBytes identifies the existing indexed `.seg` format.
Readers reject size, count, starting-ID, coverage, or digest mismatches. Indexed
rewrite outputs reset TailBytes to zero. Older readers reject the added field
through canonical catalog validation; this remains an experimental format.

A rewritten TailRef can include an optional Checkpoint with End, Last, Count,
and Framed fields. It restores original append accounting at a fixed complete-
group boundary; later appends are recovered from groups beyond that boundary.
See [the tail checkpoint contract](tail-format.md). Omission preserves existing
catalog encoding. Older experimental readers reject checkpoint-bearing catalogs
through their canonical encoding check; this is not a production upgrade path.

Data filenames must be single non-hidden path components ending in `.seg` or
`.active`. Closed append references use `.active` too; catalog position determines
whether a file is writable. Referenced files must be regular files; symbolic
links are rejected.
Limits are 65,536 ranges and 16 MiB encoded catalog payload. The directory is
trusted and exclusively managed; these checks are not a sandbox against a
concurrent process replacing files or directories.

`CreateCatalogStore` explicitly initializes a directory containing prepared data
files. It refuses existing CURRENT, catalog artifacts, and publication temporary
files. A failed initialization can therefore require manual investigation; it
never silently adopts orphaned catalogs. `OpenCatalogStore` requires CURRENT.
Missing or corrupt CURRENT, missing manifests, invalid references, and incomplete
active tails are errors. It never falls back to an earlier generation or chooses
the newest filename. No opening operation truncates tails or deletes files.

`Current` returns a detached copy of the last confirmed catalog. Store methods
serialize within one instance. The caller must enforce exclusive directory
ownership across instances/processes and fence tail mutations while publishing.
No process lock, reader pin, or automatic tail/catalog coordination exists yet.

## Encoding

Integers are little-endian; checksums are CRC32C. Checksums/digests detect damage,
not malicious modification.

A catalog file contains:

- 16-byte header: `magic[8]="SLCAT000", version:u16=0, features:u16=0,
  payload_length:u32`.
- Payload: canonical JSON from the current Catalog encoder. Fields retain their
  exported Go names; byte arrays are numeric arrays. The decoder rejects unknown
  fields, duplicate fields, trailing values, and any noncanonical representation.
- CRC32C (4 bytes) over header and payload.

Its name is `catalog-<20-digit-revision>-<full-sha256>.manifest`, where the digest
covers the entire encoded file. Content-addressed names allow a retry to use an
identical unpublished catalog without overwriting it. Existing files at that name
must match byte for byte and be synced before reuse.

CURRENT is exactly 52 bytes:

`magic[8]="SLCUR000", revision:u64, catalog_sha256[32], crc:u32`.

The CRC covers its first 48 bytes. Revision and digest derive the exact catalog
filename. CURRENT's revision must match the decoded catalog. Neither file format
is stable or adopted as a production storage contract yet.

## Standalone publication protocol

1. Validate the candidate layout and expected revision.
2. Check new or changed segment references for coverage/count, whole-file SHA-256,
   and decoded record integrity, then sync those files. Exact unchanged references
   reuse verification and durability from the last confirmed catalog. Always scan
   and sync the active tail, then sync directory entries. Published sealed files
   must remain immutable for their entire lifetime under exclusive ownership.
3. Install and sync the immutable catalog. A failed prior attempt's identical
   content-addressed catalog may be verified and reused.
4. Replace CURRENT atomically and sync its directory. Initialization installs it
   without overwriting instead.
5. Only after success, expose a detached in-memory copy of the new catalog.

Revision/input validation errors leave the handle usable. File verification,
reference sync, and publication failures poison it. Further publications perform
no I/O. `Current` returns the previous confirmed snapshot with ErrCatalogPoisoned;
callers must not treat that snapshot as proof of on-disk state after a failure.
Reopen to validate the files CURRENT actually selects and confirm their file and
directory durability. In particular, a failed final directory sync may leave a
visible new CURRENT; the code never attempts a destructive rollback.

Tests simulate failures before catalog installation, before pointer replacement,
and after replacement becomes visible. These check the state machine; they do
not simulate every possible filesystem power-loss result. Lower-level tests cover
individual write/link/rename/sync boundaries.

Publication reuse compares the complete SegmentRef, including coverage, filename,
count, and digest. It never trusts a filename alone or a failed candidate. Initial
creation verifies every reference, and reopening rebuilds trust by verifying the
whole layout. Publication no longer detects newly occurring damage in an unchanged
sealed file; reads, full recovery, rewrite preflight, and reclamation retain their
integrity checks. The catalog itself is still validated and serialized in full.

## Current limitations

- Startup reads all referenced segment contents; standalone publication reads
  new or changed sealed references and the active tail. For indexed files, SHA-256
  and block/frame verification share one read of each stored payload block. Header,
  index, and footer metadata are read for structural validation and again for
  the whole-file digest. Closed append files use separate semantic and digest
  passes. Startup work still grows with stored payload bytes.
- Catalog publication itself retains old catalogs and payload revisions. The
  managed Log now provides explicit [reclamation](reclamation.md) of recognized
  obsolete files under exclusive ownership, with reopen/retry after failure.
  Publishing an erased range alone is not physical erasure completion. Reader
  pins and captured views are not implemented.
- This layer verifies file/layout integrity, not semantic equivalence of a scrub.
  The managed Log supplies record transformations and original rotation accounting.
- Catalog publication does not yet reconcile metadata in another database, prove
  incomplete suffixes safe to discard, implement backups, or negotiate peer data.

## Managed rollover publication

Managed rollover has a private preparation contract separate from public
`CatalogStore.Publish`. Segment storage retains the synchronized old append file,
computes its digest, and durably installs the new empty tail. The catalog publisher
consumes that process-local preparation once, checking its source directory,
history, revision, and active filename. It builds and validates the successor
layout, installs its catalog, and replaces CURRENT. It does not reopen or resync
the prepared payload files or repeat the installer's directory sync.

There is no persisted preparation receipt or additional on-disk format. A failed
publication still poisons the managed log; recovery follows CURRENT and verifies
all references from disk. Standalone publication and rewrite publication retain
the verification protocol described above.
