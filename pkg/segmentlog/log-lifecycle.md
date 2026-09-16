# Managed log lifecycle (experimental)

`CreateLog`, `OpenLog`, `Append`, `Read`, `Seek`, and `Close` now connect catalogs,
active append groups, and sealed segments. This is an initial synchronous
implementation. It is not yet connected to Committed's database engine.

## Directory ownership

CreateLog requires an empty, already durably created directory; it never empties
or reinitializes a directory. It generates a history identity, durably installs an
empty active tail, and publishes the initial catalog. Failed initialization can
leave files for investigation. OpenLog follows CURRENT and validates referenced
history. Incomplete tails remain errors and are never truncated automatically.

CreateLog and OpenLog acquire a nonblocking advisory lock on the directory inode
before reading recovery state or publishing files. A competing process or instance
receives ErrLocked. The lock covers path aliases of the same directory, creates
no lock file, and is released by process termination. Failed creation/open releases
it; a poisoned Log retains it until Close. Close releases the active file before
ownership and does not force a new segment boundary.

Methods also serialize within a Log instance. All other maintenance/writers must
cooperate with this ownership protocol: the lock does not prevent direct writes
by a non-cooperating process. Do not replace/rename away the managed directory or
separately mutate its CatalogStore, tails, or files while the Log is open. Raw
catalog/tail/file primitives do not acquire the lock themselves; a future offline
maintenance API must acquire the same ownership before using them.

## Deterministic rotation

LogOptions.SegmentBytes is persisted in the catalog and cannot change through
publication. Zero at creation selects 20 MiB; explicit targets range from 16 bytes
to 32 MiB, subject to the block-index capacity for the chosen encoding options.
Standalone catalogs omit this field; OpenLog refuses to adopt those catalogs.
Encoding options may change on reopen and affect future sealed files only.

The rotation counter counts original record frames: payload bytes plus 16 bytes
per frame. It excludes append-group headers/trailers and compression. Before a
record would exceed the target, the existing nonempty tail is sealed. An oversized
record occupies a range alone, sealed when the next record arrives. A tail exactly
at the target is also sealed on the next record. No empty segment is produced.

Each sealed range retains its tail's original Start and ends at its last record
ID plus one. The new active tail starts at that exclusive end. Sparse IDs do not
shift these boundaries. Close/reopen reconstructs the current framed-byte count
from validated records. Identical records, target, and encoding produce the same
sealed ranges and bytes regardless of append batch boundaries or intervening
reopens. Append-group bytes in the active tail can differ between batch schedules.

This accounting currently assumes the active tail has not been scrubbed. Before
adding active-tail rewriting, persist original input accounting independently of
surviving payload size. The Log does not yet expose a scrub operation.

## Rotation ordering

Under the log mutex:

1. Encode the complete validated active tail into a new sealed file. Sync/install
   it and compute its whole-file SHA-256 for the catalog.
2. Durably install the next empty tail and open its appender.
3. Publish a successor catalog containing the sealed range and new active tail.
   Generation remains unchanged; this is a physical layout change.
4. Switch the in-memory active handle and close the predecessor.
5. Append the next records to the new tail and sync before acknowledging them.

Sealed files and tails receive unique names; the catalog digest identifies sealed
content. Names do not affect encoded bytes. Before step 3, CURRENT still selects
the old tail. After step 3, CURRENT selects its sealed replacement and the new
empty tail. Reopening uses that selection and ignores unpublished artifacts.

Any append or rotation failure poisons the Log. Reads and writes then refuse to
use the potentially uncertain view until Close/reopen. Recovery can retain valid
complete groups from failed append calls. A batch spanning rotations is not an
atomic transaction: a failed call may leave a durable prefix. Callers must
reconcile stable record IDs before replaying; duplicate IDs are rejected rather
than silently skipped. Invalid IDs or payload sizes anywhere in the input batch
are rejected before any of its records are written.

## Reads and current limits

Read performs exact lookup; Seek finds the first survivor at or above an ID,
including across sparse/empty ranges. They hold the Log mutex, open sealed files
on demand, and scan the active tail. Returned payloads are private to the read.
There are no long-lived iterators, reader pins, or block caches at this layer yet.

Sealing and catalog validation run synchronously while reads/appends are blocked.
Catalog publication currently revalidates full referenced files, making repeated
rotation increasingly expensive as history grows. Optimize this before throughput
claims or production adoption; it is an integrity-first lifecycle prototype.

Old tails, catalogs, sealed revisions, and crash orphans remain until explicit
`Reclaim(ctx)` validates the live set and removes recognized obsolete files.
Rotation temporarily increases disk usage until that call. See the
[reclamation contract](reclamation.md). Integrated scrubbing, retirement for
pinned views, background sealing, backup capture, and database integration remain
subsequent work. This prototype does not establish application erasure completion.

## Evidence

Tests compare ranges and sealed SHA-256 values across multiple batch sizes and
restarts; cover oversized records and gaps; ensure invalid batches do not partly
write; simulate failures preparing either file and before/after CURRENT switches;
recover acknowledged and unacknowledged durable prefixes; preserve incomplete
tails; change encoding without touching existing files; and run concurrent reads
with appends under the race detector. Fault injection tests protocol behavior,
not every possible filesystem power-loss outcome.

Ownership tests cover same-process and cross-process contention, symlink aliases,
independent directories, failed opens, poisoned handles, idempotent close, and lock
release after forced process termination. Directory locking is supported on local
Linux/macOS filesystems; unsupported platforms fail explicitly.
