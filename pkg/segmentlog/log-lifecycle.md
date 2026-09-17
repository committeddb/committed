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
catalog/tail/file primitives do not acquire the lock themselves; callers must
acquire the same ownership before using them.

## Deterministic rotation

LogOptions.SegmentBytes is persisted in the catalog and cannot change through
publication. Zero at creation selects 20 MiB; explicit targets range from 16 bytes
to 32 MiB, subject to the block-index capacity for the chosen encoding options.
Standalone catalogs omit this field; OpenLog refuses to adopt those catalogs.
Encoding options may change on reopen and affect indexed rewrite outputs.
Rollover retains the plain append format and does not compress records.

The rotation counter counts original record frames: payload bytes plus 16 bytes
per frame. It excludes append-group headers/trailers and compression. Before a
record would exceed the target, the existing nonempty tail is sealed. An oversized
record occupies a range alone, sealed when the next record arrives. A tail exactly
at the target is also sealed on the next record. No empty segment is produced.

Each sealed range retains its tail's original Start and ends at its last record
ID plus one. The new active tail starts at that exclusive end. Sparse IDs do not
shift these boundaries. Close/reopen reconstructs the current framed-byte count
from validated records and any persisted rewrite checkpoint. Identical records
and target produce the same sealed ranges regardless of append batch boundaries
or intervening reopens. Retained append-group bytes and hashes can differ between
batch schedules. Once a file becomes immutable, no-op rewrites preserve its bytes.

Whole-log rewriting persists original input accounting in a catalog checkpoint,
independently of surviving payload size. Erasing tail records does not free their
original rotation budget or reduce the recovered append frontier.

## Rotation ordering

Under the log mutex:

1. Retain the synchronized old append file under its existing name. Record its
   exact byte size and compute its SHA-256 without changing or copying its bytes.
   An entirely erased tail becomes an empty range descriptor without a file.
2. Segment storage durably installs the next empty tail and constructs its
   appender from the known empty header. This does not call recovery scanning.
3. The catalog publisher consumes the private prepared-rollover handle and
   publishes a successor catalog containing the closed range and new active tail.
   The handle is bound to the source directory, history, revision, and active file
   and can be consumed only once. Generation remains unchanged.
4. Switch the in-memory active handle and close the predecessor.
5. Append the next records to the new tail and sync before acknowledging them.

New tail and indexed rewrite files receive unique names; the catalog digest
identifies immutable content. Names do not affect encoded bytes. Before step 3,
CURRENT still selects the old tail. After step 3, CURRENT selects that same file as an immutable range
and the new empty tail. Reopening uses that selection and ignores unpublished artifacts.

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
Closed append files are validated sequentially when opened for a read, including
exact size, start, surviving count, and upper coverage bound. Their records are
then read through the tail scanner. Indexed rewrite outputs retain block-index
lookups. There are no long-lived iterators, reader pins, or block caches.

Rollover and catalog validation run synchronously while reads/appends are blocked.
Rollover performs no format conversion, compression, or replacement-file write.
Rollover publication trusts the exclusively owned appender's synchronized state
and the installer's completed durability work. It neither reopens payload files
nor repeats their file/directory synchronization. It installs the catalog and
replaces CURRENT with the existing durable filesystem primitives. The preparation
handle is in memory only; recovery never trusts it.

Standalone catalog publication and rewrite publication still verify new or changed
sealed references and the active tail. Unchanged immutable files reuse their
confirmed verification and durability.
Complete catalog metadata is still validated and serialized on each rotation.

Obsolete tails, catalogs, rewritten revisions, and crash orphans remain until explicit
`Reclaim(ctx)` validates the live set and removes recognized obsolete files.
Closed append files remain live references and are preserved by reclamation. See the
[reclamation contract](reclamation.md). Whole-log transformations now publish
through `Rewrite`; see [its contract](whole-log-rewriting.md). The managed log has
no pinned views, background sealing, backup capture, or production database
integration. It does not establish application erasure completion.

## Evidence

Tests compare ranges and counts across multiple batch sizes and restarts;
check that rollover preserves file identity and bytes; cover oversized records and gaps; ensure invalid batches do not partly
write; simulate failures preparing the new tail and before/after CURRENT switches;
recover acknowledged and unacknowledged durable prefixes; preserve incomplete
tails; change encoding without touching existing files; and run concurrent reads
with appends under the race detector. Fault injection tests protocol behavior,
not every possible filesystem power-loss outcome.

Ownership tests cover same-process and cross-process contention, symlink aliases,
independent directories, failed opens, poisoned handles, idempotent close, and lock
release after forced process termination. Directory locking is supported on local
Linux/macOS filesystems; unsupported platforms fail explicitly.
