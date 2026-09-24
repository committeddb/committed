# Permanent event-log boundary

The primary interface is **`EventLog`**, defined in **`eventlog.go`**.

```text
eventlog/
    eventlog.go       Shared storage contract and neutral types/errors
    segmented/        Implementation over pkg/segmentlog
    tidwall/          Dense-sequence implementation over the tidwall fork

wal/eventlog_*.go      Shared application adapter and reader/scrub coordination
pkg/segmentlog/       Application-independent segmented engine
internal/durablefs/   Shared publication, directory sync, and ownership primitives
```

## Code boundaries

Production `wal.Storage` selects its backend through `eventLogBinding`. The
binding supplies shared entry cursors and backend capabilities while keeping
native tidwall's physical operations in `eventlog/tidwall.LegacyLog`. The segmented
binding uses `eventlog.EventLog`, implemented by `eventlog/segmented` over
`pkg/segmentlog`. Application visibility, replay, and scrub policy stay in `wal`.

The separate private `eventLogAdapter` experiment depends only on
`eventlog.EventLog`. Its test fixtures supply either concrete implementation.

Inside `wal`, the experimental application layer is organized by responsibility:

| Files | Responsibility |
| --- | --- |
| `eventlog_adapter.go` | Shared adapter state, ownership, and lock ordering |
| `eventlog_entries.go` | Raft protobuf validation and stable record identity |
| `eventlog_append.go` | Append progress and committed-batch replay |
| `eventlog_lookup.go`, `eventlog_scan.go` | Raw entry lookup and consistent scans |
| `actual_reader.go`, `eventlog_reader.go`, `eventlog_actual.go` | Shared Actual interpretation, reader lifetimes, and applied visibility |
| `eventlog_protected_reader.go`, `eventlog_rewrite.go` | Read lifetimes and rewrite coordination |
| `eventlog_selection.go`, `eventlog_metadata_rewrite.go` | Application compaction policy |
| `eventlog_copy.go` | Bridge from production Storage into an experimental backend |

These files remain in `wal` because they share production policy helpers such as
`userTopicEntities`, `newMetadataSelection`, and `scrubFilterEntry`, along with
production error identities. Moving them into a storage backend would give that
backend responsibility for Committed's application semantics.

`eventlog_fixture_test.go` supplies backend-neutral records and type resolution.
`eventlog_backends_test.go` supplies the common backend matrix.
`eventlog_segmented_fixture_test.go` explicitly constructs a segmented fixture
for legacy comparisons and application-policy integration tests. Those tests use
`eventlog_` names; the physical churn experiments retain `segment_churn_` names.
This private adapter remains experimental; production `Storage` uses the binding
described above. See [the adapter's current behavior](../wal/eventlog_adapter.md).

## Responsibilities

| Layer | Responsibility |
| --- | --- |
| Application adapter | Protobuf serialization/identity checks, applied visibility, Actual decoding, replay policy, scrub selections/gates, protected multi-call reads |
| EventLog | Strict append, original append progress, exact/forward/range reads, atomic generation rewriting, reclamation, ownership and close |
| Concrete backend | Physical sequence/range translation, framing, persistence, publication, recovery, file retirement |

The contract uses independent Record, Coverage, Transform, RewriteResult, and
ReclaimResult types. It does not expose tidwall sequences, segment paths, block
formats, protobuf types, or one backend's physical rewrite statistics. Errors
have common identities for invalid input, absence, corruption, closed/poisoned
handles, ownership, and unsupported storage. Backend errors remain available as
wrapped causes. Rewrites report observed changed records and confirmed publication;
false Published with an error requires reopen to resolve uncertainty.

Callbacks hold a consistent storage view and must not reenter the log. Multi-call
application protections live above the interface: shared adapter locks defer
rewrites while protected readers remain. This is not a backup-capture/file-pin API.

## Production append and lookup wiring

Production `wal.Storage.appendEvent` and `appendEvents` submit logical Records to
`eventlog.Appender`, the append/progress subset embedded by `EventLog`.
Its `LastAppended` reports logical progress and distinguishes empty history
without exposing physical sequence numbers. Composition lives
in `wal/legacy_event_binding.go`. The default is `tidwall.LegacyAppender`, which
wraps the existing production handle and assigns its dense physical sequences.
The supplied codec preserves the existing checksum envelope and Raft-entry bytes;
no CURRENT file or experimental generation directory is introduced.

Fetched record batches also use `eventlog.Appender` through a native identity
codec: the already-validated peer frames are written verbatim, including unknown
protobuf fields. Unframed records are rejected by the existing checksum verifier. Local and fetched appends share `eventAppendMu`, so native
sequence assignment and application progress updates serialize. Overlap filtering
and write metrics remain in `wal`.

The native appender shares recovered logical progress between LastAppended and
Append, avoiding a second tail decode. It refreshes that progress when another
writer extends the same native handle.
The writer and cursor factory belong to one `wal.eventLogBinding`; scrub or peer
fetch replaces that complete binding. Storage has no separate writer cache.
Application replay filtering, applied progress, and metrics remain in Storage.
Production `Storage.ActualAt` uses a private `wal.entryCursor` for exact
lookup. `wal/actual_lookup.go` verifies that the seek result matches the requested
index and interprets the decoded entry. The existing event lock spans positioning
and proposal decoding. Exact replay includes metadata and does not gate reads on
AppliedIndex. The experimental adapter uses the same exact-entry and proposal
helpers, retaining its applied-watermark check. Native lookup preserves the entry
decoded during positioning without an additional payload copy or protobuf decode.

Production streaming `Reader` uses the application-side `wal.entryCursor`:
`SeekGE`, `Current`, `Advance`, and `Close`. Seeking is lazy; Current reports
storage errors and retains the decoded entry until consumption. Repeated Current
calls reuse that entry. EOF remains temporary.

Composition in `wal/legacy_event_binding.go` and `wal/legacy_event_cursor.go` binds
native tidwall positioning to the application codec. `wal/production_event_cursor.go`
tracks the published binding and scrub generation independently of the backend. The generic `tidwall.LegacyCursor`
carries the application codec's decoded result through binary search and sequential
positioning without copying the payload or interpreting protobuf. Production
streaming decodes each sequential entry once; retries reuse the decoded entry.
Native tidwall's existing read allocation remains.

The experimental reader uses the same entry-cursor contract over its raw backend
cursor, decoding each selected record once. Both production Reader and the
experimental reader use `wal/actual_reader.go` for the Actual-reading loop:
applied visibility, proposal interpretation, internal-entity filtering, deliberate
skips, and progress. The caller retains its existing lock and cursor binding;
protected-reader cancellation remains part of the shared loop. The existing publication gate
invalidates retained decoded entries before releasing readers. The application
retains applied visibility, type resolution, filtering, and the event read lock
through interpretation. Reader.Close releases its cursor and waits for an
in-flight read.

Production metadata-compaction selection and RTBF eligibility scans use
`wal.scanEntryPrefix` over `entryCursor`. They traverse inclusive logical bounds,
include metadata/control entries, and preserve the existing selection policies.
The production scan owns the event publication read lock and closes its cursor;
appends can continue during selection. Physical sequences are confined to the
native backend adapter.

The native peer-transfer resolver also uses the tidwall positioner's
`SequenceFor` operation. Physical sequence numbers remain part of that native
protocol: empty logs resolve to 1 and requests beyond the tail resolve to
last-sequence + 1. This operation is separate from the application entry cursor.

Startup and post-swap logical boundary recovery use the shared entry binding's
`LastAppended` and a cursor for the first survivor, in `wal/event_bounds.go`.
Recovered progress includes erased records; an entirely erased history has no
first survivor but keeps its append frontier. Both bounds are read successfully
before either is published. Early startup recovery restores only append progress
for the snapshot guard. Native scrub still requires recovered progress to equal
the previous EventIndex and refuses an empty native history before publishing.
Tests cover sparse history and failures, plus reopening shared backends after
removing their tail or all records without rewinding recovered progress.

Data-head recovery uses the shared entry binding's bounded `ScanReverse`.
`wal/data_head_recovery.go` retains user-data classification, the 4,096-record cap,
and warning policy. A persisted data head bypasses this fallback. Native production
binding uses its decoded positioner's reverse scan; shared EventLog binding decodes
each delivered record once. Both engines implement reverse scanning; the segmented
adapter delegates to `pkg/segmentlog`, including its bounded suffix handling for
unindexed files. Common tests cover ordering, early stop, cancellation, rewrite,
and reopen. Application tests cover the exact recovery cap and corruption stops.

Native layout snapshots, verified frame reads, and length-prefixed record
batches are implemented by `tidwall.LegacyTransfer`. It also supplies native
sequence bounds and verified payload reads for maintenance and copy operations.
The application envelope decoder runs once per read; peer reads preserve the
frame, while payload reads return its verified contents. `wal` supplies checksum
verification and retains layout freezes, generation policy, transfer budgets,
and the choice between whole files and records. These APIs describe the native
format; they do not imply interchangeability with segmented files.

Staged peer-segment inspection also lives in `eventlog/tidwall`: native filename
parsing, zstd decoding, and complete length-prefixed record validation.
`wal` supplies the checksum verifier and owns adoption alignment, layout
exclusion, rollback, and generation policy. Inspection does not modify files.

Native segment move/copy helpers live in `eventlog/tidwall` as well. Adoption
uses rename with a copy/sync/remove fallback. The caller retains directory sync,
adoption ordering, rollback, and handle replacement.

Production scrub preparation lives in `wal/scrub_plan.go`: it captures RTBF and
metadata selections plus the authorized delete-key erasure threshold in one plan.
The plan supplies the record transform and retains the erasure outcome needed
after completion. `wal/legacy_event_rewrite.go` executes that plan using the native
bulk copy, catch-up, publication, and recovery sequence. `runScrub` coordinates
preparation and execution; completion bookkeeping remains in its caller.

`Storage.rewriteSharedPlan` exercises shared-backend publication of a prepared
plan using the production reader lock and cursor generation. It serializes
appends, defers for existing protected reads or layout freezes, and holds new
protected-read registration until the step returns. Publication invalidates
retained decoded entries before releasing readers; preparation failures also
invalidate them so a poisoned backend cannot be bypassed. Tests cover native
wrapper and segmented publication with actual Storage readers. This internal
experimental step does not reclaim old files or mark scrub completion, and
`runScrub` still uses native execution.

The shared `Generation` query reports the selected rewrite generation after
reopen. Segmented storage reads only the catalog header, without loading the
range list. Appends and reclamation leave this generation unchanged; it is not
proof of scrub authorization or completion. The caller must know which plan it
assigned to that generation.

`Storage.reclaimSharedGeneration` checks that selection before removing retired
managed files under the layout and event locks. It rejects a mismatched generation
and invalidates retained decoded entries on backend errors. Failure leaves
completion bookkeeping untouched. Tests reopen Storage after injected failures
before and after reclamation, then finish reclamation without another rewrite.
This is a reclamation step; the production scrub worker still uses native execution.

For Storage opened with a managed backend, `runPendingScrub`
dispatches to the shared lifecycle in `wal/shared_scrub.go`. It assigns each
rewrite the authorized scrub upper bound as its generation. After publication,
it refreshes event bounds, reclaims retired managed files, reconciles the
delete-key cadence index from surviving records, and runs the existing scrub
completion/tombstone-compaction path. Reconciliation reads actual survivors at
IDs from the durable pending-delete index, using one shared entry cursor. Apply
records those IDs before advancing its watermark; rewriting only removes or
erases deletes, so the index remains a superset of the survivors across restart.
Completion does not walk the full event prefix or rerun the erasure gate on
already-scrubbed metadata. Reconciliation with an empty pending-delete index
requires no event reads.
Missing records and erased keys leave the cadence index; a mixed record remains
if any raw user delete survives. IDs above the scrub bound remain untouched.

On restart, an unfinished selected generation is checked against applied scrub
history and completed before a newer pending request is prepared. No additional
receipt or persisted subject-key list is used. Tests apply real scrub commands,
interrupt calls before publication, after publication with a lost acknowledgment,
and before and after reclamation, supersede pending requests,
and reopen both shared backends. They verify that the older publication is not
rewritten and that retained versus erased delete keys have the correct cadence
bookkeeping. Public `wal.Open` continues to select the native backend.

The background worker also runs against both shared backends in integration tests.
Temporary blockers (protected readers, layout freezes, and a scrub command whose
apply watermark has not advanced yet) trigger a 50 ms retry. Admission checks
avoid repeated selection scans while those blockers remain. Closing Storage stops
this retry wait even with a reader still pinned. Backend failures do not trigger
this timer; their reopen requirement remains intact. Tests also cover automatic
pending-scrub recovery on normal open.

Active shared scrub rewrites and reclamation receive cancellation from Storage's
shutdown signal. Policy and survivor-reconciliation scans check shutdown between
records. The cancellation watcher is joined before each scrub attempt returns;
Storage still joins its worker before closing the backend. Cancellation does not
interrupt a file I/O or atomic publication already in progress. Interrupted work
keeps its pending request, and restart recovery uses the selected generation to
resume. Tests stop real backends during rewrite and reclamation, then reopen and
finish the scrub.

Publication can also precede the enclosing apply batch's saved watermark. Recovery
recognizes matching durable scrub history ahead of that watermark and waits for
Raft replay before completing the selected generation. Missing history remains
an error. A restart regression test covers this window and verifies that replay
finishes the existing publication without another rewrite.



The native scrub's unpublished replacement is written by
`tidwall.LegacyRewrite`: it owns private-log creation, dense survivor numbering,
explicit sync, and sealed-segment compression. `wal` supplies framed survivor
bytes and retains selection, catch-up, directory publication, and cleanup.
Creation rejects a directory with existing append history. Its `CopyRange`
operation owns inclusive native traversal and survivor writes; application
callbacks supply the existing source-read lock lifetime and scrub transform.
Bulk copy and final catch-up retain their existing lock scopes.

The background sealer depends on the optional `eventlog.SealedCompressor`
capability carried by the published binding. `tidwall.LegacyLog` supplies it
through `LegacyCompression`, mapping retired-handle errors to `eventlog.ErrClosed`.
Shared EventLog binding discovers the optional capability by interface. The worker
idles when it is absent and checks the current binding again on the next pass.
Composition captures the capability
under the existing event lock; compression runs outside it, with the existing
layout exclusion. Pacing, retry policy, logging, and shutdown remain in `wal`.
The segmented backend encodes segments when writing and does not expose this
background capability.

Native scrub directory publication uses `tidwall.SwapLegacyDirectories` for the
two renames and restoration of the original live directory on failure. A typed
failure distinguishes an unsuccessful rollback. `wal` retains handle closing,
layout exclusion, directory sync, generation updates, cleanup, and the fatal
policy that prevents reopening a missing live directory after rollback failure.

Native peer adoption delegates sequence alignment, empty-tail removal, file
installation, and moved-file rollback tracking to `eventlog/tidwall`.
The attempt is in-memory bookkeeping only. `wal` retains handle close/reopen,
directory sync, post-install boundary verification, and recovery/fatal policy.

A whole-log catch-up reset delegates native directory removal/recreation to
`tidwall.ResetLegacyDirectory`. Its error distinguishes removal failure from
failure to recreate the removed directory. `wal` retains authorization, layout
exclusion, close/reopen, generation invalidation, and fatal-error policy.

Production startup and all event-log reopens use `tidwall.OpenLegacy`. The
backend owns native option construction and zstd selection; `wal` retains the
configured cache/segment sizes, startup corruption metrics and repair guidance,
and each replacement path's rollback/fatal policy. Opening preserves the existing
native files without introducing a CURRENT file or an ID envelope.

`tidwall.LegacyLog` owns the production native handle and supplies its append,
positioning, transfer, and compression capabilities. The native pointer stays
inside the backend; `wal.Storage` holds a binding containing the owner. Replacement creates a new owner,
and existing capabilities remain attached to the retired handle. Live backup
uses the owner's `BackupSource` capability under the existing layout freeze.

Production append, streaming, exact lookup, and prefix scans use the application-side
`entryStore` contract: a logical appender, independent decoded entry cursors, and
close. The binding keeps native maintenance capabilities separately. The same
production methods are tested against the native owner and both shared EventLog
backends, including replay, applied visibility, temporary EOF, sparse lookup,
prefix bounds, and reader rebinding after replacement. These fixtures exercise
those methods directly. An internal opening hook also runs the real Storage startup,
Raft Save, committed apply, and reopen paths against each backend. These startup
tests use safe mode to hold background maintenance; they cover replay of an
event batch made durable before applied progress was saved. Failed startup closes
all opened logs, including releasing the segmented backend's directory lock.
Public Open still selects tidwall. Segmented native maintenance and peer catch-up
are not exercised by these startup tests.

Production composition and adoption coordination still select the native backend.
These are partial boundaries, not complete backend selection;
there is no production option to open an existing data directory with the
segmented backend.

Native peer-transfer APIs explicitly require the native binding. Shared backends
return `eventlog.ErrUnsupported` for physical sequence reads, native layouts and
segment adoption.
The guard runs before adoption inspects or consumes staged files. Rejection
leaves logical reads/appends and reopening usable. This also applies to the
experimental tidwall wrapper: its CURRENT/generation layout is not the native
peer-transfer format. Native transfer conformance tests retain their existing
byte-for-byte behavior.

`Storage.AppendFetchedRecords` accepts the existing framed peer-record batches
for every backend. It verifies framing and decodes each entry once before any
append. Native storage keeps the original frame; shared backends keep the original
protobuf payload, including unknown fields. Overlap skipping, append serialization,
and event-progress accounting retain the existing receiver behavior. This supports
the record-receive step; shared segment adoption and the
complete catch-up lifecycle remain separate from it.

`Storage.ServeEvents` serves shared backends through the existing framed-record
wire format. It captures generation and original append progress under the event
layout freeze, then streams a bounded batch through a logical cursor. Native
serving retains its whole-segment path. Shared serving preserves protobuf bytes
and adds wire framing directly to the output buffer, without re-marshalling or
allocating a separate frame for each record. Appends can continue during callbacks;
records beyond the captured frontier are excluded.

The existing `LastIndex` field still means the last delivered record. An erased
suffix therefore does not advance receiver progress through that suffix; serving
reports no more surviving records. This path does not install erased append
progress or complete shared-backend snapshot recovery.
Tests cover source/receiver backend combinations, bounded ranges, oversized records,
sparse histories, generation reporting, cancellation, and interrupted streams.

`Storage.appendFetchedEntries` is an internal logical-record receive experiment
shared by native tidwall and both shared backends. It accepts unframed protobuf
bytes only while a catch-up fence is held, validates the entire ordered batch,
and requires the sender's generation to match the selected backend generation.
It preserves payload bytes, skips overlap using original append progress (including
erased records), and updates event progress only after a successful append. It
does not advance applied progress or install snapshot metadata. Shared rewrites
recheck the catch-up fence under append exclusion after policy preparation.
This method has no public transport route and does not implement cross-generation
adoption or whole-file transfer.

`Storage.initializeFetchedGeneration` initializes an empty shared receiver's
storage generation while a catch-up fence is held. It uses the existing atomic
empty-rewrite publication protocol. Changing generations requires no append
history; an entirely erased log still has append
history and is rejected. Same-generation retries are no-ops, including after a
partial receive. The operation does not update applied progress or scrub-completion
metadata. Tests reopen after failures before publication and after lost publication
acknowledgments, then resume without repeating an already-published initialization.
The private storage-only experiment does not publish application completion and
is reopened in safe mode.

The public `SetEventLogGeneration` path supports shared backends through the
existing catch-up generation convention. Under the catch-up fence, it checks that
the backend can adopt the generation, persists that generation in the existing
scrub-completion metadata, and then initializes storage. No additional receipt or
metadata file is created. Startup completes interrupted initialization only for
an empty backend whose selected generation trails that metadata. Existing append
history, including wholly erased history, cannot be relabeled. Applied progress
is preserved independently of the empty event log.

Imported completion may be ahead of this node's pending scrub request, as in
native catch-up. Shared scrub maintenance accepts the matching selected generation
without requiring a locally applied scrub command. Tests cover normal reopen after
partial receipt, interruptions on either side of publication, and retries. Shared
erased-suffix progress and the complete snapshot-install lifecycle are not
integrated by this generation-adoption path.

The matching internal `Storage.fetchEntries` returns bounded batches of original
protobuf payloads, with the selected generation, captured append frontier, resume
index, and an explicit completion flag. One publication read lock covers the
batch; concurrent appends cannot extend its captured upper bound. Sparse gaps
advance coverage without inventing records. Record count is a hard bound; the
first record may exceed the byte budget so an oversized record still makes
progress. A separate validated record cursor preserves original bytes and leaves
the decoded streaming-reader interface unchanged. Tests transfer records across
all native/shared backend pairs and reopen receivers to verify byte preservation.
The batch reports erased-tail append progress, but receiving an empty batch does
not install that accounting or application snapshots.
Concurrency tests pause serving between records to verify that new appends remain
outside the captured frontier and rewrite publication waits for the entire batch.
Returned payloads remain intact after publication and reclamation. Concurrent
retries of an identical receive batch serialize into one backend append.



## Implementations

**segmented** delegates to pkg/segmentlog. It retains sparse IDs and stable ranges,
rewrites only changed files, persists erased append accounting, and translates its
results/errors into the shared contract. `Wrap` transfers ownership of an existing
managed engine; `Create` and `Open` use the bbolt catalog. Recovery checks metadata
boundaries and the active tail without scanning historical payloads. Reclaim
drains committed retirement records. Explicit full verification and orphan
sweeping are available on the underlying segment engine. See the
[catalog contract](../../../../pkg/segmentlog/bbolt-experiment.md).

Cache budgets are backend-specific runtime options: `Create` accepts
`LogOptions.Cache`, and `Open` accepts an optional third `segmentlog.CacheOptions`
argument. Reopening must supply the budgets again; omission disables caching.
An enabled cache keeps the active tail resident, retains rollover contents in
recent-segment order, and loads historical misses into strict segment-acquisition
LRU. The active tail is additional to the two sealed-segment byte budgets.
See [cache ownership and accounting](../../../../pkg/segmentlog/segment-cache.md).

**tidwall** uses dense physical sequences internally and binary-searches their
stable record IDs. It prepares a complete replacement generation, synchronizes it,
then switches a checksummed CURRENT through the shared durablefs publisher. Old
generations remain until explicit Reclaim. It persists original append progress
and the replacement's prefix record count, so an entirely erased log can reopen
without reusing prior indexes. It verifies records when opening, scans both plain
and compressed files, and enforces exclusive directory ownership.

The tidwall implementation uses a new, explicitly created **experimental wrapper
layout**; it is not a drop-in opener for production `events/` directories. Its
opaque-record envelope stores the stable ID separately, so the backend need not
interpret protobuf to translate a tidwall sequence. See [its format](tidwall/README.md).

## Integration status

The node selects native tidwall by default or with
`COMMITTED_EVENT_LOG_BACKEND=tidwall`. Setting it to `segmented` selects the
segmented engine through `wal.WithSegmentedEventLog`. Unknown names fail startup.
The supported setup uses one backend per cluster, and existing data is reopened with that same
backend. Their on-disk formats are not interchangeable. Raft's own logs are outside
this contract.

Production `wal.Storage` uses `eventLogBinding` for append/replay, application
reads, scrub, backup/restore, peer catch-up, and maintenance capabilities. Native
tidwall retains its production layout through `tidwall.LegacyLog`; the separate
CURRENT-based tidwall `EventLog` wrapper is an experimental helper, not the
node's tidwall backend. The private `wal/eventLogAdapter` is also a test helper,
not the production binding.

The node configures segmented storage with zstd compression and separate recent
and historical cache budgets, each defaulting to 160 MiB. Configure them with
`COMMITTED_EVENT_CACHE_RECENT_BYTES` and
`COMMITTED_EVENT_CACHE_HISTORICAL_BYTES`; zero disables the corresponding budget.
Native tidwall uses `COMMITTED_EVENT_CACHE_SEGMENTS`. These are cache budgets,
not total process-memory limits. The database's background sealer drives closed
segment compression; physical compression preserves the logical scrub generation.

Validation includes homogeneous cluster lifecycle tests for replication, restart,
scrub, and catch-up, plus compressed backup/restore and peer catch-up tests against
both backends. Shared workload measurements cover
[backup bytes and unchanged files](../wal/backup-workload-results.md),
[streaming runtime](../wal/streaming-workload-results.md), and
[cache pressure](../wal/cache-pressure-results.md). These use small synthetic
histories; they do not establish performance at 100 TB or under cold-disk load.

### Legacy copy experiment

`wal.Storage.copyEventLog` copies an opened production-layout permanent log into
a privately owned, fresh EventLog for adapter experiments. The
application layer verifies legacy checksums, protobuf indexes, strict ordering,
and agreement with the source's original append frontier. It preserves exact
protobuf bytes (including unknown fields) while the selected backend supplies its
own framing. Physical tidwall sequences never become destination record IDs.

The copy takes the source's exclusive event lock for its entire duration; appends
and directory swaps wait. Callers must exclude Storage.Close. Memory retained in
the copy batches is bounded to 256 records / 1 MiB of payload, with a larger
individual record sent alone; backend read/decompression caches are additional.
Cancellation is observed between operations, not during lock waits or fsync.

Any copy error invalidates the entire destination, even if some batches are durable.
The helper rejects destinations with previous append history, including completely
erased logs, and does not resume partial copies. Only a successful return signals
completion to the caller; reopening a destination alone is not a completion
certificate. Tests cover both source compression modes and both destination
backends/codecs, reopen and committed replay, corrupt history, head mismatches,
batch limits, cancellation, and append failure after a durable prefix.

## Validation

Shared conformance tests cover both backends in plain/zstd modes: strict batches,
sparse reads, ownership, byte ownership, callback counts, partial/all/no-op rewrites,
erased append progress, reopen/reclaim, and callback-failure recovery. Tidwall
fault tests inject CURRENT failures before/after publication and check recovery
selects a complete old/new generation. The existing segmentlog and moved durablefs
suites retain their deeper fault tests. These tests do not establish filesystem
power-loss behavior or prove that an incomplete tail is safe to discard.

`TestEventLogRepeatedHistory` compares both backends with an independent in-memory
history model in plain and zstd modes. Three fixed seeds each run eight cycles of
sparse appends, partial replacements, erasure, no-op rewrites, reclamation, and
reopen. Checks compare survivor bytes, exact and forward lookups, bounded scans,
and original append progress. Invalid batches and stale generations are rejected.
Injected callback failures after a changed record check that reopening preserves
the old complete history and that failed preparation does not consume a generation.
The segmented conformance and history cases also run with recent-only,
historical-only, and combined caches in both compression modes. Small byte budgets
exercise eviction and oversized entries throughout rewrite/reopen cycles.

Application tests cover cached Actual decoding, applied visibility, and protected
reader/rewrite coordination. A streaming test runs 100 independent Actual readers
alongside appends: 96 begin near the head and four read from the beginning, across
repeated rollovers with bounded caches. It checks each cursor receives its expected
indexes in order. These are correctness workloads, not throughput or backup-size
benchmarks; managed engine operations still serialize under the log mutex.

```sh
go test -race ./internal/cluster/db/eventlog/... ./internal/durablefs/... ./pkg/segmentlog/...
go test -race ./internal/cluster/db/wal -run '^(TestEventLogAdapter|TestEventLogCopy|TestSegment|TestScrub_)'
```

## History benchmarks

[Recorded history benchmarks](history-benchmarks.md) compare append, reopen, and
middle-seek costs at two logical history sizes for both backends in plain/zstd
modes. They report local timings and allocations with explicit fixture and cache
limitations. The recorded baseline reverified full history during rotation.
Those results predate the bbolt catalog and do not describe current recovery costs
or establish production throughput.

The [on-disk lifecycle baseline](scale-benchmarks.md) measures larger histories
through recovery, a boundary append, one-record erasure, reclamation, and a full
scan. It includes survivor and post-reopen checks and reports phase costs
separately from history construction.

The [sustained rollover comparison](rollover-benchmarks.md) detects actual
segment creation in both plain backends and measures boundary batches separately
from ordinary append batches, with identical logical histories and batch sizes.

The [full-sized cache read benchmark](cache-benchmarks.md) compares warmed tail,
recent-range, and historical reads with 20 MiB segment targets. It records cache
budgets, allocation costs, and the experimental tidwall wrapper's cache and
lookup limitations separately from production behavior.

The [shared Actual-reader benchmark](../wal/shared-reader-benchmarks.md) compares
the production tidwall reader with the new engine through `db.ActualReader`,
using identical protobuf records. This includes production tidwall's persistent
sequence cursor rather than the experimental wrapper's per-record binary search.

## Per-reader cursors

`EventLog.NewCursor` creates an independent cursor implementing `Seek` and `Close`.
Seek accepts the desired logical ID on every call, so unapplied records and decode
failures can be retried without committing reader progress. Backward seeks are
also supported. EOF is temporary; subsequent appends can become visible.

The segmented cursor retains immutable cached contents and a record offset, or
references the resident active tail under the log lock. Within a retained range,
reads avoid repeated catalog lookup and cache acquisition. With caching disabled,
the cursor uses ordinary file-backed seeks. The experimental tidwall cursor keeps
a dense sequence within a rewrite generation. Neither cursor pins a generation;
successful rewrites invalidate position hints before another record is returned.

The application Actual reader owns one cursor and exposes Close to release its
retained contents. Protected-reader Close releases both generation protection and
the cursor. Callers still own logical progress, applied visibility, and decoding.
Cursor operations and Close are synchronized; the parent log must remain open.

## Backup capture and restore

`BackupSource` streams backend-relative filenames, exact lengths, and file bytes.
Both shared backends and the production native owner implement it. `wal.Storage`
retains node-wide capture ordering: application metadata, Raft state, event log,
then Raft entries. The event layout freeze covers the event-log capture step.
Archive checksums, staging, and restore use the existing backup package. Restore
preserves the source backend's format; reopening uses that same backend.

Segmented capture spools its bbolt catalog and captures the active tail length
under the log mutex. It streams selected files from the private catalog, excluding
retired payloads. Maintenance waits throughout capture, while appends and rollover
can continue after spooling. Memory does not grow with the number of references,
and the live catalog has no read transaction held across archive streaming.
The experimental dense tidwall wrapper serializes capture with its other operations;
the production native owner permits appends under the application's layout freeze.

Tests round-trip live and offline node archives through restore and the same
backend opener. Segmentlog tests also cover rollover during capture, sparse and
fully erased histories, preserved append progress, and visitor failure cleanup.

## Reset and refetch

`EventLog.Reset` selects an empty history, clearing surviving records and original
append progress, including fully erased history. Generation and configuration
are preserved. It differs from rewrite, which retains original append progress.
The segmented backend publishes an empty tail and retires old references in one
bbolt transaction. The dense tidwall wrapper publishes an empty directory through
CURRENT. Neither operation decodes the old payloads. Retired files remain for
explicit reclamation. Cursors invalidate positions from the previous selection.

`Storage.ResetEventLog` dispatches shared reset through this capability and keeps
the native directory-reset implementation. Both serialize with appends, respect
the existing event layout freeze, and clear event bounds while retaining applied
progress and generation metadata. Shared reset reclaims retired payloads before
returning. Generation adoption also drains retirements, covering an interruption
after reset publication but before cleanup. A receiver can reopen after reset, adopt its
peer's newer generation, and receive entries below its former append frontier.
Tests cover these transitions, cached cursors, erased histories, and interruption
before/after publication. The segmented crash test also exits inside the catalog
transaction and verifies that reopening retains the old selection.
