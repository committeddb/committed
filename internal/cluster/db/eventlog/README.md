# Permanent event-log boundary

The primary interface is **`EventLog`**, defined in **`eventlog.go`**.

```text
eventlog/
    eventlog.go       Shared storage contract and neutral types/errors
    segmented/        Implementation over pkg/segmentlog

wal/eventlog_*.go      Experimental application adapter and reader/scrub coordination
pkg/segmentlog/       Application-independent segmented engine
internal/durablefs/   Shared publication, directory sync, and ownership primitives
```

## Code boundaries

Production `wal.Storage` holds its event log through `eventLogBinding`
(`wal/event_log_binding.go`). The binding supplies application entry cursors
over an `eventlog.EventLog` and the backend's optional capabilities (backup
capture, sealed-segment compression). Its one implementation is
`eventlog/segmented` over `pkg/segmentlog`. Application visibility, replay, and
scrub policy stay in `wal`.

The separate private `eventLogAdapter` experiment depends only on
`eventlog.EventLog`. It is not wired into production `Storage`.

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

These files remain in `wal` because they share production policy helpers such as
`userTopicEntities`, `newMetadataSelection`, and `scrubFilterEntry`, along with
production error identities. Moving them into a storage backend would give that
backend responsibility for Committed's application semantics.

`eventlog_fixture_test.go` supplies backend-neutral records and type resolution.
`eventlog_backends_test.go` supplies the backend matrix (segmented, with and
without caches). `eventlog_segmented_fixture_test.go` explicitly constructs a
segmented adapter fixture for application-policy integration tests. Those tests
use `eventlog_` names; the physical churn experiments retain `segment_churn_`
names. See [the adapter's current behavior](../wal/eventlog_adapter.md).

## Responsibilities

| Layer | Responsibility |
| --- | --- |
| Application adapter | Protobuf serialization/identity checks, applied visibility, Actual decoding, replay policy, scrub selections/gates, protected multi-call reads |
| EventLog | Strict append, original append progress, exact/forward/range reads, atomic generation rewriting, reclamation, ownership and close |
| Concrete backend | Physical range translation, framing, persistence, publication, recovery, file retirement |

The contract uses independent Record, Coverage, Transform, RewriteResult, and
ReclaimResult types. It does not expose segment paths, block formats, protobuf
types, or a backend's physical rewrite statistics. Errors have common identities
for invalid input, absence, corruption, closed/poisoned handles, ownership, and
unsupported storage. Backend errors remain available as wrapped causes. Rewrites
report observed changed records and confirmed publication; false Published with
an error requires reopen to resolve uncertainty.

Callbacks hold a consistent storage view and must not reenter the log. Multi-call
application protections live above the interface: shared adapter locks defer
rewrites while protected readers remain. This is not a backup-capture/file-pin API.

## Production append and lookup wiring

Production `wal.Storage.appendEvent` and `appendEvents` submit logical Records to
`eventlog.Appender`, the append/progress subset embedded by `EventLog`.
Its `LastAppended` reports logical progress and distinguishes empty history
without exposing physical positions. Records carry the unframed Raft-entry
protobuf bytes; the backend supplies its own framing. Local and fetched appends
share `eventAppendMu`, so append ordering and application progress updates
serialize. Overlap filtering and write metrics remain in `wal`.

The writer and cursor factory belong to one `wal.eventLogBinding`
(`bindEventLog`, with entry decoding in `wal/event_entries.go`); replacement
swaps that complete binding. Storage has no separate writer cache.
Application replay filtering, applied progress, and metrics remain in Storage.
Production `Storage.ActualAt` uses a private `wal.entryCursor` for exact
lookup. `wal/actual_lookup.go` verifies that the seek result matches the requested
index and interprets the decoded entry. The existing event lock spans positioning
and proposal decoding. Exact replay includes metadata and does not gate reads on
AppliedIndex. The experimental adapter uses the same exact-entry and proposal
helpers, retaining its applied-watermark check.

Production streaming `Reader` uses the application-side `wal.entryCursor`:
`SeekGE`, `Current`, `Advance`, and `Close`. Seeking is lazy; Current reports
storage errors and retains the decoded entry until consumption. Repeated Current
calls reuse that entry. EOF remains temporary. `wal/production_event_cursor.go`
tracks the published binding and scrub generation; a change in either rebinds
the cursor. Production streaming decodes each sequential entry once; retries
reuse the decoded entry.

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
appends can continue during selection.

Startup and post-rewrite logical boundary recovery use the binding's
`LastAppended` and a cursor for the first survivor, in `wal/event_bounds.go`.
Recovered progress includes erased records; an entirely erased history has no
first survivor but keeps its append frontier. Both bounds are read successfully
before either is published. Early startup recovery restores only append progress
for the snapshot guard. Tests cover sparse history and failures, plus reopening
after removing the tail or all records without rewinding recovered progress.

Data-head recovery uses the binding's bounded `ScanReverse`.
`wal/data_head_recovery.go` retains user-data classification, the 4,096-record cap,
and warning policy. A persisted data head bypasses this fallback. The binding
decodes each delivered record once; the segmented backend delegates to
`pkg/segmentlog`, including its bounded suffix handling for unindexed files.
Common tests cover ordering, early stop, cancellation, rewrite, and reopen.
Application tests cover the exact recovery cap and corruption stops.

## Scrub

Production scrub preparation lives in `wal/scrub_plan.go`: it captures RTBF and
metadata selections plus the authorized delete-key erasure threshold in one plan.
The plan supplies the record transform and retains the erasure outcome needed
after completion.

`Storage.rewriteSharedPlan` (`wal/shared_event_rewrite.go`) publishes a prepared
plan using the production reader lock and cursor generation. It defers for
existing protected reads or layout freezes, or a catch-up in progress, and holds
new protected-read registration until the step returns. Publication invalidates
retained decoded entries before releasing readers; preparation failures also
invalidate them so a poisoned backend cannot be bypassed. The step does not
reclaim old files or mark scrub completion.

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

`runPendingScrub`
dispatches to the shared lifecycle in `wal/shared_scrub.go`. It assigns each
rewrite the authorized scrub upper bound as its generation. It seals the
active range (`EventLog.Seal`) under `eventAppendMu`, so every record through
the bound is sealed, then releases it and runs
`RewriteSealedWithPublicationLock`: applied events keep appending while the
sealed history is rewritten, waiting only for publication. Catch-up's reset,
generation adoption, fetched appends and reclamation wait on `eventRewriteMu`
instead. After publication,
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
and reopen the backend. They verify that the older publication is not
rewritten and that retained versus erased delete keys have the correct cadence
bookkeeping.

The background worker also runs against the segmented backend in integration tests.
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

## Sealed-segment compression

The background sealer (`wal/sealer.go`) depends on the optional
`eventlog.SealedCompressor` capability carried by the published binding;
`bindEventLog` discovers it by interface. The segmented backend implements it:
each call compresses one plain sealed segment and reclaims the replaced file.
The worker idles when no work remains and checks the current binding again on
the next pass. Compression runs outside the event lock, with the existing layout
exclusion. Pacing, retry policy, logging, and shutdown remain in `wal`.
Physical compression preserves the logical scrub generation.

## Peer catch-up

Peer catch-up transfers records only. `Storage.ServeEvents` serves the framed
record wire format (`wal/peer_record_serve.go`). It captures generation and
original append progress under the event layout freeze, then streams a bounded
batch through a logical cursor. Serving preserves protobuf bytes and adds wire
framing directly to the output buffer, without re-marshalling or allocating a
separate frame for each record. Appends can continue during callbacks; records
beyond the captured frontier are excluded. A peer that sends whole segment files
instead is refused with `db.ErrPeerSentSegmentFiles` (`db/catchup.go`).

The existing `LastIndex` field still means the last delivered record. An erased
suffix therefore does not advance receiver progress through that suffix; serving
reports no more surviving records. This path does not install erased append
progress or complete snapshot recovery.
Tests cover bounded ranges, oversized records, sparse histories, generation
reporting, cancellation, and interrupted streams.

`Storage.AppendFetchedRecords` accepts framed peer-record batches. It verifies
framing and decodes each entry once before any append, keeping the original
protobuf payload, including unknown fields. Overlap skipping, append
serialization, and event-progress accounting follow the receiver's existing
behavior; it waits on `eventRewriteMu` behind an active scrub rewrite.

`Storage.appendFetchedEntries` is an internal logical-record receive experiment.
It accepts unframed protobuf bytes only while a catch-up fence is held,
validates the entire ordered batch, and requires the sender's generation to
match the selected backend generation. It preserves payload bytes, skips overlap
using original append progress (including erased records), and updates event
progress only after a successful append. It does not advance applied progress or
install snapshot metadata. Shared rewrites recheck the catch-up fence under
append exclusion after policy preparation. This method has no public transport
route and does not implement cross-generation adoption.

`Storage.initializeFetchedGeneration` initializes an empty receiver's
storage generation while a catch-up fence is held. It uses the existing atomic
empty-rewrite publication protocol. Changing generations requires no append
history; an entirely erased log still has append
history and is rejected. Same-generation retries are no-ops, including after a
partial receive. The operation does not update applied progress or scrub-completion
metadata. Tests reopen after failures before publication and after lost publication
acknowledgments, then resume without repeating an already-published initialization.

The public `SetEventLogGeneration` path uses the catch-up generation convention.
Under the catch-up fence, it checks that the backend can adopt the generation,
persists that generation in the existing scrub-completion metadata, and then
initializes storage. No additional receipt or metadata file is created. Startup
completes interrupted initialization only for an empty backend whose selected
generation trails that metadata. Existing append history, including wholly erased
history, cannot be relabeled. Applied progress is preserved independently of the
empty event log.

Imported completion may be ahead of this node's pending scrub request. Scrub
maintenance accepts the matching selected generation without requiring a locally
applied scrub command. Tests cover normal reopen after partial receipt,
interruptions on either side of publication, and retries. Erased-suffix progress
and the complete snapshot-install lifecycle are not integrated by this
generation-adoption path.

The matching internal `Storage.fetchEntries` returns bounded batches of original
protobuf payloads, with the selected generation, captured append frontier, resume
index, and an explicit completion flag. One publication read lock covers the
batch; concurrent appends cannot extend its captured upper bound. Sparse gaps
advance coverage without inventing records. Record count is a hard bound; the
first record may exceed the byte budget so an oversized record still makes
progress. A separate validated record cursor preserves original bytes and leaves
the decoded streaming-reader interface unchanged. Tests transfer records and
reopen receivers to verify byte preservation.
The batch reports erased-tail append progress, but receiving an empty batch does
not install that accounting or application snapshots.
Concurrency tests pause serving between records to verify that new appends remain
outside the captured frontier and rewrite publication waits for the entire batch.
Returned payloads remain intact after publication and reclamation. Concurrent
retries of an identical receive batch serialize into one backend append.

## Implementation

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

## Integration status

The node runs only the segmented engine: `wal.Open` binds it by default, and
`wal.WithSegmentedEventLog` configures it. Its opener refuses a non-empty
directory holding no segmented files and names `committed wal convert`
(`wal.ConvertEventLog`), which rewrites a stopped node's tidwall-format event
log as a segmented one. The converter reads the old log with a small read-only
reader (`wal/tidwall_event_reader.go`) and does not open Storage;
`committed restore` and `committed recover` run it first. Offline tools
(`openStopped`) require a segmented event log. Raft's own logs remain on
tidwall/wal and are outside this contract.

The node configures segmented storage with zstd compression and separate recent
and historical cache budgets, each defaulting to 160 MiB. Configure them with
`COMMITTED_EVENT_CACHE_RECENT_BYTES` and
`COMMITTED_EVENT_CACHE_HISTORICAL_BYTES`; zero disables the corresponding budget.
These are cache budgets, not total process-memory limits.

Validation includes cluster lifecycle tests for replication, restart, scrub, and
catch-up, plus compressed backup/restore and peer catch-up tests. Workload
measurements cover
[backup bytes and unchanged files](../wal/backup-workload-results.md),
[streaming runtime](../wal/streaming-workload-results.md), and
[cache pressure](../wal/cache-pressure-results.md). These use small synthetic
histories; they do not establish performance at 100 TB or under cold-disk load.

`TestCompressedScrubbedPeerSnapshotBackends` covers normal snapshot installation
and reopening after the snapshot was saved but not yet installed. In both cases,
the receiver verifies fetched history, appends, reopens, and scrubs another fetched
record. A further reopen checks the new generation, erased records, all surviving
user payloads, and the post-catch-up write. These cases run in the untagged test
suite, including CI's race job.

## Validation

Conformance tests cover the segmented backend in plain/zstd modes: strict batches,
sparse reads, ownership, byte ownership, callback counts, partial/all/no-op rewrites,
erased append progress, reopen/reclaim, and callback-failure recovery. The
segmentlog and durablefs suites retain their deeper fault tests. These tests do
not establish filesystem power-loss behavior or prove that an incomplete tail is
safe to discard.

`TestEventLogRepeatedHistory` compares the backend with an independent in-memory
history model in plain and zstd modes. Three fixed seeds each run eight cycles of
sparse appends, partial replacements, erasure, no-op rewrites, reclamation, and
reopen. Checks compare survivor bytes, exact and forward lookups, bounded scans,
and original append progress. Invalid batches and stale generations are rejected.
Injected callback failures after a changed record check that reopening preserves
the old complete history and that failed preparation does not consume a generation.
The conformance and history cases also run with recent-only, historical-only, and
combined caches in both compression modes. Small byte budgets exercise eviction
and oversized entries throughout rewrite/reopen cycles.

Application tests cover cached Actual decoding, applied visibility, and protected
reader/rewrite coordination. A streaming test runs 100 independent Actual readers
alongside appends: 96 begin near the head and four read from the beginning, across
repeated rollovers with bounded caches. It checks each cursor receives its expected
indexes in order. These are correctness workloads, not throughput or backup-size
benchmarks; managed engine operations still serialize under the log mutex.

```sh
go test -race ./internal/cluster/db/eventlog/... ./internal/durablefs/... ./pkg/segmentlog/...
go test -race ./internal/cluster/db/wal -run '^(TestEventLogAdapter|TestSegment|TestScrub_)'
```

## History benchmarks

The benchmarks below run the segmented engine; their recorded result tables also
contain tidwall-engine rows.

[Recorded history benchmarks](history-benchmarks.md) measure append, reopen, and
middle-seek costs at two logical history sizes in plain/zstd modes. They report
local timings and allocations with explicit fixture and cache limitations. The
recorded baseline reverified full history during rotation. Those results predate
the bbolt catalog and do not describe current recovery costs or establish
production throughput.

The [on-disk lifecycle baseline](scale-benchmarks.md) measures larger histories
through recovery, a boundary append, one-record erasure, reclamation, and a full
scan. It includes survivor and post-reopen checks and reports phase costs
separately from history construction.

The [sustained rollover benchmark](rollover-benchmarks.md) detects actual
segment creation and measures boundary batches separately
from ordinary append batches.

The [full-sized cache read benchmark](cache-benchmarks.md) measures warmed tail,
recent-range, and historical reads with 20 MiB segment targets. It records cache
budgets and allocation costs.

The [shared Actual-reader benchmark](../wal/shared-reader-benchmarks.md) measures
the production Reader and the experimental adapter's Actual reader through
`db.ActualReader`, both over the segmented engine, using identical protobuf
records.

## Per-reader cursors

`EventLog.NewCursor` creates an independent cursor implementing `Seek` and `Close`.
Seek accepts the desired logical ID on every call, so unapplied records and decode
failures can be retried without committing reader progress. Backward seeks are
also supported. EOF is temporary; subsequent appends can become visible.

The segmented cursor retains immutable cached contents and a record offset, or
references the resident active tail under the log lock. Within a retained range,
reads avoid repeated catalog lookup and cache acquisition. With caching disabled,
the cursor uses ordinary file-backed seeks. The cursor does not pin a generation;
successful rewrites invalidate position hints before another record is returned.

The application Actual reader owns one cursor and exposes Close to release its
retained contents. Protected-reader Close releases both generation protection and
the cursor. Callers still own logical progress, applied visibility, and decoding.
Cursor operations and Close are synchronized; the parent log must remain open.

## Backup capture and restore

`BackupSource` streams backend-relative filenames, exact lengths, and file bytes.
The segmented backend implements it. `wal.Storage` retains node-wide capture
ordering: application metadata, Raft state, event log, then Raft entries. The
event layout freeze covers the event-log capture step. Archive checksums,
staging, and restore use the existing backup package. Restore converts a
tidwall-format event log before opening; a segmented one reopens as captured.

Segmented capture spools its bbolt catalog and captures the active tail length
under the log mutex. It streams selected files from the private catalog, excluding
retired payloads. Maintenance waits throughout capture, while appends and rollover
can continue after spooling. Memory does not grow with the number of references,
and the live catalog has no read transaction held across archive streaming.

Tests round-trip live and offline node archives through restore and the segmented
opener. Segmentlog tests also cover rollover during capture, sparse and fully
erased histories, preserved append progress, and visitor failure cleanup.

## Reset and refetch

`EventLog.Reset` selects an empty history, clearing surviving records and original
append progress, including fully erased history. Generation and configuration
are preserved. It differs from rewrite, which retains original append progress.
The segmented backend publishes an empty tail and retires old references in one
bbolt transaction, without decoding the old payloads. Retired files remain for
explicit reclamation. Cursors invalidate positions from the previous selection.

`Storage.ResetEventLog` dispatches through this capability. It serializes with
appends and active scrub rewrites, respects the existing event layout freeze, and
clears event bounds while retaining applied progress and generation metadata.
Reset reclaims retired payloads before returning. Generation adoption also drains
retirements, covering an interruption after reset publication but before cleanup.
A receiver can reopen after reset, adopt its peer's newer generation, and receive
entries below its former append frontier. Tests cover these transitions, cached
cursors, erased histories, and interruption before/after publication. The
segmented crash test also exits inside the catalog transaction and verifies that
reopening retains the old selection.
