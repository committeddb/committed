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

The dependency direction is `wal` → `eventlog.EventLog` ← concrete backends.
The segmented backend then depends on `pkg/segmentlog`. Shared experimental adapter source
imports neither concrete backend. Fixtures choose which backend to provide.
Production append and exact-lookup composition explicitly choose the legacy tidwall adapter.

Inside `wal`, the experimental application layer is organized by responsibility:

| Files | Responsibility |
| --- | --- |
| `eventlog_adapter.go` | Shared adapter state, ownership, and lock ordering |
| `eventlog_entries.go` | Raft protobuf validation and stable record identity |
| `eventlog_append.go` | Append progress and committed-batch replay |
| `eventlog_lookup.go`, `eventlog_scan.go` | Raw entry lookup and consistent scans |
| `eventlog_reader.go`, `eventlog_actual.go` | Actual decoding and applied visibility |
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
The adapter is experimental; production `Storage` still owns its existing tidwall
integration. See [the adapter's current behavior](../wal/eventlog_adapter.md).

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
`eventlog.Appender`, the write subset embedded by `EventLog`. Composition lives
in `wal/legacy_event_appender.go`. The default is `tidwall.LegacyAppender`, which
wraps the existing production handle and assigns its dense physical sequences.
The supplied codec preserves the existing checksum envelope and Raft-entry bytes;
no CURRENT file or experimental generation directory is introduced.

The writer is rebound when scrub or peer fetch replaces the native handle.
Application replay filtering, applied progress, and metrics remain in Storage.
Production `Storage.ActualAt` uses `eventlog.Lookup`, the exact-read subset of
EventLog. `wal/legacy_event_lookup.go` binds a `tidwall.LegacyLookup` to the
current handle for each call. The backend binary-searches stable IDs; the
application supplies checksum/protobuf decoding and interprets the result in
`wal/actual_lookup.go`. The existing event lock spans lookup and proposal decoding.
Exact replay still includes metadata and does not gate reads on AppliedIndex.
The native lookup decodes search probes to discover IDs; the application then
decodes the returned payload to interpret the matched entry. The backend copies
the matched payload to honor caller ownership, including with native NoCopy enabled.

Production streaming `Reader` uses the application-side `wal.entryCursor`:
`SeekGE`, `Current`, `Advance`, and `Close`. Seeking is lazy; Current reports
storage errors and retains the decoded entry until consumption. Repeated Current
calls reuse that entry. EOF remains temporary.

Composition in `wal/legacy_event_cursor.go` binds native tidwall positioning to
the current handle and scrub generation. The generic `tidwall.LegacyCursor`
carries the application codec's decoded result through binary search and sequential
positioning without copying the payload or interpreting protobuf. Production
streaming decodes each sequential entry once; retries reuse the decoded entry.
Native tidwall's existing read allocation remains.

The experimental reader uses the same entry-cursor contract over its raw backend
cursor, decoding each selected record once. The existing publication gate
invalidates retained decoded entries before releasing readers. The application
retains applied visibility, type resolution, filtering, and the event read lock
through interpretation. Reader.Close releases its cursor and waits for an
in-flight read.

Production recovery, scrub swaps, and peer-copy operations still use native tidwall
access. These are partial boundaries, not complete backend selection;
there is no production option to open an existing data directory with the
segmented backend.

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

The experimental `wal/eventLogAdapter` now consumes EventLog, and the same Actual
reader, exact lookup, committed replay, snapshot selection/rewrite, and protected
read logic runs with either implementation. Existing segment-specific experiments
still use the segmented factory where they measure its particular file layout.

Production `wal.Storage` has not switched to this interface. Its legacy directory
swap, Raft/BoltDB recovery, backups, catch-up, and format gates remain unchanged.
There is no backend setting or automatic experimental format activation at
startup. Raft's own logs are outside this contract.

### Legacy copy experiment

`wal.Storage.copyEventLog` provides the first conversion primitive: copy an opened
production-layout permanent log into a privately owned, fresh EventLog. The
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
