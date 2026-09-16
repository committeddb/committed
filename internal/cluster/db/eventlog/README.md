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

## Implementations

**segmented** delegates to pkg/segmentlog. It retains sparse IDs and stable ranges,
rewrites only changed files, persists erased append accounting, and translates its
results/errors into the shared contract. `Wrap` transfers ownership of an existing
managed engine; `Create` and `Open` are also available.

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
These are small correctness workloads, not throughput or backup-size benchmarks.

```sh
go test -race ./internal/cluster/db/eventlog/... ./internal/durablefs/... ./pkg/segmentlog/...
go test -race ./internal/cluster/db/wal -run '^(TestEventLogAdapter|TestEventLogCopy|TestSegment|TestScrub_)'
```

## History benchmarks

[Recorded history benchmarks](history-benchmarks.md) compare append, reopen, and
middle-seek costs at two logical history sizes for both backends in plain/zstd
modes. They report local timings and allocations with explicit fixture and cache
limitations. Segmented rotation and reopen currently verify the full referenced
history; these measurements do not establish production throughput.

The [on-disk lifecycle baseline](scale-benchmarks.md) measures larger histories
through recovery, a boundary append, one-record erasure, reclamation, and a full
scan. It includes survivor and post-reopen checks and reports phase costs
separately from history construction.
