# Experimental segment event-log adapter

`segmentEventLog` connects Committed's serialized `raftpb.Entry` records to
`pkg/segmentlog`. It is package-private and exercised only by tests. `Storage`
continues to use tidwall; there is no configuration switch, conversion command,
or automatic format activation.

## Boundary

- Input is unframed Entry protobuf bytes. Existing tidwall data must pass through
  checksum verification (`unframe`) before use. The new engine supplies its own
  record and block integrity checks; the old checksum envelope is not duplicated.
- Each storage ID is the embedded Raft index. Append validates the full batch
  before writing; read and rewrite validate outer/inner identity. Zero is invalid
  for Committed entries, although the generic engine supports it.
- Original protobuf bytes are retained, including unknown fields. An unchanged
  record is not decoded and re-encoded merely to store it.
- Exact lookup and seek-at-or-after retain sparse indexes. Missing records return
  `segmentlog.ErrNotFound`; malformed entries, mismatched indexes, and underlying
  segment corruption are classified with `ErrCorruptEntry`.
- Rewrite accepts a raw-entry transformation with the existing `scrubFilterEntry`
  signature. It validates both the source and every surviving replacement. It
  cannot hide a corrupt source by deleting it or change a surviving record's ID.

The caller owns the underlying Log and Close. Storage errors retain the engine's
poison/reopen rules. A failed append can leave a durable prefix; this adapter does
not apply entries to BoltDB or update application visibility.

## Evidence and limits

Tests write the same entries to tidwall and segmentlog and compare decoded raw
bytes. They apply the existing scrub filter to the legacy stream and compare its
output with a whole-log segment rewrite after reclamation and reopen. Cases cover
control/no-op entries, protobuf unknown fields, sparse Raft indexes, complete and
partial removal, supplied metadata-supersession selections, delete-key erasure,
retained request metadata, and idempotent repeated scrubs. Negative tests cover
invalid batches, mismatched source IDs, and attempted replacement-ID changes.

This remains an isolated adapter experiment, not a completed production scrub. The selections supplied to the filter must
already be bounded and authorized. The adapter does not compute selections,
check consumer progress, update BoltDB, or declare erasure complete.

## Recovered append progress and committed replay

`eventIndex` reports the original durable append frontier, including erased
records, and returns zero for a fresh log. It uses `Log.LastAppended`, which
recovers the frontier from tail accounting or the last sealed range boundary.
It cannot be derived from the last surviving protobuf entry.

`appendCommittedRaw` validates an entire strictly increasing batch, including
entries it will skip. It appends only indexes above recovered progress and returns
the resulting frontier. Empty or fully replayed batches make no writes. Replay
and frontier selection share the adapter's mutation lock, so concurrent calls
cannot append the same suffix twice. The strict `appendRaw` operation remains
available and still rejects duplicates.

This method is only for replay of the same already-committed history. It does not
compare skipped payloads (some have been erased), detect divergent histories, or
provide a migration/import conflict check. It does not advance AppliedIndex or
skip the application's necessary metadata replay. An error provides no usable
returned frontier; storage failure requires close/reopen before retry, which can
recover complete groups from an unacknowledged call. Incomplete suffixes continue
to fail without truncation.

Tests erase every record, reclaim and reopen, then prove replay cannot restore
those records. They check byte-identical no-op replay, malformed/reordered skipped
prefixes, concurrent identical batches, and replay refusal on a closed handle.
Engine tests cover ID zero, nonzero empty starting bounds, erased sealed ranges
with an empty tail, and progress recovered after a failed sync.

## Experimental Actual reader

`readerAt` now returns an implementation of `db.ActualReader` that resumes strictly
after a Raft-index checkpoint. It skips control/no-op entries and filters internal
entities using the existing `userTopicEntities` helper. Proposal decoding uses the
supplied TypeResolver and preserves existing unknown-system-type compatibility
behavior. Errors do not advance the failing record's cursor or reported position.

The applied watermark is supplied explicitly. Durable but unapplied entries return
EOF without type resolution or cursor advancement; a later Read can deliver them.
EOF is temporary, so a reader also observes later appends. The cursor contains no
physical sequence number and resumes correctly after its checkpoint is erased.

A complete Read holds the adapter's read lock through scanning and decoding.
Adapter appends and rewrites hold the write lock. Do not copy the adapter after
use or mutate its underlying Log directly while readers are live. Returned data
is independent of file lifetime; no view is held between Read calls. Type
resolution and watermark callbacks must not reenter the adapter. Reads currently
block appends during decoding, and repeated Seek calls rescan the tail; performance
optimization is still pending.

Tests compare Actuals, positions, sparse checkpoints, metadata filtering, and
visibility pauses against the existing tidwall-backed Reader. They also cover
type-resolution retry, unknown system types, corruption, rewrite between reads,
append after EOF, and publication exclusion during decoding.

## Exact Actual lookup

`actualAt` performs an exact Raft-index lookup without changing any streaming
reader's cursor. Like production `Storage.ActualAt`, it returns every proposal
entity, including metadata, and propagates unknown-type errors rather than using
the streaming reader's skip behavior. Missing or erased indexes and control/no-op
entries return `ErrActualNotFound`; a gap never returns the next surviving Actual.
Corruption and type-resolution failures remain errors.

The experimental method additionally requires an applied watermark. Durable but
unapplied entries return `ErrActualNotFound` before type resolution; callers can
retry after application advances. This is an explicit guard in the experiment,
not a change to production `Storage.ActualAt`. The adapter read lock covers raw
lookup and decoding, excluding rewrite publication throughout the operation.

Tests compare exact results and errors against tidwall-backed `Storage.ActualAt`
for applied entries, including metadata-only/mixed proposals, gaps, control
entries, and maximum indexes. They also verify erased-index behavior, streaming
cursor independence, visibility gating, retry after resolution failure, corrupt
identities, and both skippable and must-understand unknown system types.

## Protected multi-call reads

`protectedReaderAt` protects a logical rewrite generation across multiple Read
calls. Its context must have a deadline. Closing the reader, canceling the context,
or reaching the deadline ends its lifetime and releases protection. Close is
idempotent and safe to race with cancellation. Reads after the lifetime ends
return its cancellation cause (or `segmentlog.ErrClosed` for explicit Close).
An in-flight read checks cancellation again after decoding and does not advance
its cursor or return an Actual if cancellation occurred during decoding.

Acquisition and rewrite exclusion share the adapter lock, so a read registers
either before or after a complete rewrite. While any protected reader remains,
`rewriteRaw` returns retryable `errSegmentRewriteDeferred` before invoking a
transform or creating files. This does not poison the log or consume a generation;
the future coordinator must retain pending work and retry. Appends and rotation
remain available between Read calls. This protects logical record history, not
a fixed tail limit or a set of backup files.

Cancellation cannot interrupt a type resolver or filesystem call. Protection
remains until an in-flight Read releases the adapter lock, and
`protectedReadCount` reports these blockers without waiting for decoding. Close
waits for release, so callbacks must not close or reenter their own reader/adapter.
Always close a protected reader when finished rather than waiting for its deadline.

Tests exercise overlapping holds, appends during a hold, retry of the same rewrite
generation, automatic expiry, concurrent close/cancel, and cancellation during
decoding. Production from-zero replay is not wired to this API yet; backup capture
and file pins remain separate work.

Production integration still needs checkpoint loading, protected-reader wiring,
and storage/application coordination.
Recovered append progress and raw committed replay are implemented experimentally;
BoltDB/Raft recovery coordination and applied-index invariants remain pending.
Backup/restore, peer transfer, format gates, offline conversion, and workload
benchmarks remain separate prerequisites for activation.

Run the focused checks with:

```sh
go test -race ./internal/cluster/db/wal -run '^TestSegment'
```

## Rewrite churn measurements

The [initial filesystem experiment](segment_churn_experiment.md) compares
completed payload sizes, retained files, and new content hashes for identical
no-op, isolated, and scattered scrubs in plain and zstd storage. Survivor bytes
are verified against the tidwall rewrite. These are synthetic size/churn results,
not production performance or backup-cost projections.
