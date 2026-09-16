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
not yet implement replay deduplication or expose recovered append progress.

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

Production integration still needs the protected lifetime for multi-call reads,
checkpoint loading, exact ActualAt lookup, and storage/application coordination.
Recovery needs original append progress even when the last record was erased.
Backup/restore, peer transfer, format gates, offline conversion, and workload
benchmarks remain separate prerequisites for activation.

Run the focused checks with:

```sh
go test -race ./internal/cluster/db/wal -run '^TestSegment(Events|Reader)'
```
