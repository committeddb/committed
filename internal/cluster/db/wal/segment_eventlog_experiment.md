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

This is a raw-storage equivalence experiment, not an ActualReader implementation
or a completed production scrub. The selections supplied to the filter must
already be bounded and authorized. The adapter does not compute selections,
check consumer progress, update BoltDB, or declare erasure complete.

Next integration work must preserve the current reader's applied-index visibility
watermark, type resolution, metadata filtering, and protected read lifetime.
Recovery needs original append progress even when the last record was erased.
Backup/restore, peer transfer, format gates, offline conversion, and workload
benchmarks remain separate prerequisites for activation.

Run the focused checks with:

```sh
go test -race ./internal/cluster/db/wal -run '^TestSegmentEvents'
```
