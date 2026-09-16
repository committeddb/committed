# Experimental tidwall EventLog container

This backend supplies the common contract without exposing dense tidwall sequence
numbers. Create requires an existing empty directory with durable parents; Open
requires the checksummed CURRENT. An advisory directory lock excludes other owners.
It never auto-detects or converts an existing production event log.

## Files

- `CURRENT`: SHA-256 (32 bytes) followed by canonical JSON describing version 1,
  active generation directory, starting ID, logical generation, original last
  appended ID/has-appends flag, prefix survivor count, and persisted backend options.
  The reader caps it at 4 KiB and rejects noncanonical content/unsafe references.
- `generation-<32 hex digits>/`: ordinary files from the repository's tidwall fork.
  Each tidwall payload is `ID:u64 little-endian | opaque payload | CRC32C:u32
  little-endian`, where the checksum covers ID and payload. The payload limit is
  16 MiB. Physical sequences start at 1 and remain dense; stable IDs may have gaps.

This envelope intentionally differs from production's protobuf checksum envelope.
This container does not open production directories. The application's
`Storage.copyEventLog` helper validates and copies legacy records into a fresh
backend in isolated tests.

## Publication and recovery

A rewrite validates the input, transforms each record once, writes every survivor
to a fresh generation, syncs files and directories, drains configured compression,
and atomically replaces CURRENT. It captures original append progress independently
of survivors. Later appends extend the selected tidwall log, with sync enabled.
Open verifies the captured prefix count, original ID bound, and increasing IDs,
then recovers any complete later appends. Missing references or CURRENT fail;
orphan directories never select themselves. Callback/preparation/publication errors
poison the handle until close/reopen; uncertain publication is never rolled back.

No-op rewrites still produce a full replacement, as permitted by the contract.
This backend is a comparison implementation, not an optimization of the legacy
scrub path. It retains old generations until Reclaim verifies the selected state
and deletes recognized regular files with directory syncs. Unknown names and
symlinks are retained. Cleanup errors can leave partial progress and require reopen.

The wrapper uses the tidwall fork's native append/recovery behavior. It is not
integrated with application Raft/BoltDB recovery. Neither unit fault injection
nor successful local fsync calls substitute for power-loss testing.