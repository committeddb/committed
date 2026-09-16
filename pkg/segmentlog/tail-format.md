# Active tail: experimental format 0

This slice supplies a single-file appender and a non-mutating recovery scanner.
It does not yet choose catalog membership, rotate files, truncate suffixes, or
establish database-level durability. All integers are unsigned little-endian;
checksums are CRC32C. Payloads remain opaque, with sparse increasing IDs.

## File layout

| Region | Bytes |
| --- | --- |
| Tail header (32) | `magic[8]="SLTAIL00", version:u16=0, features:u16=0, start:u64, reserved:u64=0, crc:u32` |
| Group header (32) | `magic[8]="SLGROUP0", framed_bytes:u32, record_count:u32, last_id:u64, reserved:u32=0, crc:u32` |
| Group body | Complete record frames, using the sealed-segment frame encoding |
| Group trailer (16) | `magic[8]="SLEND000", framed_bytes:u32, group_crc:u32` |

Header CRCs cover their first 28 bytes. Group CRC covers its entire header, body,
and first 12 trailer bytes. Every record also carries its frame checksum.
Each group is nonempty and contains at most 32 MiB of framed data. A payload is
at most 16 MiB. IDs must stay at or above the tail's Start, strictly increase
within and across groups, and be less than MaxUint64. ID zero is valid.

Groups express the boundary of one local append call. They are not consensus
commits and must not determine future sealed-segment boundaries. Sealing reads
the records and reconstructs canonical blocks independently of these groups.
No compression runs on the active append path.

## Creation and append

1. Use `WriteTailHeader` through `durablefs.Dir.Install` to durably install the
   new file. The owning log must also establish its catalog membership before
   acknowledging database writes. Do not open it with `O_APPEND`.
2. `OpenTail` scans the entire file and syncs it before returning a handle. It
   refuses corruption or incomplete suffixes; it never modifies their bytes.
3. `Tail.Append` validates the complete batch before writing, writes one group
   at the verified end offset, and calls file Sync before returning success.
4. Any write, short-write, or sync failure permanently poisons that handle.
   Further calls make no I/O. Reopen/recovery is required. Invalid caller input
   returns ErrInvalid without poisoning or writing.

`Tail.State` returns the last successfully synchronized state of that handle;
it also returns ErrTailPoisoned after an I/O failure. The failed append might
have persisted a complete group beyond that state. Reopening validates and
syncs all complete groups, including such unacknowledged writes; higher layers
must use stable record IDs for replay/deduplication.

Tail serializes its own methods. The caller owns Close and must prohibit writes,
truncation, or replacement through other handles. The directory and file must
already be durably installed; file Sync alone does not establish catalog or
directory durability. There is no automatic directory locking yet.

## Scanner and recovery policy

`ScanTail(reader, capturedSize, visitor)` returns a validated prefix described
by `TailState`: original Start, record count, optional last ID, and next byte
offset. A nil visitor performs verification only. Captured bytes must remain
unchanged while scanning; appending beyond the supplied bound is acceptable.

A group is fully validated before any of its records are delivered. Prior groups
may already have been delivered when a later group fails. A visitor error stops
the scan before advancing the state for that group, even if the visitor consumed
some of its records, so callbacks must tolerate replay. Returned payloads may
retain the decoded group buffer; clone them for long-lived retention.

| Condition | Result |
| --- | --- |
| Valid header and complete groups through captured EOF | Success; End equals captured size |
| Short tail header | ErrCorrupt; cannot establish file identity |
| Partial final group header, or valid group header whose body/trailer is short | ErrIncompleteTail; state ends before that group |
| Complete invalid checksum, impossible lengths/counts, invalid order | ErrCorrupt |
| Unknown version, feature, or magic with a valid header checksum | ErrUnsupported |
| Reader failure within the declared captured size | Original I/O error |

**ErrIncompleteTail is a description, not truncation authorization.** A truncated
previously acknowledged file can look exactly like an interrupted unacknowledged
append. This layer cannot distinguish them and does not consult external
metadata/Raft bounds. The appender refuses to open such files. There is no API that silently
repairs, skips, or truncates them.

The scanner bounds group allocation before reading payloads. It retains one group
plus record descriptors at a time; callers retaining records can retain additional
groups. Append assembles one bounded group in memory. No tail index/cache is
implemented. Managed rotation and original-input accounting across rewrites
are supplied by Log and its catalog checkpoint.

## Managed rewrite checkpoints

The group format is unchanged. A rewritten tail has an optional `TailCheckpoint`
in its catalog reference: a complete-group byte boundary `End`, highest original
appended ID `Last`, original input `Count`, and original framed bytes `Framed`.
An entirely erased prefix ends immediately after the tail header.

Managed recovery verifies that boundary, requires the surviving prefix's count
and last ID not to exceed the original values, restores original accounting,
then reads later groups. Later IDs must exceed the original Last, including IDs
of erased records. Later groups add their own counts and framed bytes. Truncation
before the checkpoint or a checkpoint inside a group is corruption.

Managed `Seek`, `Read`, and `Scan` also apply this checkpoint when reading the
tail. A later group containing an ID at or below the original Last is
corrupt even if its checksums and physical record ordering are valid. Reads can
stop after finding their result; they do not verify unvisited later groups.

`TailState.Count` counts survivors; `OriginalCount` and `Framed` count original
input. On managed handles, Last and HasRecords retain append progress even when
Count is zero. Standalone `ScanTail`/`OpenTail` do not read catalog metadata and
must not be used to recover a managed log's erased append progress. Use `OpenLog`.

## Validation

Tests cover sparse/zero IDs, reopen, sealing equivalence, every truncation boundary
across two groups, single-byte corruption, malformed checksummed lengths and record
ordering, maximum payload/group limits, visitor errors, short writes, complete
writes returning errors, sync failures, poisoned handles, recovery of complete
unacknowledged groups, and integration with durable file installation. Fuzzing
exercises the scanner. These tests do not establish filesystem power-loss behavior
or the missing database-level suffix-discard proof.
