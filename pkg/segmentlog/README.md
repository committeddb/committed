# segmentlog (experimental)

An ordered segmented log being developed within the Committed repository and Go
module. It has no dependencies on Committed's application packages. Record IDs
are sparse `uint64` values; payloads are opaque bytes.

**Current scope: immutable segments and preparation of selective replacements.**
There is no durable append log or catalog yet. Neither the API nor the file format
is stable. This package is not connected to the running database.

## Boundaries

| Layer | Responsibility | Status |
| --- | --- | --- |
| Committed adapter (`internal/cluster/db/`) | Raft entry serialization, visibility, scrub policy, metadata reconciliation, backup/peer protocols | Future integration |
| Ordered log (`pkg/segmentlog`) | Append, range ownership, catalog publication, recovery, captured views, retirement | Planned |
| Segment operations (`pkg/segmentlog`) | Immutable encoding, sparse reads, range-preserving replacement | Initial implementation |
| Encoding (`pkg/segmentlog/internal/format`) | Bounded frames and CRC32C; later block codecs | Plain frames implemented |
| Durable filesystem (`pkg/segmentlog/internal/durablefs`) | File/directory sync and replacement primitives, fault injection | Planned; no empty placeholder package |

Keep segment lifecycle, catalog publication, and rewriting in one package until
separating them makes their durability rules easier to enforce. Add no separate
`go.mod`. The adapter may import this package; this package must never import the
adapter, Raft, protobuf, or bbolt. In particular, the adapter must check that an
outer record ID matches the ID inside its serialized Raft entry.

## Implemented contract

- `WriteSegment` encodes strictly increasing records within a fixed `[Start, End)`
  coverage interval. It consumes an iterator and rejects duplicate, decreasing,
  out-of-range, or oversized records.
- `OpenSegment` reads only header/footer/index metadata. `Read` performs exact
  lookup; `Seek` finds the first survivor at or after an ID; `Records` scans.
  `Verify` checks every payload. Missing IDs return `ErrNotFound`; malformed
  content and underlying I/O failures return errors.
- `Rewrite` invokes an application-supplied transformation once per examined
  record. It supports whole-record removal and partial payload replacement while
  keeping record IDs and coverage. It detects byte-identical replacements as
  no-ops, including callbacks that mutate their supplied payload in place.
- The output factory is called only after the first actual change. No-op rewrites
  create no files and write no bytes, regardless of requested encoding options.
  The unchanged prefix is read again to avoid buffering a whole segment or
  calling the transformation twice.
- The caller owns input file lifetime and immutability. Returned payloads are
  private to that read, with capacity limited to their length. Retaining a small
  payload can retain its decoded block; clone it for long-lived use.

The I/O boundary deliberately does **not** promise durability or publication.
Writing/replacing one segment is preparation only. A caller must discard partial
output on error and supply its close/sync, coherent catalog publication, and
retirement protocol. Fully erased ranges currently encode as empty segments;
the catalog layer will represent them without a payload file.

Memory scales with block data and the block index rather than the entire log.
Opening reads at most 2.5 MiB of encoded index metadata; decoded descriptors and
record slices add bounded overhead. Rewriting can hold several block-sized
buffers while copying the prefix and encoding output. No cache is implemented.

## Experimental format 0

All integers are unsigned little-endian. CRC is CRC32C (Castagnoli). Checksums
detect corruption, not malicious modification. The format currently supports
**plain blocks only**. Compression and stronger file identity belong in later
slices, before format adoption.

| Region | Byte layout |
| --- | --- |
| Header (32 bytes) | `magic[8]="SEGLOG00", version:u16=0, required_features:u16=0, start:u64, end:u64, crc:u32` |
| Record frame | `payload_length:u32, id:u64, payload[payload_length], crc:u32` |
| Block | One or more complete record frames, no padding |
| Index entry (40 bytes) | `first_id:u64, last_id:u64, offset:u64, stored_length:u32, record_count:u32, block_crc:u32, reserved:u32=0` |
| Footer (32 bytes) | `index_offset:u64, block_count:u32, total_records:u64, index_crc:u32, magic[4]="END0", crc:u32` |

Header, frame, and footer CRCs cover all preceding bytes within their region.
Index CRC covers the complete encoded index; block CRC covers all stored block
bytes. Index entries exactly partition bytes between header and index, and the
index ends immediately before the footer. Readers validate bounds before block
allocation and verify block metadata against decoded frames before yielding any
record from that block. Unknown versions, features, and reserved values fail.

Limits: 16 MiB payload, 16 MiB + 16 bytes per block, 65,536 blocks per segment.
Default block target: 256 KiB; experiments may select a different bounded target.
An oversized record gets its own block. Coverage must have `Start < End`;
`MaxUint64` can be an exclusive end but cannot be a stored ID. Empty segments have
no blocks, count zero, and retain the original coverage. These are experiment
limits, not changes to Committed's application limits.

Bytes contain no paths, timestamps, generation IDs, or host identity. Identical
records, coverage, and options produce identical bytes. Unlike the fuller design
proposal, this initial encoding omits compressed block headers and decoded-size
fields because it has only one codec. Adding codecs will require a new encoding
version; no compatibility with format 0 is promised yet.

## Next slices

1. Independent compressed blocks and measured codec/level tradeoffs; strengthen
   format fixtures and fuzzing before freezing the byte layout.
2. Durable filesystem operations and recoverable active append groups, including
   injected short-write/sync failures and crash recovery bounds.
3. Catalog publication, rotation, concurrent rewriting, captured views, and
   resumable physical retirement. Publish a whole rewrite transaction atomically.
4. Committed adapter and offline opt-in conversion, then compatibility, peer,
   backup, and baseline performance experiments.

The full requirements remain in [the design draft](../../docs/event-segment-design.md).

## Validation

```sh
go test -race ./pkg/segmentlog/...
go test ./pkg/segmentlog -run '^$' -fuzz FuzzSegment -fuzztime 10s
```

Tests cover sparse reads, empty and oversized blocks, invalid ordering, every
single-byte corruption and truncation of a sample segment, malformed index bounds,
selective/all/no-op/in-place transformations, callback counts, file identity and
hash preservation, cancellation, output failures, and concurrent reads.
