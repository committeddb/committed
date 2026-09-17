# segmentlog (experimental)

An ordered segmented log being developed within the Committed repository and Go
module. It has no dependencies on Committed's application packages. Record IDs
are sparse `uint64` values; payloads are opaque bytes.

**Current scope: an experimental synchronous append/read/rotate lifecycle,
immutable segments, transactional whole-log rewriting, and atomic local
catalogs.** Explicit reclamation of obsolete managed files is implemented.
Background sealing, concurrent rewriting, and pinned views are not implemented.
Neither the API nor the file format is stable. This package is not connected to
the running database. The shared [EventLog
contract](../../internal/cluster/db/eventlog/README.md) now has separate tidwall
and segmented implementations. A [raw-entry adapter
experiment](../../internal/cluster/db/wal/segment_eventlog_experiment.md)
compares Committed protobuf records and existing scrub transformations with
tidwall.

## Bbolt catalog

`CreateLog`/`OpenLog` use bbolt metadata in `metadata.db`. Managed operations use
targeted state/range queries; rollover and rewrite publish metadata transactions.
Open validates metadata boundaries and the active tail. Full metadata and payload
verification is explicit through `Log.Verify`. Reclaim drains committed retirements;
`Log.ReclaimOrphans` separately scans for unpublished files.
See the [catalog contract and experimental evidence](bbolt-experiment.md).

`Log.InspectCatalog` returns a detached metadata snapshot for diagnostics. It
allocates the complete range list and does not pin the referenced files.

## Boundaries

| Layer | Responsibility | Status |
| --- | --- | --- |
| Experimental adapter (`internal/cluster/db/wal/`) | Raft entry serialization, visibility, replay, selection, protected readers | Implemented in isolated tests; production Storage uses legacy tidwall |
| Ordered log (`pkg/segmentlog`) | Append, range ownership, catalog publication, recovery, rewriting, reclamation | Synchronous operations; no pinned views |
| Segment operations (`pkg/segmentlog`) | Immutable encoding, sparse reads, range-preserving replacement, active append groups | Initial implementation |
| Encoding (`pkg/segmentlog/internal/format`) | Bounded frames, CRC32C, and block codecs | Plain and zstd implemented |
| Durable filesystem (`internal/durablefs`) | File/directory sync and replacement primitives, fault injection | Immutable installation and pointer replacement implemented |

For managed rollover, `segmentStorage` prepares physical files and a private,
single-use handle. The catalog publisher consumes that handle to select the new
layout without repeating payload verification or synchronization. Recovery and
rewrite publication establish trust from disk. See the
[ownership contract](rollover-ownership.md).

Segment lifecycle, catalog publication, and rewriting share one package in the
repository's Go module. The package does not import the application adapter,
Raft, protobuf, or bbolt. The adapter checks that an outer record ID matches the
ID inside its serialized Raft entry.

## Implemented contract

- `WriteSegment` encodes strictly increasing records within a fixed `[Start, End)`
  coverage interval. It consumes an iterator and rejects duplicate, decreasing,
  out-of-range, or oversized records.
- `OpenSegment` reads only header/footer/index metadata. `Read` performs exact
  lookup; `Seek` finds the first survivor at or after an ID; `Records` scans.
  A lookup validates its complete selected block while retaining only the matching
  record descriptor. `Verify` checks every payload. Missing IDs return `ErrNotFound`; malformed
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

The standalone segment APIs encode indexed files. Closed append-file ranges are
opened internally using their catalog references. Their filenames and bytes stay
unchanged until a rewrite replaces them; no-op rewriting does not re-encode them.

The public I/O boundary deliberately does **not** promise durability or publication.
Writing/replacing one segment is preparation only. A caller must discard partial
output on error and supply its close/sync, coherent catalog publication, and
retirement protocol. The managed `Rewrite` operation supplies these steps
through publication and represents fully erased ranges without a payload file;
physical retirement remains an explicit `Reclaim` operation.

The internal [durable filesystem layer](../../internal/durablefs/README.md) now supplies
no-clobber immutable installation and atomic pointer replacement, with explicit
results for uncertain durability and cleanup failures. The catalog layer
now uses these primitives; the public segment encoding APIs remain I/O-based.

For a single segment, memory scales with block data and its block index.
Opening a segment reads at most 3 MiB of encoded index metadata; decoded descriptors and
record slices add bounded overhead. Rewriting can hold several block-sized
buffers while copying the prefix and encoding output. No cache is implemented.

## Experimental format 1

All integers are unsigned little-endian. CRC is CRC32C (Castagnoli). Checksums
detect corruption, not malicious modification. Each block is either plain or an
independent zstd frame. File-level identity and publication belong to the
catalog layer.

| Region | Byte layout |
| --- | --- |
| Header (32 bytes) | `magic[8]="SEGLOG00", version:u16=1, required_features:u16=0, start:u64, end:u64, crc:u32` |
| Record frame | `payload_length:u32, id:u64, payload[payload_length], crc:u32` |
| Block | Plain record frames or one zstd encoding of those frames |
| Index entry (48 bytes) | `first_id:u64, last_id:u64, offset:u64, stored_length:u32, record_count:u32, block_crc:u32, decoded_length:u32, codec:u16, reserved:u16=0, reserved:u32=0` |
| Footer (32 bytes) | `index_offset:u64, block_count:u32, total_records:u64, index_crc:u32, magic[4]="END1", crc:u32` |

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
records, coverage, options, and encoder version produce identical bytes. Block
metadata lives in the checksummed index rather than a duplicated block header.
Readers also accept experimental format 0 (40-byte index entries, plain blocks,
reserved zero at offset 36, and END0 footer). New writes always use format 1.
A fixed format-0 fixture tests that reader compatibility; old readers reject v1.
Neither experimental format is an adopted production storage contract.

## Compression policy

`Options.Compression` supports `NoCompression` (default), `ZstdFast`,
`ZstdDefault`, `ZstdBetter`, and `ZstdBest`. The four zstd policies map to the
existing dependency's named effort levels, not numeric zstd CLI levels. All use
one encoder worker, a 1 MiB window, and zstd checksums. Encoding options never
change record IDs, coverage, or decoded block boundaries.

Codec IDs are `0=plain` and `1=zstd`. If compression would not shrink a block, the
writer stores it plain; a segment can contain both codecs. The reader checks the
stored CRC before decompression, caps the output at the declared decoded length
and global limit, caps the decoder window at 1 MiB, and requires the exact decoded
length. Unknown codecs fail at open. Reads instantiate a decoder per compressed
block, so concurrent readers share no decoder state; pooling is not implemented.
Verification instead owns one decoder and reusable output buffer per pass. It
retains no payloads and closes that decoder when the pass completes.

No-op rewrites still create no output even when requested compression differs.
Use an explicit `WriteSegment` over `Records()` for deliberate re-encoding.
[Synthetic benchmark notes](compression-benchmarks.md) record initial CPU, size,
and allocation tradeoffs; these are not production storage or durability results.

## Active tail

`WriteTailHeader`, `OpenTail`, `Tail.Append`, and `ScanTail` now provide bounded,
checksummed append groups with sync-before-success and permanently poisoned
handles after I/O failure. Reopening recovers and syncs complete groups. The
scanner reports incomplete suffixes without truncating; it cannot prove that
discarding them is safe. See [the tail format and recovery contract](tail-format.md).

Verification-only tail scans validate frames without collecting record lists.
Scans with visitors allocate one list per group and validate the complete group
before delivery. See [tail allocation measurements](tail-benchmarks.md).

## Managed log

`CreateLog` and `OpenLog` connect the tail, segments, and catalog. `Append`
syncs before success and rotates by a persisted original-frame byte target;
`Read` and `Seek` span immutable ranges and the active tail. Rollover retains the
old append file unchanged, recording its size in the catalog; it does not encode
or compress a replacement. Changed ranges can become indexed rewrite outputs.
Batch boundaries and restarts do not alter sealed ranges. `LastAppended` reports original append
progress even when the highest record or every record has been erased. It
distinguishes an empty log from an appended ID zero and refuses poisoned handles
until recovery. A nonblocking directory lock enforces exclusive managed log
ownership until Close, including across processes. Rollover currently blocks
other operations; old files remain until explicit reclamation. See [the
lifecycle contract and limitations](log-lifecycle.md).

Managed sealed-file reads check coverage and record count against the catalog
before looking up or delivering records. A mismatch returns `ErrCorrupt`, including
an unexpectedly empty file. These metadata checks complement selected-block
validation; individual reads do not recompute the whole-file digest.

## Streaming scans

`Log.Scan(ctx, bounds, visit)` reads surviving records in a half-open ID interval
under one managed view. Scans skip unrelated ranges; indexed ranges also skip
unrelated blocks. Closed append files use sequential validation and scanning.
The active tail prefix is scanned once. Memory is bounded by decoded blocks/groups
unless callbacks retain payloads. Callbacks must not reenter the log. Appends,
rewrites, and reclamation wait until completion; this is not a long-lived file pin.
Cancellation and callback errors stop delivery without poisoning the log. Errors
can follow a delivered prefix. Selected blocks/groups are fully checked before
delivery, but unrelated payloads are not verified by a bounded scan.

## Sealed rewriting

`Log.RewriteSealed(ctx, generation, transform)` prepares only changed sealed
ranges, then publishes their replacements together in one catalog update.
Unchanged files retain their names, bytes, and coverage. Fully erased ranges
retain coverage without payload files. This operation excludes the active tail.
See [the transaction contract](sealed-rewriting.md).

`Log.Rewrite` includes the active tail in the same transaction. A catalog
checkpoint preserves its highest appended ID and original rotation accounting,
even when no records survive. See [whole-log rewriting](whole-log-rewriting.md).

## Local catalogs

The managed engine uses a bbolt catalog; see the [metadata contract](bbolt-experiment.md).
There is no manifest/CURRENT catalog format, conversion, or format autodetection.

## Reclamation

`Log.Reclaim(ctx)` drains the committed retirement queue without a directory scan.
`Log.ReclaimOrphans(ctx)` separately validates range metadata and scans the directory
in bounded batches for unselected managed data and temporary files. It preserves
unknown files, directories, and symlinks, and does not verify live payloads. Both
operations hold the log mutex throughout, blocking reads and writes. Errors can
report partial progress; filesystem failures require reopening before retry.
See [reclamation](reclamation.md).

## Rewrite churn experiment

A [Committed-record comparison](../../internal/cluster/db/wal/segment_churn_experiment.md)
now measures completed-file churn and checks scrub equivalence against tidwall.
It separates local file replacement from new content hashes and compares plain
and zstd workloads. The synthetic results do not establish production backup costs.

The [storage overview](../../docs/event-segment-design.md) describes the current
engine, application boundary, and experimental limitations.

## Validation

[Live-segment churn and Linux validation](live-churn.md) covers repeated scrubs,
metadata page reuse, file retirement, recovery, and post-churn rollover. The
[full-size rewrite experiment](full-size-churn.md) extends these checks to a
1,300 MiB original-frame history and measures local replacement bytes.

[Full-size rollover measurements](rollover-size.md) cover 1, 20, and 32 MiB
targets, full-active-tail recovery, and installation/metadata commit timing.

[Concurrency tests](concurrency-testing.md) exercise paused readers, cancellation,
competing mutations, concurrent shutdown, and mixed operation histories. The
application adapter's protected-reader test runs against both storage backends.

```sh
go test -race ./pkg/segmentlog/...
go test ./pkg/segmentlog -run '^$' -fuzz '^FuzzSegment$' -fuzztime 10s
go test ./pkg/segmentlog -run '^$' -fuzz '^FuzzSegmentIndexValidation$' -fuzztime 20s
go test ./pkg/segmentlog -run '^$' -fuzz '^FuzzTail$' -fuzztime 10s
go test ./pkg/segmentlog -run '^$' -fuzz '^FuzzTailGroupValidation$' -fuzztime 20s
go test ./pkg/segmentlog -run '^$' -fuzz FuzzCatalog -fuzztime 10s
go test ./pkg/segmentlog/internal/format -run '^$' -fuzz FuzzDecode -fuzztime 10s
go test ./pkg/segmentlog -run '^$' -bench BenchmarkCompression -benchtime 100ms
```

Tests cover sparse reads, empty and oversized blocks, invalid ordering, every
single-byte corruption and truncation of a sample segment, malformed index bounds,
selective/all/no-op/in-place transformations, callback counts, file identity and
hash preservation, cancellation, output failures, and concurrent reads.

The structured tail-group fuzzer generates valid outer checksums while varying
record IDs, declared counts, and the declared last ID. It compares verification
and delivery against the input's expected validity and checks that rejected
groups deliver nothing and leave the validated prefix unchanged.

The structured segment-index fuzzer replaces plain-block frames and index claims
while preserving valid file checksums. It checks opening, full verification,
record delivery, and sparse lookup against the input IDs. Cases include duplicate
or decreasing IDs, incorrect first/last IDs and counts, and the reserved maximum
ID. The raw segment fuzzer also includes compressed file seeds.

Compression tests also cover mixed codecs, deterministic output, sparse block-local
seeks, compressed partial rewrites, maximum-size records, decoded-size mismatches,
malformed streams, concatenated-frame output bounds, and format-0 compatibility.

## Verification allocations

[Verification measurements](verification-benchmarks.md) isolate sealed-file
verification from filesystem sync and catalog publication. The verifier and read
path share block/frame validation; only reads collect records for their callers.
Frozen append-file verification combines record validation and whole-file hashing
in one pass, reading each byte once.

## Sparse lookup allocations

[Lookup measurements](seek-benchmarks.md) compare allocations for an in-memory
sparse lookup before and after eliminating the temporary per-block record slice.
The lookup still decodes and validates the entire selected block before returning.

## Scan allocations

[Scan measurements](scan-benchmarks.md) compare allocations before and after
preallocating the record-descriptor slice using a bounded block record count.
Scans retain all descriptors for the selected block and validate the whole block
before delivering records.
