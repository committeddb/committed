# Whole-log rewriting

`Log.Rewrite(ctx, generation, transform)` transforms all surviving records while
excluding other mutations. It prepares affected sealed ranges and the active tail,
then publishes them together through one catalog update. Unchanged payload files
retain their identity. `RewriteSealed` remains available for deliberately narrower
scope. Neither operation implements Committed's application scrub policy or
metadata reconciliation.

## Original append accounting

Erasing the most recent record must not allow its ID to be appended again.
Shrinking or deleting records must not change the future rotation boundary.
A rewritten tail therefore carries a catalog checkpoint describing the original
highest appended ID, record count, and framed byte count, together with the byte
end of the rewritten prefix. These values describe input history, not survivors.

Recovery validates the prefix, restores the checkpoint, then accumulates later
append groups. Further rewrites capture the combined accounting in a replacement
checkpoint. A no-op leaves the file and checkpoint unchanged. This works for
sparse indexes, ID zero, repeated erasure, growth of surviving payloads, and an
entirely erased active tail. Rotation seals the original logical range; if no
records survive, it publishes an empty descriptor without creating a segment file.

The checkpoint is checksummed as part of the catalog. Existing tails keep their
format and need no checkpoint until changed. Older experimental readers reject
catalogs containing the new field. No production format activation is implied.

## Preparation and publication

An internal `preparedLogRewrite` owns the replacement references and any opened
replacement-tail handle. Preparation leaves catalog selection, the live resident
tail, and cursor identities unchanged. Publication selects all replacements in
one catalog transaction, then transfers the tail handle to the log and invalidates
cursor hints. Cleanup closes any replacement handle that was not transferred;
unpublished files remain subject to orphan reclamation. Preparation and publication retain a maintenance mutex throughout, excluding
other rewrites, reclamation, and Close. Whole-log rewrites also hold the mutation
mutex exclusively to stabilize the active tail; sealed-only rewrites share that
mutex with appends. Preparation releases the log mutex while transforming and
writing each replacement; publication holds it. Failure and cancellation
semantics remain unchanged.

Replacement encoding lives in `rewriteWriter`. It receives replayable records,
source descriptors, captured tail accounting, encoding options, and a file
installer; it does not access the live log or catalog. The managed wrapper owns
source acquisition and release. Tail inputs still borrow the active file or
resident contents. The mutation mutex prevents append, another rewrite,
reclamation, and Close from changing or retiring those inputs until publication
finishes. Ordinary reads and scans can inspect the old generation during
replacement writing. Source acquisition and publication still hold the log mutex.

The tail reuses the segment transformation machinery: scan until the first change,
then reread only the unchanged prefix without invoking callbacks twice. A change
to the first record needs no prefix replay. Output streams
into a newly installed tail. Survivors are packed into groups targeting 256 KiB
of framed data; a larger record occupies a group alone. Frames are copied into
the group before invoking the next transform, so callbacks may reuse payload
buffers. A separate 256 KiB output buffer combines file writes. Both the final
group and output buffer must flush successfully before the installer syncs and
publishes the file; a write or flush failure prevents publication. Grouping uses
the existing tail format and reduces per-group scan work. Payloads remain bounded
by the existing per-record limit. A conservative framed-byte bound guarantees
that survivors fit the segment format's block index and reserves capacity for
later appends up to the original rotation target. Adjacent encoded blocks
together contain more than one block target of framed data, so limiting total
bytes to `floor(blockSize / 2) * (MaxBlocks - 1)` bounds the block count.
Excessive payload growth fails before publication; preparation does not encode
a second, discarded copy of the tail to establish this capacity.

Publication verifies and syncs replacement files. It preserves an unchanged
active tail and its checkpoint without reopening or rescanning that file. A
no-op rewrite needs only the metadata commit at publication; preparation still
scans its requested scope. `RewriteSealed` does not validate the active tail;
`Verify` explicitly checks the entire log.

All replacements are installed and synced before metadata selection changes. On success,
the managed handle switches to the replacement tail; the original byte accounting
is unchanged. If an entirely empty log has no changes, only the requested newer
generation is published. The result embeds sealed preparation counts and adds
`TailChanged`. Preparation counters do not prove publication.

Any preparation/publication failure poisons the handle. Close and reopen to
resolve uncertainty and select one complete layout. `Published` is true only after
confirmed catalog publication; a later old-handle close failure can return an
error with Published true. Cancellation is cooperative between records and does
not interrupt callbacks, syncs, or whole-file verification.

Logical publication retains old files. `Reclaim` removes obsolete published tails
and segments; `ReclaimOrphans` separately removes unpublished preparation files.
Physical erasure requires cleanup. The API has no pinned readers or backup views.
Rewrites scan records while preparing changes. Transform callbacks must not
reenter the log; a concurrent scan callback must also avoid reentry. The
application adapter still holds its own exclusive lock across a scrub.

## Evidence

Tests compare rotation boundaries with an untouched log across rewrites, payload
shrink/growth, complete erasure, appends, and restarts. They also exercise no-op
identity, empty-range rotation, checkpoint validation, forbidden reuse of erased
IDs, catalog copy isolation, and old/new recovery after callback, cancellation,
replacement installation, and metadata publication failures. Capacity tests accept
growth exactly at the byte bound, reject one byte beyond it, consume the reserved
append budget, and rewrite the resulting frozen tail as an immutable segment.
An adverse packing test uses alternating 16- and 18-byte frames with a 33-byte
block target to exercise partially filled blocks at the byte bound. Existing race tests,
format checks, and lint remain part of validation. Filesystem power-loss testing
and application integration are still required before production use.

## Buffered tail-write measurements

`BenchmarkTailRewriteWrites` fills a 20 MiB active tail with 5,120 records in
64-record batches, then changes the first payload byte of the first record.
A test-only installer wrapper counts underlying writes while timing the complete
managed Rewrite call, including validation, installation, and metadata commit.
It validates each replacement record, checks the byte count, reopens, verifies
the file and append progress,
and appends again. Fixture creation and those checks are outside `rewrite-ms`;
overall Go `ns/op` includes them. The benchmark also reports `reopen-ms` for
OpenLog after the rewrite.

Recorded September 17, 2026, Go 1.26.6, Linux/arm64, Alpine 3.20 on the local
OrbStack VM. Data resides on disposable container overlay filesystems. Baseline
`9639f47` and buffered binaries ran three sequential pairs after tests and lint,
reversing order in the second pair. The host and VM were not load-controlled.
All six runs passed their checks.

| Implementation | File writes/rewrite | Replacement bytes | Median rewrite ms | Range of rewrite ms |
| --- | ---: | ---: | ---: | ---: |
| Unbuffered | 5,121 | 21,217,312 | 410.2 | 264.7–439.6 |
| 256 KiB output buffer | 81 | 21,217,312 | 416.6 | 377.4–668.0 |

Write calls fell by about 98%, but this small, variable sample does not establish
an end-to-end latency improvement. Buffering adds 256 KiB of temporary memory
for a changed active tail. It does not combine append groups, reduce encoded
size, or remove the later validation/recovery work. No-op tails do not create an
output buffer.

Failure tests inject errors and short writes during a full-buffer write, a small
final flush, and a final flush following a successful full-buffer write. All must
prevent publication, poison the managed handle, and recover the original catalog
and payloads. The byte-equivalence regression, storage/backend race tests, Linux
storage suite, lint, and gosec pass.

```sh
go test ./pkg/segmentlog -run '^$' -bench '^BenchmarkTailRewriteWrites$' -benchtime=1x -count=3
```

## Unchanged-tail publication measurements

`BenchmarkRewriteSealedWithFullTail` creates a 20 MiB active tail containing
5,120 records in 64-record batches, with no sealed ranges. It repeatedly advances
the sealed-only generation. The callback must never receive an active record.
The benchmark times the complete RewriteSealed call, including its bbolt commit;
fixture creation and final full-log verification are outside the timer. This
isolates the cost of publication when the active tail is outside the rewrite's
scope, not the cost of scanning or rewriting historical ranges.

Recorded September 17, 2026, Go 1.26.6, Linux/arm64, Alpine 3.20 on the local
OrbStack VM, using disposable container overlay storage. Baseline `a0614c6` and
changed binaries ran three sequential pairs of 20 iterations, reversing order
in the second pair, after tests and lint finished. All runs passed. The host and
VM were not load-controlled.

| Measurement | Baseline | Replacement-only publication |
| --- | --- | --- |
| Mean operation time, run 1 | 8.692 ms | 1.861 ms |
| Mean operation time, run 2 | 8.711 ms | 2.101 ms |
| Mean operation time, run 3 | 9.559 ms | 1.706 ms |
| Median allocated bytes per operation | 288,212 | 15,565 |

The median run time fell from 8.711 ms to 1.861 ms. Publication no longer reads
or syncs the unchanged active file and skips the directory sync when no payload
references change. Whole-log rewrite preparation still scans the active tail,
with the mutation mutex excluding appends and other maintenance.

## Grouped tail-rewrite measurements

The same 20 MiB `BenchmarkTailRewriteWrites` fixture compares baseline `712887a`
(one group per survivor) with 256 KiB group packing. Both versions use the
256 KiB output buffer. The benchmark checks every replacement ID and payload,
verifies the reopened log and original append progress, and appends again.
`rewrite-ms` times the complete rewrite; `reopen-ms` times the subsequent
OpenLog call. Setup and data checks are outside these two timings.

Recorded September 17, 2026, Go 1.26.6, Linux/arm64, Alpine 3.20 on the local
OrbStack VM, using disposable container overlay storage. Three sequential pairs
ran one iteration each after tests and lint, reversing order in the second pair.
All six runs passed; the host and VM were not load-controlled.

| Measurement | One record per group | Bounded groups |
| --- | --- | --- |
| Groups in replacement | 5,120 | 80 |
| Replacement bytes | 21,217,312 | 20,975,392 |
| Underlying write calls | 81 | 81 |
| Rewrite time, run 1 | 75.25 ms | 67.21 ms |
| Rewrite time, run 2 | 75.56 ms | 63.45 ms |
| Rewrite time, run 3 | 73.26 ms | 52.37 ms |
| Reopen time, run 1 | 16.81 ms | 17.41 ms |
| Reopen time, run 2 | 18.81 ms | 12.67 ms |
| Reopen time, run 3 | 14.86 ms | 12.89 ms |

Median rewrite time decreased from 75.25 ms to 63.45 ms; median reopen time
from 16.81 ms to 12.89 ms. This small sample establishes no production latency
bound. The reduction of 241,920 bytes comes entirely from group overhead;
payload compression is unchanged. Preparation holds a reusable encoded-group
buffer in addition to its output buffer; a record above the target enlarges the
group buffer, bounded by the maximum record size plus framing and group overhead
(apart from allocator capacity rounding). Replacement writing permits engine
reads while mutations remain excluded. Tests cover reused callback payload buffers, sparse IDs, erasure,
records at and above the group target, the maximum payload size, checkpoint
recovery, later appends, and output failures before publication.

## Appends during sealed-only preparation

`RewriteSealed` captures the current sealed end. It fetches one range descriptor
at a time, closing its catalog read transaction before transforming the range.
This bounds metadata memory and avoids a read transaction blocking bbolt mmap
growth while a concurrent rollover holds the log mutex.

Appends and rollover can proceed during sealed replacement writing. Existing
sealed ranges cannot change because other maintenance remains excluded. At
publication, the rewrite checks the log is still usable, reads the current
catalog revision, and atomically replaces only its captured ranges, preserving
newly sealed ranges and the current tail. Its returned `SealedEnd` remains the
original boundary. An append failure poisons the log and prevents publication;
cancellation preserves acknowledged appends when the log is reopened.

Whole-log `Rewrite` continues to exclude appends throughout preparation because
its input includes the mutable tail. The application scrub adapter still uses
whole-log rewriting and retains its exclusive lock.
