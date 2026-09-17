# Whole-log rewriting

`Log.Rewrite(ctx, generation, transform)` transforms all surviving records under
one managed log lock. It prepares affected sealed ranges and the active tail,
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

The tail reuses the segment transformation machinery: scan until the first change,
then reread the unchanged prefix without invoking callbacks twice. Output streams
into a newly installed tail, currently one append group per survivor. A 256 KiB
output buffer combines small file writes without changing those encoded groups.
The buffer must flush successfully before the installer syncs and publishes the
file; a write or flush failure prevents publication. One group per survivor can
increase group overhead. Payloads remain bounded
by the existing per-record limit, and a streaming trial encoding verifies that
the result can be sealed within the segment format's block/index limits. A
conservative byte bound also reserves index capacity for later appends up to
the original rotation target; excessive payload growth fails before publication.

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
Rewrites block all managed operations and scan records while preparing changes.

## Evidence

Tests compare rotation boundaries with an untouched log across rewrites, payload
shrink/growth, complete erasure, appends, and restarts. They also exercise no-op
identity, empty-range rotation, checkpoint validation, forbidden reuse of erased
IDs, catalog copy isolation, and old/new recovery after callback, cancellation,
replacement installation, and metadata publication failures. Existing race tests,
format checks, and lint remain part of validation. Filesystem power-loss testing
and application integration are still required before production use.

## Buffered tail-write measurements

`BenchmarkTailRewriteWrites` fills a 20 MiB active tail with 5,120 records in
64-record batches, then changes the first payload byte of the first record.
A test-only installer wrapper counts underlying writes while timing the complete
managed Rewrite call, including validation, installation, and metadata commit.
It compares the replacement's digest with the original one-group-per-record
encoding, checks its byte count, reopens, verifies the file and append progress,
and appends again. Fixture creation and those checks are outside `rewrite-ms`;
overall Go `ns/op` includes them.

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
