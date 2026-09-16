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
into a newly installed tail, currently one append group per survivor. This can
increase group overhead. Payloads remain bounded
by the existing per-record limit, and a streaming trial encoding verifies that
the result can be sealed within the segment format's block/index limits. A
conservative byte bound also reserves index capacity for later appends up to
the original rotation target; excessive payload growth fails before publication.

All replacements are installed and synced before CURRENT changes. On success,
the managed handle switches to the replacement tail; the original byte accounting
is unchanged. If an entirely empty log has no changes, only the requested newer
generation is published. The result embeds sealed preparation counts and adds
`TailChanged`. Preparation counters do not prove publication.

Any preparation/publication failure poisons the handle. Close and reopen to
resolve uncertainty and select one complete layout. `Published` is true only after
confirmed catalog publication; a later old-handle close failure can return an
error with Published true. Cancellation is cooperative between records and does
not interrupt callbacks, syncs, or whole-file verification.

Logical publication retains old files. `Reclaim` removes obsolete tails, segments,
catalogs, and recognized orphans. Physical erasure requires that cleanup; readers
and backups with captured views are not implemented yet. Rewrites still block
all managed operations, and verification still scans historical payloads.

## Evidence

Tests compare rotation boundaries with an untouched log across rewrites, payload
shrink/growth, complete erasure, appends, and restarts. They also exercise no-op
identity, empty-range rotation, checkpoint validation, forbidden reuse of erased
IDs, catalog copy isolation, and old/new recovery after callback, cancellation,
replacement installation, and CURRENT publication failures. Existing race tests,
format checks, and lint remain part of validation. Filesystem power-loss testing
and application integration are still required before production use.
