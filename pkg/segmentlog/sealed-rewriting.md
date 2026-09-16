# Transactional sealed-range rewriting

`Log.RewriteSealed(ctx, generation, transform)` applies the generic keep/replace/
remove callback to sealed records and publishes the resulting layout atomically.
This is experimental and is not connected to Committed's RTBF protocol.

## Scope and identity

The returned `SealedEnd` is the exclusive end of the sealed scope. The active
tail is untouched, including its append frontier and original rotation accounting.
An application cannot treat this operation as completing erasure of active records.
Use `Log.Rewrite` when the tail must join the transaction. It preserves original
accounting and append progress through a catalog checkpoint; see
[whole-log rewriting](whole-log-rewriting.md).

Each examined record is transformed once. Payloads may be modified in place;
IDs and range coverage cannot change. Unchanged ranges keep their exact file
names and bytes, even if current encoding options differ. Changed ranges use
the current encoding policy. Entirely erased ranges retain coverage in the
catalog with zero count and no payload file. Subsequent scans skip those ranges.
A changed range is never merged with a neighbor.

The caller supplies a strictly newer generation. Even a no-op publishes that
generation in a new catalog revision, without creating payload files. This is a
scoped storage transaction, not evidence of a completed Committed scrub generation.
The future adapter must coordinate full scope and application metadata.

## Transaction and recovery

1. Hold the managed log mutex, validate arguments, and verify current references.
2. Scan sealed ranges. At the first semantic change within a range, prepare its
   transformed stream. Re-read unchanged prefix records without transforming twice.
3. Install and sync each nonempty replacement. Use bounded decoded blocks and
   streaming output; no whole-segment memory buffer or temporary spool is needed.
4. Publish all changed descriptors, revision, and generation through one catalog
   update and CURRENT replacement. Old files remain available until `Reclaim`.

Managed reads, appends, rotations, reclamation, and other rewrites are blocked
throughout. Callbacks must not reenter this Log. Cancellation is checked between
records and ranges; a callback or a full-file verification must return before
cancellation can be observed. Verification still scans full history; bounded
startup and publication work remain future requirements.

Invalid arguments and cancellation before work starts leave the handle usable.
Once preparation starts, errors conservatively poison it, including callback
errors and cancellation. Close and reopen before retrying or reclaiming. Prepared
but unpublished files are orphans eligible for explicit reclamation. If CURRENT
publication fails, reopening selects exactly the old or the new complete layout;
it never adopts files merely because they look newer.

`ChangedSegments` and `EmptiedSegments` describe completed preparation, including
on failure; changed includes emptied. `Published` is true only on confirmed
successful publication. A false value with an error does not prove that CURRENT
stayed unchanged: reopening resolves publication uncertainty.

Publication is logical replacement, not physical erasure. Call `Reclaim` to remove
obsolete segments, old tails, and catalogs. Until then those files can retain the
original payloads. Future captured readers and backups will require retirement
blockers; the current managed API serializes operations and exposes no pinned views.

## Validation

Tests cover unchanged inode/mtime/bytes, callback counts, sparse reads after
removal, partial and whole-range removal, in-place edits inside a multi-record
range, no-op generations, empty ranges, active-tail exclusion, append/rotation
after reopening, and reclamation. Injected callback, cancellation, installation,
and CURRENT replacement failures exercise old/new recovery selection. These
operation-failure tests do not substitute for power-loss filesystem testing.
