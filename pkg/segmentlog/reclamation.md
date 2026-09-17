# Explicit reclamation (experimental)

This page describes the complete-catalog backend. The bbolt backend uses a
retirement queue for `Reclaim` and a separate `ReclaimOrphans` directory sweep;
see [bbolt reclamation](bbolt-experiment.md#reclamation).

`Log.Reclaim(ctx)` removes obsolete managed files after validating the currently
selected history. It is explicit: opening or rotating a log does not silently
sweep its directory. It runs under the Log mutex and advisory ownership lock, so
managed reads, appends, rotation, and Close cannot overlap cleanup.

## Selection and ordering

Before deleting anything, reclamation:

1. Reads CURRENT and its catalog, requiring an exact match with the handle's last
   confirmed catalog. A poisoned handle cannot start reclamation.
2. Verifies all referenced sealed contents and active groups and syncs the files,
   selected catalog, CURRENT, and directory. Missing/corrupt references or failed
   durability checks stop cleanup before deletion.
3. Builds the live filename set: CURRENT, its exact catalog, every nonempty sealed
   reference, and the active tail. Live references are preserved regardless of
   filename convention.
4. Lists candidates and selects only unreferenced regular files matching managed
   filename shapes. Unknown names, directories, and symbolic links are skipped.
5. Removes each selected entry and syncs the directory before moving to the next.

Recognized shapes are the managed log's `segment-<20-digit-start>-<32-lowercase-hex>.seg`
and `tail-<20-digit-start>-<32-lowercase-hex>.active`, content-addressed
`catalog-<20-digit-revision>-<64-lowercase-hex>.manifest`, and durablefs temporary
names `.segmentlog-<decimal-uint32>`. Numeric fields must fit their integer width.
These are reserved namespaces within the exclusively managed directory. Cleanup
never follows symlinks or recurses into a subdirectory.

Unpublished catalogs are candidates even when their revision is larger than the
current revision: CURRENT alone selects history. Likewise, completed files from
failed rotation attempts are not live until referenced. Old selected catalogs
are not kept as fallback recovery candidates; corrupt CURRENT must fail closed.

Imported/custom files with other names remain untouched after they become
unreferenced. This conservative boundary needs an explicit migration/ownership
policy before such files can participate in erasure completion.

## Results, failures, and retries

ReclaimResult reports RemovedFiles, RemovedBytes (sum of logical file lengths),
and SkippedEntries. Live references are not included in SkippedEntries. Logical
lengths do not measure reclaimed filesystem blocks: hard links, snapshots, and
filesystem allocation can make those numbers differ.

A nil error confirms all candidates found by this call were durably removed.
An error can accompany partial progress. A reported removal followed by directory
sync failure describes observed unlink success, not proven durable completion.
Filesystem or catalog consistency errors poison the Log; Close/reopen validates
CURRENT again, then another Reclaim discovers remaining obsolete names. No
persistent cleanup journal is needed for this serialized, unpinned lifecycle:
all deletions are outside the already confirmed live set and are rediscoverable.
After a crash, some deleted names may reappear, but current references are retained.

Cancellation is checked between phases, during candidate enumeration, and before
each removal. Completed removals were already synced, so cancellation alone does
not poison the Log. Full reference verification is currently uninterruptible;
cancellation can be delayed by that existing expensive verification pass.

## Reader lifetime and limits

Managed reads finish and close their file handles while holding the same mutex.
Returned Record payloads are memory buffers, so callers may retain them across
cleanup. There are currently no public pinned file views or streaming iterators.
External raw file handles and lower-level maintenance must not bypass managed
ownership. Before adding backup captures, concurrent background sealing, or
long-lived readers, reclamation must acquire proper file pins and account for
retirement blockers rather than relying only on this mutex.

This operation removes storage artifacts; it does not select records for erasure
or implement an integrated scrub. It cannot prove an application's RTBF request
is complete, and it does not remove external backups, filesystem snapshots, or
bytes retained in memory/device media.

Reclamation still performs full history verification and one directory sync per
removed file. Those are deliberate correctness-first choices, not optimized
throughput claims. Lower-level CatalogStore publication itself still retains
obsolete files; only the managed Reclaim operation performs this cleanup.

## Evidence

Tests cover cleanup after rotation, future unpublished catalogs and temporary
files, stable current identities/bytes, idempotence, unknown/nonregular entries,
corrupt or mismatched current state, cancellation, failures before and after
unlink, reopen/retry, and concurrent rotation. durablefs tests separately verify
unlink/directory-sync ordering and uncertainty reporting. Filesystem power-loss
validation remains an adoption requirement.
