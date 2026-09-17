# Explicit reclamation

`Log.Reclaim(ctx)` drains committed retirements in batches of up to 128 entries.
It checks queued filenames against the selected active tail and range index,
removes each obsolete file, syncs the directory, then acknowledges the batch in a
metadata transaction. Already-removed files are valid on retry. No directory scan
or full payload verification occurs in this operation.

`Log.ReclaimOrphans(ctx)` is separate full-directory maintenance. It validates
range metadata before deletion, then checks managed data names against the live
index and removes unselected regular files and reserved installation temporary
files. Directory entries are processed in bounded batches. Unknown names,
directories, and symlinks are preserved. It can remove queued obsolete files;
`Reclaim` subsequently acknowledges those absent names safely.

Both operations hold the log mutex and advisory directory ownership throughout.
They exclude reads, writes, rewriting, and Close. Cancellation may leave durable
partial progress. Other failures poison the handle; close, reopen, and retry.
Neither operation repairs metadata or establishes erasure from backups,
filesystem snapshots, or physical device cells. They are explicit, never part of
open or append. Full integrity verification is available through `Log.Verify`.

See the [metadata and cleanup contract](bbolt-experiment.md#reclamation) for the
exact checks and crash-recovery evidence.
