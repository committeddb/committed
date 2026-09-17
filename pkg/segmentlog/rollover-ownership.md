# Rollover ownership

The managed log serializes rollover under its mutex and directory ownership lock.

| Layer | Responsibility |
| --- | --- |
| Log | Decide boundaries using original append accounting and switch active handles |
| Segment storage | Retain and hash the synchronized old file, durably install the successor header and first append group together |
| Catalog | Consume the prepared handle once and atomically select the closed range and new tail |
| durablefs | Install and sync payload files and directory entries |
| bbolt | Commit the metadata transaction durably |
| Recovery | Read committed metadata and validate the selected active tail |

`preparedRollover` is a private in-memory handle bound to a directory, history,
revision, and source tail. It is not persisted. Publication trusts the preparation
layer's completed durability work; it does not reopen or resync payload files.
One metadata transaction adds the closed range and updates the active tail and
revision. An entirely erased predecessor is queued for retirement.

After publication succeeds, Log switches appenders; the first group is already
durable. Remaining groups use ordinary append/sync.
A publication error poisons the handle without deleting prepared files. Recovery
uses committed metadata to resolve the outcome. Unpublished files remain for
explicit `ReclaimOrphans`.

Rollover still hashes the closed append file and synchronously installs the new
tail before publishing metadata. It does not convert or compress the predecessor,
copy its bytes, or serialize a full historical range list. Tests cover consumed,
stale, and wrong-directory preparation, interrupted installation and commit,
process termination, and preservation of acknowledged record prefixes.
