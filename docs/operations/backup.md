# Backup and restore

This runbook covers `committed backup` and `committed restore` — the offline
primitive for archiving a node's state to a portable tarball and reconstituting
it into a fresh data directory.

It complements [rebuild.md](rebuild.md). Rebuild recovers a *single* dead node
by rsyncing from a *healthy peer* — it needs a surviving peer and only ever
restores in place. Backup/restore covers what rebuild can't:

- **Point-in-time archival** — keep a snapshot of the cluster's state.
- **A known-good export before a risky change** — a type migration, a metadata
  scrub, a bulk delete — so you can roll back the *data*, not just the binary.
- **Off-box disaster recovery** — ship the tarball to object storage.
- **Total-loss recovery** — rebuild a cluster where *every* node lost its local
  state at once (no healthy peer to rsync from). On ephemeral/instance-store
  disks this is a routine risk — a fleet recycle or AZ event can take out a
  quorum of disks together, which raft replication alone cannot survive.

## Why it's offline

A running node holds an **exclusive lock** on its BoltDB metadata for its whole
life, so a second process can't read the directory consistently — and a naive
copy of a live directory would be torn. `committed backup` therefore operates
on a **stopped** node's data directory, which is quiescent and trivially
consistent, and it **refuses to run against a live node** (it probes the lock
and errors if the node is up).

This is a deliberate, in-lane primitive: it does not run against a live
database, and there is no backup *endpoint*. Automation (periodic backups,
shipping to S3, retention) is the operator's to build on top — see *Shared
responsibility* below.

## Taking a backup

Stop the node, then archive its data directory:

```bash
# (the node must be stopped — SIGTERM it first; see shutdown.md)
committed backup --data /var/lib/committed --to /backups/committed-2026-06-19.tar.gz
```

- `--data` defaults to `$COMMITTED_DATA_DIR` (or `./data`).
- `--to` is required. A `.gz` suffix gzip-compresses the archive.
- `--node-id` is recorded in the manifest for provenance (defaults to
  `$COMMITTED_NODE_ID`).

The archive is written atomically — a failed backup leaves nothing at `--to`.
It contains a `MANIFEST.json` plus the node's raft logs, permanent event log,
and BoltDB metadata.

### Backing up a live cluster without downtime

You don't have to take the whole cluster down. Back up **one follower** at a
time, exactly like a rolling upgrade ([upgrade.md](upgrade.md)):

1. `GET /v1/membership` to find a follower (not the leader).
2. `SIGTERM` it and wait for it to exit (quorum holds on the remaining nodes —
   the cluster keeps serving).
3. `committed backup --data <its dir> --to <tar>`.
4. Start it again; it rejoins and catches up.

A follower's backup is a complete, restorable snapshot of the cluster's
committed state — every node holds the full event log.

`--data` must point at the node's **real** directory. Backup fails closed on a
symlinked data root or a symlinked store under it (and on a directory with no
files), rather than silently walking past it and writing a hollow archive that
would still pass every restore check. If your deployment mounts the data
directory behind a symlink, resolve it to the real path before backing up.

## Restoring

Restore into a **fresh, empty** directory (restore refuses to overwrite
existing data), then start a node against it:

```bash
committed restore --from /backups/committed-2026-06-19.tar.gz --data /var/lib/committed-restored
# then start a node pointed at the restored dir, with the SAME identity:
COMMITTED_NODE_ID=1 COMMITTED_PEERS='1=http://...' COMMITTED_DATA_DIR=/var/lib/committed-restored committed node
```

The restored directory **is** a node's directory — the node recovers from it
exactly as it would after a normal restart. Start it with the **same**
`COMMITTED_NODE_ID` and `COMMITTED_PEERS` the source node used. Restore drops a
`RESTORED.json` marker recording where the backup came from.

Restore validates the manifest, refuses an archive that isn't a committed
backup or declares an incompatible format version, and rejects any archive
entry whose path would escape the target directory.

Restore is **atomic on failure**: it unpacks into a staging directory alongside
the target and renames it into place only after the whole archive validates, so a
failed restore (a truncated archive, a full disk) leaves the target directory
untouched and a retry is never blocked by a half-restored directory. This is
visibility-atomicity, not crash-durability — the staging files aren't `fsync`'d
before the publish rename, so a power loss *during* the rename leans on the
[crash-consistent filesystem](../storage-architecture.md) requirement; restore is
fully re-runnable.

### Cluster identity

A restored node keeps the **source cluster's identity**. Restoring is for
recovering or *cloning* a cluster, not merging two — pulling data from a
different cluster into this one is an ETL job, not a restore.

### Version compatibility

Restore validates the archive's **format version** (it refuses a non-committed or
future-format archive), but it does **not** check the binary version — a restore
across binary versions that share the archive format is *allowed but unvalidated*.
Recommendation: back up and restore with the **same binary version**, then
upgrade (see [upgrade.md](upgrade.md)); a cross-version restore is yours to reason
about against the on-disk compatibility contract in
[api-compatibility.md](../api-compatibility.md).

## Off-box shipping

The tarball is a plain file — ship it wherever your DR policy requires:

```bash
aws s3 cp /backups/committed-2026-06-19.tar.gz s3://my-bucket/committed/
# or rclone / restic / scp — committed does not integrate object storage directly
```

## Shared responsibility: backups and right-to-be-forgotten

A RTBF delete on a live cluster is scrubbed from the event log within the scrub
window, so the subject's PII leaves the *running* nodes promptly. **A backup is
a frozen copy and is never scrubbed.** Two consequences fall to the operator,
not committed:

- **At rest:** any backup taken before a subject was scrubbed still contains
  that subject's PII — including backups from long before the delete was ever
  issued. The real "forgotten" window is therefore the **lifetime of every
  backup**, not the scrub latency. Bound backup retention to your RTBF
  obligations and expire/destroy old backups accordingly.
- **On restore:** restoring a backup **resurrects** the PII of every subject
  deleted-and-scrubbed *after* that backup was taken — those deletes happened
  later and aren't in the archive. committed keeps **no ledger of erased
  subjects** — a right-to-be-forgotten delete physically removes the data from
  the log, leaving nothing to enumerate. You must therefore record RTBF requests
  in your own compliance system and **re-issue any that post-date the backup**
  after a restore. The restored node's `applied_index` (from `GET /v1/membership`)
  marks how far the backup covered, so you know which of your tracked requests to
  re-apply.

committed gives you the primitive and the frozen manifest; because erasure is
physical (committed retains no record of what was deleted), managing backup
retention, the record of RTBF requests, and post-restore re-forgetting within
your compliance regime is the operator's responsibility.
