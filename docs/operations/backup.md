# Backup and restore

This runbook covers `committed backup` and `committed restore` — archiving a
node's state to a portable tarball and reconstituting it, on new machines, as
a new cluster. A backup is taken either from a **stopped** node's data directory
or, with `--live`, from a **running** node over its API; the archive is the
same and restores the same way.

It complements [rebuild.md](rebuild.md). A node that fell behind, or a fresh
node replacing a dead one, takes the cluster's history from a *healthy peer*
by itself — that needs a surviving peer. Backup/restore covers what a peer
can't:

- **Point-in-time archival** — keep a snapshot of the cluster's state.
- **A known-good export before a risky change** — a type migration, a metadata
  scrub, a bulk delete — so you can roll back the *data*, not just the binary.
- **Off-box disaster recovery** — ship the tarball to object storage.
- **Total-loss recovery** — rebuild a cluster where *every* node lost its local
  state at once (no healthy peer to fetch from). On ephemeral/instance-store
  disks this is a routine risk — a fleet recycle or AZ event can take out a
  quorum of disks together, which raft replication alone cannot survive.

## Offline or live

A running node holds an **exclusive lock** on its BoltDB metadata for its whole
life, so a second process can't read the directory consistently — and a naive
copy of a live directory would be torn. There are two ways to a consistent
archive:

- **Offline** — `committed backup --data <dir>` archives a **stopped** node's
  data directory, which is quiescent and trivially consistent. It takes a
  **shared lock** on the BoltDB file and **holds it for the whole archive**: a
  live node is refused (its exclusive lock blocks the shared one), and a node
  that tries to start part-way through the copy is blocked (its exclusive
  open fails) so it can't write into the directory mid-walk.
- **Live** — `committed backup --live --target <node>` asks a **running** node
  to stream its own archive (`GET /v1/node/backup`). The node reads its four
  stores in an order a restart tolerates — metadata, then the raft state log,
  then the permanent event log, then the raft entry log, so the raft log
  always covers every event the archive holds — and holds each log's
  maintenance still only while that log streams: segment compression and a
  scrub's swap during the event log, raft-log compaction during the entry log.
  The cluster keeps serving throughout. See *Taking a backup live* below.

> The shared lock guards BoltDB, which is the only lock in the data dir, and a
> starting node acquires it as its FIRST act — before any of its WAL-recovery
> steps touch the directory — so a node racing to start while the backup runs
> fails immediately, having mutated nothing. The same stopped-node lock gates
> `committed wal repair` and `committed wal convert` (exclusively — a
> rewrite must not run under a concurrent backup either). For belt-and-braces
> atomicity, snapshot the filesystem (LVM/ZFS/reflink) and back up the
> snapshot. Simplest of all: keep the node stopped for the backup, as intended.

Automation (periodic backups, shipping to S3, retention) is the operator's to
build on top of either — see *Shared responsibility* below.

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

### Taking a backup live

A running node streams its own archive; nothing stops:

```bash
committed backup --live --target http://n2:8080 --to /backups/committed-2026-09-11.tar.gz
```

- `--target` is a node's API base URL (default: this host's
  `COMMITTED_API_ADDR`). Target a node **directly**, not a load balancer —
  the archive is that node's state — and prefer a follower: the node's raft
  loop keeps running, but the stream is disk and network work on that node.
- `--token` (default `COMMITTED_MEMBERSHIP_TOKEN`, falling back to
  `COMMITTED_API_TOKEN`) authenticates; `--insecure` skips
  TLS verification for an https target.
- A live archive does **not** carry projection stage stores (`projections/`
  under the data dir); an offline archive does. After restoring a live
  archive, each projection that declares stages rebuilds its store from the
  log before it resumes, without writing to its destination. Until then it
  lags: its status shows `workerState: "re-deriving"` and the progress in
  `stageRecovery`.
- The download is **verified as it arrives**: every entry is hashed and
  checked against the trailing manifest before the file is published, so a
  file at `--to` is a restorable archive. A node that is catching up from
  a peer refuses the backup before streaming anything (`503 catching_up`,
  which the command reports; take it once `catchingUp` clears on
  `GET /v1/node/status`). A backup the node abandons mid-stream — its own
  maintenance overtook the read (a snapshot install, a raft-log
  truncation), or a catch-up began while it streamed — ends with the
  node's reason, which the command reports; a stream cut by the connection
  or a stall ends without one. Either way nothing is left at `--to`; take
  it again.
- One live backup streams per node at a time (a second is refused with 409).

The manifest records `live: true`, the `appliedIndex` the archive restores to,
and the event log's `eventLogGeneration` (see *Shared responsibility* below).
Any member's backup is a complete, restorable snapshot of the cluster's
committed state — every node holds the full event log.

The offline alternative for a live cluster is still the rolling one: back up
**one follower** at a time, exactly like a rolling upgrade
([upgrade.md](upgrade.md)) — `SIGTERM` it, `committed backup --data <its dir>`,
start it again; it rejoins and catches up by itself.

`--data` must point at the node's **real** directory. Backup fails closed on a
symlinked data root or a symlinked store under it (and on a directory with no
files), rather than silently walking past it and writing a hollow archive that
would still pass every restore check. If your deployment mounts the data
directory behind a symlink, resolve it to the real path before backing up.

## Restoring

A restore recreates the cluster on new machines from **one archive** — the
newest you have — restored onto every machine, each as a different member of
the backed-up cluster. The restored cluster is a **new cluster** with its own
cluster id: it never exchanges a message with the cluster the archive came
from, or with another restore of the same archive.

Restore each machine into a **fresh, empty** directory (restore refuses to
overwrite existing data). The first machine mints the new cluster and prints
a seed; the others pass that seed:

```bash
# machine 1
committed restore --from /backups/committed-2026-06-19.tar.gz \
  --data /var/lib/committed --node-id 1 --new-cluster
# restored 41 files into /var/lib/committed (backup taken 2026-06-19T02:00:00Z, from node 2)
# node 1 of a new cluster 5f0c2a61-8d3e-4b7a-9c15-2e7d4f8a90b3 (members: 1, 2, 3)
# restore the other members from this same archive with --cluster-id 9b2e7d10-4c6a-4f2e-8b91-73a5c0d2e6f4

# machines 2 and 3
committed restore --from /backups/committed-2026-06-19.tar.gz \
  --data /var/lib/committed --node-id 2 --cluster-id 9b2e7d10-4c6a-4f2e-8b91-73a5c0d2e6f4
committed restore --from /backups/committed-2026-06-19.tar.gz \
  --data /var/lib/committed --node-id 3 --cluster-id 9b2e7d10-4c6a-4f2e-8b91-73a5c0d2e6f4
```

Then start each node with the `COMMITTED_NODE_ID` it was restored as and a
`COMMITTED_PEERS` naming the **new** machines' addresses:

```bash
COMMITTED_NODE_ID=1 \
COMMITTED_PEERS='1=http://new-n1:9022,2=http://new-n2:9022,3=http://new-n3:9022' \
COMMITTED_DATA_DIR=/var/lib/committed committed node
```

- **`--node-id`** must be a member of the backed-up cluster (restore lists
  the members, and refuses an id that isn't one). Give every machine a
  different one. The restored directory remembers it: a node started on it
  under another `COMMITTED_NODE_ID` refuses to start. Two machines restored
  under the same id are refused by the cluster, and stop
  ([membership.md](membership.md#peer-addresses-and-cluster-identity)).
- **A single-node cluster** is the same with one machine: `--node-id 1
  --new-cluster`.
- **Addresses come from `COMMITTED_PEERS`.** Restore drops the old cluster's
  recorded peer and API addresses, so nothing sends the restored nodes to the
  old machines; each node announces its own API address when it starts.
- **Ingestables and syncables start automatically.** The restored cluster is
  the production cluster now: it resumes reading its sources and writing its
  destinations from where the archive left off.

Restore drops a `RESTORED.json` marker recording where the backup came from.
If the archive was taken live, staged projections rebuild their stores from
the log before resuming (see [Taking a backup live](#taking-a-backup-live)).

Restore validates the manifest, refuses an archive that isn't a committed
backup or declares an incompatible format version, and rejects any archive
entry whose path would escape the target directory.

Restore is **atomic and crash-durable**: it unpacks into a staging directory
alongside the target and renames it into place only after the whole archive
validates and the directory has been rewritten for the new cluster, so a
failed restore (a truncated archive, a full disk, a `--node-id` that isn't a
member) leaves the target directory untouched and a retry is never blocked by
a half-restored directory. Before the publish it `fsync`s every staged file's
content and every staged directory, and after the rename it `fsync`s the
target's parent — so a crash can never surface a torn or zero-length restored
file. The publish itself is a single rename, which the
[crash-consistent filesystem](../architecture/storage-architecture.md)
requirement makes atomic: after any crash the target is either absent (retry
it) or fully populated and durable, never partial. Restore is fully
re-runnable.

### One archive, every machine

Restore every machine from the **same** archive. Archives of different nodes,
or taken at different times, hold different histories, and raft cannot
reconcile them: a cluster formed from them could lose or rewrite committed
entries. So the cluster id folds in the archive's contents, and a machine
restored from a different archive gets a different id even with the same seed
— the others refuse it at first contact (`412`, logged as `refused a peer
request from another cluster`). Any member's archive is complete, since every
node holds the full log; use the newest.

### Restore is for recovery, not cloning

A restored cluster runs as production: its ingestables read their sources and
its syncables write their destinations. Running one beside the cluster it was
restored from would have two clusters doing that work — consuming the same
replication slots, writing the same tables. Restore when the original is gone
(total loss), or after stopping it for good. Copying a cluster for testing
beside a live original is not supported.

Restore is also **not** how you recover one member of a running cluster: a
restored node belongs to a new cluster, so the running one refuses it.
Recovering one member is the **rebuild** flow — a fresh node that fetches
current state from a healthy peer ([rebuild.md](rebuild.md)). A cluster that
lost its majority but still has a survivor is brought back with
`committed recover` ([rebuild.md](rebuild.md#when-a-majority-is-lost)), not a
restore.

### Version compatibility

Restore validates two things about the archive and refuses on either.

The **archive format version**, which must match exactly. A future-format
archive is refused, and so is an older **v1** archive — its manifest carried
only file paths, with no per-file integrity, and current binaries no longer
decode it. Releases from 0.7.4 on write the current format, so this gate bites
only on an archive taken with 0.7.3 or earlier; re-take those from the running
cluster rather than trying to restore them.

The **feature level of the binary that wrote it**, recorded in the manifest: a
build refuses an archive stamped above its own, naming the level you need. This
gate is one-directional — an archive from an older binary restores onto a newer
one, never the reverse. It is deliberately coarse, because an archive can be
unreadable by an older build for a reason no per-entry check would find: from
0.8.0 sealed **event-log** segments are compressed at rest, and a pre-0.8.0
binary does not fail on them — it silently opens a **partial log**.

> **Archives from before 0.8.3.** They hold the tidwall event log, which
> 0.8.3 refuses to start on. Restore as usual: `committed restore` converts
> the event log to the segmented engine as it restores, and says so (see
> [upgrade.md](upgrade.md#upgrading-from-082-to-083)). Data that has run on
> 0.8.3 cannot move onto an older binary: its event log is segmented, which no
> earlier release opens. Check the rest of the one-way transitions in
> [api-compatibility.md](../architecture/api-compatibility.md#one-way-transitions-no-rollback-past-these).

Within a single release series, back up and restore with the same binary
version where you can; a cross-version restore that the gate allows is still
yours to reason about against the on-disk compatibility contract.

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
  issued. A backup taken shortly *after* a scrub can also still carry the
  erased value: the raft consensus buffer's transient copy (see the
  architecture doc) isn't clearing while the node is stopped, and rides along
  in `raft/` segments — inert, never served or re-applied, but present. Either
  way the rule is the same: the real "forgotten" window is the **lifetime of
  every backup**, not the scrub latency. Bound backup retention to your RTBF
  obligations and expire/destroy old backups accordingly.
- **On restore:** restoring a backup **resurrects** the PII of every subject
  deleted-and-scrubbed *after* that backup was taken — those deletes happened
  later and aren't in the archive. (The manifest's `eventLogGeneration` is the
  scrub bound the archived event log reflects.) committed keeps **no ledger of erased
  subjects** — a right-to-be-forgotten delete physically removes the data from
  the log, leaving nothing to enumerate. You must therefore record RTBF requests
  in your own compliance system and **re-issue any that post-date the backup**
  after a restore. The restored node's `appliedIndex` (from `GET /v1/node/status`;
  a live archive's manifest also records it) marks how far the backup covered,
  so you know which of your tracked requests to re-apply.

committed gives you the primitive and the frozen manifest; because erasure is
physical (committed retains no record of what was deleted), managing backup
retention, the record of RTBF requests, and post-restore re-forgetting within
your compliance regime is the operator's responsibility.
