# Rebuilding a Committed node

This runbook is for operators. It describes the manual rebuild procedure
an operator runs when a Committed node cannot continue safely — either
because it fell too far behind its cluster to catch up via normal raft
replication, or because an operator is standing up a new node and wants
to avoid the long initial replay.

Background: the relevant design lives in
[`docs/event-log-architecture.md`](../event-log-architecture.md). Read
the "Catch-up taxonomy" and "Storage invariant" sections before running
this procedure.

## When to rebuild

Rebuild is the right response when any of these hold:

- The node logged a fatal error matching:

  ```
  storage invariant violation: permanent event log is behind raft applied index.
  The cluster has compacted past this node's recovery point. Run the rebuild
  procedure at docs/operations/rebuild.md.
  ```

  This means the cluster has compacted past this node's recoverable
  window. Normal raft replication cannot close the gap in v1 — the
  leader's raft log no longer carries the entries between the node's
  `EventIndex` and `AppliedIndex`, and the metadata snapshot the
  leader shipped cannot fill in the event log. The node has already
  fatal-exited; continuing to run it is not safe.

- A brand-new node is joining a cluster that has been running long
  enough for its raft log to have compacted past `raft index 1`. In
  practice that's any multi-day-old cluster with non-trivial write
  traffic. Starting the new node with an empty data directory would
  leave it in the same "severely behind" state the fatal exit is
  describing — rebuild it up-front instead.

- The node's disk is damaged (bit rot, incomplete write after power
  loss, partial restore) and `committed` refuses to start. The rebuild
  procedure replaces the data directory with a known-good copy.

Rebuild is NOT the right response when:

- The node briefly fell behind because of a network blip, restart, or
  GC pause. Normal raft replication handles this case automatically
  via `AppendEntries`; there is nothing for an operator to do.

- The cluster has lost quorum. Rebuild won't help — you need a quorum
  of live peers to copy from. Fix quorum first (bring up enough
  surviving nodes, or perform a consensus-recovery operation).

## What the procedure does

At a high level: stop the broken node, copy its data directory from a
healthy peer with `rsync`, reset ownership, and restart. The copied
data is a byte-for-byte snapshot of the healthy peer at the moment the
rsync ran — meaning `P_local == R_local` holds on startup, the node
joins raft cleanly, and the small gap that accrued during the rsync
fills via normal replication.

Apply determinism (see `docs/event-log-architecture.md` §
"Determinism requirement") is what makes this O(diff) cheap on
subsequent rebuilds: rsync's `--inplace` mode only transfers segments
that changed, and every node's permanent event log is byte-identical
under determinism. On the very first rebuild rsync ships everything; on
later rebuilds it ships only what diverged.

## Prerequisites

- A **healthy peer** — another node in the cluster whose data
  directory is complete and current. Verify this by confirming the
  peer is accepting writes and serving the `/ready` probe with
  `applied_index > 0`.

- **SSH access** from the failed node to the healthy peer (or,
  equivalently, a shared filesystem or object-store staging area).

- **Root or sudo** on the failed node so you can stop and start the
  service and fix ownership after rsync.

- Enough **local disk** to hold a full copy of the healthy peer's
  data directory. Rsync can stream over a partial failed state if
  `--inplace` is set, reducing the transfer size on repeat rebuilds.

## Procedure — existing follower

Run these commands on the failed node. Adjust paths if your
`committed` service stores data somewhere other than
`/var/lib/committed`.

```bash
# 1. Stop the service so nothing is writing to the data directory
#    while rsync runs.
sudo systemctl stop committed

# 2. Clear the stale state. You can leave the directory in place if
#    you prefer to let rsync's --delete handle it, but a clean slate
#    makes the first rebuild less surprising and matches the shape
#    of a brand-new-node bootstrap.
sudo rm -rf /var/lib/committed/*

# 3. Rsync from a healthy peer. --inplace makes later rebuilds of
#    the same node much faster (only the diff transfers). -a
#    preserves permissions, times, and symlinks; -v gives you a
#    running progress log you can capture for post-mortem.
sudo rsync -av --inplace healthy-peer:/var/lib/committed/ /var/lib/committed/

# 4. Restore ownership. rsync preserves numeric UIDs by default; if
#    the committed user has a different UID on this host, fix it.
sudo chown -R committed:committed /var/lib/committed

# 5. Start the service. The node loads the rsync'd state, finds
#    P_local == R_local, joins raft, catches up the small gap that
#    accrued during the rsync, and resumes serving.
sudo systemctl start committed
```

If the service fails to start, check `journalctl -u committed` for the
specific error. The most common cause is a half-complete rsync — retry
from step 2 with a full (not `--inplace`) transfer.

## Procedure — adding a new node

Same as the rebuild procedure, with one extra step: propose a raft
configuration change adding the new node's ID.

```bash
# 1-5. Same as "existing follower" above: stop the (empty) service,
#     rsync from a healthy peer, restore ownership, start the service.
#
# The node will come up in a "waiting for cluster membership" state —
# it has rsynced state but no peer has told it it's a cluster member
# yet.

# 6. From any existing cluster node, propose a conf change adding the
#    new node ID. Use the cluster's normal admin API for this; the
#    exact command depends on your deployment.
```

After the conf change commits, the new node transitions to a voting
member. Normal raft replication fills the gap between the rsync time
and the conf-change time.

The new node does not need any `--joining` flag or a separate
bootstrap subcommand. Whether it is a member of the cluster is
determined entirely by whether a conf change has been propagated for
it. Until then, the binary runs in "standalone" mode, receiving
heartbeats but not participating in voting. That's deliberate — it is
also the state every brand-new cluster member was in for a few
seconds during its first startup.

## Verification

After the service is running, confirm the node is healthy:

```bash
# /ready returns 200 only once raft has elected a leader AND this
# node has applied at least one entry. An unready node blocks the
# health check until the gap fills.
curl -sf http://localhost:8080/ready

# /health is a lighter-weight liveness probe.
curl -sf http://localhost:8080/health
```

If `/ready` stays 5xx for more than a few minutes after startup, the
node is stuck. Likely causes:

- The healthy peer you rsynced from was behind the cluster. Rsync
  from a different peer.

- The conf change (for new nodes) has not committed yet. Check the
  proposer's logs.

- The data directory permissions are wrong and the binary can't open
  bbolt. Re-run the `chown` step.

## Why this is "manual"

v1 intentionally leaves severe-lag catchup to an operator. The
architecture doc is explicit:

> This is intentionally fail-fast. A node that cannot satisfy the
> storage invariant must not be running, because it could otherwise
> serve stale syncable reads or (if elected leader, however briefly)
> confuse the cluster.

A future v2 will automate this by streaming event log segments
between peers inside the process, so an operator only needs to do
disk-level recovery (bit rot, lost host) and not routine "my follower
fell behind during a long deploy" recovery. Until then, this runbook
is the supported path.
