# Rebuilding a Committed node

This runbook is for operators. It describes when a Committed node has to be
rebuilt — replaced by a fresh node that takes the cluster's history from a
peer — and how. Falling behind is **not** one of those times: a node whose
permanent event log is behind the cluster catches up by itself (below).

Background: the relevant design lives in
[`docs/event-log-architecture.md`](../event-log-architecture.md) — read the
"Catch-up taxonomy" and "The central invariant" sections first.

## Falling behind is handled automatically

A node that comes back after the cluster has compacted its raft log past the
node's position — a long outage, a slow deploy — or a brand-new node that
starts over an empty data directory, cannot be served by normal raft
replication: the leader can only ship a metadata snapshot, and the node's
permanent event log is behind it. The node **catches up on its own**: it
fetches the events it is missing from a peer, over the peer transport, then
installs the snapshot and resumes normal replication. Nothing to configure,
nothing to run.

While it does, the node reports the catch-up on its own status and answers
`/ready` with 503:

```bash
curl -s http://n4:8080/v1/node/status | jq .catchingUp
# { "have": 118203, "need": 2410077, "since": "2026-09-11T14:02:10Z", "peer": 2 }
```

`have` is the raft index the node's event log has reached; `need` is the
index of the snapshot waiting to install; the block disappears once
replication has taken over. A catch-up that makes no progress logs a warning
every 30 seconds naming the reason (usually: no peer is reachable, or every
peer is on an older binary that does not serve fetches — see
[upgrade.md](upgrade.md)); it keeps retrying until a peer answers.

Two things to know:

- **It needs a live peer with the history.** Any member that holds the
  events serves them (the fetch tries peers in turn). The cluster keeps
  serving throughout; the serving peer defers its own log maintenance —
  raft-log compaction, segment compression, a scrub's swap — only while an
  exchange is in flight, a few seconds at a time.
- **A node that missed a right-to-be-forgotten scrub fetches its log whole.**
  If a scrub completed while the node was away, its own event log still holds
  the erased data and can no longer be brought forward, so the node discards
  it and takes the cluster's copy in full. This is by design; expect a full
  transfer in that case.

The previous procedure — rsync a healthy peer's data directory onto the node
— is no longer needed for a node that fell behind, and no longer documented.

## When to rebuild

Rebuild — retire the node's identity and bring up a fresh one — when the
node's own state cannot be trusted:

- The node logged a fatal error beginning:

  ```
  raft state rewound: the leader's heartbeat commits beyond this node's log.
  ```

  This node's data directory went *backward* — it previously acknowledged
  (and possibly voted on) entries its log no longer contains. In practice:
  a member was restored from a backup. This is not the same as being
  behind (the node catches that up automatically); a rewound node has
  broken its promises to the cluster, and running it endangers cluster
  safety, so it exits immediately and on every restart until rebuilt.
  **Members are rebuilt, never restored** — backup/restore is for
  single-node deployments and whole-cluster recovery, where every node
  rewinds together and no stale memory of the old state survives.

- The node's disk is damaged (bit rot, a partial restore, or an in-record
  corruption of a committed entry) and `committed` refuses to start, or a
  syncable's read wedges on a corrupt record.

  Each WAL entry (raft log, permanent event log, and state log) carries a
  CRC32C checksum, verified on read. A detected mismatch fails the read
  with:

  ```
  wal: entry checksum mismatch (data corruption); see docs/operations/rebuild.md
  ```

  and increments the `committed_wal_corrupt_entries_total` metric (labelled
  by `log`). Hit during the startup recovery reads, this aborts `Open` and
  the node fatal-exits. Hit on a syncable's read of the permanent event
  log, it instead wedges that syncable: the reader holds position (nothing
  is skipped), an error is logged on each retry, and the node stays up —
  raft, ingest, the API, and every other syncable keep running while you
  diagnose. The same repair flow below applies, on your schedule.

  **First, rule out a torn tail — it doesn't need a rebuild.** A power loss
  mid-append can leave a partial *trailing* record; that record was never
  acknowledged, so dropping it is safe. With the node stopped, run:

  ```
  committed wal repair --data <node-data-dir>
  ```

  It reports what it finds and changes nothing; re-run with `--commit` to
  truncate a torn tail, after which the node restarts cleanly. The tool
  **refuses** anything that is not a torn tail — a bitflip in a committed
  record, mid-log — because that data is genuinely gone locally. *That* is
  when you rebuild (below) — or, on a **single node**, splice the bytes
  back from a backup of that node:

  ```
  committed wal repair --data <node-data-dir> --from <backup.tar.gz>
  ```

  The record at a given log position is byte-identical everywhere, so a
  backup that covers it holds the correct bytes. The tool reports the plan
  (which record, from which archived segment) and changes nothing; re-run
  with `--commit` to apply it. A corrupt record in a plain segment is
  spliced byte-for-byte; a corrupt compressed segment (one flipped byte
  fails the whole zstd frame) is replaced by the backup's copy. Every
  splice is verified before it is written — the archive entry matches the
  backup's manifest, the two logs agree byte-for-byte on every other record
  they share (so a backup taken before a scrub or a truncation can never
  re-introduce rewritten bytes), the record's raft index continues its
  neighbours, and the assembled segment re-scans clean — and is refused
  otherwise, leaving the log untouched. Corruption the backup does not
  cover (it predates the record) still needs a rebuild or a restore.

  Every log a supported deployment can hold is checksummed end to end
  (framing shipped in v0.5-beta, well below the data-directory floor);
  unframed or torn bytes are corruption, never trusted content.

- A node ran out of disk and you can't expand the volume in place.
  The cluster keeps admitting writes while a quorum of voters has disk
  headroom (see [disk-limits.md](disk-limits.md)), which deliberately
  sacrifices the constrained node — its copy of the replicated log
  keeps growing until it exits. Rebuild it onto a bigger volume.

- The node logged:

  ```
  storage invariant violation: permanent event log is behind raft applied index.
  ```

  This should not happen: the automatic catch-up fills the event log before
  a snapshot installs. If it does, report it; rebuilding the node (below)
  recovers it meanwhile.

Rebuild is NOT the right response when:

- The node fell behind — a long outage, a slow deploy, a brand-new node.
  It catches up by itself (above); there is nothing for an operator to do
  but watch `catchingUp`.

- The cluster has lost quorum. Rebuild won't help — a fresh node needs a
  live quorum to join and a peer to fetch from. Fix quorum first (bring up
  enough surviving nodes, or perform a consensus-recovery operation).

## Procedure

A rebuild is a membership change: a fresh node joins under a **new id**,
over an empty data directory, catches up like any new node, and the damaged
node's id is removed once the replacement is a voter. Reusing the old id is
not safe — the cluster remembers what that id acknowledged and voted for,
and a node coming back empty under it is exactly the rewound member the
`raft state rewound` guard exits on.

```bash
# 1. Stop the damaged node and clear its data directory.
sudo systemctl stop committed
sudo rm -rf /var/lib/committed/*

# 2. Give it a NEW id and start it in join mode (see membership.md for
#    the full environment). It comes up empty and waits to be added.
#      COMMITTED_NODE_ID=4
#      COMMITTED_JOIN=true
#      COMMITTED_PEERS=1=http://n1:9022,2=http://n2:9022,3=http://n3:9022,4=http://n3:9022
sudo systemctl start committed

# 3. Add it as a learner, watch it catch up, promote it.
committed member add --id 4 --url http://n3:9022 --learner --target http://n1:8080
curl -s http://n3:8080/v1/node/status | jq .catchingUp   # until the block is gone
committed member promote --id 4 --target http://n1:8080

# 4. Only now retire the damaged node's id.
committed member remove --id 3 --target http://n1:8080
```

Adding before removing keeps quorum arithmetic honest: a three-voter
cluster with one member down is at two of three throughout, and never at
two of two — the replacement joins as a learner (no effect on quorum), is
promoted only once caught up, and the old id leaves last.

## Verification

Verify through the node's **own API** — `/ready`, `/health`, and its own
`appliedIndex` advancing — never through the leader's `matchIndex` for it.
matchIndex is leader-side memory of the highest index the member ever
acknowledged: it is not a liveness signal, and it reads "healthy" for a node
that is stopped, dead, or wiped, until live commits visibly outrun it.

Behind a load balancer (no per-node addressing), use the `active` field on
`GET /v1/membership` instead: it is raft's own recent-activity signal — the
leader heard from the member within roughly the last election timeout — and
it reads `false` for exactly the stopped/dead/wiped states `matchIndex`
masks. `active: true` plus `matchIndex` closing on `commitIndex` is a
complete remote verification; the node's own `/ready` remains the direct
confirmation when you can reach it. (See membership.md for the one
sampling caveat on `active`.)

```bash
# /ready returns 200 only once raft has elected a leader, this node has
# applied at least one entry, and no catch-up is in progress.
curl -sf http://localhost:8080/ready

# /health is a lighter-weight liveness probe.
curl -sf http://localhost:8080/health
```

If `/ready` stays 5xx for more than a few minutes after startup, check
`/v1/node/status`:

- `catchingUp` present and `have` advancing: it is working; a large log
  takes a while.
- `catchingUp` present and `have` not advancing: no peer is serving. Check
  that the node can reach its peers (`COMMITTED_PEERS`, network) and that at
  least one peer runs a binary that serves fetches (0.8.0 or later).
- No `catchingUp` and `leader` is 0: the conf change (for new nodes) has
  not committed yet, or the node cannot reach the cluster.
