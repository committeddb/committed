# Rolling upgrades

This runbook is for operators upgrading a running Committed cluster from
one release to another **without downtime**. It covers the node-by-node
procedure, how to verify each step, and how to roll back.

The short version: replace the binary on one node at a time, leader last,
waiting for each node to report `/ready` and the new `/version` before
moving on. A node only ever changes its **binary** — its
`COMMITTED_*` environment (node id, peers, data directory) stays the
same, and its on-disk state is reused in place.

## Why this works

Two properties make a rolling upgrade safe:

- **Quorum survives one node at a time.** A 3-node cluster tolerates one
  node down; a 5-node cluster tolerates two. As long as you take down at
  most that many at once (this procedure takes down exactly one), the
  cluster keeps electing a leader and committing writes throughout.
- **A new binary reads the old node's on-disk state.** The WAL, the
  permanent event log, and the BoltDB metadata are forward-compatible
  across a release within the same major line — a node upgraded in place
  reads everything its predecessor wrote. This is a contract, documented
  in [api-compatibility.md → On-disk and wire compatibility](../api-compatibility.md#on-disk-and-wire-compatibility);
  read it before upgrading, especially the **one-way transitions** list.

If a node ever *can't* read its on-disk state it fails to start with a
clear error (see [rebuild.md](rebuild.md)) rather than running on
corrupt data — so a node that comes back up `/ready` has, by
construction, read its prior state successfully.

**One exception — some releases can't be rolled.** A release that introduces a
new *internal* entry type is a **full-stop** upgrade: a not-yet-upgraded node
fatal-exits when it applies an entry type its binary doesn't recognize, so
rolling would crash the nodes you haven't upgraded yet. See the warning under
*Before you upgrade*.

## Before you upgrade

1. **Read the release notes** for every version between your current one
   and the target, and the [on-disk compatibility
   contract](../api-compatibility.md#on-disk-and-wire-compatibility).
   Confirm the upgrade does not cross a major version (`/v1` → `/v2`) or
   trigger a one-way transition you can't roll back from.
2. **Confirm the cluster is healthy first.** `GET /v1/membership` on any
   node should show every member present with `appliedIndex` close to
   `commitIndex`. Don't start an upgrade on a degraded cluster — you'd
   be removing redundancy you may need.
3. **Have a recovery path.** A node's data directory is reusable by the
   previous binary (rollback, below); a node that loses its disk entirely
   is recovered from a healthy peer per [rebuild.md](rebuild.md).
4. **Size the shutdown window.** Each node is stopped with `SIGTERM` and
   drains gracefully; make sure `COMMITTED_SHUTDOWN_TIMEOUT` and your
   orchestrator's kill grace period are set per
   [shutdown.md](shutdown.md) so the graceful path isn't `SIGKILL`ed
   mid-drain.

> **⚠️ Some upgrades must be full-stop, not rolling.** A release that
> introduces a new *internal* entry type crosses a forward-only boundary: once
> the first upgraded node commits an entry of that type, every node still on the
> old binary **fatal-exits** when it applies it (it cannot resolve a type it
> doesn't know). Upgrade such a release **all nodes at once** — stop the whole
> cluster, replace every binary, restart — rather than node-by-node. It is a
> one-time boundary; once every node carries the change, rolling upgrades are
> safe again. The release notes for such a version say so explicitly.
>
> **This applies to any pre-0.7.2 → 0.7.2+ multi-node upgrade.** 0.7.2
> introduced the cluster feature-level mechanism, which announces a new internal
> entry. Single-node deployments and fresh installs are unaffected. If you have
> already started rolling and old nodes are crash-looping, do **not** roll back —
> finish upgrading them all; they recover once the whole cluster is on 0.7.2+.
>
> **It also applies to any 0.7.2 → 0.7.3+ multi-node upgrade**, with a sharper
> failure mode: 0.7.3 encodes every committed entity in a typed envelope
> (see [api-compatibility.md → Log entities](../api-compatibility.md#log-entities-protobuf)),
> which a 0.7.2 node decodes **without error as an empty entity** and silently
> misapplies — no crash-loop warns you. Never run a mixed 0.7.2/0.7.3 cluster:
> stop all nodes, replace every binary, restart. Single-node deployments and
> fresh installs are unaffected, and rolling back a node to ≤ 0.7.2 after any
> 0.7.3 entry is committed means a [rebuild](rebuild.md), not a binary swap.

## The procedure

Upgrade **one node at a time, followers first, the leader last.** Find
the leader with `GET /v1/membership` (the `leaderId` field, or the
member whose `isLeader` is true).

For each node:

1. **Take it out of rotation** (if it serves API reads behind a load
   balancer): let your readiness check deregister it, or `preStop`-sleep,
   so new requests stop arriving before the drain starts. Peer (raft)
   traffic needs no action — the other nodes route around it.
2. **Stop it with `SIGTERM`** (`kill`, `systemctl stop`, pod eviction).
   The node drains HTTP, stops raft, and closes the WAL cleanly; see
   [shutdown.md](shutdown.md) for the log lines to expect. Wait for the
   process to exit.
3. **Replace the binary.** Swap in the new `committed` binary. **Do not
   change** `COMMITTED_NODE_ID`, `COMMITTED_PEERS`, `COMMITTED_PEER_URL`,
   or `COMMITTED_DATA_DIR` — the node must come back as the *same* member
   over the *same* data directory.
4. **Start it.** It recovers raft state from its WAL + BoltDB and rejoins
   via the peers it already knows.
5. **Wait for `/ready` to return `200`.** While catching up it returns
   `503`; `200` means it has a known leader and has applied through the
   committed log. (`/health` flips to `200` as soon as the process is
   live — that is liveness, not readiness; gate the next step on
   `/ready`.)
6. **Confirm the new build.** `GET /version` should report the new
   `version`/`commit`. Re-confirm membership: `GET /v1/membership`'s
   `appliedIndex` for this node should be tracking `commitIndex`.
7. **Move to the next node.** Only proceed once the just-upgraded node is
   `/ready` and caught up, so you never have two nodes out at once.

**The leader goes last.** On a graceful stop the leader hands off to a
caught-up peer before it exits, so a new leader is usually in place
almost immediately rather than after a full election (see
[shutdown.md → Leadership handoff](shutdown.md#leadership-handoff)). Any
writes in flight during the brief handoff retry; reads with
`?consistency=stale` and already-committed data are unaffected. After the
new leader settles, upgrade the old leader like any other follower.

## Verifying the upgrade

After the last node:

- **Every node reports the new build.** `GET /version` on each node.
- **Every node is caught up.** `GET /v1/membership` shows every member's
  `appliedIndex` close to `commitIndex`, and there is exactly one
  leader.
- **A write round-trips.** `POST /v1/proposal` (or any config write) and
  confirm it commits — proof the new cluster is accepting and applying
  writes.
- **No corruption surfaced.** Corrupt or unreadable entries make a node
  *fail to start* (see [rebuild.md](rebuild.md)), so a fully `/ready`
  cluster has already cleared this bar. The
  `committed.wal.corrupt_entries` counter (in your OTel/metrics backend —
  Committed exports metrics via OTLP, it does not serve a `/metrics`
  endpoint) should be flat.

> **Feature activation is automatic.** A release may carry features that
> stay dormant until *every* node is upgraded — anything an older peer
> would mishandle is held back by the [cluster feature
> level](../api-compatibility.md#cluster-feature-level-semantic-compatibility-gate)
> until the whole cluster advertises support, then activates on its own.
> You don't enable anything; just finish the roll. The corollary: don't
> run half-upgraded indefinitely (a stalled roll leaves those features
> off), and once a feature has activated, rolling a node back below it is a
> one-way transition (see the compatibility contract).

## Rolling back

If the new binary misbehaves on a node — fails to start, fails `/ready`,
or shows a regression — roll that node back the same way you upgraded it:
`SIGTERM`, put the **previous** binary back, start over the same data
directory, wait for `/ready`. The previous binary can read the state the
new one wrote, **except across a one-way transition** — those are listed
in the [on-disk compatibility
contract](../api-compatibility.md#on-disk-and-wire-compatibility) and are
the reason you check it before upgrading. If you've crossed one, rollback
of that node is a [rebuild from a healthy peer](rebuild.md) still running
the old binary, not an in-place binary swap.

Roll back node-by-node, leader last, exactly as you rolled forward — the
same quorum rule applies in reverse.

## Notes and limits

- **Mixed-version window.** During the roll the cluster runs mixed
  versions (some nodes new, some old) for the duration of the procedure.
  That's expected and safe within a major line; the forward/backward
  read compatibility contract is what makes it so. Keep the window short
  — finish the roll rather than parking the cluster mid-upgrade.
- **Feature coordination is automatic, not manual.** Committed *does* gate a
  semantically-incompatible feature on the [cluster feature
  level](../api-compatibility.md#cluster-feature-level-semantic-compatibility-gate):
  it stays dormant until every node advertises support, then activates on its
  own (see the callout under *Verifying the upgrade*). You never run a manual
  version-bump barrier — just finish the roll. (Introducing the mechanism itself
  is the one full-stop exception; see *Before you upgrade*.)
- **Scaling during an upgrade.** Don't combine a membership change
  (add/remove/promote a node) with a rolling upgrade — finish one before
  starting the other, so you're only changing one variable at a time. See
  [membership.md](membership.md).
