# Disk-space limits and cluster-aware write admission

This runbook is for operators. It describes how Committed behaves as
disk fills — the per-node **disk-usage watcher** that classifies free
space, and the **cluster-aware write admission** built on top of it
that decides, cluster-wide, whether writes are accepted at all — plus
the knobs, metrics, and what to actually do during a disk-pressure
incident.

The short version: nodes degrade gracefully instead of fatal-exiting
on `ENOSPC`. A write is admitted iff the leader and a quorum of voters
have disk headroom; a constrained leader hands leadership away; a
constrained follower is deliberately sacrificed and is yours to fix.

Request-size caps and HTTP timeouts live in
[http-limits.md](http-limits.md); compaction (what actually frees
disk) is covered in
[event-log-architecture.md](../event-log-architecture.md).

## The per-node watcher

Each node runs a background **disk-usage watcher** that every 30s
`statfs`-es the data directory and classifies free space into one of
four states:

| State | Default threshold | Effect |
|---|---|---|
| `ok` | free > 20% | All writes flow. |
| `warn` | free ≤ 20% | Logged warning. All writes still flow. |
| `critical` | free ≤ 10% | **User-data proposals rejected** with 507 (API writes + ingest data). Config changes and checkpoints (sync index / ingest position bumps) still flow, and compaction is nudged to run sooner to free raft-log disk. |
| `full` | free ≤ 3% | **User data and config frozen** with 507. Checkpoints still flow so syncables keep delivering and recording progress; compaction continues. |

Rejections surface as:

```
507 Insufficient Storage
{"code":"insufficient_storage","message":"the cluster (or this node) is low on disk space and is rejecting writes; see GET /v1/node/status disk.admission, retry once disk space recovers"}
```

The gate lives at the propose choke point (`db.proposeAsync`), so it
covers the HTTP API, the ingest path, and internal config proposes
uniformly. Checkpoints are deliberately *not* blocked at `full`: they
are tiny (an index integer), and blocking them would make a syncable
re-deliver the same record in a loop and turn its "at most one
duplicate on crash" guarantee into a duplicate storm. The ingest path
treats a rejection as backpressure — it pauses (retries) rather than
dropping the proposal, so a full disk never advances an ingestable's
resume position past data that didn't commit.

**Recovery is automatic**: when free space climbs back above the warn
threshold the node returns to `ok` and re-enables writes, logging the
transition. Every state change is logged (critical/full at error
level), and three gauges track it for alerting:

- `committed_disk_free_bytes`
- `committed_disk_free_percent`
- `committed_disk_state{level="ok|warn|critical|full"}` — exactly one
  level is `1`. Alert on `level="critical"` or `level="full"`.

| Env var | Default | Notes |
|---|---|---|
| `COMMITTED_DISK_WARN_PERCENT` | `20` | Free-space percent for the warn state. |
| `COMMITTED_DISK_CRITICAL_PERCENT` | `10` | Free-space percent at which user-data writes start returning 507. |
| `COMMITTED_DISK_FULL_PERCENT` | `3` | Free-space percent at which config writes are also frozen. |

Thresholds must be descending (`warn > critical > full`). A value
outside `(0, 100)` logs a warning and falls back to its default. This
only nudges the existing compaction trigger to free space; it does
*not* delete WAL segments or event-log data on its own — that remains
compaction's job (see
[event-log-architecture.md](../event-log-architecture.md)). The
watcher is unavailable on Windows (no `statfs`); there it disables
itself and the node stays fully writable.

## Cluster-aware write admission

Every node stores the full replicated log, so a node fills from the
leader's replication stream regardless of where writes enter — a
node-local gate can't make a cluster-correct decision. Write admission
is therefore a **cluster** decision:

> a write is admitted iff the leader is healthy AND at least a quorum
> of voters are healthy, where "healthy" = above the critical
> threshold.

Concretely, for a 3-node cluster {L, F1, F2} (quorum = 2):

| Scenario | Verdict |
|---|---|
| Leader critical/full | **reject** (and transfer leadership — see below) |
| One follower critical/full, leader + one follower healthy | **admit** (even for writes arriving at the constrained follower) |
| Two followers critical/full, leader healthy | **reject** — quorum at risk |

The verdict preserves the severity ladder: a cluster-effective
`critical` rejects user data, `full` also freezes config, and
sync/ingest checkpoints always flow. Raft **membership changes are
never gated** (they travel a separate channel), so you can still add a
relief node mid-incident.

**How state moves.** Each member POSTs its disk state to the leader's
announced API URL every `COMMITTED_DISK_REPORT_INTERVAL` (default 10s)
at `POST /v1/node/disk-report`, and the response carries the leader's
current verdict back; the member caches it and enforces it at its own
propose gate. Disk state deliberately does *not* travel through raft
proposals — a node can't reliably write "I'm full" when it is full.
Enforcing at every ingress also closes the leak where a follower
forwarded proposals to a full leader over the raft transport,
bypassing the leader's gate.

This requires `COMMITTED_API_URL` on every node and a cluster-uniform
`COMMITTED_API_TOKEN` (the report endpoint is authenticated like every
other write). Setting `COMMITTED_DISK_REPORT_INTERVAL=0` disables
cluster-aware admission.

**Staleness and degraded modes — the gate fails open.** A verdict (or
a member report, on the leader's side) is trusted for 3× the report
interval. A node with no fresh verdict — no leader known, the leader
never announced an API URL, reports failing — falls back to the
node-local decision; a voter the leader hasn't heard from counts as
healthy in the quorum math (a crashed follower must not freeze a
healthy cluster). So the worst degraded mode is exactly the node-local
behavior, never below it, and a stale verdict can over- or under-admit
for at most ~30s (3× the default interval).

**Leadership transfer.** When the *leader's own* disk reaches critical
and some voter has a fresh report below critical, the leader hands
leadership to the healthiest such voter (ties broken by replication
progress) — converting "leader full: cluster rejects everything" into
"follower full: cluster keeps admitting". Transfers are rate-limited
(one per minute) and never target a node that hasn't recently reported
healthy. The `committed_disk_leadership_transfers` counter tracks them.

**A constrained follower is an operator job, not an automatic one.**
The cluster keeps admitting writes while a quorum is healthy, which
deliberately sacrifices the constrained follower (the replicated log
keeps growing on it). Expand its disk or replace it (see
[rebuild.md](rebuild.md)); the node does **not** remove or demote
itself — auto-membership-changes risk flapping, and a 2-node cluster
is *less* fault-tolerant than 3-with-one-degraded. For the same reason
`/ready` ignores disk state: it feeds load balancers, and draining a
low-disk node would also drain the reads it can still serve fine. The
queryable "writable" signal is `GET /v1/node/status` (`disk.admission`)
and the gauges below.

| Env var | Default | Notes |
|---|---|---|
| `COMMITTED_DISK_REPORT_INTERVAL` | `10s` | Report/verdict cadence (Go duration). `0` disables cluster-aware admission; the gate is then node-local only. |

## Observability and alerting

Per node:

- `committed_disk_state{level=...}` — this node's own disk level.
  Alert on `critical`/`full`: that node needs disk or replacement.
- `committed_write_admitted` — 1/0: does this node's gate admit
  user-data writes right now. Alert on 0.
- `committed_write_admission_reason{reason=...}` — exactly one of
  `ok | leader_disk | quorum_at_risk | cluster_reject | local_fallback`
  is 1. `quorum_at_risk` is the cluster-outage early warning: expand
  disk or replace nodes **before** quorum is lost. A persistent
  `local_fallback` means cluster admission isn't wired (missing
  `COMMITTED_API_URL`/token) or the leader is unreachable.
- `committed_disk_cluster_state{level=...}` — the cluster-effective
  level the gate is enforcing, same shape as `committed_disk_state`.
- `committed_disk_leadership_transfers` — disk-pressure leadership
  hand-offs.

For a one-off diagnosis, `GET /v1/node/status` returns the same
picture as JSON: `disk.state` (this node's level) and
`disk.admission.{admitted, state, reason, source, leader}` (the
decision its gate is applying, and whether it came from the leader's
verdict or the node-local fallback).

## During an incident

1. **`warn` on any node** — schedule disk expansion; nothing is
   rejected yet. Check compaction is keeping up
   ([event-log-architecture.md](../event-log-architecture.md)).
2. **One node `critical`/`full`, cluster still admitting** — the
   cluster routed around it. Expand the node's disk or replace it via
   [rebuild.md](rebuild.md). Do **not** remove it from the membership
   to "fix" the alert — that shrinks quorum and reduces fault
   tolerance.
3. **`quorum_at_risk`** — a quorum of voters is low; writes are frozen
   to protect the cluster from losing quorum to `ENOSPC` exits. Add
   disk to (or replace) enough voters to restore a healthy quorum;
   membership changes still work while admission is frozen. Admission
   resumes automatically as reports recover.
4. **`leader_disk`** — the leader itself is constrained and no healthy
   transfer target existed (otherwise it would have moved leadership
   already). Same fix: free disk on any voter, and the leader will
   hand off on its next cycle.
