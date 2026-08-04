# Safe mode

Safe mode is the operator escape hatch for a node that cannot complete a
normal boot because of the *work* it starts, not the data it holds: a
syncable or ingestable whose worker deterministically crashes the process, or
a scrub that fails the same way on every resume. A crashlooping node leaves
no API window to intervene — safe mode restores that window.

```
COMMITTED_SAFE_MODE=1 committed node
```

## What runs, what doesn't

Booted with `COMMITTED_SAFE_MODE` (any truthy value), the node runs
**everything except background work**:

- **Runs normally**: raft (the node participates in consensus, votes, and
  applies committed entries), the full HTTP API, config reads and writes,
  the disk watcher.
- **Held**: every sync worker, every ingest worker, and the background
  scrub. Configs still apply and list; no worker goroutine starts for them.
  A pending scrub bound stays durably recorded and resumes on the next
  normal boot.

Nothing about safe mode is persisted and nothing enables it automatically —
it is an explicit, per-boot flag. Restarting without it resumes all workers
and any deferred scrub from the stored configs.

## Multi-node clusters: leadership matters

Safe mode is per-node, but sync/ingest work is **owner-gated**, and the
default owner is the raft leader. That means:

- A safe-mode **follower** is nearly free: its held workers would have been
  idle anyway, and config writes proxy to the leader as usual.
- While a safe-mode node **is the leader**, every leader-owned syncable and
  ingestable pauses cluster-wide — the other replicas' workers are running
  but defer to the owner. Keep the safe-mode window short, or arrange for
  another node to hold leadership while you diagnose.
- On a **single-node cluster** — the typical crashloop scenario — the node
  is necessarily the leader, and that is fine: config deletes need this
  node to commit them, and its syncables were the ones down anyway.

## The procedure

1. Boot the failing node with `COMMITTED_SAFE_MODE=1`. The startup log
   carries a `SAFE MODE` banner, and `GET /v1/node/status` reports
   `"safeMode": true` — confirmation that workers are deliberately held,
   not mysteriously absent.
2. Diagnose over the API: list syncables/ingestables, check
   `/v1/node/status` for degraded configs, read the crash logs from the
   previous boots to identify the offending config.
3. Fix it: `DELETE` the config (or re-`POST` a corrected one — the config
   applies; its worker stays held like the rest). **Deleting in safe mode
   skips external teardown** (no worker was ever built to do it): a deleted
   syncable's destination table remains, and a deleted ingestable's
   replication slot/publication remains at the source — an orphaned slot
   pins the source's WAL, so drop it manually (see
   [CDC setup](cdc-setup.md)). The node logs a warning naming exactly this
   on each safe-mode delete.
4. Restart without `COMMITTED_SAFE_MODE`. Workers spawn from the stored
   configs as on any normal boot.

## Safe mode or a stopped node?

Pick by what you need to do, not by how broken things feel:

- **Reading is a stopped-node activity.** If you only need to inspect data
  — dump the log, examine cursors, capture evidence — stop the node, copy
  the data directory, and work on the copy offline. No cluster, no safe
  mode, no risk of mutating what you're examining (even opening a WAL
  read-only can mutate it).
- **Safe mode is for when the fix must commit.** Deleting or replacing a
  config is a replicated state change — it needs a running, committing
  member. That is the one thing a stopped node cannot do, and it is the
  only reason safe mode exists.

A safe-mode node stays a **full raft voter** deliberately: its
consensus-relevant state passed the same startup integrity checks as any
normal boot (a node with damaged storage never gets this far — it fails in
recovery and takes the [rebuild path](rebuild.md)), and as a voter it still
appends and acks entries, preserving the cluster's fault tolerance while
you diagnose. What's held is the *work*, not the node's standing. If a node
crashes even in safe mode, that itself is the diagnosis — the failure is
not worker-class; stop the node and take the offline path.

## What safe mode is not

- It is **not a data-repair tool.** A node that fails boot on damaged
  storage (a checksum mismatch during startup recovery) fails before safe
  mode matters — that path is [`committed wal repair` and the rebuild
  procedure](rebuild.md).
- It is **not automatic.** Crash-count heuristics that silently flip a
  system of record into a degraded mode were considered and rejected; an
  operator asks for this state explicitly or gets normal behavior.
- It is **not a way to run indefinitely.** Syncables fall behind and a
  deferred scrub delays RTBF physical erasure while the node stays in safe
  mode. Diagnose, fix, restart.

## See also

- [Rebuild procedure](rebuild.md) — for data damage rather than work damage.
- `GET /v1/node/status` (`api/openapi.yaml`) — the `safeMode` field.
