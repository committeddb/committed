# Request size limits and HTTP timeouts

This runbook is for operators. It describes three orthogonal knobs:

1. the **proposal-size limit** — a cluster-wide cap on how big any
   single entry can be before raft will refuse it,
2. the **disk-space limits** — a per-node free-space watcher that
   rejects writes before the filesystem fills,
3. the **HTTP server timeouts** — per-connection deadlines on the
   API listener.

Rate limiting is *not* in scope here — per-caller volume control
belongs at the reverse proxy / Ingress / API gateway, where the
trusted-proxy context needed to key on real client IPs lives.

## Proposal size limit

Every proposal — whether it arrives via the HTTP API, an ingest
worker, or an internal config propose — passes through a single
choke point (`db.proposeAsync`) that caps the marshaled byte size.
Over-limit proposals are rejected with `cluster.ErrProposalTooLarge`
before they reach raft. HTTP translates that to:

```
413 Request Entity Too Large
{"code":"proposal_too_large","message":"<resource> exceeds the configured proposal size limit"}
```

The check lives at the propose layer (not at the HTTP body read)
because raft is what the cap actually protects: an oversize entry
replicates to every peer's WAL, stalls the message pipeline, and
bloats snapshot/compaction work. Putting the check at HTTP would
miss the ingest path entirely.

| Env var | Default | Notes |
|---|---|---|
| `COMMITTED_MAX_PROPOSAL_BYTES` | `16777216` (16 MiB) | Integer bytes. Applies to the *marshaled* proposal, so the framed wire size. |

Proposals and configs are KB-sized in practice; 16 MiB is generous
headroom for schema-heavy configs while still keeping any one entry
small enough that raft replication doesn't stall the cluster. Lower
it if your workload is uniformly small and you want a tighter bound;
raise it if you legitimately post larger configs and start seeing
413s.

## Disk-space limits

Each node runs a background **disk-usage watcher** that every 30s
`statfs`-es the data directory and classifies free space into one of
four states. As space falls the node degrades gracefully instead of
the raft loop fatal-exiting on `ENOSPC` once the filesystem fills:

| State | Default threshold | Effect |
|---|---|---|
| `ok` | free > 20% | All writes flow. |
| `warn` | free ≤ 20% | Logged warning. All writes still flow. |
| `critical` | free ≤ 10% | **User-data proposals rejected** with 507 (API writes + ingest data). Config changes and checkpoints (sync index / ingest position bumps) still flow, and compaction is nudged to run sooner to free raft-log disk. |
| `full` | free ≤ 3% | **User data and config frozen** with 507. Checkpoints still flow so syncables keep delivering and recording progress; compaction continues. |

Rejections surface as:

```
507 Insufficient Storage
{"code":"insufficient_storage","message":"the node is low on disk space and is rejecting writes; retry once disk space recovers"}
```

The gate lives at the same propose choke point as the size limit
(`db.proposeAsync`), so it covers the HTTP API, the ingest path, and
internal config proposes uniformly. Checkpoints are deliberately *not*
blocked at `full`: they are tiny (an index integer), and blocking them
would make a syncable re-deliver the same record in a loop and turn its
"at most one duplicate on crash" guarantee into a duplicate storm. The
ingest path treats a rejection as backpressure — it pauses (retries)
rather than dropping the proposal, so a full disk never advances an
ingestable's resume position past data that didn't commit.

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

### Multi-node behavior (current limitations)

The gate is **node-local**: it fires on whichever node receives the
proposal, based on *that node's* disk. Every node also stores the full
replicated log, so a node fills from the leader's replication stream
regardless of where writes enter — a local gate can slow intake but
cannot stop a node filling, nor save it once it is behind.

Consequences to be aware of in a cluster:

- **A full follower the cluster could route around.** If one follower
  is full but the leader and a quorum are healthy, writes that happen
  to land on the full follower get a 507 even though the cluster could
  serve them; clients/load balancers should retry another node. The
  follower will also eventually fall behind and exit — replace it
  (see [rebuild.md](rebuild.md)); don't remove it from the
  configuration (that shrinks quorum and *reduces* fault tolerance).
- **A quorum running low is not yet handled.** If a majority of voters
  are low on disk but the leader is healthy, the leader keeps accepting
  writes (its own disk is fine), which drives those followers toward
  exit and risks losing quorum. The node-local gate does not prevent
  this.

Cluster-aware admission — propagating disk state between nodes, a
quorum-aware leader decision, and transferring leadership away from a
constrained leader — is tracked separately (`resource-limits-disk-cluster-aware`).
Until then, alert on `committed_disk_state` per node and expand disk or
replace nodes well before any node reaches `full`.

## HTTP server timeouts

The HTTP server enforces four timeouts so a slow or malicious client
can't pin a connection. Defaults:

| Env var | Default | Purpose |
|---|---|---|
| `COMMITTED_HTTP_READ_HEADER_TIMEOUT` | `10s` | Defeats Slowloris on the request line + headers. |
| `COMMITTED_HTTP_READ_TIMEOUT` | `30s` | Full request read, including body. |
| `COMMITTED_HTTP_WRITE_TIMEOUT` | `30s` | Full response write. |
| `COMMITTED_HTTP_IDLE_TIMEOUT` | `120s` | Keepalive idle cap. |

Values use Go duration syntax (`500ms`, `45s`, `2m`). An unset or
unparseable value logs a warning and keeps the default — a misspelled
knob should be visible, not silently reverted.

Raise `READ_TIMEOUT` / `WRITE_TIMEOUT` if you post very large configs
over slow links and see truncated requests; lower them if you front
Committed with a CDN that enforces its own tighter deadlines and you
want the node to give up first.

## Linearizable-read timeout

Default `GET` reads run a raft ReadIndex round-trip to confirm the
serving node still holds quorum before answering (see
[consistency.md](../consistency.md)). This knob bounds how long that
confirmation waits before the handler returns `503 not_linearizable` —
so a partitioned node fails fast instead of holding the connection until
`WRITE_TIMEOUT`.

| Env var | Default | Purpose |
|---|---|---|
| `COMMITTED_HTTP_READ_INDEX_TIMEOUT` | `5s` | Max wait for ReadIndex quorum confirmation on a default (linearizable) GET before returning 503. |

Keep it comfortably below `WRITE_TIMEOUT`. Clients that don't need
linearizability can bypass the round-trip entirely per request with
`?consistency=stale`.
