# Read consistency

This explains what a `GET` against Committed actually guarantees, what it
costs, and how to trade that guarantee away when you don't need it. It's for
anyone building a client against the HTTP API and for operators reasoning
about behaviour during partitions and failovers.

Committed is a commit log, not a query engine: the only data you read back
over HTTP is **configuration** — databases, ingestables, syncables, types,
and their version history, plus replicated syncable status/dead-letter
state. (Topic data is never read over HTTP; you sync it to an external store
and query there.) Everything below is about those config/status reads.

## The default: linearizable

By default, every `GET` is **linearizable**. Concretely: a read reflects
every write that completed (got a successful `POST` response) before the
read began, and it never returns data from a node that has silently fallen
out of the cluster.

That guarantee matters because Committed is multi-node and Raft-replicated.
Without it, two failure modes leak stale data:

- A **follower partitioned from the leader** keeps its last-known state and
  would happily serve it for as long as the partition lasts — long after the
  rest of the cluster has moved on.
- A **deposed leader that doesn't know it yet** would answer as if it were
  still authoritative.

Reading local state on the serving node, with no check that the node is
still part of the quorum, can't rule either out. Linearizable reads do.

### How it works

Each default `GET` runs the Raft **ReadIndex** protocol before it reads:

1. The handler calls `cluster.LinearizableRead(ctx)`.
2. Raft asks the leader to confirm it still holds quorum. The leader
   broadcasts a heartbeat and waits for a majority to ack before replying
   with the log index at which a read is safe (the commit index at
   confirmation time). On a follower, the request is forwarded to the leader
   transparently.
3. The node waits for its **local applied index** to reach that confirmed
   index, so the state machine reflects every entry committed before the
   read.
4. Only then does the handler read and serve.

A node that can't reach a leader — a partitioned-away follower, or a leader
that has lost quorum (CheckQuorum forces it to step down) — never gets step 2
confirmed. The read blocks until it times out and the handler returns
`503 not_linearizable`. That 503 is the feature working: the node is
refusing to answer a read it can't prove is current.

### What it costs

One heartbeat round-trip per read. Under load this amortizes: etcd-raft
**coalesces** concurrent ReadIndex requests behind a single heartbeat, so N
reads in flight together cost roughly one round-trip, not N. The
applied-index wait (step 3) is usually a no-op on the leader, which has
already applied up to its commit index.

The end-to-end latency is recorded as the
`committed_read_index_duration_seconds` histogram (the ReadIndex round-trip
plus the apply-catch-up wait), recorded only on success.

The per-read timeout is bounded by the server's read-index timeout (default
5s, well under the 30s write timeout) so a partitioned node returns `503`
promptly instead of holding the connection open.

> **Single-node deployments.** With one node, the leader's quorum is itself,
> so ReadIndex confirms on the next Raft tick and linearizable reads are
> effectively free. The guarantee only starts doing visible work once you
> run a real multi-node cluster — which is exactly when you need it.

## Opting out: `?consistency=stale`

Append `?consistency=stale` to any `GET` to skip the quorum round-trip and
serve local applied state immediately:

```
GET /v1/syncable                  # linearizable (default)
GET /v1/syncable?consistency=stale # local read, no quorum check
```

A stale read is cheaper (no heartbeat, never blocks on quorum) but weaker: a
node partitioned from the leader can return out-of-date data, and a stale
read never returns `503` for a consistency reason. Use it when staleness is
acceptable and latency or availability matters more — for example a
dashboard polling syncable status, or a tailer that re-reads frequently and
tolerates lag.

The parameter accepts exactly two values:

- `linearizable` (the default; also applied when the parameter is absent)
- `stale`

Any other value is rejected with `400 invalid_consistency`.

## Which endpoints are gated

| Endpoint group | Consistency |
| --- | --- |
| `GET /v1/{database,ingestable,syncable,type}` (list) | linearizable by default |
| `GET /v1/{resource}/{id}/versions` and `.../versions/{version}` | linearizable by default |
| `GET /v1/syncable/{id}/errors`, `GET /v1/syncable/{id}/status` | linearizable by default |
| `GET /health`, `GET /ready`, `GET /version` | **never** — these are liveness/readiness/build probes; gating `/ready` on quorum would defeat its purpose |
| `GET /v1/node/status` | **never** — node-local diagnostics (applied index, degraded configs) are deliberately a per-node view, not a replicated one |
| `GET /openapi.yaml`, `GET /docs` | **never** — static API documentation |

`POST` writes are unaffected: they already go through Raft and return only
after their proposal commits.

## Failure behaviour

- **No quorum / partitioned node, default read** → `503 not_linearizable`.
  Retry (a transient election settles in a few hundred ms), or fall back to
  `?consistency=stale` if your caller can tolerate stale data.
- **No quorum / partitioned node, stale read** → `200` with possibly-stale
  local data. Stale reads never fail for a consistency reason.
- **Bad `consistency` value** → `400 invalid_consistency`.

## Out of scope (for now)

- **Leader-lease reads.** A leader holding a valid lease (within the
  election timeout of its last heartbeat-confirmed quorum) can serve a
  linearizable read *without* a fresh round-trip. It's cheaper than
  ReadIndex but weaker (it trusts a time bound rather than confirming
  quorum), so v1 defaults to ReadIndex everywhere and leaves leases as a
  future optimization.
- **Follower read-index forwarding tuning / multi-region routing.** A
  follower already forwards ReadIndex to the leader transparently; smarter
  routing (read from the nearest replica, hedged reads) is future work.
- **Client-side caching / cache-control headers.** Not emitted today.
