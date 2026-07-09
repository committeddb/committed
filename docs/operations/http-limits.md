# Request size limits and HTTP timeouts

This runbook is for operators. It describes two orthogonal knobs:

1. the **proposal-size limit** — a cluster-wide cap on how big any
   single entry can be before raft will refuse it,
2. the **HTTP server timeouts** — per-connection deadlines on the
   API listener.

Disk-space limits and cluster-aware write admission (the 507
`insufficient_storage` rejections) are a separate runbook:
[disk-limits.md](disk-limits.md). Rate limiting is *not* in scope
here either — per-caller volume control belongs at the reverse proxy
/ Ingress / API gateway, where the trusted-proxy context needed to
key on real client IPs lives.

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

The *exact* size check lives at the propose layer (not the HTTP body
read) because raft is what the cap protects: an oversize entry
replicates to every peer's WAL, stalls the message pipeline, and
bloats snapshot/compaction work. Putting the exact check at HTTP would
miss the ingest path entirely.

But the HTTP handler must buffer the whole request body into memory
before it can marshal and size-check the proposal, so a separate
**request-body cap** guards the node's heap: every API request body is
wrapped in a `MaxBytesReader`, and an over-cap body is rejected with
`413 request_too_large` *at the read*, before it can OOM the node. The
body cap tracks the proposal cap with headroom (2×, since the JSON/TOML
body is larger than the marshaled proposal it produces), so raising
`COMMITTED_MAX_PROPOSAL_BYTES` raises it too — there is no separate knob.
The two 413s are distinct: `proposal_too_large` means the marshaled entry
exceeds the raft cap; `request_too_large` means the raw body exceeded the
memory guard.

Watch the body cap in the wild: each `request_too_large` rejection is
counted on the `committed.http.request_too_large` metric (labeled by
`route`) and logged with its route. A rising count on `/v1/proposal` means
the cap may be too tight for legitimate proposals — raise
`COMMITTED_MAX_PROPOSAL_BYTES` (which raises the body cap with it).

| Env var | Default | Notes |
|---|---|---|
| `COMMITTED_MAX_PROPOSAL_BYTES` | `16777216` (16 MiB) | Integer bytes. Applies to the *marshaled* proposal, so the framed wire size. |

Proposals and configs are KB-sized in practice; 16 MiB is generous
headroom for schema-heavy configs while still keeping any one entry
small enough that raft replication doesn't stall the cluster. Lower
it if your workload is uniformly small and you want a tighter bound;
raise it if you legitimately post larger configs and start seeing
413s.

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
