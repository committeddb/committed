# Request size limits and HTTP timeouts

This runbook is for operators. It describes two orthogonal knobs:

1. the **proposal-size limit** — a cluster-wide cap on how big any
   single entry can be before raft will refuse it,
2. the **HTTP server timeouts** — per-connection deadlines on the
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
