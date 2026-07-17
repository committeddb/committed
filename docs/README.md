# Documentation

A map of the docs. Start with the quickstart; reach for the rest as you need them.

## Get started

- [Quickstart](quickstart.md) — one `docker compose up` takes a normalized movie
  catalog to a single denormalized table you query with no joins.
- [Read models (SQL projections)](read-models.md) — fold CDC topics into
  current-state tables: rules, multi-source rows, aggregates, and enrichment.
- [Writing a webhook receiver](webhook-receiver.md) — consume a topic over HTTP
  instead of SQL: the request/payload contract, the upsert/delete/refresh ops,
  and the idempotency and sweep responsibilities you own on the receiver side.

## Concepts

- [Consistency model](consistency.md) — what committed guarantees for writes and
  reads, and what it deliberately doesn't.
- [On-disk and wire compatibility](api-compatibility.md) — the forward/backward
  compatibility contract that makes rolling upgrades safe.
- [Event-log architecture](event-log-architecture.md) — the design doc behind
  the log, entity kinds, and apply determinism (background, not a how-to).
- [Storage architecture](storage-architecture.md) — the three write-ahead logs
  plus BoltDB, why raft persistence is two logs (not one), and the crash-recovery
  invariants that hold them together.

## Operations

- [CDC setup](operations/cdc-setup.md) — point ingest at your own Postgres or
  MySQL: the source-side settings, what committed creates, and common failures.
- [Cluster membership](operations/membership.md) — add, remove, and grow nodes
  safely, and read cluster state.
- [Disk limits](operations/disk-limits.md) — how the cluster protects itself as
  disks fill, the thresholds and metrics, and the incident playbook.
- [Stuck syncables](operations/stuck-syncables.md) — spot a syncable that's
  wedged or lagging, skip a bad record, and replay it after a fix.
- [Rebuilding a node](operations/rebuild.md) — recover a node that fell too far
  behind or lost its disk by copying from a healthy peer.
- [Backup and restore](operations/backup.md) — archive a stopped node to a
  portable tarball for disaster recovery and total-loss rebuilds.
- [Rolling upgrades](operations/upgrade.md) — upgrade node-by-node with no
  downtime, and roll back if needed.
- [Authentication](operations/authentication.md) — turn on the bearer token, API
  TLS, and peer mTLS.
- [Secrets](operations/secrets.md) — keep database passwords and tokens out of
  the log with `${VAR}` config interpolation.
- [Graceful shutdown](operations/shutdown.md) — what `SIGTERM` does and how to
  tune the drain deadline.
- [HTTP limits](operations/http-limits.md) — the proposal-size cap and HTTP
  server timeouts.
