# Committed

[![CI](https://github.com/committeddb/committed/actions/workflows/ci.yml/badge.svg)](https://github.com/committeddb/committed/actions/workflows/ci.yml)

A single-binary, Raft-backed distributed commit log that is its own source of truth — write events in, sync them out to systems built for querying.

Committed is a distributed commit log designed to store data long term in a log structure. Instead of the typical implementation where you are given simple read and write primitives and have to build, use adddons, or 3rd party software to aid in your read activites, Committed's two primitives are write and sync. The sync primitive is designed to move your data somewhere else. The purpose of this is to make it easy to transform or multiplex streams of data in a value added manner, or to move data into a system that has efficient querying (like a traditional SQL or NoSQL database). Committed even works with ephemeral data storage because it provides an efficient way to recreate the ephemeral storage if it fails (think Redis).

Another way to think of Committed is as a serious system for creating distributed Command Query Responsibility Segregation (CQRS) systems. In this case Committed would work as the Command database and provide powerful semantics for replicating transformed data into Query systems.

A final way to think of Committed is as a functional database with powerful data stream transformation capabilities.

Committed is specifically NOT a databse designed for querying.

## What makes it distinctive

- **vs. Kafka**: a single replicated log instead of partitioned topics, with built-in *output-side* projection (syncables) rather than consumer offsets. Configs live in the log, not ZooKeeper/KRaft.
- **vs. etcd**: same Raft substrate, but append-only log semantics instead of KV — and a worker model for ingest/sync that etcd doesn't have.
- **vs. an RDBMS / Debezium pipeline**: Committed collapses "replicated log + CDC source + sink connectors" into one process. You don't need Kafka + Debezium + Kafka Connect + a separate consensus layer; the same binary holds the log, the source, and the destination.

[Quickstart](docs/quickstart.md)

[Running Committed](docs/running.md)

[Developing Committed](docs/development.md)

[API tour](docs/api-tour.md)

### Concepts

- **[Type](docs/types.md)** — schema/metadata for a topic's payload. Identified by ID; supports explicit-migration versioning.
- **[Proposal](docs/proposals.md)** — a write *request*: one or more entities (each tagged with a Type ID) offered to the log together. You propose a Proposal; consensus decides its fate. It has no place in the order yet.
- **[Actual](docs/actuals.md)** — a committed fact: the Proposal that consensus ordered and wrote to the log at a fixed Index. You propose Proposals, you *sync* Actuals — a Syncable is handed Actuals (in Index order), never Proposals.
- **[Database](docs/databases.md)** — connection config for an external SQL syncable that syncables reference.
- **[Ingestable](docs/ingestables.md)** — pulls data into the log from an external source.
- **[Syncable](docs/syncables.md)** — projects committed Actuals out to an external system, either as faithful per-table mirrors (`sql`) or by transforming and folding topics into the denormalized shape you query — multi-topic BFF rows, collection aggregates, cross-topic enrichment.

## Operations

### Storage layout

A Committed node's data directory looks like this:

```
<datadir>/
├── raft/
│   ├── log/         # raft consensus log; bounded (10GB / 1hr)
│   └── state/       # HardState + ConfState
├── events/          # permanent event log; infinite retention
└── metadata/
    └── bbolt.db     # types, databases, syncables, applied index
```

The permanent event log under `events/` is the canonical record of
every write; syncables read from it, and it is never compacted.
`raft/log/` is consensus transport only and is compacted aggressively.
See [`docs/event-log-architecture.md`](docs/event-log-architecture.md)
for the rationale.

### Runbooks

Operational guides live in [`docs/operations/`](docs/operations/):

- [Ingestables](docs/ingestables.md) — point ingest at your own Postgres,
  MySQL, or SQL Server: the source-side settings, what committed creates for
  you, and the failures you're most likely to hit.
- [Cluster membership](docs/operations/membership.md) — add, remove, and grow
  nodes safely (learner → catch up → promote), and read cluster state.
- [Disk limits](docs/operations/disk-limits.md) — how the cluster protects
  itself as disks fill, the thresholds and metrics to watch, and what to do in
  an incident.
- [Stuck syncables](docs/operations/stuck-syncables.md) — spot a syncable that's
  wedged or falling behind, skip a bad record, and replay it after a fix.
- [Rebuilding a node](docs/operations/rebuild.md) — a node that fell behind
  catches up by itself; replace one whose disk or state cannot be trusted.
- [Backup and restore](docs/operations/backup.md) — archive a node, stopped or
  live, to a portable tarball for disaster recovery and total-loss rebuilds.
- [Rolling upgrades](docs/operations/upgrade.md) — upgrade the cluster
  node-by-node with no downtime, and roll back if needed.
- [Authentication](docs/operations/authentication.md) — turn on the bearer
  token, API TLS, and peer mTLS.
- [Secrets](docs/operations/secrets.md) — keep database passwords and tokens out
  of the log with `${VAR}` config interpolation.
- [Graceful shutdown](docs/operations/shutdown.md) — what `SIGTERM` does and how
  to tune the drain deadline.
- [HTTP limits](docs/operations/http-limits.md) — the proposal-size cap and HTTP
  server timeouts.
