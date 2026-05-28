# Committed

[![CI](https://github.com/philborlin/committed/actions/workflows/ci.yml/badge.svg)](https://github.com/philborlin/committed/actions/workflows/ci.yml)

A single-binary, Raft-backed CDC pipeline with the log as its own source of truth.

Committed is a distributed commit log designed to store data long term in a log structure. Instead of the typical implementation where you are given simple read and write primitives and have to build, use adddons, or 3rd party software to aid in your read activites, Committed's two primitives are write and sync. The sync primitive is designed to move your data somewhere else. The purpose of this is to make it easy to transform or multiplex streams of data in a value added manner, or to move data into a system that has efficient querying (like a traditional SQL or NoSQL database). Committed even works with ephemeral data storage because it provides an efficient way to recreate the ephemeral storage if it fails (think Redis).

Another way to think of Committed is as a serious system for creating distributed Command Query Responsibility Segregation (CQRS) systems. In this case Committed would work as the Command database and provide powerful semantics for replicating transformed data into Query systems.

A final way to think of Committed is as a functional database with powerful data stream transformation capabilities.

Committed is specifically NOT a databse designed for querying.

## What makes it distinctive

- **vs. Kafka**: a single replicated log instead of partitioned topics, with built-in *output-side* projection (syncables) rather than consumer offsets. Configs live in the log, not ZooKeeper/KRaft.
- **vs. etcd**: same Raft substrate, but append-only log semantics instead of KV — and a worker model for ingest/sync that etcd doesn't have.
- **vs. an RDBMS / Debezium pipeline**: Committed collapses "replicated log + CDC source + sink connectors" into one process. You don't need Kafka + Debezium + Kafka Connect + a separate consensus layer; the same binary holds the log, the source, and the sink.

## Version 0.3

A beta release with a functioning 3-node Raft cluster, REST API, optional
bearer-token auth and TLS, optional OpenTelemetry metrics, and a working
end-to-end CDC pipeline (Postgres logical replication ingestable → Raft
log → SQL syncable).

### Concepts

- **Type** — schema/metadata for a topic's payload. Identified by ID;
  supports explicit-migration versioning.
- **Proposal** — the atomic unit written to the Raft log. Carries one
  or more entities, each tagged with a Type ID.
- **Database** — connection config for an external SQL system (MySQL or
  PostgreSQL).
- **Ingestable** — pulls data into the log from an external source.
  Today: PostgreSQL via logical replication (pgoutput), MySQL via
  binlog.
- **Syncable** — projects log entries out to an external system.
  Today: SQL (MySQL/PostgreSQL) and HTTP.

### Running

Build the binary:

```sh
make build
```

Single node — defaults to ID=1, HTTP at `:8080`, data dir `./data`:

```sh
./committed node
```

Three-node cluster via [goreman](https://github.com/mattn/goreman) and
the included `Procfile`:

```sh
goreman start
```

You'll see leader-election logs on each node. The current leader serves
HTTP writes; followers forward proposals over Raft.

### API tour

Routes are served by Chi from `internal/cluster/http/`. The full
OpenAPI spec is available at `/openapi.yaml` with a Swagger UI at
`/docs`. The endpoints below all sit under that router; bearer-token
auth is applied when `COMMITTED_API_TOKEN` is set (see
[`docs/operations/authentication.md`](docs/operations/authentication.md)).

Canonical example configs live in [`demo/`](demo/) — copy from there
for working values.

Define a type:

```sh
curl -X POST -H 'Content-Type: text/toml' \
  --data-binary @demo/type.toml \
  http://localhost:8080/type/simple
```

Configure a database to write into (sink):

```sh
curl -X POST -H 'Content-Type: text/toml' \
  --data-binary @demo/database.toml \
  http://localhost:8080/database/foo
```

Configure a syncable that projects a type out to that database:

```sh
curl -X POST -H 'Content-Type: text/toml' \
  --data-binary @demo/syncable-one.toml \
  http://localhost:8080/syncable/one
```

Configure an ingestable that pulls from an external source into the
log (MySQL example; see `internal/cluster/ingestable/sql/postgres_ingestable.toml`
for a Postgres logical-replication config):

```sh
curl -X POST -H 'Content-Type: text/toml' \
  --data-binary @demo/ingestable.toml \
  http://localhost:8080/ingestable/foo
```

Append a proposal directly (without going through an ingestable):

```sh
curl -X POST -H 'Content-Type: application/json' \
  --data-binary @demo/proposal.json \
  http://localhost:8080/proposal
```

Read recent proposals for a type:

```sh
curl 'http://localhost:8080/proposal?type=simple&number=100'
```

Every config kind has versioned history and rollback under
`GET /{kind}/{id}/versions[/{version}]` and `POST /{kind}/{id}/rollback`.
Health endpoints `/health`, `/ready`, `/version` are exempt from auth.

### Testing

| Target | Scope |
|---|---|
| `make test` | Fast unit tests (`-short`) |
| `make test/ci` | Full unit suite with `-race` and coverage |
| `make test/integration` | `-tags integration` (HTTP server, MySQL/Postgres dialects) |
| `make test/cdc` | End-to-end CDC pressure-test harness (`e2e/cdc/`) |
| `make test/adversarial` | Multi-node raft adversarial suite, `-race -count=20` |
| `make test-all` | Everything (docker + integration tags) |

All five are wired into CI on every push and PR except `test/adversarial`,
which runs on pushes to `main` only because of its multi-minute runtime.

## Operations

### Storage layout

A Committed node's data directory looks like this:

```
<datadir>/
├── raft/
│   ├── log/         # raft consensus log; bounded (10GB / 1hr)
│   └── state/       # HardState + ConfState
├── events/          # permanent event log; infinite retention
├── metadata/
│   └── bbolt.db     # types, databases, syncables, applied index
└── time-series/     # derived view regenerable from events/
```

The permanent event log under `events/` is the canonical record of
every write; syncables read from it, and it is never compacted.
`raft/log/` is consensus transport only and is compacted aggressively.
See [`docs/event-log-architecture.md`](docs/event-log-architecture.md)
for the rationale.

### Runbooks

- [`docs/operations/authentication.md`](docs/operations/authentication.md) — bearer token + mTLS setup
- [`docs/operations/shutdown.md`](docs/operations/shutdown.md) — `SIGTERM` handling and the graceful-shutdown deadline
- [`docs/operations/http-limits.md`](docs/operations/http-limits.md) — proposal-size cap and HTTP server timeouts
- [`docs/operations/rebuild.md`](docs/operations/rebuild.md) — rebuilding a node after a `storage invariant violation` fatal. Short version: stop the node, rsync its data directory from a healthy peer, restart. Apply determinism keeps subsequent rebuilds O(diff).
