# Committed

[![CI](https://github.com/committeddb/committed/actions/workflows/ci.yml/badge.svg)](https://github.com/committeddb/committed/actions/workflows/ci.yml)

A single-binary, Raft-backed CDC pipeline with the log as its own source of truth.

Committed is a distributed commit log designed to store data long term in a log structure. Instead of the typical implementation where you are given simple read and write primitives and have to build, use adddons, or 3rd party software to aid in your read activites, Committed's two primitives are write and sync. The sync primitive is designed to move your data somewhere else. The purpose of this is to make it easy to transform or multiplex streams of data in a value added manner, or to move data into a system that has efficient querying (like a traditional SQL or NoSQL database). Committed even works with ephemeral data storage because it provides an efficient way to recreate the ephemeral storage if it fails (think Redis).

Another way to think of Committed is as a serious system for creating distributed Command Query Responsibility Segregation (CQRS) systems. In this case Committed would work as the Command database and provide powerful semantics for replicating transformed data into Query systems.

A final way to think of Committed is as a functional database with powerful data stream transformation capabilities.

Committed is specifically NOT a databse designed for querying.

## What makes it distinctive

- **vs. Kafka**: a single replicated log instead of partitioned topics, with built-in *output-side* projection (syncables) rather than consumer offsets. Configs live in the log, not ZooKeeper/KRaft.
- **vs. etcd**: same Raft substrate, but append-only log semantics instead of KV — and a worker model for ingest/sync that etcd doesn't have.
- **vs. an RDBMS / Debezium pipeline**: Committed collapses "replicated log + CDC source + sink connectors" into one process. You don't need Kafka + Debezium + Kafka Connect + a separate consensus layer; the same binary holds the log, the source, and the sink.

## Version 0.5

A beta release that hardens the log's correctness guarantees and makes the
sync/ingest pipeline operable under failure. It keeps 0.4's productionized
backend — 3-node Raft cluster, `COMMITTED_*` env-var configuration and
static multi-node bootstrap, optional bearer-token auth and TLS, graceful
shutdown, configurable server limits, optional OpenTelemetry metrics, and
the end-to-end CDC pipeline (Postgres logical replication / MySQL binlog
ingestable → Raft log → SQL/HTTP syncable) — and adds:

- **The Actual concept** — a committed Proposal is now a first-class
  *Actual*: the fact consensus ordered at a fixed Index. Syncables consume
  Actuals, never Proposals (see [Concepts](#concepts)). Inconsistent
  read-side concepts ("proposal time", reading a proposal back) were
  removed, so the log stays write-only over HTTP by design.
- **Stronger consensus guarantees** — JointImplicit configuration changes
  to prevent split brains during membership changes, linearizable reads of
  snapshotted data, and WAL checksums to detect on-disk corruption (etcd
  raft updated to the latest release).
- **Effectively-once ingest** — durable ingestable positions plus
  duplicate-storm-on-crash fixes, so ingest resumes where it left off
  across restarts and leader changes behind a supervised ingest worker.
- **Operable syncables** — a replay API
  (`POST /v1/syncable/{id}/replay/{index}`), dead-letter handling with
  metrics so durability failures are visible, bounded transient-retry
  config, and syncables that correctly restart and resume on node restart.
  Deletes now propagate through both ingestables and syncables.
- **Diagnostics, versioned APIs & config secrets** — a
  `GET /v1/node/status` endpoint reporting locally-degraded configs,
  versioned (`/v1`) endpoints with per-config history and rollback,
  `${VAR}` substitution to keep secrets out of Raft/bbolt, and configurable
  CORS.
- **Official container image** — a distroless, static, multi-arch
  (`linux/amd64` + `linux/arm64`) image published to Docker Hub at
  [`committeddb/committed`](https://hub.docker.com/r/committeddb/committed),
  alongside the prebuilt binaries (darwin/linux on arm64 + amd64,
  windows/amd64) attached to each release.

### Concepts

- **Type** — schema/metadata for a topic's payload. Identified by ID;
  supports explicit-migration versioning.
- **Proposal** — a write *request*: one or more entities (each tagged with
  a Type ID) offered to the log together. You propose a Proposal; consensus
  decides its fate. It has no place in the order yet.
- **Actual** — a committed fact: the Proposal that consensus ordered and
  wrote to the log at a fixed Index. You propose Proposals, you *sync*
  Actuals — a Syncable is handed Actuals (in Index order), never Proposals.
- **Database** — connection config for an external SQL system (MySQL or
  PostgreSQL).
- **Ingestable** — pulls data into the log from an external source.
  Today: PostgreSQL via logical replication (pgoutput), MySQL via
  binlog.
- **Syncable** — projects committed Actuals out to an external system.
  Today: SQL (MySQL/PostgreSQL) and HTTP.

### Running

Download a prebuilt binary for your platform from the
[releases page](https://github.com/committeddb/committed/releases), or
build from source:

```sh
make build
```

Single node — defaults to ID=1, HTTP at `:8080`, data dir `./data`:

```sh
./committed node
```

Or run the published container (distroless, static, runs as nonroot uid
65532). It reads the same `COMMITTED_*` env vars and persists WAL/state
under `/home/nonroot/data`:

```sh
docker run --rm -p 8080:8080 -p 9022:9022 \
  -v committed-data:/home/nonroot/data \
  committeddb/committed:0.5-beta
```

`docker run committeddb/committed:0.5-beta --version` prints the build
identity; `:latest` tracks the most recent release. See
[Configuration](#configuration) for the env vars and `docker-compose.yml`
for a local single-node setup.

Three-node cluster via [goreman](https://github.com/mattn/goreman) and
the included `Procfile`:

```sh
goreman start
```

You'll see leader-election logs on each node. The current leader serves
HTTP writes; followers forward proposals over Raft.

#### Configuration

A node is configured entirely through environment variables, so the
same image can be templated per-node by an orchestrator:

| Variable | Default | Purpose |
| --- | --- | --- |
| `COMMITTED_NODE_ID` | `1` | Raft node ID. Must be unique and appear in `COMMITTED_PEERS`. |
| `COMMITTED_API_ADDR` | `:8080` | HTTP API listen address. |
| `COMMITTED_DATA_DIR` | `./data` | Directory for the WAL, raft state, and metadata. |
| `COMMITTED_PEER_URL` | `http://127.0.0.1:9022` | This node's advertised raft peer URL. Used when `COMMITTED_PEERS` is unset. |
| `COMMITTED_PEERS` | _(unset)_ | Full static cluster membership as `id=url` pairs, e.g. `1=http://n1:9022,2=http://n2:9022,3=http://n3:9022`. Give the same value to every node; it must include this node's `COMMITTED_NODE_ID`. |

`COMMITTED_PEERS` is consumed only on a node's **first** boot
(`raft.StartNode`). After that, membership is restored from the WAL on
restart, so editing `COMMITTED_PEERS` has no effect — use the
conf-change API for live membership changes. When `COMMITTED_PEERS` is
unset the node bootstraps a single-node cluster advertising
`COMMITTED_PEER_URL`.

Additional operational variables (peer/API mTLS, proposal-size and HTTP
timeout limits, graceful-shutdown deadline, OTel export) are documented
under [`docs/operations/`](docs/operations/).

### API tour

Routes are served by Chi from `internal/cluster/http/`. The full
OpenAPI spec is available at `/openapi.yaml` with a Swagger UI at
`/docs`. Every API endpoint lives under a `/v1` prefix (see
[`docs/api-compatibility.md`](docs/api-compatibility.md)); bearer-token
auth is applied when `COMMITTED_API_TOKEN` is set (see
[`docs/operations/authentication.md`](docs/operations/authentication.md)).

Canonical example configs live in [`demo/`](demo/) — copy from there
for working values.

Define a type:

```sh
curl -X POST -H 'Content-Type: text/toml' \
  --data-binary @demo/type.toml \
  http://localhost:8080/v1/type/simple
```

Configure a database to write into (sink):

```sh
curl -X POST -H 'Content-Type: text/toml' \
  --data-binary @demo/database.toml \
  http://localhost:8080/v1/database/foo
```

Configure a syncable that projects a type out to that database:

```sh
curl -X POST -H 'Content-Type: text/toml' \
  --data-binary @demo/syncable-one.toml \
  http://localhost:8080/v1/syncable/one
```

Configure an ingestable that pulls from an external source into the
log (MySQL example; see `internal/cluster/ingestable/sql/postgres_ingestable.toml`
for a Postgres logical-replication config):

```sh
curl -X POST -H 'Content-Type: text/toml' \
  --data-binary @demo/ingestable.toml \
  http://localhost:8080/v1/ingestable/foo
```

Append a proposal directly (without going through an ingestable):

```sh
curl -X POST -H 'Content-Type: application/json' \
  --data-binary @demo/proposal.json \
  http://localhost:8080/v1/proposal
```

`/v1/proposal` is write-only — there is no endpoint to read the log back
over HTTP. That's by design: Committed is a commit log, not a query
interface. To read committed data, replicate it to a queryable store with a
syncable and query there.

Every config kind has versioned history and rollback under
`GET /v1/{kind}/{id}/versions[/{version}]` and `POST /v1/{kind}/{id}/rollback`.
Operational endpoints `/health`, `/ready`, `/version` stay unprefixed and
are exempt from auth.

`GET /v1/node/status` returns per-node diagnostics for the node that
answers — notably the configs it persisted but could not build locally
(degraded, usually a missing `${VAR}` secret on that node), each with the
failing variable named. It is the queryable, authenticated counterpart of
the `committed_config_build_errors` gauge.

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
└── metadata/
    └── bbolt.db     # types, databases, syncables, applied index
```

The permanent event log under `events/` is the canonical record of
every write; syncables read from it, and it is never compacted.
`raft/log/` is consensus transport only and is compacted aggressively.
See [`docs/event-log-architecture.md`](docs/event-log-architecture.md)
for the rationale.

### Runbooks

- [`docs/operations/authentication.md`](docs/operations/authentication.md) — bearer token + mTLS setup
- [`docs/operations/secrets.md`](docs/operations/secrets.md) — `${VAR}` interpolation to keep DB credentials and tokens out of Raft/bbolt (Kubernetes + systemd patterns)
- [`docs/operations/shutdown.md`](docs/operations/shutdown.md) — `SIGTERM` handling and the graceful-shutdown deadline
- [`docs/operations/http-limits.md`](docs/operations/http-limits.md) — proposal-size cap and HTTP server timeouts
- [`docs/operations/rebuild.md`](docs/operations/rebuild.md) — rebuilding a node after a `storage invariant violation` fatal. Short version: stop the node, rsync its data directory from a healthy peer, restart. Apply determinism keeps subsequent rebuilds O(diff).
- [`docs/operations/stuck-syncables.md`](docs/operations/stuck-syncables.md) — spotting a syncable wedged on a transient error (the `committed_sync_stuck` gauge, `GET /v1/syncable/{id}/status`), skipping the bad proposal (`POST /v1/syncable/{id}/deadletter/`), and re-driving it after a fix (`POST /v1/syncable/{id}/replay/{index}`), plus dead letters and `GET /v1/syncable/{id}/errors`.
