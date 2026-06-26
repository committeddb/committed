# Committed

[![CI](https://github.com/committeddb/committed/actions/workflows/ci.yml/badge.svg)](https://github.com/committeddb/committed/actions/workflows/ci.yml)

A single-binary, Raft-backed distributed commit log that is its own source of truth — write events in, sync them out to systems built for querying.

> **New here? Start with the [Quickstart](docs/quickstart.md)** — one `docker compose up` takes a normalized movie catalog to a single denormalized table you query with no joins (named cast and all). For everything else, see the [documentation index](docs/README.md).

Committed is a distributed commit log designed to store data long term in a log structure. Instead of the typical implementation where you are given simple read and write primitives and have to build, use adddons, or 3rd party software to aid in your read activites, Committed's two primitives are write and sync. The sync primitive is designed to move your data somewhere else. The purpose of this is to make it easy to transform or multiplex streams of data in a value added manner, or to move data into a system that has efficient querying (like a traditional SQL or NoSQL database). Committed even works with ephemeral data storage because it provides an efficient way to recreate the ephemeral storage if it fails (think Redis).

Another way to think of Committed is as a serious system for creating distributed Command Query Responsibility Segregation (CQRS) systems. In this case Committed would work as the Command database and provide powerful semantics for replicating transformed data into Query systems.

A final way to think of Committed is as a functional database with powerful data stream transformation capabilities.

Committed is specifically NOT a databse designed for querying.

## What makes it distinctive

- **vs. Kafka**: a single replicated log instead of partitioned topics, with built-in *output-side* projection (syncables) rather than consumer offsets. Configs live in the log, not ZooKeeper/KRaft.
- **vs. etcd**: same Raft substrate, but append-only log semantics instead of KV — and a worker model for ingest/sync that etcd doesn't have.
- **vs. an RDBMS / Debezium pipeline**: Committed collapses "replicated log + CDC source + sink connectors" into one process. You don't need Kafka + Debezium + Kafka Connect + a separate consensus layer; the same binary holds the log, the source, and the sink.

## Version 0.7

A beta release that grows SQL projections into a full read-model engine —
assembling a row from several topics, folding collections into JSON-array
columns, and enriching from lookup dimensions on other topics — and makes
change-data-capture **observable and failover-safe**: every ingestable,
syncable, and pipeline reports how far behind the source it is and whether it
is caught up, and MySQL ingest is rebuilt on GTID positioning so resume
survives a source failover. It keeps 0.6's core — entity kinds, declarative
SQL projections, learner-based cluster growth, leader-proxied membership, and
disk-pressure admission control — and adds:

- **Read models from many topics** — a `sql-projection` can fold several
  topics into one "BFF" row, fold a collection into a single JSON-array column
  (with per-element identity and targeted child removal), and enrich folded
  data from a lookup dimension on another topic that re-materializes when it
  changes. A drifted projection rebuilds in place
  (`POST /v1/syncable/{id}/rebuild`), and a column-set change is a clean
  delete-and-replace rather than a silent no-op. See
  [Read models](docs/read-models.md).
- **Failover-safe MySQL CDC** — MySQL ingest is rebuilt on an owned binlog
  reader with **GTID positioning**, so resume survives a source failover (a
  replica promoted to primary) instead of breaking on a server-local
  file:offset. When the source purges binlogs past what was consumed, committed
  reports `reSnapshotRequired` and re-snapshots rather than losing data
  silently. See [CDC setup](docs/operations/cdc-setup.md).
- **Ingest is observable** — `GET /v1/ingestable/{id}/status` reports the
  snapshot/streaming phase, per-table snapshot progress, the CDC cursor, lag,
  and caught-up state; `GET /v1/type/{topic}/pipeline` stitches the producer,
  the log head, and every consuming syncable into one caught-up answer; and
  `committed.ingest.lag` / `committed.sync.lag` are exported for alerting.
- **Ingest correctness** — composite primary keys, MySQL ENUM/SET decoded to
  their labels, a fix for silent row loss, and a config-time preflight that
  refuses an ingestable whose source can't carry a delete's key, so deletes are
  never silently dropped.
- **Backup and restore** — offline `committed backup` / `committed restore`
  tools to snapshot and rehydrate a node's state. See
  [backup](docs/operations/backup.md).
- **Log housekeeping** — dead letters are compacted as snapshots, a metadata-GC
  scrub pass reclaims superseded internal state, checkpoint cadence is
  configurable per syncable, and truncated proposals are detected and signaled
  (`ErrProposalLost`) instead of lost.

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
  committeddb/committed:0.7.0-beta
```

`docker run committeddb/committed:0.7.0-beta --version` prints the build
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
| `COMMITTED_API_URL` | _(unset)_ | This node's advertised HTTP API base URL (e.g. `http://n1:8080`), self-announced into the cluster so followers can proxy leader-only reads. Set it on every node. |

`COMMITTED_PEERS` is consumed only on a node's **first** boot
(`raft.StartNode`). After that, membership is restored from the WAL on
restart, so editing `COMMITTED_PEERS` has no effect — use the
membership API for live changes (see
[`docs/operations/membership.md`](docs/operations/membership.md)). When
`COMMITTED_PEERS` is unset the node bootstraps a single-node cluster
advertising `COMMITTED_PEER_URL`.

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

Runnable example configs live in [`examples/movies/`](examples/movies/), wired
together end to end in the [Quickstart](docs/quickstart.md). The snippets below
reference them as standalone illustrations of each endpoint.

Define a type:

```sh
curl -X POST -H 'Content-Type: text/toml' \
  --data-binary @examples/movies/type-movie.toml \
  http://localhost:8080/v1/type/movie
```

#### Entity kinds

A type can declare what the entities written under it are, ordered here
by how much interpretation a consumer needs to apply one:

```toml
[type]
name          = "TenantEvents"
entityKind    = "event"
discriminator = "$.event_type"
```

- **`snapshot`** — full objects, "tenant X is now {…}". Apply =
  overwrite (last-write-wins per key). Kafka compacted-topic semantics.
- **`delta`** — state-relative patches, "set tier=prod / add 3".
  **Rejected at type creation**: syncables deliver at-least-once, and a
  redelivered non-idempotent patch corrupts. Model the changes as
  events instead.
- **`event`** — domain facts, "tenant.provisioned happened". Apply =
  fold via domain rules; partial and implicative by design. An event
  type may also declare a `discriminator` — the jsonpath of the field
  that distinguishes its variants — for projection tooling to consume.
- **`command`** — requests, "please provision X". Apply = execute side
  effects; replay is dangerous, and the lifecycle (dedup, acks)
  belongs to the consumer. Committed carries commands; it does not
  execute them.
- **`standalone`** — facts with no aggregate to converge on (audit,
  telemetry). Apply = append; never folded.
- **`revision`** — full states in a retained, ordered series. Like
  `snapshot` (self-contained, no folding) but every prior version is kept
  and individually addressable — roll back, or read "as of" a version.
  The latest is current; the history is part of the data. This is the
  shape of committed's own versioned configs (type/database/syncable/
  ingestable, with their rollback endpoints) and of any record you keep
  as a revision history rather than just a current value.

The snapshot-vs-event fork is the one that matters in practice. Choose
**snapshot** when one writer owns the object and no read model needs
history — the topic carries current state and is key-compactable by
definition. Choose **event** when there are multiple writers, the
entities are audit or intent, or you want many read models folded from
the same history.

The entity kind is advisory metadata, validated at config time.
Declaring none (`unspecified`, the default for every type written
before the field existed) changes nothing — no enforcement ever applies
to it. A declared kind is immutable: a version bump must restate it,
and changing it means a new type/topic. Today the validation matrix
warns (log + `committed_entity_kind_misuse` metric) when a leaf-mapped
`sql` syncable targets an `event`-kind topic — events are partial by
design, so each variant either dead-letters on the jsonpaths it doesn't
carry or clobbers columns it didn't mean to write. The whole-payload
`"$"` mapping shape below is the right way to land an event topic in
SQL and is exempt from the warning.

Configure a database to write into (sink):

```sh
curl -X POST -H 'Content-Type: text/toml' \
  --data-binary @examples/movies/db-bff.toml \
  http://localhost:8080/v1/database/bff
```

Configure a syncable that projects a topic out to that database. The simplest
shape is a plain `sql` syncable with `[[sql.mappings]]` extracting leaf values
into columns:

```sh
curl -X POST -H 'Content-Type: text/toml' --data-binary @- \
  http://localhost:8080/v1/syncable/movies-mirror <<'EOF'
[syncable]
name = "movies-mirror"
type = "sql"

[sql]
topic = "movie"
db = "bff"
table = "movies"
primaryKey = "movie_id"

[[sql.mappings]]
jsonPath = "$.movie_id"
column = "movie_id"
type = "TEXT"
[[sql.mappings]]
jsonPath = "$.title"
column = "title"
type = "TEXT"
EOF
```

A `[[sql.mappings]]` block usually extracts one leaf value
(`jsonPath = "$.title"`) into a column. Setting `jsonPath = "$"` instead
maps the *whole* payload — the exact JSON document as submitted — into a
single column. That is the conventional event-log shape: a couple of
scalar envelope columns for indexing plus one payload column the read
side unmarshals and folds into current state:

```toml
[[sql.mappings]]
jsonPath = "$.event_id"
column   = "event_id"
type     = "VARCHAR(64)"

[[sql.mappings]]
jsonPath = "$"
column   = "payload"
type     = "JSONB"
```

A `"$"` mapping requires a JSON or text column type (`JSONB`, `JSON`,
`TEXT`, `VARCHAR`, `CHAR`, `NVARCHAR`, `LONGTEXT`, `MEDIUMTEXT`, `CLOB`);
anything else is rejected when the syncable config is submitted. `TEXT`
and `VARCHAR` columns receive the payload byte-for-byte, preserving key
order and number formatting exactly. Native JSON columns (`JSONB` in
particular) normalize what they store — key order, duplicate keys, number
formatting — so expect a semantically equal document back, not identical
bytes.

#### SQL projections

The plain `sql` syncable lands a *history* table: one row per event.
Applications usually query *current state*: one row per entity, which
requires folding each entity's events in log order. A
`type = "sql-projection"` syncable expresses that fold declaratively —
in the one place that sees every event exactly in order — so the log
stays the source of truth, the rules live in version-controlled TOML,
reads are O(1), and the table is disposable: rebuild it by replaying
from index 0. Use `sql` for event-log/history tables (and for
`snapshot`-kind topics, which are total updates with nothing to fold);
use `sql-projection` to maintain current-state tables from an
`event`-kind topic. One topic typically feeds both.

```toml
[syncable]
name = "tenants"
type = "sql-projection"
mode = "always-current"        # rules target the current type version

[sql-projection]
topic      = "controlplane-event"
db         = "hosted-projection"
table      = "tenants"
primaryKey = "tenant_id"
# keyPath  = "$.tenant_id"     # optional; defaults to $.<primaryKey>

[[sql-projection.columns]]
name = "tenant_id"
type = "VARCHAR(256)"

[[sql-projection.columns]]
name = "tier"
type = "VARCHAR(32)"

[[sql-projection.columns]]
name = "state"
type = "VARCHAR(32)"

[[sql-projection.columns]]
name = "allocs"
type = "JSONB"

[[sql-projection.rules]]
when = [ { path = "$.event_type", equals = "tenant.created" } ]
set  = [
  { column = "tier",  from  = "$.tier" },
  { column = "state", value = "pending" },
]

[[sql-projection.rules]]
when = [
  { path = "$.event_type", equals = "tenant.provisioned" },
  { path = "$.tier",       equals = "prod" },
]
set  = [
  { column = "state",  value = "active" },
  { column = "allocs", from  = "$.allocs" },
]

[[sql-projection.rules]]
when = [ { path = "$.event_type", equals = "tenant.deprovisioned" } ]
set  = [
  { column = "state",  value = "deprovisioning" },
  { column = "allocs", null  = true },
]
```

That single-source fold is the simplest shape. A projection can also fold
several topics into one denormalized "BFF" row (a **spine** plus
**contributors**), fold a collection of child rows into one JSON column
(**aggregate**), and enrich folded elements from a dimension topic
(**lookup**) — each with its own delete lifecycle, plus dimension fan-out for
out-of-order dimensions. The full reference, with rule semantics and worked
examples, is in **[docs/read-models.md](docs/read-models.md)**.

Configure an ingestable that pulls from an external source into the log (a
Postgres logical-replication source; the [Quickstart](docs/quickstart.md) wires
four of these):

```sh
curl -X POST -H 'Content-Type: text/toml' \
  --data-binary @examples/movies/ingest-movie.toml \
  http://localhost:8080/v1/ingestable/movie-ingest
```

Check how an ingestable is doing — snapshot vs. streaming phase, per-table
snapshot progress, the CDC position, source lag, and whether it has caught up:

```sh
curl http://localhost:8080/v1/ingestable/movie-ingest/status
# {"phase":"streaming","snapshotProgress":[{"table":"ingress.movie","complete":true}],
#  "position":"0/1A2B3C8","lag":0,"caughtUp":true}
```

Append a proposal directly (without going through an ingestable) — entities are
`{ typeId, key, data }`:

```sh
curl -X POST -H 'Content-Type: application/json' --data-binary @- \
  http://localhost:8080/v1/proposal <<'EOF'
{
  "entities": [
    {
      "typeId": "movie",
      "key": "mv0000099",
      "data": { "movie_id": "mv0000099", "title": "Plan 9 from Inner Join", "year": 1959 }
    }
  ]
}
EOF
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

`GET /v1/membership` lists the cluster — each member's role (voter or
learner) and replication progress — and is transparently proxied to the
leader when a follower answers. `POST /v1/membership`,
`POST /v1/membership/{id}/promote`, and `DELETE /v1/membership/{id}`
change membership live; see
[`docs/operations/membership.md`](docs/operations/membership.md).

### Syncable checkpoint cadence

After each successful sync a syncable durably checkpoints its progress
(`SyncableIndex`) through raft, so recovery is deterministic — but that is one
raft round-trip per checkpoint. Two optional `[syncable]` fields tune how often
it happens, trading that round-trip against how many already-synced proposals a
crash re-delivers:

```toml
[syncable]
name = "warehouse"
type = "sql"
checkpointEvery   = 500      # checkpoint once per 500 successful syncs (default 1)
checkpointMaxAge  = "1s"     # ...or once 1s elapses since the first pending one
```

- **`checkpointEvery`** (default `1`) — persist the checkpoint once per this
  many successful syncs. The worker always also flushes when it catches up
  (reaches the end of the log), so a low-traffic syncable never lags its
  checkpoint regardless of this value.
- **`checkpointMaxAge`** (a duration like `"500ms"`; default: no age bound for
  single syncables, 50ms for batch) — flush a pending checkpoint after this
  long even if `checkpointEvery` hasn't been reached.

**Duplicate bound:** a crash re-delivers **at most `checkpointEvery`**
already-synced proposals. The default of `1` re-delivers at most one — keep it
at `1` for **non-idempotent** sinks (an HTTP webhook, an event stream: every
duplicate is externally visible). Raise it only for **idempotent** sinks (the
`sql` / `sql-projection` dialects upsert, so a replay is a harmless no-op),
where it trades that bounded duplicate exposure for substantially fewer raft
round-trips on a fast destination. For a `BatchSyncable` (the SQL dialects)
`checkpointEvery` is the batch size and `checkpointMaxAge` the batch-age flush.

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

Operational guides live in [`docs/operations/`](docs/operations/):

- [CDC setup](docs/operations/cdc-setup.md) — point ingest at your own Postgres
  or MySQL: the source-side settings, what committed creates for you, and the
  failures you're most likely to hit.
- [Cluster membership](docs/operations/membership.md) — add, remove, and grow
  nodes safely (learner → catch up → promote), and read cluster state.
- [Disk limits](docs/operations/disk-limits.md) — how the cluster protects
  itself as disks fill, the thresholds and metrics to watch, and what to do in
  an incident.
- [Stuck syncables](docs/operations/stuck-syncables.md) — spot a syncable that's
  wedged or falling behind, skip a bad record, and replay it after a fix.
- [Rebuilding a node](docs/operations/rebuild.md) — recover a node that fell too
  far behind or lost its disk by copying from a healthy peer.
- [Backup and restore](docs/operations/backup.md) — archive a stopped node to a
  portable tarball for disaster recovery and total-loss rebuilds.
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
