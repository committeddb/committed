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
- **vs. an RDBMS / Debezium pipeline**: Committed collapses "replicated log + CDC source + sink connectors" into one process. You don't need Kafka + Debezium + Kafka Connect + a separate consensus layer; the same binary holds the log, the source, and the sink.

## Version 0.6.1

A critical bugfix release for 0.6 — every 0.6 deployment should upgrade.

- **Unbounded disk growth fixed** — the raft state log grew continuously
  during normal operation and was never truncated, enough to fill a 50 GB
  disk in days. The first boot after upgrading reclaims the space
  automatically, and term/vote durability across restarts is restored.
- **Snapshot catch-up fixed** — a follower catching up via a leader
  snapshot could silently stop replicating; snapshot installs now work
  in place and complete themselves on the next boot if interrupted.
- **Projection language null support** — `set` entries can write SQL
  NULL via `null = true`, and `when` clauses match a present JSON `null`
  (an absent field remains "no match"). See
  [SQL projections](#sql-projections).

## Version 0.6

A beta release that gives the log a data model — types now declare what
their entities *are*, and syncables can fold event history into
current-state tables — and makes the cluster elastic and self-protecting,
with learner-based growth, leader-proxied membership reads, and
disk-pressure admission control. It keeps 0.5's hardened core — the
Proposal/Actual model, joint-consensus membership changes, linearizable
reads, WAL checksums, effectively-once ingest, operable syncables
(replay, dead letters, bounded retry), versioned `/v1` APIs with
per-config history and rollback, `${VAR}` config secrets, and the
official container image — and adds:

- **Entity kinds** — a type declares what the entities written under it
  are (`snapshot`, `event`, `command`, `standalone`; `delta` is rejected
  by design), plus an optional `discriminator` for event variants. Kinds
  are validated at config time, and a misuse matrix warns (log +
  `committed_entity_kind_misuse` metric) when a leaf-mapped `sql`
  syncable targets an `event`-kind topic. See
  [Entity kinds](#entity-kinds).
- **SQL projections** — a `sql-projection` syncable folds an
  `event`-kind topic into a current-state table with declarative TOML
  rules (`when`/`set`), idempotent absolute writes, hard deletes for
  right-to-be-forgotten, and fresh-table replay instead of ALTER. See
  [SQL projections](#sql-projections).
- **Whole-payload mappings** — `jsonPath = "$"` in a `sql` syncable maps
  the entire payload into one JSON/text column: the conventional
  event-log shape of scalar envelope columns plus a payload column.
- **Cluster growth with learners** — add a node as a non-voting learner
  (`member add --learner`), watch it catch up, then `member promote` it
  to a voter — growing the cluster without touching quorum math. See
  [`docs/operations/membership.md`](docs/operations/membership.md).
- **Membership observability & leader proxy** — `GET /v1/membership`
  reports each member's role and replication progress. Followers
  transparently proxy the read to the leader, so any node — including
  one behind a load-balancer VIP — answers leader-truthfully; nodes
  self-announce their API address via `COMMITTED_API_URL`.
- **Disk-pressure protection** — a per-node free-space watcher
  (warn/critical/full thresholds, `507` rejections) plus cluster-aware
  write admission: writes are admitted only while the leader and a
  quorum of voters have disk headroom, and leadership transfers off a
  constrained leader. See
  [`docs/operations/disk-limits.md`](docs/operations/disk-limits.md).
- **Graceful type-migration failure handling** — a migration program
  that fails at runtime dead-letters against the type
  (`GET /v1/type/{id}/migration-errors`), can be re-run after a fix
  (`POST /v1/type/{id}/migration-retry/{index}`), and feeds
  `committed_type_migration_errors_total`; `validate_against` pre-flights
  a program against a sample document at propose time.
- **HTTP & SQL hardening** — panic recovery on every handler, sanitized
  `500` responses whose cause is logged server-side with the request id,
  and SQL syncable transaction hygiene (validate before `BeginTx`,
  rollback on every error path). Viper was replaced with go-toml/v2 +
  mapstructure, with tolerance tests pinning config-parsing behavior.

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
  committeddb/committed:0.6.1-beta
```

`docker run committeddb/committed:0.6.1-beta --version` prints the build
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

Canonical example configs live in [`demo/`](demo/) — copy from there
for working values.

Define a type:

```sh
curl -X POST -H 'Content-Type: text/toml' \
  --data-binary @demo/type.toml \
  http://localhost:8080/v1/type/simple
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
  --data-binary @demo/database.toml \
  http://localhost:8080/v1/database/foo
```

Configure a syncable that projects a type out to that database:

```sh
curl -X POST -H 'Content-Type: text/toml' \
  --data-binary @demo/syncable-one.toml \
  http://localhost:8080/v1/syncable/one
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

Rule semantics:

- **`when` is data, not an expression**: an array of
  `{ path, equals }` clauses, all of which must hold (AND); express OR
  as another rule. `{ path, null = true }` matches a *present* JSON
  null (the flag form again, since TOML cannot write `equals = null`);
  there is no negation. A missing path is "no match", never an error —
  including for `null` clauses, so an absent field never matches. If
  the topic's type declares a `discriminator`, a rule can use the
  shorthand `when = "tenant.created"` — sugar for equality on the
  discriminator path.
- **Each matched rule is one prepared upsert** restricted to its
  columns, executed in manifest order inside the Actual's transaction.
  When two matched rules set the same column, the last rule wins.
  Zero matching rules → zero SQL: no ghost rows, and the
  `committed_sync_rules_unmatched` counter ticks — the signal that a
  new event variant shipped without a rule.
- **Writes are absolute** (`from` extracts from the payload, `value`
  is a literal, `null = true` writes SQL NULL — exactly one per `set`
  entry; the flag form exists because TOML has no null literal).
  Delivery is at-least-once and idempotent re-apply is the recovery
  mechanism, which is why there are no aggregations (`col = col + 1`
  would corrupt on redelivery).
- **Errors fail fast**: config misuse (unknown column, a `set` entry
  without exactly one of `from`/`value`/`null`, a `when` entry without
  exactly one of `equals`/`null`, a rule setting the primary key, a
  rule without a `when`) is rejected at config time; a *matched* rule
  whose `from` path is missing — or a matched event with no value at
  `keyPath` — dead-letters as a permanent error rather than wedging
  the worker.
- **Deletes are honored**: a delete Actual hard-deletes the projected
  row by entity key (right-to-be-forgotten), distinct from soft-delete
  rules like `state = "deprovisioning"` — both exist. Deleting an
  absent row is a no-op, which is what makes a fresh replay of an
  already-scrubbed log correct.
- **Schema evolution = fresh-table replay, not ALTER.** DDL is
  `CREATE TABLE IF NOT EXISTS` only. To add a column or rule: point a
  new syncable at a new table and let it replay from index 0 — cheap,
  because the log is permanent. Cut reads over when it converges.

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

`GET /v1/membership` lists the cluster — each member's role (voter or
learner) and replication progress — and is transparently proxied to the
leader when a follower answers. `POST /v1/membership`,
`POST /v1/membership/{id}/promote`, and `DELETE /v1/membership/{id}`
change membership live; see
[`docs/operations/membership.md`](docs/operations/membership.md).

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
- [`docs/operations/membership.md`](docs/operations/membership.md) — adding/removing nodes with joint consensus, growing safely via learner add → catch-up → promote, and observing membership (`GET /v1/membership`, the leader-read proxy, `COMMITTED_API_URL`)
- [`docs/operations/disk-limits.md`](docs/operations/disk-limits.md) — disk-pressure behavior: the per-node free-space watcher (warn/critical/full thresholds, 507 rejections) and cluster-aware write admission (admit iff the leader and a quorum of voters have disk headroom; leadership transfers off a constrained leader), with the alerting and incident playbook
- [`docs/operations/rebuild.md`](docs/operations/rebuild.md) — rebuilding a node after a `storage invariant violation` fatal. Short version: stop the node, rsync its data directory from a healthy peer, restart. Apply determinism keeps subsequent rebuilds O(diff).
- [`docs/operations/stuck-syncables.md`](docs/operations/stuck-syncables.md) — spotting a syncable wedged on a transient error (the `committed_sync_stuck` gauge, `GET /v1/syncable/{id}/status`), tracking how far behind a syncable is (`checkpoint_index`/`head_index`/`lag`/`caught_up` on the same status endpoint, where `lag == 0` ⇔ caught up), skipping the bad proposal (`POST /v1/syncable/{id}/deadletter/`), and re-driving it after a fix (`POST /v1/syncable/{id}/replay/{index}`), plus dead letters (`GET /v1/syncable/{id}/errors`) and the type-migration recovery loop (`GET /v1/type/{id}/migration-errors`, `POST /v1/type/{id}/migration-retry/{index}`).
