# Committed

[![CI](https://github.com/committeddb/committed/actions/workflows/ci.yml/badge.svg)](https://github.com/committeddb/committed/actions/workflows/ci.yml)

A single-binary, Raft-backed distributed commit log that is its own source of truth — write events in, sync them out to systems built for querying.

> **New here? Start with the [Quickstart](docs/quickstart.md)** — one `docker compose up` takes a normalized movie catalog to a single denormalized table you query with no joins (named cast and all).

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
  are (`snapshot`, `event`, `command`, `standalone`, `revision`; `delta`
  is rejected by design), plus an optional `discriminator` for event
  variants. Kinds
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
  exactly one of `equals`/`null`, a rule setting the primary key) is
  rejected at config time; a *matched* rule whose `from` path is
  missing — or a matched event with no value at `keyPath` —
  dead-letters as a permanent error rather than wedging the worker. A
  rule with **no** `when` matches every event of its source (the topic
  is the discriminator) — the natural shape for folding a `snapshot`
  source that has one event variant.
- **Deletes are honored**: a delete Actual hard-deletes the projected
  row by entity key (right-to-be-forgotten), distinct from soft-delete
  rules like `state = "deprovisioning"` — both exist. Deleting an
  absent row is a no-op, which is what makes a fresh replay of an
  already-scrubbed log correct.
- **Schema evolution = fresh-table replay, not ALTER.** DDL is
  `CREATE TABLE IF NOT EXISTS` only, so re-POSTing a config with a
  changed column set is *rejected* (409 `schema_change_requires_rebuild`,
  with the changed columns in `details`) rather than silently no-op'd. To
  add or change a column, replace the syncable in place:
  `DELETE /v1/syncable/{id}` (removes the config + checkpoint atomically
  and drops the table), then re-POST the new config — the fresh table
  replays from index 0. `?keepData=true` on the DELETE preserves the
  destination (e.g. another consumer reads the table). To re-materialize a drifted or
  corrupted projection *without* a schema change,
  `POST /v1/syncable/{id}/rebuild` does the drop + replay-from-0 in place
  under the same name. The log is permanent, so replay is cheap.

**Folding several topics into one row (multi-source).** A projection can
consume more than one topic and fold them into a single denormalized
"BFF" row — e.g. a `movie_card` built from a normalized `movie` topic and
a `rating` topic, one row per `movie_id`. Replace the single top-level
`topic`/`rules` with a `[[sql-projection.source]]` block per topic. The
**topic is the discriminator** (an event only ever runs its own source's
rules), and because each rule sets only its own columns, two sources fold
into one row without clobbering. Each source also declares what its delete
does to the row via `onDelete`: `delete-row` (the *spine* — its delete
drops the row), `clear` (a *contributor* — its delete NULLs only the
columns it owns, the row survives), or `ignore`.

```toml
[sql-projection]
db = "bff"; table = "movie_card"; primaryKey = "movie_id"
# … columns: movie_id, title, year, genres, score, votes …

[[sql-projection.source]]
topic    = "movie"          # this source's discriminator
keyPath  = "$.movie_id"     # correlate by the shared aggregate key
onDelete = "delete-row"     # movie is the spine: its delete drops the row
  [[sql-projection.source.rules]]
  set = [ { column = "title",  from = "$.title" },
          { column = "year",   from = "$.year" },
          { column = "genres", from = "$.genres" } ]

[[sql-projection.source]]
topic    = "rating"
onDelete = "clear"          # a contributor: its delete NULLs its columns, keeps the row
  [[sql-projection.source.rules]]
  set = [ { column = "score", from = "$.score" },
          { column = "votes", from = "$.votes" } ]
```

The single-topic form above is the one-source special case. Source topics
are commonly `snapshot`-kind (ingested current-state tables), which fold
cleanly here — the snapshot-topic misuse warning only fires for a
single-source projection, where there is genuinely nothing to fold.

**Folding a collection into one column (aggregate).** Some BFF columns are
*collections*: a movie's `top_cast[]` folds many `credit` rows into one
JSON array on the movie row. A source declares an `aggregate` block instead
of `rules`: `column` is the array column, `elementKey` a jsonpath to each
child's identity within the array (its sort key, and what makes a
re-delivered child replace rather than duplicate), and `element` an
array-of-tables naming the per-child object's fields (an array, not an
inline map, so field names survive byte-exact). `elementKeyType` picks
`number` (sort 1,2,…,10) or `text` (lexical, the default). A child delete
removes exactly its element via `onDelete = "remove-from-aggregate"`,
leaving the row.

Because several sources may share one topic, a source's optional `when`
filters which of the topic's events it folds — so one topic splits into
different columns. Here the `credit` topic feeds `top_cast` (actors) and
`directors` (directors):

```toml
[[sql-projection.source]]
topic   = "credit"
keyPath = "$.movie_id"               # which movie row this child folds into
when    = [ { path = "$.role", equals = "actor" } ]
  [sql-projection.source.aggregate]
  column         = "top_cast"
  elementKey     = "$.billing"       # billing order: identity + numeric sort
  elementKeyType = "number"
    [[sql-projection.source.aggregate.element]]
    field = "person_id"
    from  = "$.person_id"
    [[sql-projection.source.aggregate.element]]
    field = "billing"
    from  = "$.billing"

[[sql-projection.source]]
topic   = "credit"
keyPath = "$.movie_id"
when    = [ { path = "$.role", equals = "director" } ]
  [sql-projection.source.aggregate]
  column     = "directors"
  elementKey = "$.billing"
    [[sql-projection.source.aggregate.element]]
    field = "person_id"
    from  = "$.person_id"
```

Each aggregate column is backed by a `<table>__<column>` sidecar table the
projection creates and tears down with the projection — one normalized row
per child, so the array column is a pure `jsonb_agg` of it (deterministic,
rebuildable from index 0) and a delete — which carries only the child Key —
finds and removes the right element without the payload. The `read` column
stays a clean array. Deterministic ordering is a PostgreSQL guarantee;
MySQL aggregate ordering is best-effort (`JSON_ARRAYAGG` ignores `ORDER
BY`).

**Enriching folded data from another topic (lookup).** A folded element often
carries a foreign key — `top_cast` holds each cast member's `person_id`, but the
*interesting* single-table query wants the actor's name, which lives in a
`person` topic keyed by `person_id`. A **lookup source** ingests that topic into
a keyed dimension table, and an aggregate element resolves the key into it by a
join — so the column carries the name and the query needs no join of its own.
A lookup source declares a `lookup` block (its `name`, referenced by
enrichments, and the `field`s it stores) instead of `rules`/`aggregate`; an
element field then declares `lookup`/`on`/`select` instead of `from` (`on`
names the plain element field holding the foreign key). Several enriched fields
sharing a dimension coalesce into one join:

```toml
[[sql-projection.source]]
topic = "person"                       # the dimension topic, keyed by person_id
  [sql-projection.source.lookup]
  name = "people"                      # referenced by element enrichments below
    [[sql-projection.source.lookup.field]]
    field = "name"
    from  = "$.name"

[[sql-projection.source]]
topic   = "credit"
keyPath = "$.movie_id"
  [sql-projection.source.aggregate]
  column     = "top_cast"
  elementKey = "$.billing"
    [[sql-projection.source.aggregate.element]]
    field = "person_id"                # the foreign key, stored
    from  = "$.person_id"
    [[sql-projection.source.aggregate.element]]
    field  = "name"                    # resolved from the people dimension
    lookup = "people"
    on     = "person_id"               # join the element's person_id …
    select = "name"                    # … to the dimension's name
```

The dimension is the source of truth (a `<table>__lookup_<name>` housekeeping
table); the array column joins to it at materialize, so the resolved value is
never copied and stays fresh. Cross-topic order is handled: a dimension row
that arrives *after* the facts that reference it — or a later change to it —
**fans out**, re-materializing every parent whose elements reference that key,
so the names fill in (a delete nulls the field but keeps the element). Fan-out
is synchronous; for hot, fast-changing dimensions a batched option is planned.
One syncable fills one table — a dimension is its own internal housekeeping, so
two syncables that need the same data each keep their own copy.

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
Making a larger cadence *safe* on a non-idempotent sink is separate work; see
the cross-reference in `sync-fail-fast-bump-tracking`.

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

- [`docs/operations/cdc-setup.md`](docs/operations/cdc-setup.md) — setting up CDC ingest against your own database: Postgres (`wal_level`, the REPLICATION role, `REPLICA IDENTITY`, the publication/slot committed auto-creates, the slot's WAL-retention disk cost) and MySQL (binlog defaults, `binlog_row_image`, the replication grant, and why MySQL `lag`/`caughtUp` read `null`/never-true); plus the shared snapshot→streaming lifecycle and per-engine troubleshooting
- [`docs/operations/authentication.md`](docs/operations/authentication.md) — bearer token + mTLS setup
- [`docs/operations/secrets.md`](docs/operations/secrets.md) — `${VAR}` interpolation to keep DB credentials and tokens out of Raft/bbolt (Kubernetes + systemd patterns)
- [`docs/operations/shutdown.md`](docs/operations/shutdown.md) — `SIGTERM` handling and the graceful-shutdown deadline
- [`docs/operations/upgrade.md`](docs/operations/upgrade.md) — zero-downtime rolling upgrades: the node-by-node procedure (leader last), `/ready` + `/version` gating, and rollback; pairs with the on-disk forward/backward-compatibility contract in [`docs/api-compatibility.md`](docs/api-compatibility.md#on-disk-and-wire-compatibility)
- [`docs/operations/http-limits.md`](docs/operations/http-limits.md) — proposal-size cap and HTTP server timeouts
- [`docs/operations/membership.md`](docs/operations/membership.md) — adding/removing nodes with joint consensus, growing safely via learner add → catch-up → promote, and observing membership (`GET /v1/membership`, the leader-read proxy, `COMMITTED_API_URL`)
- [`docs/operations/disk-limits.md`](docs/operations/disk-limits.md) — disk-pressure behavior: the per-node free-space watcher (warn/critical/full thresholds, 507 rejections) and cluster-aware write admission (admit iff the leader and a quorum of voters have disk headroom; leadership transfers off a constrained leader), with the alerting and incident playbook
- [`docs/operations/rebuild.md`](docs/operations/rebuild.md) — rebuilding a node after a `storage invariant violation` fatal. Short version: stop the node, rsync its data directory from a healthy peer, restart. Apply determinism keeps subsequent rebuilds O(diff).
- [`docs/operations/backup.md`](docs/operations/backup.md) — offline `committed backup` / `committed restore`: archive a stopped node's data directory to a portable tar (back up a follower with no cluster downtime) for archival, pre-migration safety, off-box DR, and total-loss recovery rebuild can't cover; includes the backup × right-to-be-forgotten shared-responsibility model
- [`docs/operations/stuck-syncables.md`](docs/operations/stuck-syncables.md) — spotting a syncable wedged on a transient error (the `committed_sync_stuck` gauge, `GET /v1/syncable/{id}/status`), tracking how far behind a syncable is (`checkpoint_index`/`head_index`/`lag`/`caught_up` on the same status endpoint, where `lag == 0` ⇔ caught up), skipping the bad proposal (`POST /v1/syncable/{id}/deadletter/`), and re-driving it after a fix (`POST /v1/syncable/{id}/replay/{index}`), plus dead letters (`GET /v1/syncable/{id}/errors`) and the type-migration recovery loop (`GET /v1/type/{id}/migration-errors`, `POST /v1/type/{id}/migration-retry/{index}`).
