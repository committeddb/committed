# API tour

Routes are served by Chi from `internal/cluster/db/http/`. The full
OpenAPI spec is available at `/openapi.yaml` with a Swagger UI at
`/docs`. Every API endpoint lives under a `/v1` prefix (see
[`docs/api-compatibility.md`](api-compatibility.md)); bearer-token
auth supports a legacy shared token or separate API, membership, and peer
credentials (see
[`docs/operations/authentication.md`](operations/authentication.md)).

Runnable example configs live in [`examples/movies/`](../examples/movies/), wired
together end to end in the [Quickstart](quickstart.md). The snippets below
reference them as standalone illustrations of each endpoint; run the commands
from the repository root.

Define a type:

```sh
curl -X POST -H 'Content-Type: text/toml' \
  --data-binary @examples/movies/type-movie.toml \
  http://localhost:8080/v1/type/movie
```

## Entity kinds

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

## The shape census: drafting a contract from the data

The reference for the census, the gate, and the tripwire — every key, the
status fields, and the event payload — is
[docs/schema-contracts.md](schema-contracts.md); this section is the why.

The hardest part of blessing a contract on an existing table is that nobody
knows what's in it — JSON columns whose shape lives only in what developers
happened to write, evolved over years, often with several shapes interleaved.
Committed takes that census for free: the ingest snapshot pass already streams
every historical row, and the worker folds each row's payload **shape** (paths
and types — never values) into a per-topic census, published as replicated
state and served on `GET /v1/ingestable/{id}/status`:

- every path with its observed types, row count, and first/last-seen row —
  including paths nested inside JSON columns;
- each **distinct shape** with its count and row range — the interleaved-shapes
  evidence;
- a **draft JSON Schema** mirroring the observed structure
  (`additionalProperties: false` at every level), ready to review and POST as
  a type — with `validate = "announce"` it becomes the tripwire's contract. Inference
  is bootstrap-only: the draft is never auto-blessed, and the runtime never
  infers — after blessing, the tripwire reconciles reality against the
  declared contract.

The census is **on by default** (it only happens while a snapshot streams the
table — getting one later costs a full re-snapshot) and adds roughly a
microsecond per row; `census = false` in the `[ingestable]` section opts out.
Value tracking is **opt-in**: `censusValues = true` keeps a bounded set of
distinct values per string field (`censusValueLimit`, default 16), and the
drafter then proposes `enum` for fields whose values repeat — so a brand-new
enum value later fires the tripwire as an ordinary schema violation. Opt-in
because it puts source values into replicated state; by default the census
carries types and paths only.

Two limits, stated plainly: the census describes **only what was written** —
it is first-row detection over history, not pre-deploy contract enforcement
(that would need the producer's write path, which committed does not have).
And a discriminant the producer never recorded is unrecoverable by any
system: if two shapes interleave with no field telling them apart, the
census shows you the interleaving, but no tool — here or anywhere — can
conjure the missing label. Fix that at the producer, or bind readings by row
range with restatements.

## Schema validation: gate or tripwire

A type with a schema chooses what happens when a payload doesn't match it,
via `validate`:

- `"none"` (the default). Payloads are never checked.
- `"schema"` — the **gate**. A direct proposal whose payload violates the
  schema is rejected with 400. Right for topics whose producers you control.
- `"announce"` — the **tripwire**. The payload **still commits** — a
  non-conformant CDC row is a true fact about the source, and refusing to
  record it would make the log less true — and the first occurrence of each
  *distinct divergent shape* emits a **ContractExtension** event to the topic
  named by `schemaChangeTopic`. Right for CDC-fed topics, where the contract
  polices reality instead of blocking it.

```toml
# 1. Declare the events topic first (any non-announce type works):
#    POST /v1/type/schema-changes
[type]
name = "SchemaChanges"
entityKind = "standalone"

# 2. Declare the announce-typed topic pointing at it:
#    POST /v1/type/photo-meta
[type]
name = "PhotoMeta"
schemaType = "JSONSchema"
schema = '{"type":"object","properties":{"caption":{"type":"string"}},"additionalProperties":false}'
validate = "announce"
schemaChangeTopic = "schema-changes"
```

Attach any ordinary syncable to `schema-changes` — a SQL projection into a
warehouse table your dbt tests watch, or an `http` syncable posting to a
webhook — and the data team learns *the day reality diverges from the
contract*, with the diff in hand. The event carries the type id/name, the
version validated against, the divergent shape's fingerprint, the observed
shape, and the structured violations (paths and types only — never sample
values), plus the source position when it arrived via CDC. Events are keyed
`typeID:version:fingerprint` and delivery is at-least-once: one event per
distinct shape (replicated dedupe, surviving restarts and failover), with the
rare concurrent-detection duplicate converging in keyed syncables. The tripwire
runs on every write path — CDC ingest (snapshot and streaming) and direct
proposals — and it never pauses or fails a write; for a schema to catch
*added* fields, declare `additionalProperties: false`.

A running CDC worker validates against the contract that was current when the
worker was built (the same point the type version it stamps is fixed). After
blessing a new contract on an already-flowing topic, re-POST the ingestable —
or restart the node — so its worker picks the contract up; direct proposals
always validate against the current version.

## Repairing the reading: restatements

An entity's stamped version is a *cache of the default reading in force at
write time* — for CDC data, committed classified it; nobody asserted anything.
When the census or the tripwire reveals that months of actuals are stamped v1
but were really v2-shaped (the unannounced writer), the log's bytes are still
true; the *reading* is wrong. There is no ALTER TABLE and no rewrite:
**restatements** are append-only, consensus-ordered statements that rebind readings:

```toml
# POST /v1/restatement/photos-backfill-v2
[restatement]
topic = "photo-meta"        # the type (topic) whose readings rebind
fromIndex = 1200           # inclusive raft-index range of existing actuals
toIndex = 8400
readAsVersion = 2        # read these as v2
fromVersion = 1            # optional: only entities STAMPED v1
# predicate = '.writer == "app-b"'   # optional: the interleaved-writers case,
                                     # a deterministic jq program (no now(), no env)
```

Every syncable — both modes — receives the authoritative reading: stamp ⊕
restatement fold. A rebound entity dispatches, and enters the always-current
migration chain, at its *effective* version, so a v2-shaped row wrongly
stamped v1 is never mangled by the v1→v2 transform. Restatements are **immutable**:
a wrong restatement is corrected by appending another — among matching restatements,
later in the log wins, matching always against the stamp — and replaying data
plus restatements history is deterministic at every `(data index, interpretation
index)` pair. Topics with no restatements pay nothing on the read path.

A restatement also marks dependent syncables **stale**
(`GET /v1/syncable/{id}/status` → `interpretationStale`): rows synced before
it landed were derived under the superseded reading. Staleness is loud and
queryable, never auto-healed — re-derivation is yours to trigger:
`POST /v1/syncable/{id}/rematerialize` replays the topic from index 0 through
the current projection + interpretation while the keyed syncable keeps serving
(rows converge in place; a completion sweep removes rows the replay no longer
produces), or blue-green a replacement for syncable shapes that can't converge
in place. `GET /v1/restatement` lists the registry.

Configure a database to write into (destination):

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
# Composite source keys: primaryKey = ["tenant_id", "project_id"] declares a
# real multi-column PRIMARY KEY. The columns and their ORDER must match the
# producing ingestable's primaryKey — delete tombstones decode positionally.

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

## SQL projections

The plain `sql` syncable lands a *history* table: one row per event.
Applications usually query *current state*: one row per entity, which
requires folding each entity's events in log order. A
`type = "projection"` syncable expresses that fold declaratively —
in the one place that sees every event exactly in order — so the log
stays the source of truth, the rules live in version-controlled TOML,
reads are O(1), and the table is disposable: rebuild it by replaying
from index 0. Use `sql` for event-log/history tables (and for
`snapshot`-kind topics, which are total updates with nothing to fold);
use `projection` to maintain current-state tables from an
`event`-kind topic. One topic typically feeds both.

> **The former `sql-projection` spelling was removed in 0.8.0.** The type
> is `projection` with a `[projection]` section, and the array tables are
> plural (`[[projection.sources]]`, `[[projection.stages]]`,
> `[[…aggregate.fields]]`, `[[…aggregate.scalars]]`, `[[…lookup.fields]]`,
> `[[…stages.joins]]`). Posting the old spelling is
> refused with a message naming the rename, and a config stored under it
> parks on an upgraded node until re-POSTed renamed — its declared content is
> unchanged, so it resumes with its stores and checkpoint (see
> [upgrade.md](operations/upgrade.md#before-you-upgrade)).

```toml
[syncable]
name = "tenants"
type = "projection"
mode = "always-current"        # rules target the current type version

[projection]
topic      = "controlplane-event"
db         = "hosted-projection"
table      = "tenants"
primaryKey = "tenant_id"
# keyPath  = "$.tenant_id"     # optional; defaults to $.<primaryKey>

[[projection.columns]]
name = "tenant_id"
type = "VARCHAR(256)"

[[projection.columns]]
name = "tier"
type = "VARCHAR(32)"

[[projection.columns]]
name = "state"
type = "VARCHAR(32)"

[[projection.columns]]
name = "allocs"
type = "JSONB"

[[projection.rules]]
when = [ { path = "$.event_type", equals = "tenant.created" } ]
set  = [
  { column = "tier",  from  = "$.tier" },
  { column = "state", value = "pending" },
]

[[projection.rules]]
when = [
  { path = "$.event_type", equals = "tenant.provisioned" },
  { path = "$.tier",       equals = "prod" },
]
set  = [
  { column = "state",  value = "active" },
  { column = "allocs", from  = "$.allocs" },
]

[[projection.rules]]
when = [ { path = "$.event_type", equals = "tenant.deprovisioned" } ]
set  = [
  { column = "state",  value = "deprovisioning" },
  { column = "allocs", null  = true },
]
```

That single-source fold is the simplest shape. A projection can also fold
several topics into one denormalized "BFF" row (a **spine** plus
**contributors**), fold a collection of child rows into one JSON column
(**aggregate**) or into scalar aggregate columns (count/sum/min/max/
countDistinct over the child set), compute columns with exact-decimal
expressions (**expr** — coalesce/nullif/round/trunc, `case`, string
functions and `like`, casts, math and date/time functions, and
arithmetic, no floats anywhere), and enrich folded elements from a dimension topic
(**lookup**), fan one event's array into N rows (**forEach**, with
absolute reconciliation and cascading parent deletes), and chain internal
**stages** — private keyed refolds (filter → aggregate → aggregate, and
window functions over an entity's ordered history: `lag`/`lead`, running
totals, `row_number`/`rank`) a
table source consumes by name, with retraction and row ownership built
in (one source declares `rowOwner = true` and owns row existence; the
others decorate) — each with its own delete lifecycle, plus dimension fan-out for out-of-order dimensions. The full reference, with rule semantics and worked
examples, is in **[docs/read-models.md](read-models.md)**.

Configure an ingestable that pulls from an external source into the log (a
Postgres logical-replication source; the [Quickstart](quickstart.md) wires
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
[`docs/operations/membership.md`](operations/membership.md).
