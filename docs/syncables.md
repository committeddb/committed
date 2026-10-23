# Syncables

A **syncable** projects committed Actuals out to an external system. It is
handed [Actuals](actuals.md) in log order, applies each one to its
destination, and checkpoints its progress through consensus, so a restart
resumes where it left off and a replay from index 0 rebuilds the destination.
The log stays the source of truth; every destination a syncable maintains is
disposable.

| Kind | Destination | Reference |
|---|---|---|
| `sql` | A MySQL or PostgreSQL table: a keyed mirror, or a keyless history table | [SQL mirrors and history tables](#sql-mirrors-and-history-tables-sql) (below) |
| `projection` | A current-state SQL table folded from one or more topics | [read-models.md](read-models.md) |
| `http` | A webhook | [webhook-receiver.md](webhook-receiver.md) |
| `iceberg` | An Apache Iceberg table on S3 | [iceberg-syncable.md](iceberg-syncable.md) |
| `loopback` | A derived topic in the same cluster | [loopback.md](loopback.md) |

SQL kinds write to a [database](databases.md) config by name (`db = "..."`).

| Endpoint | |
|---|---|
| `POST /v1/syncable/{id}`, `GET /v1/syncable`, `DELETE /v1/syncable/{id}` | Create or change, list, delete. |
| `GET /v1/syncable/{id}/versions[/{version}]`, `POST /v1/syncable/{id}/rollback` | Config history and rollback. |
| `GET /v1/syncable/{id}/status` | Progress, ownership, staleness, derivation links. |
| `POST /v1/syncable/{id}/rebuild` | Drop the destination and replay from index 0. |
| `POST /v1/syncable/{id}/rematerialize` | Replay in place for a keyed `sql` mirror or a `loopback`; see below. |
| `GET /v1/syncable/{id}/errors`, `.../deadletter`, `.../replay/{index}` | Dead letters and stuck syncables; see [stuck-syncables.md](operations/stuck-syncables.md). |
| `POST /v1/syncable/dryrun` | Rehearse a projection against the real log; see [read-models.md](read-models.md#rehearsing-a-config-before-it-exists-dry-run). |

A syncable can be pinned to a zone, so a node near the destination serves it;
see [zones.md](operations/zones.md).

## Choosing a shape

committed gives you three ways to serve queries, and **the projection is the
primary pattern** — reach for the others when their specific reason applies,
not by default:

| You want | Use | Why |
|---|---|---|
| A derived/denormalized table you query directly — a BFF row, a flat table replacing a complex join, current state per entity | **`projection`** | It maintains the table *incrementally, per event*: continuously current (no refresh step), folds **multiple topics into one row**, aggregates children into array columns, enriches from other topics. This is committed's centerpiece use case. |
| Raw per-table replicas — ad-hoc SQL, joins you own, feeding views/BI on your terms | plain **`sql`** syncable with a `primaryKey` (a mirror) | Faithful keyed copies of source tables. If you build a materialized view over mirrors, *you* own its refresh cost and staleness — committed keeps only the mirrors current. |
| One row per **event** — an audit/history log | plain **`sql`** syncable with no `primaryKey` | Append-only, replay-safe history. Add `keyColumn` if the topic can carry deletes — without it they dead-letter (below). |

The trade to understand: a materialized view over mirrors is stale from the
moment its (often expensive) refresh completes; a projection is current
within seconds of the source commit, forever, with no refresh to schedule.
If the table you're building is *the thing the application reads*, start
from `projection` and use mirrors only for the parts a projection
cannot yet express.

## SQL mirrors and history tables (`sql`)

A `sql` syncable lands a topic in a SQL table through mappings. With a
`primaryKey` it upserts: a keyed **mirror**, one row per entity key, and a
source delete removes the row. Without one it appends: a **history table**,
one row per event. Use `sql` for mirrors, history tables, and
`snapshot`-kind topics, which are total updates with nothing to fold; to
maintain one current-state row per entity from an `event`-kind topic, use a
[projection](read-models.md). One topic typically feeds both.

The simplest shape is a plain `sql` syncable with `[[sql.mappings]]`
extracting leaf values into columns:

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

### Landing a whole payload in one column

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

### Secondary indexes

`[[sql.indexes]]` declares an index on the destination table, for the
columns your queries filter or join on:

```toml
[[sql.indexes]]
name  = "orders_by_customer"
index = ["customer_id", "placed_at"]   # or one string: "customer_id, placed_at"
```

`name` is the index's name in the database and `index` its columns, in
order. committed creates the indexes with the table. On PostgreSQL it also
creates an index missing from an existing table each time the syncable
starts; on MySQL, an index added to the config after the table exists must
be created by hand.

### History tables

A keyless history table (no `primaryKey`) is **replay-safe**: committed dedups
each appended row on a hidden sidecar (`<table>__committed_applied`, keyed by the
event's raft index and its ordinal within the proposal), so a crash mid-batch, a
leader-change re-sync, or a corrupt-checkpoint restart re-applies as a no-op
rather than duplicating rows. The sidecar is committed-managed and never queried
by the application — your history table keeps exactly the columns you mapped. (A
`snapshot`-kind or otherwise keyed `sql` syncable needs none of this: its upsert
is already idempotent on the key.) Because the sidecar name is derived from the
table name, a keyless syncable's table must be short enough that
`<table>__committed_applied` fits the database's 63-char identifier limit;
committed rejects a longer one at config time.

**A keyless table cannot honor a delete unless you tell it where the key is.**
A keyed syncable deletes by its `primaryKey`; a keyless one has no key to bind,
so a delete tombstone — including a
[right-to-be-forgotten erasure](operations/rtbf.md) — **dead-letters** rather
than being silently dropped, and the syncable keeps running. Set `keyColumn` to
the column whose value equals the entity's key and deletes translate to
`DELETE FROM <table> WHERE <keyColumn> = ?`, removing the subject's whole
history from the table:

```toml
[sql]
db = "bff"; table = "order_events"      # no primaryKey: append-only history
keyColumn = "order_id"                  # ...but deletes still erase by this column
[[sql.mappings]]
jsonPath = "$.order_id"; column = "order_id"; type = "TEXT"
```

`keyColumn` is single-column and cannot be combined with a composite
`primaryKey`. Leaving it unset is a legitimate choice for a topic that never
receives deletes — committed logs a warning at config time naming the
consequence, so the decision is yours and visible. If you leave it unset on a
topic that may carry erasures, erasing that table is your responsibility.

The sidecar keys on the event's *raft index*, so it makes re-applying the **same
committed event** a no-op — but each *distinct* event is still its own row. A
history table records one row per event by design, so the same logical data
committed as two separate events lands as two rows. That happens two ways:

- **You propose the same unkeyed data more than once.** Direct proposals are
  yours to control, and committed faithfully appends each.
- **An ingestable re-emits a row.** A snapshot is at-least-once at the source-row
  level: a crash mid-snapshot re-reads the current window, and a *full
  re-snapshot* — a Postgres replication-slot recreate, deleting and recreating
  the ingestable, or a node rebuild — re-reads the whole source table, so one
  source row can arrive as several events. (CDC *stream* changes are deduplicated
  by source sequence and do not duplicate this way; only the snapshot phase
  does.)

Either way the history table faithfully records each event — that is what an
append log is. If you want one row per logical entity instead, give the `sql`
syncable a `primaryKey` (its upsert then converges on the key) or maintain a
current-state table with `projection`.

`primaryKey` takes one column or a list: `primaryKey = ["tenant_id",
"project_id"]` declares a real composite `PRIMARY KEY (tenant_id,
project_id)` — upserts converge on the combination and a source delete
removes the row matching every key column. **The columns and their order
must match the producing ingestable's `primaryKey`**: a delete tombstone
carries only the encoded key, and its values decode positionally in the
producer's column order — a mismatched order mis-addresses rows, and nothing
can detect it mechanically (topics decouple the two configs on purpose).
A mismatched column COUNT, by contrast, is caught at delete-apply time and
dead-letters loudly — a composite tombstone hitting a single-key destination (or
the reverse) would otherwise execute a WHERE that matches nothing and
silently strand the deleted row. Every key column must also appear in the
mappings; the parser rejects a config where it doesn't.

### Re-materializing a keyed mirror in place

For a keyed `sql` syncable, `POST /v1/syncable/{id}/rematerialize` converges
the live table without dropping it: the worker replays from index 0 through
the current mappings, type migrations, and restatements, keyed upserts
overwrite rows in place while the table keeps serving reads, and a completion
sweep removes rows the replay never re-emitted. It is restart-resumable, and
it refreshes the syncable's `interpretationPin` (clearing
`interpretationStale`). Keyless syncables and projections refuse the verb:
rebuild those (`POST /v1/syncable/{id}/rebuild`), or stand a projection's
replacement up beside it
([blue-green](read-models.md#changing-the-rules-after-a-projection-is-live)).
The verb also needs every member of the cluster on 0.8.0 or later: on a
mixed-version cluster it answers 503 `cluster_below_feature_level` until the
rolling upgrade completes, because an older node taking ownership mid-replay
would write rows the completion sweep then deletes.

## SQL destinations: the note table and ownership

A SQL syncable keeps a small table called `committed__destinations` in the
destination database: one row per table it writes, a note saying which
rendering version of committed wrote its rows and whether committed
created the table. A worker reads the version before serving, and if a
newer committed writes rows differently than the note says, the syncable
stops and tells you how to bring the table forward instead of mixing two
renderings in one table (see
[api-compatibility.md](architecture/api-compatibility.md#derived-state-stage-stores-and-destination-renderings)).

Whether committed created the table decides what a `DELETE` does. The
rule is the one committed applies everywhere it touches your systems: it
removes what it created and leaves what it did not. A table committed
created (there was none when the syncable was first POSTed) is dropped
with the syncable. A table you created first and pointed a syncable at
stays, rows and all. The helper tables are always committed's and always
go. `?keepData=true` on the DELETE hands a table committed created over
to you: nothing is removed, and committed stops counting the table as
its own, so a later DELETE leaves it too.

## Consumer stances: how a syncable meets a versioned topic

The log is permanently heterogeneous: entities carry the type version that was
current when they were written, and there is no ALTER TABLE — only new writes
under new versions. Every syncable takes one of three stances toward that:

- **Version-pinned** (`mode = "as-stored"`, handling one version): the syncable
  sees the exact bytes written, and its mappings target one version's shape.
  Nothing pins it, though: there is no version selector in the vocabulary, so
  entities written under a later version still arrive, as written, and the
  mappings meet a shape they were not written for. Take this stance for a
  topic whose version is frozen; a topic that will move needs one of the two
  below.
- **Version-aware** (`mode = "as-stored"`, dispatching on version): the
  syncable sees raw bytes plus each entity's stamped version and handles the
  differences itself — the stance for consumers that genuinely want each era's
  shape.
- **Always-current** (`mode = "always-current"`): committed runs each entity
  through the type's migration chain (older-version data upgraded step by step)
  before the syncable sees it, so mappings only ever target the current shape.

Always-current is a **promise, and it is checked at admission**: it is only
admissible when the migration chain can actually carry every version's data to
the current shape. A version declared `nonConvertible = true` in its
`[migration]` section — meaning that version requires information older data
never contained — breaks the chain: `POST /v1/syncable/{id}` refuses an
always-current syncable over such a topic, naming the gap, and
`POST /v1/type/{id}` refuses a nonConvertible bump that would strand *existing*
always-current syncables, naming them (re-POST with `?force=true` to
acknowledge the stranding deliberately). A force-stranded syncable does not
silently receive unconverted data, and it does not shunt the whole topic to
dead letters either. The first entities below the break dead-letter at the
migration chain (replayable); once ten consecutive distinct rows have failed
there, the failure is established as config-shaped and the worker **wedges**
on the next one, visibly, rather than dead-lettering everything older than
the break (see [stuck-syncables.md](operations/stuck-syncables.md)). The
repair is the reading, not the syncable: a restatement rebinding the
below-break range lets the wedged worker resume on its own, after which the
dead letters replay.

## Checkpoint cadence

After each successful sync a syncable durably checkpoints its progress
(`SyncableIndex`) through raft, so recovery is deterministic — but that is one
raft round-trip per checkpoint. Two optional `[syncable]` fields tune how often
it happens, trading that round-trip against how many already-synced proposals a
crash re-delivers:

```toml
[syncable]
name = "warehouse"
type = "sql"
checkpointEvery   = 500      # checkpoint once per 500 successful syncs
checkpointMaxAge  = "1s"     # ...or once 1s elapses since the first pending one
```

- **`checkpointEvery`** — persist the checkpoint once per this many successful
  syncs. Default `1` for a single syncable (`http`) and `2500` for a batch
  syncable (`sql`, `projection`). The worker always also flushes when it
  catches up (reaches the end of the log), so a low-traffic syncable never
  lags its checkpoint regardless of this value.
- **`checkpointMaxAge`** (a duration like `"500ms"`; default: no age bound for
  single syncables, 50ms for batch) — flush a pending checkpoint after this
  long even if `checkpointEvery` hasn't been reached.

Both keys apply to the kinds that batch or checkpoint per sync — `sql`,
`projection`, and `http`. `iceberg` batches by its own `flushRows` /
`flushInterval`, and `loopback` checkpoints per Actual; both **refuse** these
keys rather than accepting and ignoring them.

**Duplicate bound:** a crash re-delivers already-synced proposals — at most
`checkpointEvery` for a single syncable, and up to `checkpointEvery` plus the
batch cap for a batch syncable, since the two are sized separately. Keep it at
`1` for **non-idempotent** destinations (an HTTP webhook, an event stream:
every duplicate is externally visible). Raise it only for **idempotent**
destinations (the `sql` and `projection` kinds upsert, so a replay is a
harmless no-op), where it trades that bounded duplicate exposure for
substantially fewer raft round-trips on a fast destination.

The cadence matters most during **replays** (initial destination builds, rebuilds):
every checkpoint persist is a consensus round trip, and many syncables replaying
with a tight cadence can bottleneck on checkpoint traffic rather than data.
The default (2500) keeps replays fast out of the box; raising it further
(e.g. 5000) buys a little more replay throughput at a proportionally larger
re-delivery window. Caught-up syncables persist on catch-up regardless of
cadence, so steady-state checkpoint freshness does not depend on this value.
