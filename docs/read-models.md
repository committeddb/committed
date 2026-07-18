# Read models (SQL projections)

A read model is a current-state table (one row per entity) that a
`sql-projection` syncable maintains by folding an event topic in log order. The
log stays the source of truth; the table is disposable and rebuildable from
index 0.

This guide is the full projection reference: single-source folds, rule
semantics, multi-source "BFF" rows, collection aggregates, cross-topic
enrichment, delete lifecycles, and dimension fan-out.

> New to committed? The [Quickstart](quickstart.md) takes a normalized movie
> catalog to a single denormalized table you query with no joins — a working
> read model in one `docker compose up`. This guide explains the mechanics
> behind it.

## History tables vs. read models

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

## A single-source projection

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

### Rule semantics

A rule is an optional `when` (match clauses) plus a `set` (the columns to write).
Six properties govern its behavior — matching, application order, write shape,
error handling, deletes, and schema evolution:

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
  missing (or a matched event with no value at `keyPath`)
  dead-letters as a permanent error rather than wedging the worker. A
  rule with **no** `when` matches every event of its source (the topic
  is the discriminator) — the natural shape for folding a `snapshot`
  source that has one event variant.
- **Deletes are honored**: a delete Actual hard-deletes the projected
  row by entity key (right-to-be-forgotten), distinct from soft-delete
  rules like `state = "deprovisioning"` — both exist. Deleting an
  absent row is a no-op, which is what makes a fresh replay of an
  already-scrubbed log correct. (Multi-source rows have richer delete
  behavior — see [Deletes and partial deletes](#deletes-and-partial-deletes).)
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

## Folding several topics into one row (multi-source)

A projection can consume more than one topic and fold them into a single
denormalized "BFF" row — e.g. a `movie_card` built from a normalized `movie`
topic and a `rating` topic, one row per `movie_id`. Replace the single top-level
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

## Folding a collection into one column (aggregate)

Some BFF columns are *collections*: a movie's `top_cast[]` folds many `credit`
rows into one JSON array on the movie row. A source declares an `aggregate` block
instead of `rules`:

- **`column`** — the array column.
- **`elementKey`** — a jsonpath giving each child's **sort position** within the
  array (ordered per `elementKeyType`). It is *only* the sort key: a re-delivered
  child replaces rather than duplicates because the sidecar is keyed on the child
  **entity's Key**, not on `elementKey`. (Two *distinct* child entities that
  happen to share an `elementKey` value therefore both appear — dedup is by
  entity identity, not by `elementKey`.)
- **`elementKeyType`** — `number` (sort 1, 2, …, 10) or `text` (lexical, the
  default).
- **`element`** — an array-of-tables naming the per-child object's fields (an
  array, not an inline map, so field names survive byte-exact).

A child delete removes exactly its element via `onDelete = "remove-from-aggregate"`,
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
rebuildable from index 0) and a delete (which carries only the child Key)
finds and removes the right element without the payload. The `read` column
stays a clean array. Deterministic ordering is a PostgreSQL guarantee;
MySQL aggregate ordering is best-effort (`JSON_ARRAYAGG` ignores `ORDER
BY`).

## Enriching folded data from another topic (lookup)

A folded element often carries a foreign key — `top_cast` holds each cast
member's `person_id`, but the *interesting* single-table query wants the actor's
name, which lives in a `person` topic keyed by `person_id`. A **lookup source**
ingests that topic into a keyed dimension table, and an aggregate element
resolves the key into it by a join — so the column carries the name and the
query needs no join of its own. A lookup source declares a `lookup` block (its
`name`, referenced by enrichments, and the `field`s it stores) instead of
`rules`/`aggregate`; an element field then declares `lookup`/`on`/`select`
instead of `from` (`on` names the plain element field holding the foreign key).
Several enriched fields sharing a dimension coalesce into one join:

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
never copied and stays fresh.

## Deletes and partial deletes

A projection honors deletes, and what a delete *does* depends on the source it
arrives on — the partial-delete behavior that lets a denormalized row degrade
gracefully instead of vanishing:

- **Single-source / spine (`onDelete = "delete-row"`).** A delete Actual
  hard-deletes the projected row by entity key — right-to-be-forgotten, distinct
  from a soft-delete *rule* like `state = "deprovisioning"` (both exist). In a
  multi-source row the **spine** owns the row's existence: its delete drops the
  whole row, contributor columns and all.
- **Contributor (`onDelete = "clear"`) — a partial delete.** A contributor's
  delete NULLs only the columns that source owns and leaves the row (and every
  other source's columns) intact. A `movie_card` whose `rating` is deleted keeps
  its title, genres, and cast; only `score`/`votes` go NULL.
- **`onDelete = "ignore"`.** The delete changes nothing — for a source whose
  events should never retract what they wrote.
- **Aggregate element (`onDelete = "remove-from-aggregate"`).** A child delete
  removes exactly its element from the collection column — matched by the child
  **entity's Key** (the sidecar's key), so the delete needs only that key, not
  the payload — and leaves the row and the other elements.
- **Enriched element (lookup).** Deleting a dimension row NULLs the enriched
  field on every element that referenced it but keeps the element itself: the
  stored foreign key remains, only the joined value clears.

Deleting an absent row (or an element that isn't there) is a no-op, which is
what makes a fresh replay of an already-scrubbed log correct.

## Dimension fan-out

A lookup dimension is the source of truth for the values it resolves, and it can
arrive or change in any order relative to the facts that reference it. Committed
handles cross-topic order by **fanning out**: when a dimension row arrives
*after* the facts that reference it (or a later change updates it), the
projection re-materializes every parent row whose elements reference that key,
so the resolved values fill in (and a dimension delete NULLs the field on those
elements while keeping them). Fan-out is synchronous — a dimension change
re-materializes its dependents inside the same transaction, so the read model
stays consistent at every checkpoint. One syncable fills one table — a
dimension is its own internal housekeeping, so two syncables that need the same
data each keep their own copy.

## See also

- [Quickstart](quickstart.md) — a working four-topic `movie_card` read model
  (spine + contributor + aggregate + lookup) end to end.
- [docs/event-log-architecture.md](event-log-architecture.md) — why the log is
  the source of truth and the read model is disposable.
- [docs/operations/rebuild.md](operations/rebuild.md) and
  [docs/operations/stuck-syncables.md](operations/stuck-syncables.md) —
  rebuilding a projection and recovering a wedged one.
