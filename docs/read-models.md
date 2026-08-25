# Read models (SQL projections)

A read model is a current-state table (one row per entity) that a
`projection` syncable maintains by folding an event topic in log order. The
log stays the source of truth; the table is disposable and rebuildable from
index 0.

This guide is the full projection reference: single-source folds, rule
semantics, multi-source "BFF" rows, collection aggregates, cross-topic
enrichment, delete lifecycles, and dimension fan-out.

> New to committed? The [Quickstart](quickstart.md) takes a normalized movie
> catalog to a single denormalized table you query with no joins — a working
> read model in one `docker compose up`. This guide explains the mechanics
> behind it.

## Choosing your read-model shape (read this first)

committed gives you three ways to serve queries, and **the projection is the
primary pattern** — reach for the others when their specific reason applies,
not by default:

| You want | Use | Why |
|---|---|---|
| A derived/denormalized table you query directly — a BFF row, a flat table replacing a complex join, current state per entity | **`projection`** | It maintains the table *incrementally, per event*: continuously current (no refresh step), folds **multiple topics into one row**, aggregates children into array columns, enriches from other topics. This is committed's centerpiece use case. |
| Raw per-table replicas — ad-hoc SQL, joins you own, feeding views/BI on your terms | plain **`sql`** syncable with a `primaryKey` (a mirror) | Faithful keyed copies of source tables. If you build a materialized view over mirrors, *you* own its refresh cost and staleness — committed keeps only the mirrors current. |
| One row per **event** — an audit/history log | plain **`sql`** syncable with no `primaryKey` | Append-only, replay-safe history. |

The trade to understand: a materialized view over mirrors is stale from the
moment its (often expensive) refresh completes; a projection is current
within seconds of the source commit, forever, with no refresh to schedule.
If the table you're building is *the thing the application reads*, start
from `projection` and use mirrors only for the parts a projection
cannot yet express.

One projection scope line to know before you design: a projection's
`primaryKey` takes one column or a list (`primaryKey = ["visit_id",
"workarea_id"]` — a latest-event-wins fold on a composite identity). A
composite-keyed projection folds via **set rules only**: aggregate
columns, lookup dimensions, and lookup enrichment keep the single-column
key model (the config is rejected loudly if you combine them). As with
the plain syncable, **the list order must match the producing side's key
order** — delete tombstones carry the composite key encoding and decode
positionally, and a key-shape mismatch in either direction (composite
tombstone against a single-key projection, or the reverse) dead-letters
loudly instead of silently deleting nothing. Lookups enrich both
aggregate elements and scalar spine columns (see "Enriching folded
data").

## History tables vs. read models

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

> **Renamed from `sql-projection`.** The projection language is not
> SQL-specific in principle, so the type is now `projection` with a
> `[projection]` section. The old spelling — `type = "sql-projection"`
> with a `[sql-projection]` section, always renamed together — remains
> accepted for a deprecation period; posting it succeeds and the
> response carries a `warnings[]` entry naming the rename.

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
dead-letters loudly — a composite tombstone hitting a single-key sink (or
the reverse) would otherwise execute a WHERE that matches nothing and
silently strand the deleted row. Every key column must also appear in the
mappings; the parser rejects a config where it doesn't.

## A single-source projection

```toml
[syncable]
name = "tenants"
type = "projection"
mode = "always-current"        # rules target the current type version

[projection]
topic      = "controlplane-event"
db         = "hosted-projection"
table      = "tenants"
primaryKey = "tenant_id"       # or a list: ["tenant_id", "region"] — composite folds via set rules only
# keyPath  = "$.tenant_id"     # optional; defaults to one $.<col> per primaryKey column, in order

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

### Rule semantics

A rule is an optional `when` (match clauses) plus a `set` (the columns to write).
Six properties govern its behavior — matching, application order, write shape,
error handling, deletes, and schema evolution:

- **`when` is data, not an expression**: an array of clauses, all of
  which must hold (AND); express OR as another rule. Each clause is one
  `path` plus exactly one predicate: `equals`, `null = true` (matches a
  *present* JSON null — the flag form, since TOML cannot write
  `equals = null`), `notEquals`, `notNull` (present and
  non-null — SQL's IS NOT NULL, the merge's left/inner gate), or
  `greaterThan`/`lessThan` (numeric literals, strict). Comparisons follow SQL: a missing or null value
  matches NO comparison — including `notEquals` — and a value of a
  different scalar family (a string where the literal is a number)
  matches neither `equals` nor `notEquals`. That is the whole language —
  single field, compare-to-literal; boolean expressions stay out.
  A missing path is "no match", never an error — including for `null`
  clauses, so an absent field never matches. Inside a fan (a forEach
  stage's `when`/`deleteWhen`, a forEach source's rule `when`) clauses
  evaluate PER ELEMENT, and a `$parent.` path reaches the enclosing
  event — the natural spelling for a parent-level predicate like
  `$parent.EventType`; anywhere without an enclosing scope, `$parent`
  is rejected at admission. If
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
  `CREATE TABLE IF NOT EXISTS` only, so re-POSTing a config that changes the
  destination shape — the **column set**, *or* a projection's **aggregate/lookup
  shape** (an element field, `elementKey`, `elementKeyType`, or a lookup field) —
  is *rejected* (409 `schema_change_requires_rebuild`, with what changed in
  `details`) rather than silently no-op'd. To
  add or change a column, replace the syncable in place:
  `DELETE /v1/syncable/{id}` (removes the config + checkpoint atomically
  and drops the table), then re-POST the new config — the fresh table
  replays from index 0. `?keepData=true` on the DELETE preserves the
  destination (e.g. another consumer reads the table). To re-materialize a drifted or
  corrupted projection *without* a schema change,
  `POST /v1/syncable/{id}/rebuild` does the drop + replay-from-0 in place
  under the same name. The log is permanent, so replay is cheap.

### Computed columns (`expr`)

A set entry's fifth arm computes its column from the event payload with a
closed function set — no user code, no SQL fragments:

```toml
set = [
  { column = "remaining", expr = "$.quoted_total - coalesce($.invoiced_total, 0)" },
  { column = "unit_price", expr = "round(($.Cost + $.Overhead) / nullif(1 - $.Margin, 0), 2)" },
]
```

The language: `+ - * /`, comparisons (`= <> < <= > >=`), the boolean
predicates — `or`/`and`/`not` (SQL keywords, case-insensitive),
`x in (…)`, `x is [not] null`, `true`/`false` literals — plus `coalesce`, `nullif`,
`round(x, scale)` (half away from zero), `trunc(x[, scale])` (toward
zero), decimal and `'string'` literals, and `$.…` paths into the
payload. Connectives follow SQL's three-valued logic — `null or true`
is TRUE, `null and false` is FALSE — the one deliberate exception to
blanket null propagation; `is [not] null` never returns null, and it
accepts a row-valued path (`$.cust is not null` asks whether the
looked-up row exists). Arithmetic is **exact** — values are arbitrary-precision decimals
built from the payload's source digits, never floats, and nothing rounds
except your explicit `round`/`trunc`. Because only division can produce a
non-terminating value, **every `/` must sit under a `round` or `trunc`**
(comparisons excepted — they consume exact quotients); a bare quotient is
rejected at POST, not at apply. A missing field is null; null propagates
through arithmetic and comparison; `coalesce`/`nullif` are the null-aware
escape hatches (`nullif(x, 0)` is the division-by-zero guard).

## Folding several topics into one row (multi-source)

A projection can consume more than one topic and fold them into a single
denormalized "BFF" row — e.g. a `movie_card` built from a normalized `movie`
topic and a `rating` topic, one row per `movie_id`. Replace the single top-level
`topic`/`rules` with a `[[projection.source]]` block per topic. The
**topic is the discriminator** (an event only ever runs its own source's
rules), and because each rule sets only its own columns, two sources fold
into one row without clobbering. Each source also declares what its delete
does to the row via `onDelete`: `delete-row` (the *spine* — its delete
drops the row), `clear` (a *contributor* — its delete NULLs only the
columns it owns, the row survives), or `ignore`.

```toml
[projection]
db = "bff"; table = "movie_card"; primaryKey = "movie_id"
# … columns: movie_id, title, year, genres, score, votes …

[[projection.source]]
topic    = "movie"          # this source's discriminator
keyPath  = "$.movie_id"     # correlate by the shared aggregate key
onDelete = "delete-row"     # movie is the spine: its delete drops the row
  [[projection.source.rules]]
  set = [ { column = "title",  from = "$.title" },
          { column = "year",   from = "$.year" },
          { column = "genres", from = "$.genres" } ]

[[projection.source]]
topic    = "rating"
onDelete = "clear"          # a contributor: its delete NULLs its columns, keeps the row
  [[projection.source.rules]]
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
[[projection.source]]
topic   = "credit"
keyPath = "$.movie_id"               # which movie row this child folds into
when    = [ { path = "$.role", equals = "actor" } ]
  [projection.source.aggregate]
  column         = "top_cast"
  elementKey     = "$.billing"       # billing order: identity + numeric sort
  elementKeyType = "number"
    [[projection.source.aggregate.element]]
    field = "person_id"
    from  = "$.person_id"
    [[projection.source.aggregate.element]]
    field = "billing"
    from  = "$.billing"

[[projection.source]]
topic   = "credit"
keyPath = "$.movie_id"
when    = [ { path = "$.role", equals = "director" } ]
  [projection.source.aggregate]
  column     = "directors"
  elementKey = "$.billing"
    [[projection.source.aggregate.element]]
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

### Scalar aggregates over the child set

The same child set can also fold into **scalar** columns — count, sum,
min, max, countDistinct — recomputed absolutely from the sidecar at every
child change (never incremented, so redelivery and rebuild converge):

```toml
  [projection.source.aggregate]
  elementKey = "$.id"
    [[projection.source.aggregate.element]]
    field = "hours"
    from  = "$.hours"
    [[projection.source.aggregate.element]]
    field = "status"
    from  = "$.status"
    [[projection.source.aggregate.scalar]]
    column = "visit_count"
    fn     = "count"
    [[projection.source.aggregate.scalar]]
    column = "hours_sum"
    fn     = "sum"
    of     = "hours"
    [[projection.source.aggregate.scalar]]
    column = "done_count"
    fn     = "count"
    where  = [ { field = "status", equals = "done" } ]
```

- **`fn`** — `count`, `sum`, `min`, `max`, or `countDistinct`. `count`
  folds rows (no `of`); the others fold the element field named by `of`.
- **`ofType`** — `text` (default) or `number`: how `min`/`max` order and
  `countDistinct` compare. ISO dates order correctly as text; numeric
  fields want `number`. `sum` always folds numerically.
- **`where`** — equality clauses over element fields, restricting which
  children fold (filtered counts).
- The array `column` becomes **optional** when scalars are present — a
  pure `visit_count` needs no array.

SQL semantics apply at the empty set: `count` is 0, `sum`/`min`/`max` are
NULL when no children qualify.

## Staged computation (internal stages)

Some read models are a *pipeline*: filter, then aggregate, then aggregate
again. `[[projection.stage]]` blocks declare internal stages — private
keyed refolds held in a node-local stage store, never topics, never sink
writes (only the table is outward-facing) — and a table source consumes a
stage with `from = "<stage name>"`:

```toml
[[projection.stage]]
name    = "live"                    # private label; stages chain by name
from    = "txns"                    # a topic — or a PRIOR stage's name
keyPath = "$.id"
emit    = [ { field = "job", from = "$.jobId" },
            { field = "amt", from = "$.amount" } ]

[[projection.stage]]
name    = "by-job"
from    = "live"                    # chained: consumes the stage above
keyPath = "$.job"
reduce  = "aggregate"
emit    = [ { field = "total", sum = "$.amt" },
            { field = "n",     count = true } ]

[[projection.source]]
from    = "by-job"                  # a stage-fed table source
[[projection.source.rules]]
set = [ { column = "total", from = "$.total" },
        { column = "n",     from = "$.n" } ]
```

- **Every stage is a keyed refold**: an input lands in its key's retained
  set and the key recomputes from that set — never delta arithmetic — so
  redelivery, rekeying, and replay converge to identical bytes. Key
  comparisons are byte-exact; when two producers render the same logical
  key differently (SQL Server CDC writes GUIDs UPPERCASE, most JSON
  serializers lowercase), declare `normalize = "lower"` on the
  key-bearing declaration — a stage's `keyPath`, a join (which folds
  BOTH the `on` rendering and, for a topic join, the topic's entity-key
  rendering), or a table source's `keyPath` (which also folds its
  delete-tombstone binding, so a producer's UPPERCASE tombstone still
  addresses the lowercased row). Keys only — payload values are never
  normalized. Sources sharing a normalized key space (a topic owner
  beside stage-fed decorators) must declare the same normalize.
  Aggregate folds (`sum`/`min`/`max` are closed-language expressions,
  plus `count` and `collect`) follow SQL semantics: `sum` is exact
  decimal arithmetic; `min`/`max` order ANY scalar — numbers
  numerically, text lexically (`max` of ISO date strings is SQL's
  latest-date), bools false<true; null operands skip; an emptied key
  retracts entirely, cascading. `collect` is `array_agg` with determinism SQL doesn't
  promise: values fold into an ALWAYS-SORTED array (numbers
  numerically, then strings, then bools), `distinct = true` dedupes,
  and the array lands in the sink as JSON. Any fold arm takes a
  per-emit `where` — SQL's `FILTER (WHERE …)`: `{ field = "reviewed",
  count = true, where = [ { path = "$.billed", equals = "true" } ] }`
  folds only matching inputs for THAT field, while row membership stays
  the key's.
- **Filtering is refold**: an input that stops matching a stage's `when`
  retracts from its key — predicate rows leave the read model when the
  predicate flips off, and re-enter when it flips back.
- **`when` takes an `expr` arm**: `when = [ { expr = "coalesce($.n, 0)
  > 0 and $.pricing in (0, 2)" } ]` — the expression language as a
  predicate, so compute-then-filter is ONE stage instead of an
  emit-a-gate-field stage plus a filter stage. Only TRUE matches;
  false, null, and data errors match nothing (SQL's WHERE). The scalar
  arms (`equals`, `null`, `notNull`, `notEquals`, `greaterThan`,
  `lessThan`) remain for the common cases, and the two-stage gate idiom
  stays legal where you want the intermediate value probeable.
- **Stages fan out too**: `forEach` on a stage fans each input's array
  elements into element-inputs — `keyPath` is the reduce key,
  `elementKey` the element's identity (required with a reduce, so two
  same-key elements both count), `$parent.` reaches the enclosing
  event — so fan-then-fold (elements → sums by workarea) is ONE stage.
  Re-emitted inputs reconcile; the input's tombstone retracts all its
  elements. A long run of inputs whose forEach path yields no array
  warns once (a serialized-JSON string column reads as one string and
  fans ZERO elements, silently — decode it at ingest with
  `jsonColumns`); a legitimately empty array is healthy and never
  warns.
- **Joins FILTER** (`[[projection.stage.join]]`): an input participates
  only while the joined topic's row — addressed by the input's `on`
  value against the joined entity's key — exists and matches every
  `where` clause. `on` takes one path, or a list addressing a
  composite-keyed dimension (positional values, the producer's own
  encoding; a stage join's arity must match the joined stage's
  `keyPath`). Dimension changes refold dependents (reverse-index
  fan-out); a late dimension heals; a flipped predicate or a dimension
  delete retracts dependents.
- **A join that NAMES its row can READ it** (`as`): `{ topic =
  "projects", on = "$.projectId", as = "project", where = [ … ] }`
  scopes the matched row under the alias for the stage's emits, fold
  arms, and per-emit `where` — `$.project.tenantId` — so filter-and-pull
  is one join on one stage (SQL: the joined table's columns), not
  lift-and-rekey scaffolding. Lookups are refold-time state, never
  retained copies: when the referenced row changes, dependents refold
  and pulled values track it. `optional = true` (requires `as`) is the
  LEFT JOIN — a missing row, or one failing `where`, scopes the alias
  as null instead of gating membership (`$.cust is not null` makes the
  flag column). The stage's `when` and `keyPath` see only the input;
  filter on the joined row with the join's `where` (enforced at
  admission — a fold-time path addressing an alias is rejected loudly,
  never silently null). Decision rule
  refined: SAME-KEY correlation of stages → `merge`; REFERENCE lookup
  (foreign key, or a key part) → a named join.
- **`reduce = "liveSet"` is created-minus-deleted**: a key is live while
  it has qualifying inputs and ZERO inputs matching `deleteWhen` — a set
  difference, no ordering involved, so a delete-shaped event retracts
  regardless of arrival position (it is retained as negative evidence
  even past the `when` filter), and retracting the delete event itself
  un-deletes the key. A liveSet never fans (`forEach` is rejected
  with it): delete evidence on a fanned stage would be per-element,
  and an elementless delete-shaped event would be silently lost — fan
  in a prior stage and liveSet its outputs.
- **`absent = true` inverts a join into an ANTI-join**: the input
  participates only while NO dimension row matches (none exists, or
  none passes `where`) — "jobs with no posted invoice." Arrival
  retracts, departure or a `where` mismatch heals back in, and a
  missing `on` reference is vacuously absent.
- **`merge` combines PRIOR stages BY KEY** — SQL's outer join, aliased:
  `merge = [ "quoted", { stage = "invoiced-sums", as = "invoiced" } ]`
  makes each key ANY side holds a tuple scoping every side's current
  output under its alias (`$.quoted.total`; absent sides are explicit
  nulls, exactly as an outer join produces NULL columns). Gate to
  left/inner with `when` `notNull`/`null` on an alias path — the when
  unit rule: `when` always filters the stage's FOLD UNIT (an input, a
  fanned element, or a merged tuple). A merge declares NO
  keyPath/keyType/normalize: its key space is inherited from the merged
  stages, which admission requires to agree. Value resolution lives
  here — `expr` with `coalesce` subtracts, sums, and unions across
  sides — and merged values are ordinary emitted data, so a downstream
  stage can key by them (attribution re-keys through the existing
  machinery). Decision rule: SAME-KEY correlation → merge; FOREIGN-KEY
  reference → join. A merge feeding a single table source is the
  primary multi-stage-table pattern (one source, whole-row writes);
  rowOwner remains the form for mixed topic/stage tables.
- **Joins can address a PRIOR stage** (`from = "<stage>"` on a join):
  its outputs are the dimension rows, maintained live by the drain — so
  cross-stage correlation is a join, not a second input; heal, flip,
  and retraction all flow through the same fan-out.
- **`reduce = "latest"` is argmax by a business field** (`orderBy` — never
  arrival order, so backfills converge with steady state), with `tieBy`
  a MANDATORY deterministic tiebreak (`orderByType`/`tieByType` choose
  numeric vs lexical; text default). `when` filters before the argmax,
  and a retracted winner promotes the runner-up from the retained set.
- **Stage-fed sources key rows by the stage's key** (no keyPath
  resolution — an aggregate's emit carries folds, never its key). A
  retraction — the stage's key going away, or a live output that stops
  matching the source's `when` — retracts the source's contribution:
  the row (`onDelete = "delete-row"`), its own columns (`"clear"`), or
  nothing (`"ignore"`).
- **Introspection**: `GET /syncable/{id}/status?stages=true` reports
  each stage's row — `keys` (the store's current output count) plus
  `inputs`/`fanned` flow counters since the worker started, and
  `unkeyedDeletes` (delete-shaped inputs whose key would not resolve or
  render — LOST retractions: nonzero here answers "the delete event
  arrived but the key is still live" in one read; each also warns once
  in the log). The three
  numbers split every silent-empty state: `inputs` 0 = that topic's log
  region not reached (mid-replay zeros are healthy); `inputs` > 0 with
  `fanned` 0 = the forEach fan finds no array; flow > 0 with `keys` 0 =
  the when/joins rejected everything (a per-element `when` referencing
  parent fields needs `$parent.`). `?probeStage=<stage>&probeKey=<part>`
  answers whether one specific key is currently held — one probeKey per
  keyPath position, in order; the stage renders and composes the parts
  (its normalize applies server-side), so probe values in your own
  vocabulary. Numbers go in canonical digits (`5`, not `5.0000`); text
  is never re-parsed.
- **Key identity is canonical — and declarable**: numeric key parts
  render canonically (`5`, `5.0000`, and `5e0` are ONE key; `5.25`
  keeps its digits), and strings are never re-parsed by default
  (`"007"` stays text). `keyType = ["text","number"]` (per keyPath
  position; one value broadcasts) is SQL's declared-column-type model:
  under `"number"` a STRING rendering coerces too — a producer that
  serializes `5` as `"5.0000"` folds onto the same key — and an
  unrenderable value is non-membership, like a missing key part. Topic
  joins declare `onType` for their reference side; a stage join
  inherits the joined stage's `keyType` (like `normalize`). Declared
  types also render `?probeKey` parts, removing the canonical-digits
  probe obligation. Source digit strings are always preserved in
  VALUES; keys are identity.
- **Upgrades never reset unchanged configs**: the store fingerprint
  covers DECLARED content only, so new vocabulary in a new binary
  leaves untouched configs' stores intact (pinned by a golden contract
  test). When a store genuinely resets (a stage edit, an ownership
  move), the worker re-derives before consuming and the status endpoint
  says so: `workerState: "re-deriving"` with `stageRecovery {folded,
  target}` progress on the default call — lag climbs by design until
  the fold-only pass reaches the checkpoint.
- Stage state lives in one bbolt file per syncable under
  `<dataDir>/projections/` — derived, node-local, rebuildable from the
  log. **Editing stage definitions requires a rebuild** (the
  config-change guard enforces it): changed stages must re-derive from
  index 0, exactly like a changed table schema.

### Row ownership (`rowOwner`)

When several sources fold one row and any of them is stage-fed, the
table must declare which source owns row existence:

```toml
[[projection.source]]              # the row owner: admits and removes rows
topic    = "jobs"
keyPath  = "$.id"
rowOwner = true
[[projection.source.rules]]
set = [ { column = "name", from = "$.name" } ]

[[projection.source]]              # a decorator: fills its own columns
from = "latest-proposal"           # decorators must be stage-fed
[[projection.source.rules]]
set = [ { column = "latest_proposal_id", from = "$.pid" } ]
```

- **The owner's writes create and delete rows.** Every other row-writing
  source is a *decorator*: update-only (it never creates a row the owner
  has not admitted), retracting by clearing its own columns
  (`onDelete = "clear"`, its default) — never by removing the row.
- **An owner write pulls each decorator's retained stage output** for
  its key, so a decoration lands no matter which side arrived first —
  and survives an owner delete/re-admit cycle. The stage store is the
  retention; without the pull, delta suppression would leave a
  re-admitted row undecorated forever.
- Decorators must be stage-fed: a topic source has no retention, so a
  value arriving before the owner admits its row would be silently
  lost — feed it through a stage. Lookup and aggregate sources are
  orthogonal machinery and coexist unchanged.
- A decorator's rules may **enrich** (the `lookup` set arm): the FK and
  its display field ride the decorator's own update, and dimension
  fan-out heals referencing rows regardless of which source wrote the
  FK.
- Moving the `rowOwner` declaration changes which writes create and delete
  rows — the config-change guard demands a rebuild, like any other
  shape change.
- A single row-writing source needs no declaration (it trivially owns
  its rows).

## Fanning one event into N rows (forEach)

Some events *contain* the rows you want: a transaction event whose
`items[]` array holds the billable elements. A `forEach` source fans each
element into its own row:

```toml
[[projection.source]]
topic    = "txn"
forEach  = "$.items[*]"            # deliberately multi-valued
keyPath  = "$.sku"                 # resolves against EACH ELEMENT
onDelete = "delete-rows"           # the default: parent delete cascades
  [[projection.source.rules]]
  set = [
    { column = "amount",  from = "$.amount" },     # element scope
    { column = "txn_id",  from = "$parent.id" },   # the enclosing event
  ]
```

- The source's rules apply once **per element**; `keyPath` and every
  `from`/`expr` path resolve against the element, and a `$parent.` prefix
  reaches the enclosing event payload. Row identity is the element's key.
- **Reconciliation is absolute**: a re-emitted parent's rows converge on
  its current elements — a vanished element's row is deleted (tracked via
  a per-source reconciliation sidecar, `ForEachSidecarName`), so replay
  and redelivery are safe like every other projection write.
- A parent tombstone **cascades** to every row it fanned
  (`onDelete = "delete-rows"`), or is dropped with `ignore`.
- One forEach source per topic per projection; elements that match no
  rule fan no row (and reconcile away if they previously did). A long
  run of events whose forEach path yields no array warns once — the
  serialized-JSON-column trap; see the staged-computation note.

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
[[projection.source]]
topic = "person"                       # the dimension topic, keyed by person_id
  [projection.source.lookup]
  name = "people"                      # referenced by element enrichments below
    [[projection.source.lookup.field]]
    field = "name"
    from  = "$.name"

[[projection.source]]
topic   = "credit"
keyPath = "$.movie_id"
  [projection.source.aggregate]
  column     = "top_cast"
  elementKey = "$.billing"
    [[projection.source.aggregate.element]]
    field = "person_id"                # the foreign key, stored
    from  = "$.person_id"
    [[projection.source.aggregate.element]]
    field  = "name"                    # resolved from the people dimension
    lookup = "people"
    on     = "person_id"               # join the element's person_id …
    select = "name"                    # … to the dimension's name
```

The dimension is the source of truth (a `<table>__lookup_<name>` housekeeping
table); the array column joins to it at materialize, so the resolved value is
never copied and stays fresh.

### Enriching a spine column (a parent display field)

Lookups also enrich **scalar spine columns** — the canonical BFF ask, "the
job row carries the customer's display name as a real column":

```toml
[[projection.rules]]
when = [ { path = "$.kind", equals = "job" } ]
set = [
  { column = "customer_id",   from = "$.CustomerId" },
  { column = "customer_name", lookup = "customers", on = "customer_id", select = "display_name" },
]
```

The semantics are **join-equivalence**: the column always equals what
`LEFT JOIN <dimension> ON key = customer_id` would return right now. A
customer rename fans out to every referencing row; a customer delete NULLs
them; a job row applied before its customer row starts NULL and heals when
the customer arrives. committed auto-indexes the `on` column (the fan-out's
WHERE clause), so you don't pay a scan per dimension event.

Three contracts to know:
- `on` must name a column **the same rule sets** with `from`/`value` — the
  FK and its resolved value are written atomically, so they can never
  desync.
- The `on` column's declared type must be **integer-family or text-family**
  (it holds another topic's entity key; fractional types have ambiguous
  renderings and are rejected with an explanation).
- One column joins **one** dimension: enriching the same column from two
  different `(lookup, on, select)` tuples is rejected.

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

## Changing the rules after a projection is live

A projection is a **disposable view of an immutable log** — its fold rules, and
the type migration that feeds it, are *derivation logic*, not data. When you
change that logic, committed applies the new logic to Actuals it processes *from
that point on*; it does **not** retroactively re-render rows already written to
the sink. Correcting history is a deliberate rebuild, and running it is your
responsibility.

**Changing one projection's own rules — blue-green.** Because a projection
replays from index 0 and the log stays the source of truth, the clean way to fix
or evolve a syncable's rules is to stand a *second* projection up beside the
first rather than mutate a live table:

1. `POST` a new `projection` with the corrected rules, pointed at a **new
   table**. It replays the whole log from the start and materializes the fixed
   view — the old table keeps serving reads the entire time, so there is no
   downtime.
2. Watch it catch up (its progress reaches the log head) and validate the new
   table against the old however you trust it.
3. Cut your readers over to the new table, then delete the old syncable.

This is the recommended pattern precisely because the view is disposable: you
never mutate a live table in place, and you get to *verify* the fix before
trusting it.

**Fixing a type's migration — the fan-out case.** A type's `[migration]` jq (the
transform that brings older-version Actuals up to the current schema) is read by
**every** projection on that topic. Committed lets you correct a buggy migration
in place, at the same version — but that edit, too, changes only how *future*
reads transform the data. Every row already synced through the old migration
stays as it was, on **all** dependent projections, and committed does not
currently tell you which projections are now stale or rebuild them for you.

**After an in-place migration fix, rebuilding the dependent projections is your
responsibility** — blue-green each one as above — for the correction to reach
already-synced history. Treat a migration edit as "applies going forward;
rebuild the dependents to fix the past." If you only need the correction for new
data and accept the historical rows as-is, that is a valid choice — just make it
knowingly, because nothing rebuilds them on your behalf.

## Checkpoint cadence and replay throughput

A syncable's `checkpointEvery` (TOML, `[syncable]` section) is its **checkpoint
cadence**: how many synced records may accumulate before the resume checkpoint
is durably persisted. It is also the crash re-delivery bound — a restart
re-delivers at most that many already-synced records, which keyed sinks absorb
idempotently. It does **not** control sink transaction size: batches are capped
internally (a few hundred rows) regardless of cadence.

The cadence matters most during **replays** (initial sink builds, rebuilds):
every checkpoint persist is a consensus round trip, and many sinks replaying
with a tight cadence can bottleneck on checkpoint traffic rather than data.
The default (2500) keeps replays fast out of the box; raising it further
(e.g. 5000) buys a little more replay throughput at a proportionally larger
re-delivery window. Caught-up syncables persist on catch-up regardless of
cadence, so steady-state checkpoint freshness does not depend on this value.

## Destination limits: when a row cannot fit the sink

Every SQL engine has physical limits on its tables, and a projection can hit
them two ways — at **table-creation time** (loud, immediate, nothing synced
yet) or at **apply time** (a specific row fails while its neighbors succeed).
Survey your widest tables against these before creating mirrors:

- **PostgreSQL**: a row must fit an 8 KB heap page after compression/TOAST —
  very wide tables (many hundreds of columns of inline data) can produce
  individual rows that exceed it at apply time ("row is too big"). And a
  text value with an **embedded U+0000** is rejected at apply time
  (SQLSTATE 22021) even though MySQL and SQL Server store it — a row that
  flowed through every other engine dead-letters only at a PG sink. The
  dead-letter message names the offending payload field(s), so triage is
  read-the-record, not hand-hunting the byte across every string column;
  the remedy is fixing the value at the source (CDC delivers the
  correction — then acknowledge the record) or excluding the column.
- **MySQL/InnoDB**: ~1,017 columns per table and index-key length limits
  (a `TEXT`/long-`VARCHAR` primary key needs a prefix or a different key
  column) — both fail at table creation, loudly. `TEXT` caps at 64 KB —
  an over-long value fails at apply time ("data too long"); use `LONGTEXT`
  for unbounded content columns.

**The two engines respond differently to the apply-time case, and both are
deliberate.** committed skips a row (recording a durable **dead letter**)
only when the destination's error proves the failure is *entry-specific* —
this row's own value, where every other row would apply. It wedges the
worker (sticks-and-waits, resumes on fix) whenever the error could be
schema- or config-shaped. MySQL reports an over-long value as a per-column
data error, which proves entry-specificity → the row is dead-lettered and
the sink keeps flowing. PostgreSQL reports its row-size wall as a program
limit, which doesn't → the worker wedges visibly on it. Same principle,
different engine error vocabularies.

**Skipped is never silent, and never final.** `GET /syncable/{id}/status`
reports `deadLetters` (and the latest skipped index) alongside `caughtUp` —
the honest completeness check for a mirror is `caughtUp && deadLetters == 0 &&
workerState == "running"` (a `parked` or `degraded` worker isn't syncing at
all — see
[operations/stuck-syncables.md](operations/stuck-syncables.md)).
List the skipped proposals with `GET /syncable/{id}/errors`; after fixing
the destination (e.g. `ALTER ... LONGTEXT`), re-drive each with
`POST /syncable/{id}/replay/{index}`, which applies the row and clears its
record. If instead the fix happened **at the source** and a later CDC event
already corrected the sink row, replaying the stale proposal would regress
it — acknowledge the record instead
(`POST /syncable/{id}/deadletter/{index}/acknowledge`; see the triage flow
in [operations/stuck-syncables.md](operations/stuck-syncables.md)). A
wedged worker needs no replay — it resumes by itself once the destination
accepts the row.

## See also

- [Quickstart](quickstart.md) — a working four-topic `movie_card` read model
  (spine + contributor + aggregate + lookup) end to end.
- [docs/event-log-architecture.md](event-log-architecture.md) — why the log is
  the source of truth and the read model is disposable.
- [docs/operations/rebuild.md](operations/rebuild.md) and
  [docs/operations/stuck-syncables.md](operations/stuck-syncables.md) —
  rebuilding a projection and recovering a wedged one.
