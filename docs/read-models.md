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

The plain `sql` syncable lands a *history* table: one row per event, or a
keyed mirror of a source table; see
[syncables.md](syncables.md#sql-mirrors-and-history-tables-sql), which also
covers [choosing between them and a projection](syncables.md#choosing-a-shape).
A projection maintains a *current-state* table: one row per entity, folding
each entity's events in log order.

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
  `greaterThan`/`lessThan` (strict; a number literal compares numbers,
  a text literal compares strings in byte order — `greaterThan =
  "2026-01-01"` is an ISO-date window — and a quoted number like `"5"` is
  rejected, since as text `"10"` sorts before `"9"`). Comparisons follow SQL: a missing or null value
  matches NO comparison — including `notEquals` — and a value of a
  different scalar family (a string where the literal is a number)
  matches no comparison either. That is the whole language —
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
  `committed_sync_rules_unmatched_total` counter ticks — the signal that a
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
  and drops the table if committed created it — a table you created
  stays, so drop it yourself first), then re-POST the new config — the
  fresh table replays from index 0. `?keepData=true` on the DELETE hands
  the table over instead (e.g. another consumer reads it): nothing is
  dropped, and committed no longer counts the table as its own. To
  re-materialize a drifted or corrupted projection *without* a schema change,
  `POST /v1/syncable/{id}/rebuild` does the drop + replay-from-0 in place
  under the same name, for a table committed created. On a table you
  created it refuses (409 `destination_not_owned`) rather than replay over
  rows it cannot drop: drop the table yourself and re-POST, or rematerialize
  a keyed syncable. A drop the destination refuses is reported (502
  `destination_teardown_failed`) rather than replayed over silently — the
  replay still runs, so the table is current, but rows it does not reproduce
  remain until you sweep them. The log is permanent, so replay is cheap.

### Computed columns (`expr`)

A set entry's fifth arm computes its column from the event payload with a
closed function set — no user code, no SQL fragments:

```toml
set = [
  { column = "remaining", expr = "$.quoted_total - coalesce($.invoiced_total, 0)" },
  { column = "unit_price", expr = "round(($.Cost + $.Overhead) / nullif(1 - $.Margin, 0), 2)" },
]
```

The language: `+ - * /`, comparisons (`= <> < <= > >=` — numbers
numerically, strings in byte order, `false < true`; ordering a string
against a number is an error), the boolean
predicates — `or`/`and`/`not` (SQL keywords, case-insensitive),
`x [not] in (…)`, `x [not] like pattern`, `x is [not] null`, `x is [not] distinct from y` (null-safe comparison: two nulls are not distinct, never null), `true`/`false`/`null` literals — plus `coalesce`, `nullif`,
`round(x, scale)` (half away from zero), `trunc(x[, scale])` (toward
zero), SQL's `case` conditional, `x [not] between a and b`, the string functions, casts, math, and date/time functions below, decimal and `'string'` literals, and `$.…` paths into the
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

`case` takes both SQL forms — searched (`case when … then … else … end`)
and simple (`case $.status when 1 then 'open' when 2 then 'closed' end`):

```toml
set = [
  { column = "size_band", expr = "case when $.hours < 8 then 'small' when $.hours < 40 then 'medium' else 'large' end" },
  { column = "rate",      expr = "case when $.hours = 0 then null else round($.amount / $.hours, 2) end" },
]
```

The first `when` that is TRUE selects its result; a null or false `when`
falls through (in the simple form a null operand matches nothing), and no
match without an `else` is null. Only the selected branch is evaluated, so
a guarded branch never fails the event — `rate` above never divides by
zero. A `/` inside a result still needs a `round`/`trunc` (one around the
whole `case` covers every branch), and literal results of different kinds
(`then 1 else 'x'`) are rejected at POST. `case` works everywhere `expr`
does, including fold arms — SQL's conditional sum is
`{ field = "done_hours", sum = "case when $.status = 'done' then $.hours else 0 end" }`.

The string functions follow PostgreSQL, counting in characters (code
points), never bytes:

| Function | Result |
|---|---|
| `a \|\| b` | concatenation; null if either side is null |
| `concat(a, …)` | concatenation that skips nulls (`concat(null)` is `''`) |
| `lower(s)`, `upper(s)` | case mapping |
| `trim(s[, chars])`, `ltrim`, `rtrim` | strip spaces — or the characters in `chars` — from both ends, the start, or the end |
| `substring(s, start[, count])` | characters from 1-based `start`; a `start` below 1 eats into `count` |
| `length(s)`, `char_length(s)` | character count |
| `replace(s, from, to)` | every literal occurrence of `from` replaced (no regex) |
| `starts_with(s, prefix)` | boolean |
| `s [not] like pattern` | case-sensitive whole-string match: `%` any run, `_` one character, `\` escapes the next |

```toml
set = [
  { column = "display_name", expr = "trim(coalesce($.first, '') || ' ' || coalesce($.last, ''))" },
  { column = "code",         expr = "upper(trim($.code))" },
]
when = [ { expr = "$.sku like 'SVC-%'" } ]
```

A null argument makes any of them null except `concat`. `||` binds looser
than `+`/`-` and tighter than comparisons, as in PostgreSQL (`'#' || $.n + 1`
concatenates the sum). `||` and `concat` render a number in canonical digits
and a boolean as `true`/`false`; every other string function, and `like`,
needs a string (`lower` of a number fails the event — there is no implicit
conversion). `trim` strips spaces only, not tabs or newlines, matching
PostgreSQL. Four deliberate differences from PostgreSQL keep the results
identical on every replay:

- **Case mapping is locale-free** — Unicode's simple one-to-one mappings,
  so `upper('ß')` stays `ß` (PostgreSQL can give `ẞ`, depending on its locale). The Unicode tables
  ship with committed's binary, and Unicode very rarely changes an existing
  mapping (it gave Georgian new capitals in 2018); should an upgrade carry
  such a change, rows written before it keep the old result until you
  rebuild.
- **Numbers render in canonical digits** — `'n=' || $.n` over a payload
  `1.50` is `n=1.5` (PostgreSQL keeps the source scale, `n=1.50`).
- **Booleans render as `true`/`false` in `concat`** too (PostgreSQL's
  `concat` writes `t`/`f`, its `||` writes `true`/`false`).
- **Strings order by byte (code point)** — PostgreSQL's `COLLATE "C"`,
  not a locale collation, which a library upgrade can reorder: `'B' < 'a'`
  is true (a locale puts `a` first). ISO dates and same-case codes order
  identically either way, and the `min`/`max` folds use the same order.

Casts convert between numbers, text, and booleans with PostgreSQL's rules —
`cast(x as t)` or the shorthand `x::t`, for `numeric` (`decimal`), `integer`
(`int`), `bigint`, `text` (`varchar`), and `boolean` (`bool`):

```toml
set = [
  { column = "qty",         expr = "greatest(cast($.qty as numeric), 0)" },
  { column = "status_code", expr = "$.Status::integer" },
  { column = "discount",    expr = "coalesce(try_cast($.discount as numeric), 0)" },
]
```

- **`numeric`** reads a string in PostgreSQL's numeric syntax, exactly:
  surrounding spaces, a sign, a fraction, an exponent (`1e3`), `0x`/`0o`/`0b`
  prefixes, and `_` between digits (`1_000`).
- **`integer`/`bigint`** round a number half away from zero (`2.5` → `3`)
  but read a string only in integer syntax (`'2.5'::integer` fails — go
  through `::numeric` first), map a boolean to `1`/`0`, and fail outside
  their range.
- **`boolean`** reads PostgreSQL's spellings — `true`/`false`, `yes`/`no`,
  `on`/`off`, `1`/`0`, or any unambiguous prefix (`t`, `n`), in any case —
  and casts an integer as nonzero-is-true.
- **`text`** renders a number in canonical digits and a boolean as
  `true`/`false`.

A cast that cannot convert its value fails the event, as in PostgreSQL.
**`try_cast(x as t)`** — committed's one addition — is the same conversion,
yielding null instead, for a producer whose strings are sometimes junk;
pair it with `coalesce` for a default. `NaN` and `Infinity`, which
PostgreSQL's numeric accepts, have no exact value here and are a failed
conversion.

The math functions: `abs`, `ceil` (`ceiling`), `floor`, `mod(a, b)` (the
remainder takes the dividend's sign; `mod(x, 0)` fails), and
`greatest(…)`/`least(…)`, which ignore nulls (null only when every argument
is) and order values like the comparisons do. `ceil`, `floor`, and an
`integer` cast settle a division the way `round` does, so
`ceil($.minutes / 60)` needs no `round`.

#### Dates and times

Four PostgreSQL types compute over time: `timestamptz`, `timestamp`,
`date`, and `interval`. A payload carries times as strings, so you reach
them with a cast or a typed literal:

```toml
set = [
  { column = "due",       expr = "cast($.created as timestamptz) + interval '30 days'" },
  { column = "days_open", expr = "trunc(extract(epoch from ($.closed::timestamptz - $.created::timestamptz)) / 86400)" },
  { column = "month",     expr = "date_trunc('month', $.paid_at::timestamptz)" },
]
when = [ { expr = "$.paid_at::date >= '2026-01-01'" } ]
```

- **Input** is the ISO 8601 family every source writes: `2024-01-15`,
  then a `T` or a space and `HH:MM[:SS[.fraction]]`, then an optional
  offset `Z`, `+05:30`, `+0530`, or `+05`. That covers Postgres, MySQL,
  and SQL Server ingest renderings and RFC 3339 producer timestamps alike.
  **A time without an offset is UTC.**
- **Output** is canonical text: a `timestamptz` as
  `2024-01-15 07:00:45.123456+00:00`, a `timestamp` without the offset, a
  `date` as `2024-01-15`, and an `interval` in PostgreSQL's form
  (`1 year 2 mons 3 days 04:05:06.5`). Every SQL destination column
  accepts these (MySQL rejects PostgreSQL's own `+00` and a trailing `Z`,
  which is why the offset is written `+00:00`).
- **Typed literals**: `date '2024-01-15'`, `timestamptz '…'`,
  `interval '30 days'` — intervals also read `'1 day 02:00'`,
  `'2 hours 30 mins'`, `'1.5 months'`, `'2 weeks ago'`, and ISO 8601
  `'P1DT2H'`.
- **Arithmetic** follows PostgreSQL: `timestamp ± interval`;
  `timestamp - timestamp` is an interval; `date ± n` moves whole days;
  `date - date` is a day count; `date ± interval` is a `timestamp`;
  intervals add, subtract, negate, and multiply by a number. Months are
  calendar months — `date '2024-01-31' + interval '1 month'` is Feb 29.
- **Functions**: `date_trunc('unit', t)` (`microseconds` through
  `millennium`, `week` starting Monday), `extract(field from t)` /
  `date_part('field', t)` (`year`, `quarter`, `month`, `week`, `day`,
  `dow`, `isodow`, `doy`, `hour`, `minute`, `second`, `epoch`, …),
  `to_timestamp(epoch_seconds)`, and `make_date(y, m, d)`. The unit is a
  literal, checked when the config is posted.
- **Comparisons** between times — including against a string, which is
  read as the other side's type (`$.paid_at::date >= '2026-01-01'`) —
  compare instants; a date is its midnight. Two plain strings still
  compare as text, which orders ISO strings correctly only when they share
  one rendering: `'2024-01-15T10:00Z'` and `'2024-01-15 09:00+00'` do NOT
  compare as times until you cast them. `min`/`max`/`collect` order time
  values chronologically.

**Time zones: UTC only.** There are no named zones and no `AT TIME ZONE`:
a zone's rules change between releases of the time-zone database, and a
replay must compute what the first pass computed. So
`date_trunc('day', …)` starts days at UTC midnight — a daily rollup for a
non-UTC business shifts its boundaries by the offset; shift the timestamp
first (`$.at::timestamptz - interval '7 hours'`) if a fixed offset fits.
There is no `now()` either: a read model computed from "now" would change
on every replay.

The other differences from PostgreSQL: years run 0001–9999 (no BC, no
`infinity`), and PostgreSQL's free-form inputs (`January 8, 2024`,
`epoch`, `today`) are failed conversions — `try_cast` turns them into
null. Precision is PostgreSQL's microseconds, rounded the way it rounds,
so a SQL Server `datetime2(7)` value loses its seventh digit exactly as
PostgreSQL would drop it.

## Folding several topics into one row (multi-source)

A projection can consume more than one topic and fold them into a single
denormalized "BFF" row — e.g. a `movie_card` built from a normalized `movie`
topic and a `rating` topic, one row per `movie_id`. Replace the single top-level
`topic`/`rules` with a `[[projection.sources]]` block per topic. The
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

[[projection.sources]]
topic    = "movie"          # this source's discriminator
keyPath  = "$.movie_id"     # correlate by the shared aggregate key
onDelete = "delete-row"     # movie is the spine: its delete drops the row
  [[projection.sources.rules]]
  set = [ { column = "title",  from = "$.title" },
          { column = "year",   from = "$.year" },
          { column = "genres", from = "$.genres" } ]

[[projection.sources]]
topic    = "rating"
onDelete = "clear"          # a contributor: its delete NULLs its columns, keeps the row
  [[projection.sources.rules]]
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
- **`fields`** — an array-of-tables naming the per-child object's fields (an
  array, not an inline map, so field names survive byte-exact).

A child delete removes exactly its element via `onDelete = "remove-from-aggregate"`,
leaving the row.

Because several sources may share one topic, a source's optional `when`
filters which of the topic's events it folds — so one topic splits into
different columns. Here the `credit` topic feeds `top_cast` (actors) and
`directors` (directors):

```toml
[[projection.sources]]
topic   = "credit"
keyPath = "$.movie_id"               # which movie row this child folds into
when    = [ { path = "$.role", equals = "actor" } ]
  [projection.sources.aggregate]
  column         = "top_cast"
  elementKey     = "$.billing"       # billing order: identity + numeric sort
  elementKeyType = "number"
    [[projection.sources.aggregate.fields]]
    field = "person_id"
    from  = "$.person_id"
    [[projection.sources.aggregate.fields]]
    field = "billing"
    from  = "$.billing"

[[projection.sources]]
topic   = "credit"
keyPath = "$.movie_id"
when    = [ { path = "$.role", equals = "director" } ]
  [projection.sources.aggregate]
  column     = "directors"
  elementKey = "$.billing"
    [[projection.sources.aggregate.fields]]
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
  [projection.sources.aggregate]
  elementKey = "$.id"
    [[projection.sources.aggregate.fields]]
    field = "hours"
    from  = "$.hours"
    [[projection.sources.aggregate.fields]]
    field = "status"
    from  = "$.status"
    [[projection.sources.aggregate.scalars]]
    column = "visit_count"
    fn     = "count"
    [[projection.sources.aggregate.scalars]]
    column = "hours_sum"
    fn     = "sum"
    of     = "hours"
    [[projection.sources.aggregate.scalars]]
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
again. `[[projection.stages]]` blocks declare internal stages — private
keyed refolds held in a node-local stage store, never topics, never destination
writes (only the table is outward-facing) — and a table source consumes a
stage with `from = "<stage name>"`:

```toml
[[projection.stages]]
name    = "live"                    # private label; stages chain by name
from    = "txns"                    # a topic — or a PRIOR stage's name
keyPath = "$.id"
emit    = [ { field = "job", from = "$.jobId" },
            { field = "amt", from = "$.amount" } ]

[[projection.stages]]
name    = "by-job"
from    = "live"                    # chained: consumes the stage above
keyPath = "$.job"
reduce  = "aggregate"
emit    = [ { field = "total", sum = "$.amt" },
            { field = "n",     count = true } ]

[[projection.sources]]
from    = "by-job"                  # a stage-fed table source
[[projection.sources.rules]]
set = [ { column = "total", from = "$.total" },
        { column = "n",     from = "$.n" } ]
```

- **Every stage is a keyed refold**: an input lands in its key's retained
  set and the key's output is what recomputing from that set gives, so
  redelivery, rekeying, and replay converge to identical bytes. An
  aggregate stage without joins also keeps each key's fold state (exact
  counts and sums, the current extremes, ordered value lists) and updates
  it from the inputs that changed rather than re-reading the set; wherever
  that state cannot show the same result, the key recomputes from its set. Key
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
  and the array lands in the destination as JSON. Any fold arm takes a
  per-emit `where` — SQL's `FILTER (WHERE …)`: `{ field = "reviewed",
  count = true, where = [ { path = "$.billed", equals = "true" } ] }`
  folds only matching inputs for THAT field, while row membership stays
  the key's.
- **An aggregate emit can be an expression over aggregate calls** —
  SQL's select list over a GROUP BY, so a ratio, an average, or a
  conditional count is one emit instead of a fold stage plus a reshape:

  ```toml
  emit   = [ { field = "margin",   expr = "round((sum($.rev) - sum($.cost)) / nullif(sum($.rev), 0), 4)" },
             { field = "avg_rate", expr = "round(avg($.rate), 2)" },
             { field = "techs",    expr = "count(distinct $.tech_id)" },
             { field = "done",     expr = "count(*) filter (where $.status = 'done')" },
             { field = "source",   expr = "'teamup'" } ]
  having = "sum($.hours) > 0"
  ```

  The calls are `sum`, `min`, `max`, `avg`, `count(*)`, `count(x)`
  (non-null values), `array_agg(x [order by …])`, and
  `string_agg(x, 'sep' [order by …])`, with `distinct` on all but
  `min`/`max` and `filter (where …)` on any of them. Each is folded over the key's inputs
  with the same refold-from-scratch rules as the arms, and SQL's
  empty-set results (`count` 0, the others null). `avg` is an exact
  quotient, so like `/` it sits under `round`/`trunc`; `sum` also adds
  intervals. Outside an aggregate call an expression reads no input path
  — which input would it come from? — so a constant is fine and the key
  reaches the table through the stage key. The arm forms (`sum = "$.x"`)
  remain as shorthand for one call. Aggregate calls belong to aggregate
  stages' emits and `having` only.
- **`array_agg` and `string_agg` order by a business field**:
  `array_agg($.person_id order by $.billing)`,
  `string_agg($.name, ', ' order by $.billing desc)`. Each `order by` key
  takes `asc`/`desc` and `nulls first`/`last` (default: nulls last
  ascending, first descending). They follow PostgreSQL: `array_agg` keeps
  null elements, `string_agg` skips nulls and takes text only (cast a
  number: `$.n::text`), the delimiter is a string literal, both are null
  — not empty — when no input qualifies, and with `distinct` the `order
  by` must be the argument itself. Where PostgreSQL leaves the order
  unspecified — no `order by`, or a tie — committed sorts by the value
  (nulls last), so a refold is byte-identical. The `collect` arm is the
  older, unordered form: it skips nulls and yields `[]` for an empty
  set.
- **Subscripts and slices take the top N as one column** —
  PostgreSQL's array subscripts on an aggregate's array:
  `(array_agg($.person_id order by $.billing))[1:5]` is the top five
  cast, `[1]` the lead, `[2:]` everyone after, and `cardinality(…)` the
  element count. Positions are 1-based (PostgreSQL's arrays): a subscript
  outside the array is null, a slice clips to the array and is `[]` when
  nothing remains, a null array or bound gives null, and a non-integer
  position rounds. The parentheses are required, as in PostgreSQL. Payload
  arrays are different: they're addressed by jsonpath, 0-based
  (`$.items[0]`).
- **`having` filters the folded keys** — SQL's HAVING: an expression over
  aggregate calls, evaluated after the fold. A key whose `having` is not
  TRUE has no output, so it retracts downstream like an emptied key and
  re-enters when the predicate flips back; the stage's `havingRejected`
  counter (below) tells "folded but filtered" apart from "no inputs".
- **Keys can be computed** — SQL's `GROUP BY <expression>`: a `keyPath`
  entry may be an expression instead of a path, so a rollup by month, a
  normalized email, or a `case` bucket is one stage:
  `keyPath = [ "$.customer", "date_trunc('month', $.paid_at::timestamptz)" ]`.
  The value keys exactly as a stored value renders (a month is
  `2024-03-01 00:00:00+00:00`, which lands in a `TIMESTAMPTZ` key column
  as is), `keyType`/`normalize` apply after it, an input whose expression
  is null or fails is not a member (the first failure is logged), and an
  input whose value changes moves to its new key like any rekey. Probe it
  with the computed value (`probeKey=2024-03-01 00:00:00+00:00`). An
  entry is a path whenever it is one — `$.id`, and also the bare
  `tenant_id` form — so existing keys never change meaning. Computed keys
  belong to stages and topic merge sides; a table source keys rows by a
  path (feed it from the stage instead).
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
  elements into element-inputs — and `fan` is its multi-arm form
  (SQL's UNION ALL of lateral fans): `fan = [ { forEach = "$.a[*]",
  when = [ … ] }, … ]` — each arm's `when` selects which INPUTS it
  applies to, every matching arm fans its path, and element identity
  is namespaced by arm, so created-elements and added-elements fold
  through ONE stage instead of parallel fans merged back together.
  Either form fans each input's array
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
- **Joins FILTER** (`[[projection.stages.joins]]`): an input participates
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
  rowOwner remains the form for mixed topic/stage tables. A side may
  also be a TOPIC — `{ topic = "workareas", keyPath = "$.Id", as =
  "wa" }`, with its own declared key space (keyType/normalize as
  needed) — the lift stage, synthesized: the engine folds the topic
  through a hidden identity reshape (`<topic>[<keyPath>]` in stats)
  feeding the merge as a normal side.
- **A list `from` is UNION ALL**: `from = [ "order-placed",
  "order-shipped", "order-cancelled" ]` folds the rows of every listed
  topic or prior stage as ONE input stream — for a history split across
  topics (one topic per event type), or one entity fed by two systems.
  Every stage kind takes a list, so `avg`, `count(distinct …)`, ordered
  `array_agg`, `latest` ("last activity of any kind"), a window over a
  cross-topic timeline, and a `liveSet` whose create and delete events
  live in different topics all work across the sources. Each input's
  identity is (source, key), so equal keys from different sources stay
  separate rows, and each source's tombstones retract its own rows only.
  `source()` returns the input's source name — SQL's per-branch literal
  (`SELECT 'placed' AS kind … UNION ALL …`); it has no PostgreSQL
  counterpart. It reads the input, so it works in `when`, `deleteWhen`,
  `keyPath`, a window's or latest's order keys, emits, and inside
  aggregate calls (`count(*) filter (where source() = 'returns')`), but
  not in a join's `where`, a merge, or a table rule. In a single-source
  stage it returns that source's name. Inputs keep their own shapes: align
  them with expressions (`coalesce($.job_id, $.jobId)`), or reshape each
  source in its own stage and list those. A window over a list keys its
  rows by (source, input key); a table fed by it decodes that as a
  two-column `primaryKey`. Stats and the dry-run split inputs per source
  (`inputsBySource`). Decision rule: SAME-KEY correlation → `merge`
  (it pairs sides into one tuple); ROWS from several sources → a list
  `from` (it keeps every row).

  ```toml
  [[projection.stages]]
  name        = "order-timeline"
  from        = [ "order-placed", "order-shipped", "order-cancelled" ]
  reduce      = "window"
  partitionBy = "$.order_id"
  orderBy     = [ "$.at::timestamptz" ]
  tieBy       = [ "source()" ]
  emit = [ { field = "kind",            expr = "source()" },
           { field = "secs_since_prev", expr = "extract(epoch from ($.at::timestamptz - lag($.at::timestamptz)))" } ]
  ```
- **Joins can address a PRIOR stage** (`from = "<stage>"` on a join):
  its outputs are the dimension rows, maintained live by the drain — so
  cross-stage correlation is a join, not a second input; heal, flip,
  and retraction all flow through the same fan-out. And with `field`,
  a stage join addresses an EMITTED FIELD instead of the stage's key —
  SQL's join-on-any-column: the engine synthesizes the re-key stage
  you used to write by hand (visible in stats as `<stage>[<field>]`,
  same cost, later fusable). Non-unique field values resolve to the
  bytewise-largest producer key's row, deterministically; with `field`,
  `normalize`/`onType` are declarable (a non-key field is not governed
  by the producer's key space).
- **`fan = true` makes a named join multiply rows**, as SQL's join does
  over a non-unique column — for many-to-many, where neither side can key
  the result: memberships × group permissions = a user-permissions table.

  ```toml
  [[projection.stages]]
  name    = "user-perms"
  from    = "memberships"                     # { user_id, group_id }
  joins   = [ { from = "group-perms", field = "$.group_id", on = "$.group_id", as = "perm", fan = true } ]
  keyPath = [ "$.user_id", "$.perm.permission" ]
  reduce  = "aggregate"                       # two groups granting one permission fold into one row
  emit    = [ { field = "groups", expr = "string_agg($.group_id, ',' order by $.group_id)" } ]
  ```

  The stage's fold unit becomes the PAIR (input, matched row): one per
  row whose `field` matches the input's `on` and passes the join's
  `where`, so `keyPath`, `when`, and emits read the matched row as
  `$.<as>.` (at fold time too: the row is part of the pair). An aggregate
  folds over pairs, double counting included, as SQL's GROUP BY after a
  join does. With `optional = true`, an input with no match keeps one pair
  with a null alias (LEFT JOIN; key it with `coalesce`). A fan join needs
  `as` and `field` (a join by key matches at most one row); `field` works
  on a topic join when it fans (every entity carrying the value matches).
  One fan join per stage, none beside `forEach`, and none on window or
  liveSet stages — fan in a prior stage instead. A matched row arriving,
  changing, or leaving updates one pair in every input referencing it,
  and an input's change re-fans its own pairs; stats count pairs as
  `fanned`. The output is the product of the sides, as in SQL: a value
  shared by many rows on both sides (one group with thousands of members
  and permissions) makes that many pairs.
- **`orderBy`/`tieBy` choose which matching row a join resolves to, and
  `asOf` bounds it in business time** — SQL's ASOF JOIN (PostgreSQL's
  `LEFT JOIN LATERAL (… WHERE p.product_id = o.product_id AND
  p.effective_from <= o.ordered_at ORDER BY p.effective_from DESC,
  p.change_id DESC LIMIT 1)`). An order prices at the rate in effect at
  its own time, not today's:

  ```toml
  joins = [ { topic = "price-changes", field = "$.product_id", on = "$.product_id",
              orderBy = [ "$.effective_from::timestamptz" ], tieBy = [ "$.change_id" ],
              asOf = "$.ordered_at::timestamptz", as = "price", optional = true } ]
  ```

  Among the rows whose `field` matches the input's `on` (and that pass the
  join's `where`), the join resolves to the greatest by `orderBy`, then
  `tieBy` (required), then the producer key. Both take the window's
  order-key form and read the JOINED row. With `asOf` (an expression over
  the input; no aliases), only rows whose single, ascending `orderBy`
  value is at or before the input's bound are eligible: inclusive and
  backward only. A null bound or a null version time is never eligible,
  and comparing different kinds (a time against a number) is a data error,
  so cast both sides alike. A version is valid until the next version for
  the same reference (there is no `validTo`). All join modes compose:
  - inner drops an input before any version;
  - `optional` scopes it null;
  - `absent` keeps exactly the inputs with no version in effect.

  Without `asOf`, an ordered join is "the latest by business order" (a
  ticket's current assignee by assignment time, not by key bytes); without
  `orderBy`, a `field` join keeps its original rule (the bytewise-largest
  producer key's row). `field` is declarable on a topic join when the join
  orders. Corrections propagate exactly: a version inserted, corrected,
  moved, or deleted at time T re-resolves only the inputs bounded in
  [T, the next version), and an input whose own time changes re-resolves
  just itself. Stats and the dry-run count them per join (`reresolved`).
  Transaction-time as-of ("what the log believed then") is deliberately
  not offered: it is arrival order, which a backfill or an ingest snapshot
  makes meaningless.

  As-of versus copying the value into the event: copying a price or rate
  into the event at command time works only for data the command side
  had, and it freezes mistakes. An as-of join resolves at fold time from
  the versions themselves, so a backfilled or corrected version reaches
  every event in its range.
- **`reduce = "latest"` is argmax by a business field** (`orderBy` — never
  arrival order, so backfills converge with steady state), with `tieBy`
  a MANDATORY deterministic tiebreak (`orderByType`/`tieByType` choose
  numeric vs lexical; text default). `when` filters before the argmax,
  and a retracted winner promotes the runner-up from the retained set.
  `orderBy`/`tieBy` also take the window's order-key form — a list, an
  expression, a direction: `orderBy = [ "$.at::timestamptz", "$.priority
  desc" ]`, `tieBy = [ "$.id" ]`. The winner is then the LAST row in that
  order, compared in the expression language's value order (timestamps as
  instants, numbers numerically): `orderBy = [ "$.at::timestamptz desc"
  ]` keeps the earliest. It is PostgreSQL's `DISTINCT ON (key) … ORDER BY
  key, <each key reversed>`, nulls included: `$.at` (nulls last) is `at
  DESC` (nulls first), so a null time wins unless written `$.at nulls
  first`. In this form, cast instead of `orderByType`. A single bare path
  keeps the original comparison.
- **`reduce = "window"` keeps one row per input** — SQL's window
  functions, `f(…) OVER (PARTITION BY … ORDER BY …)`, for the
  computations over an entity's ordered history that keep a row per
  event: time in each status, a running balance after each transaction,
  "changed from X to Y" audit rows, the top N per group. The window is
  declared once on the stage (SQL's named `WINDOW`), and the emit
  `expr`s call window functions:

  ```toml
  [[projection.stages]]
  name        = "spans"
  from        = "job-status"
  reduce      = "window"
  partitionBy = "$.job"                       # path or expression, like keyPath; omit = one partition
  orderBy     = [ "$.at::timestamptz" ]       # each: expr [asc|desc] [nulls first|last]
  tieBy       = [ "$.seq" ]
  emit = [ { field = "job",    from = "$.job" },
           { field = "status", from = "$.status" },
           { field = "secs",   expr = "extract(epoch from (lead($.at::timestamptz) - $.at::timestamptz))" },
           { field = "prev",   expr = "lag($.status)" } ]

  [[projection.stages]]
  name    = "time-in-status"                  # then fold the spans
  from    = "spans"
  keyPath = [ "$.job", "$.status" ]
  reduce  = "aggregate"
  emit    = [ { field = "secs", expr = "sum($.secs)" } ]
  ```

  The functions are `row_number()`, `rank()`, `dense_rank()`,
  `lag(x [, n [, default]])`, `lead(…)`, `first_value(x)`,
  `last_value(x)`, and the running aggregates `sum`, `count(*)`,
  `count(x)`, `min`, `max`, and `avg` (under `round`, like `/`), each
  with `filter (where …)`. They follow PostgreSQL's default frame — from
  the partition's first row through the current row's last PEER (rows
  with equal `orderBy` values) — so peers share `rank`, `dense_rank`,
  running aggregates, and `last_value`, as in PostgreSQL. Each output is
  keyed by its input's own key (a topic entity key, or the upstream
  stage's key, whose key space it inherits); paths outside the calls read
  the row's own fields, and a named join's row is readable as in any
  stage. The stage's `when` filters inputs before windowing (SQL's WHERE),
  and required joins decide membership. Deleting an input retracts its
  row and its neighbors recompute; an input whose partition or order
  changes moves. A stage holds one window — for a second, chain another
  window stage.

  **Ties are deterministic.** Where PostgreSQL (like every other SQL
  engine) leaves the order within a tie unspecified, committed decides
  it: `tieBy` orders rows that are equal on `orderBy`, and the input key
  breaks any tie left after that. `tieBy` is required when a stage uses a
  function whose result depends on that order (`row_number`, `lag`,
  `lead`, `first_value`, `last_value`); `rank`, `dense_rank`, and the
  running aggregates treat ties as peers and need none. Other deliberate
  differences from PostgreSQL: `lag`/`lead`'s offset is an integer
  literal from 0 to 1000 (not a per-row expression); running aggregates
  take no `distinct` or `order by` (PostgreSQL implements neither for
  window aggregates) and no `array_agg`/`string_agg` — collapse with an
  aggregate stage; and a window stage does not fan (fan in a prior stage
  keyed by the element, and window its outputs).

  **Frames.** A call can narrow or widen its frame with `over (…)` —
  SQL's `OVER (w frame)`, refining the stage's window (partition and
  order stay the stage's):

  ```toml
  emit = [ { field = "balance", expr = "sum($.amount)" },                                   # default frame
           { field = "ma7",     expr = "round(avg($.reading) over (rows between 6 preceding and current row), 2)" },
           { field = "hour_n",  expr = "count(*) over (range between interval '1 hour' preceding and current row)" },
           { field = "final",   expr = "last_value($.status) over (rows between unbounded preceding and unbounded following)" } ]
  ```

  `rows` counts rows (peers are separate rows, in `tieBy` order);
  `range` measures by the single `orderBy` value — a number with a number
  offset, a date/time with an interval offset (calendar months included)
  — so peers are always in or out together, and a null value's frame is
  its peers. Bounds are `unbounded preceding|following`, `current row`,
  or a constant `n preceding|following` (`rows 3 preceding` is short for
  `between 3 preceding and current row`), with PostgreSQL's rules (a frame
  cannot start after it ends). Frames apply to the aggregates and
  `first_value`/`last_value`; an empty frame gives null (`count` 0). A
  rolling window is anchored to each row's own timestamp, never the
  clock: "the hour before this login", not "the last hour". Not
  supported: `groups` frames and `exclude`.

  **Partition by entity.** Rows are kept in business order, and each row
  stores its running state, so an event appended in order recomputes
  only the rows whose frames (or `lag`/`lead`) reach it — usually one or
  two. A late or backfilled event recomputes the rows after it that
  really change: under a bounded frame, just the rows whose frames hold
  it; under a running total, the rest of the partition. A frame ending
  `unbounded following` makes every row depend on every later one, so
  each change recomputes its whole partition; and a join dimension
  change recomputes the dependent partitions whole. Partition
  by the entity whose history you're computing over (an account, a job),
  not a tenant or the whole stream: the stage warns once when a partition
  passes 50,000 rows, and `windowRowsRecomputed`/`windowLargestPartition`
  (below) show the cost. Top N per group is a window plus a filter — emit
  `row_number()` and keep `$.n <= 3` in a downstream stage's `when`; a row
  that slides out of the top N retracts.
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
  render), `havingRejected` (refolds an aggregate's `having` held back),
  `windowRowsRecomputed`/`windowLargestPartition` (a window stage's
  recompute cost and its largest partition), and per-join resolution counters (`joins`: hits/misses — LOST retractions: nonzero here answers "the delete event
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
- **A store resets when what the stages compute changes, not their
  spelling**: the store fingerprint is the fingerprint of the stages'
  compiled plan, so an edit that computes the same thing (a default
  written out, such as `keyType = ["text"]`; an expression's case,
  spacing, or redundant parentheses; `when` clauses reordered) changes
  nothing, and new vocabulary in a new binary leaves untouched configs'
  stores intact (pinned by reference contract tests). When a store
  genuinely resets (a stage edit, an ownership move, a store format
  change on upgrade), the worker re-derives before consuming and the
  status endpoint says so: `workerState: "re-deriving"` with
  `stageRecovery {folded, target}` progress on the default call — lag
  climbs by design until the fold-only pass reaches the checkpoint.
- **An aggregate's cost per input does not depend on its group's size,
  for most arms.** In an aggregate stage without joins, an input updates
  its key's fold state: `count`, `sum` and `avg` adjust a running total,
  `min` and `max` move the extreme, and `array_agg`, `string_agg`,
  `collect` and any `distinct` insert into an ordered list (so they cost
  in proportion to that list, which a `filter` keeps short). On the
  reference machine
  ([performance.md](operations/performance.md#reference-measurements)) the
  event-store recipe's five-emit aggregate takes about 0.06 ms per
  appended event at both 1,000 and 10,000 events per key. A key recomputes
  from every input it holds — a cost proportional to its group's size —
  when: the input that held a `min` or `max` leaves or moves the wrong way;
  two values tie for an extreme but print differently (a date and its
  midnight timestamp); an `interval` sum changes; or the stage has joins
  (every change refolds the key). Inputs apply in log order, so a large
  recompute holds up the inputs behind it. Group by the entity whose
  history you are aggregating (an order, an account, a device), not a
  tenant or a whole topic, and give the stage a `when` that drops inputs
  no emit reads: a rejected input is not retained, so it costs neither
  refold time nor disk.
- Stage state lives in one database per node under
  `<dataDir>/projections/`, each syncable's state in its own key range —
  derived, node-local, rebuildable from the log. It retains every input of every key, so it grows with the topics
  it reads, in proportion to the number and size of inputs and the
  number of stages that keep them: one aggregate stage over 1,000,000
  events of about 260 bytes held 841 MB. **Editing stage definitions
  requires a rebuild** (the config-change guard enforces it): changed
  stages must re-derive from index 0, exactly like a changed table
  schema.

### Row ownership (`rowOwner`)

When several sources fold one row and any of them is stage-fed, the
table must declare which source owns row existence:

```toml
[[projection.sources]]              # the row owner: admits and removes rows
topic    = "jobs"
keyPath  = "$.id"
rowOwner = true
[[projection.sources.rules]]
set = [ { column = "name", from = "$.name" } ]

[[projection.sources]]              # a decorator: fills its own columns
from = "latest-proposal"           # decorators must be stage-fed
[[projection.sources.rules]]
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
[[projection.sources]]
topic    = "txn"
forEach  = "$.items[*]"            # deliberately multi-valued
keyPath  = "$.sku"                 # resolves against EACH ELEMENT
onDelete = "delete-rows"           # the default: parent delete cascades
  [[projection.sources.rules]]
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
`name`, referenced by enrichments, and the `fields` it stores) instead of
`rules`/`aggregate`; an element field then declares `lookup`/`on`/`select`
instead of `from` (`on` names the plain element field holding the foreign key).
Several enriched fields sharing a dimension coalesce into one join:

```toml
[[projection.sources]]
topic = "person"                       # the dimension topic, keyed by person_id
  [projection.sources.lookup]
  name = "people"                      # referenced by element enrichments below
    [[projection.sources.lookup.fields]]
    field = "name"
    from  = "$.name"

[[projection.sources]]
topic   = "credit"
keyPath = "$.movie_id"
  [projection.sources.aggregate]
  column     = "top_cast"
  elementKey = "$.billing"
    [[projection.sources.aggregate.fields]]
    field = "person_id"                # the foreign key, stored
    from  = "$.person_id"
    [[projection.sources.aggregate.fields]]
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

## Derived topics (loopback)

A `loopback` syncable applies one transform inside the cluster and proposes
the result to a derived topic that downstream syncables consume; see
[loopback.md](loopback.md).

## Consumer stances

How a syncable meets a versioned topic — version-pinned, version-aware, or
always-current — is common to every kind; see
[syncables.md](syncables.md#consumer-stances-how-a-syncable-meets-a-versioned-topic).
## Rehearsing a config before it exists (dry-run)

`POST /v1/syncable/dryrun` takes the same document a syncable POST
takes, folds a bounded sample of the real log through the config's
REAL stage graph into a throwaway store — nothing admitted, nothing
written, the destination database never touched — and returns a
diagnostic report in seconds: per-stage `inputs`/`fanned`/`keys`/
`unkeyedDeletes`, per-join resolution hits/misses, a few sample output
objects per stage (value-shaped bugs — null columns, wrong paths — are
invisible in counts), per-source and per-rule match counts, and
auto-generated **findings** that name the silent-empty signatures: a
`when` clause that never matched reports the value FAMILIES actually
seen at its path ("`equals = 'true'` (string) never matched; values
seen: boolean×142"), a join that never resolved says so, a fan that
never fanned points at serialized-JSON columns. The sampling budget
(`?maxEntries`, default 100k, cap 10M) spreads across evenly-spaced
windows of the log so late-clustering topics are covered; `?fromIndex`
drills into one region, and `?timeoutSeconds` (default 120, max 600)
buys a coverage-hungry run its time. The deadline — or a log read
failure — yields a PARTIAL report (`truncated` says why), never an
error, and every report carries phase timings (`parseMs`, `readMs`,
per-window `ms`) so a slow run names its slow phase. Data mirrors the
live worker: an undecodable entity or an emit data error DEAD-LETTERS
and the rehearsal continues (`deadLetters` in the report — the same
health number replay verdicts lead with); a fold failure that would
STICK the live worker is reported as the truncation reason. Coverage
is stated precisely — consumed topics the windows never reached are
named in one aggregate finding, listed after any real signatures. The
config still passes full admission validation (400 with the parser's
actual words), so a dry run also answers "would this admit?". Note:
findings and dead-letter samples can quote SOURCE DATA VALUES verbatim
(an expression error names the string it choked on) — that precision
is the diagnostic point and stays within the trusted-appliance model,
but treat pasted dry-run output like any other data-bearing log
excerpt.
Authoring loop: dry-run until the findings list is empty, then POST
for real — "valid config, wrong result, no error" costs minutes
instead of a full replay against an oracle.

## Restatements

A restatement rebinds how already-committed entities are read, and marks
every consumer of the topic `interpretationStale`; see
[types.md](types.md#restatements-changing-how-history-is-read).

## Changing the rules after a projection is live

A projection is a **disposable view of an immutable log** — its fold rules, and
the type migration that feeds it, are *derivation logic*, not data. When you
change that logic, committed applies the new logic to Actuals it processes *from
that point on*; it does **not** retroactively re-render rows already written to
the destination. Correcting history is a deliberate rebuild, and running it is your
responsibility.

A plain keyed `sql` mirror can instead be re-materialized in place; see
[syncables.md](syncables.md#re-materializing-a-keyed-mirror-in-place).
Projections refuse that verb.

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

**Fixing a type's migration** changes only future reads on every projection of
the topic; rebuilding the dependents is yours. See
[types.md](types.md#versions-and-migrations).

## Checkpoint cadence and replay throughput

See [syncables.md](syncables.md#checkpoint-cadence).

## Destination limits: when a row cannot fit the destination

Every SQL engine has physical limits on its tables, and a projection can hit
them two ways — at **table-creation time** (loud, immediate, nothing synced
yet) or at **apply time** (a specific row fails while its neighbors succeed).
Survey your widest tables against these before creating mirrors:

- **PostgreSQL**: a row must fit an 8 KB heap page after compression/TOAST —
  very wide tables (many hundreds of columns of inline data) can produce
  individual rows that exceed it at apply time ("row is too big"). And a
  text value with an **embedded U+0000** is rejected at apply time
  (SQLSTATE 22021) even though MySQL and SQL Server store it — a row that
  flowed through every other engine dead-letters only at a Postgres destination. The
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
the destination keeps flowing. PostgreSQL reports its row-size wall as a program
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
already corrected the destination row, replaying the stale proposal would regress
it — acknowledge the record instead
(`POST /syncable/{id}/deadletter/{index}/acknowledge`; see the triage flow
in [operations/stuck-syncables.md](operations/stuck-syncables.md)). A
wedged worker needs no replay — it resumes by itself once the destination
accepts the row.

## See also

- [Quickstart](quickstart.md) — a working four-topic `movie_card` read model
  (spine + contributor + aggregate + lookup) end to end.
- [docs/architecture/event-log-architecture.md](architecture/event-log-architecture.md) — why the log is
  the source of truth and the read model is disposable.
- [docs/operations/rebuild.md](operations/rebuild.md) and
  [docs/operations/stuck-syncables.md](operations/stuck-syncables.md) —
  rebuilding a projection and recovering a wedged one.
