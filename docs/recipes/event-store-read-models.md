# Read models from your event store

Keep your event store. If your events already live in a SQL table (a
homegrown events or outbox table, or any event store built on Postgres,
MySQL, or SQL Server), Committed can build the read models from it. Nothing
on the write side changes: your application keeps appending events exactly
as it does now.

The trial is one ingestable, one projection, and one read model. If it isn't
better than what you have, delete them and the source is as you found it.

## The events table

This recipe uses an events table of the usual shape: one row per event, a
global sequence, and a per-stream version.

```sql
CREATE TABLE events (
  seq_id    bigserial   PRIMARY KEY,
  stream_id uuid        NOT NULL,
  version   int         NOT NULL,
  type      text        NOT NULL,
  data      jsonb       NOT NULL,
  timestamp timestamptz NOT NULL DEFAULT now(),
  UNIQUE (stream_id, version)
);
```

The streams are orders, with four event types: `order_placed`
(`{"customer": "ada"}`), `item_added` (`{"sku": "pen", "qty": 2, "price":
1.50}`), `order_shipped`, and `order_cancelled`. Your column names will
differ; only the paths in the configs below change.

## 1. Ingest the events table

The source needs logical replication, and Committed needs a role that may
use it; see the [PostgreSQL prerequisites](../ingestables.md#prerequisites-operator)
(or the [MySQL](../ingestables.md#mysql) and SQL Server sections).

```toml
# POST /v1/type/order-events
[type]
name          = "order-events"
entityKind    = "event"
discriminator = "$.type"
```

```toml
# POST /v1/ingestable/order-events
[ingestable]
name = "order-events"
type = "sql"

[sql]
dialect          = "postgres"
topic            = "order-events"
connectionString = "postgres://committed:${APP_DB_PASSWORD}@db:5432/app?sslmode=disable"
primaryKey       = "seq_id"
tables           = ["events"]
mapAllColumns    = true

[sql.options]
slotName    = "committed_order_events"
publication = "committed_order_events"
```

The ingestable snapshots the existing events, then streams every new one as
it commits. Each event becomes one entity in the `order-events` topic, keyed
by `seq_id`, with `data` decoded as JSON.

- **JSON in a text column.** On MySQL or SQL Server the payload column is
  often `TEXT` or `NVARCHAR`; list it under `jsonColumns` so projections can
  read into it. See [`jsonColumns`](../ingestables.md#configuration).
- **Partitioned tables.** Watch the partitioned root and set `primaryKey` to
  the table's full primary key; see
  [Partitioned tables](../ingestables.md#partitioned-tables). A table
  partitioned on a boolean column (for example, active and archived events)
  is not supported yet.

## 2. Build the read model

One row per order, in your own database:

```toml
# POST /v1/database/read
[database]
name = "read"
type = "sql"

[sql]
dialect          = "postgres"
connectionString = "postgres://committed:${APP_DB_PASSWORD}@db:5432/app?search_path=read&sslmode=disable"
```

```toml
# POST /v1/syncable/orders
[syncable]
name = "orders"
type = "projection"

[projection]
db         = "read"
table      = "orders"
primaryKey = "order_id"

[[projection.columns]]
name = "order_id"
type = "UUID"
[[projection.columns]]
name = "customer"
type = "TEXT"
[[projection.columns]]
name = "status"
type = "TEXT"
[[projection.columns]]
name = "items"
type = "INT"
[[projection.columns]]
name = "total"
type = "NUMERIC(12,2)"
[[projection.columns]]
name = "version"
type = "INT"

# One key per stream: the row is recomputed from all of the stream's events.
[[projection.stages]]
name    = "order"
from    = "order-events"
keyPath = "$.stream_id"
reduce  = "aggregate"
emit = [
  { field = "customer", expr = "(array_agg($.data.customer) filter (where $.type = 'order_placed'))[1]" },
  { field = "status",   expr = "(array_agg($.type order by $.version desc) filter (where $.type in ('order_placed', 'order_shipped', 'order_cancelled')))[1]" },
  { field = "items",    expr = "coalesce(sum($.data.qty) filter (where $.type = 'item_added'), 0)" },
  { field = "total",    expr = "coalesce(sum($.data.qty * $.data.price) filter (where $.type = 'item_added'), 0)" },
  { field = "version",  expr = "max($.version)" },
]

[[projection.sources]]
from = "order"
[[projection.sources.rules]]
set = [
  { column = "customer", from = "$.customer" },
  { column = "status",   from = "$.status" },
  { column = "items",    from = "$.items" },
  { column = "total",    from = "$.total" },
  { column = "version",  from = "$.version" },
]
```

```
               order_id               | customer |     status      | items | total | version
--------------------------------------+----------+-----------------+-------+-------+---------
 11111111-1111-1111-1111-111111111111 | ada      | order_shipped   |     3 |  7.25 |       4
 22222222-2222-2222-2222-222222222222 | grace    | order_placed    |     3 |  6.00 |       2
```

The stage groups events by stream, like SQL's `GROUP BY stream_id`, and each
`emit` is an aggregate over the stream's events. Two properties make this the
right shape for an event store:

- **Your stream's order, not arrival order.** The latest status is the event
  with the highest `version`, whatever order the events reached the log in.
  A snapshot, a replay, and live streaming all produce the same row.
- **Deleting an event recomputes its stream.** The stage keeps every event of
  a stream and recomputes the row from them, so an event deleted at the
  source leaves the totals and the status as if it had never been appended.

The cost of the second property is disk: the stage keeps each stream's
events on the node, under the data directory. The expression language
follows PostgreSQL; see [Staged computation](../read-models.md#staged-computation-internal-stages)
for everything a stage can do, including joins to other topics and window
functions over a stream's history.

## 3. Rehearse a change, then roll it out without downtime

Before posting a new or changed projection, dry-run it against the log:

```sh
curl -X POST -H 'Content-Type: text/toml' --data-binary @orders.toml \
  localhost:8080/v1/syncable/dryrun
```

The report shows sample rows for each stage and flags configs that would
produce nothing. See [dry-run](../read-models.md#rehearsing-a-config-before-it-exists-dry-run).

To change a live read model, post the new version as a second projection
with its own table (`orders_v2`). It builds from the start of the log while
`orders` keeps serving; when it has caught up, switch your readers to it and
delete the old syncable. Your event store is never read again for a rebuild:
the log already holds every event. See
[Changing the rules after a projection is live](../read-models.md#changing-the-rules-after-a-projection-is-live).

## 4. Know when it stops

A projection never falls silently behind. A row the destination keeps
refusing stops it, and its status says `stuck`; a row that can never be
written is set aside as a dead letter and counted:

```sh
curl localhost:8080/v1/syncable/orders/status
# {"stuck":false,"workerState":"running","lag":0,"caughtUp":true,"deadLetters":0,…}
```

Alert on the `committed.sync.stuck` and `committed.sync.lag` metrics; see
[Stuck syncables](../operations/stuck-syncables.md) for clearing one.

## If your store compacts streams

Some event stores can compact a long stream: they delete its earlier events
and keep one snapshot event that carries the stream's state at that point.
Each deleted event reaches the log as a delete, so the stage recomputes the
stream from what is left: the snapshot event and anything appended after it.
A projection that only knows the ordinary event types sees none of them and
blanks the row.

Read the snapshot event as the starting point and add the later events to it.
With a snapshot event of type `order_snapshot` whose `data` holds `customer`,
`status`, `items`, and `total`, the stage's emits become:

```toml
emit = [
  { field = "customer", expr = "coalesce((array_agg($.data.customer) filter (where $.type = 'order_placed'))[1], (array_agg($.data.customer) filter (where $.type = 'order_snapshot'))[1])" },
  { field = "status",   expr = "(array_agg(case when $.type = 'order_snapshot' then $.data.status else $.type end order by $.version desc) filter (where $.type in ('order_placed', 'order_shipped', 'order_cancelled', 'order_snapshot')))[1]" },
  { field = "items",    expr = "coalesce(sum($.data.items) filter (where $.type = 'order_snapshot'), 0) + coalesce(sum($.data.qty) filter (where $.type = 'item_added'), 0)" },
  { field = "total",    expr = "coalesce(sum($.data.total) filter (where $.type = 'order_snapshot'), 0) + coalesce(sum($.data.qty * $.data.price) filter (where $.type = 'item_added'), 0)" },
  { field = "version",  expr = "max($.version)" },
]
```

The row is the same before and after a compaction, and events appended after
it add to the snapshot's totals. Deploy these emits before the store first
compacts a stream.

## Erasure

Deleting a subject's events from your events table is still yours to do.
Each delete reaches the log as a tombstone, every read model built from the
topic recomputes without the event, and the scrub removes the event from the
log. See [Right-to-be-forgotten erasure](../operations/rtbf.md).

## Where this fits

- **Not a fit yet:** event stores that are not SQL tables (Kurrent, Axon
  Server), projections that call out to arbitrary code, and inline
  projections that must update in the same transaction as the append.
- **Several read models from one topic.** Every projection of
  `order-events` reads the same log, so adding a read model never touches
  the events table again.
