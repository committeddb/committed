# CDC setup: Postgres and MySQL

This guide is for operators standing up **change data capture** — ingesting a
SQL database's inserts, updates, and deletes into a committed topic. It covers
what the source database needs, what committed sets up for you, the
snapshot→streaming lifecycle, what to watch, and how to fix the common failures.
Both supported engines are here: PostgreSQL (logical replication) and MySQL
(binlog).

For the end-to-end walkthrough (source → ingest → topic → projection → query),
see the [quickstart](../quickstart.md); this guide is the reference for the
ingest half. For the output half (projecting a topic to a SQL table), see the
README § SQL projections.

## How ingest works (both engines)

An `ingestable` watches one or more source tables and turns every committed row
change into a proposal on a topic. It runs in two phases:

1. **Snapshot.** On first start the worker reads the current contents of each
   watched table in primary-key order, in bounded batches (keyset pagination),
   and proposes each row. It records a per-table cursor as it goes, so a restart
   mid-snapshot resumes where it left off rather than starting over.
2. **Streaming.** Once the snapshot completes, the worker follows the database's
   change stream (Postgres logical replication / MySQL binlog) from the position
   it captured at the start of the snapshot, proposing each insert, update, and
   delete as it commits at the source.

A source `DELETE` becomes a **keyed tombstone** (a delete entity with no
payload), not an upsert of the old row — that is what makes right-to-be-forgotten
flow all the way through to downstream projections. For that to work the change
stream has to carry the row's key on a delete; the per-engine sections below say
how to guarantee that, and committed's **preflight** check refuses to start an
ingestable whose source can't, so it fails loudly at config time instead of
silently dropping deletes.

Ingest is **effectively-once**: committed checkpoints its stream position into
its own log, and on restart it resumes from that checkpoint and de-duplicates any
re-delivered changes by source sequence. You do not get duplicates in the topic
across a restart.

### One writer per topic

A topic is reconciled against a **single producer**, so it must have exactly one:

- **One ingestable per topic — enforced.** Creating a second ingestable on a
  topic another ingestable already produces is rejected at config time (`POST
  /v1/ingestable` returns `400`, naming the topic and the ingestable that already
  owns it). The two would reconcile the topic independently, and one's
  reconciliation would delete the rows the other produced. If you need to move a
  topic to a different ingestable, delete the old one first.

- **No direct writes into an ingest-fed topic — unsupported (not blocked).** A
  topic fed by an ingestable should not also receive direct `POST /v1/proposal`
  writes. Such rows don't come from the source, so a reconciliation never accounts
  for them: they are never removed even when they should be, and a manual row whose
  key collides with a source row is overwritten by ingest. This is *not* enforced —
  a proposal is data-plane traffic, not config — so it's on you to keep an
  ingest-fed topic ingest-only. If you must hand-seed a topic before ingesting into
  it, load it before the ingestable starts, and note the seeded rows stay outside
  reconciliation.

### Snapshot consistency (the convergent contract)

The snapshot is **not** a single point-in-time read. To keep its load on the
source bounded (short per-batch transactions, no long-held read view or table
lock) and to stay resumable mid-snapshot, committed reads each batch in its own
transaction **while the source keeps changing**. Correctness comes from the
change stream, not from freezing the source: streaming begins at a position
captured *before* the first snapshot read, so every change that races the
snapshot is also replayed from the stream. Because an upsert is keyed and a
delete is a keyed tombstone, re-applying a change is idempotent and the last
write wins — so consumers **converge to the exact current source state**,
including rows inserted, updated, or deleted during the snapshot.

The visible cost is a brief transient: a row changed during the snapshot can
appear at an intermediate value until the stream replays its latest change, then
settles correct. For committed's eventually-consistent read models this is
expected. Both engines use this same convergent model — Postgres does not use an
exported snapshot, MySQL does not hold a consistent-snapshot transaction.

### Reconciling refresh: how a re-snapshot removes rows deleted at the source

Several recovery paths re-run a **full snapshot** against a topic that already has
rows downstream: rebuilding an ingestable, the Postgres lost-slot recovery
(above), or deleting an ingestable and recreating it on the same topic. A snapshot
is **upsert-only** — it enumerates the rows that *exist* in the source, so it has
no way to emit a delete for a row removed while it wasn't watching. committed
reconciles those deletions with a **generation watermark** instead of a diff — on
sinks that can apply it. **Keyed** SQL sinks (and HTTP receivers that honor
`op:"refresh"`) reconcile automatically; **keyless/append and projection sinks
cannot sweep and are NOT reconciled** — see [Sinks that don't
reconcile](#sinks-that-dont-reconcile) below.

Each keyed SQL sink carries one committed-managed column, `committed_generation`.
Every ingest snapshot runs at a generation `G` (a per-topic number that increases
by one on each full refresh), stamps every row it emits with `G`, and closes with
a one-entity **refresh-boundary marker** carrying `G`. The syncable applies the
stream in commit order: each upsert writes the row *and* its generation; the
marker runs

```
DELETE FROM <sink> WHERE committed_generation >= 1 AND committed_generation < G
```

This is deletion-by-omission: a full refresh re-stamps every surviving row at `G`,
so anything the sink is still holding *below* `G` was not re-emitted — it no longer
exists in the source — and the sweep removes it. (The `>= 1` floor spares
generation-0 rows: direct `POST /v1/proposal` writes committed does not own.)

Worked example — a row deleted at the source between two refreshes. The topic is
snapshotted at generation 1 (rows 1, 2, 3); row 2 is later deleted at the source
while nothing is watching; then the topic is refreshed again at generation 2:

```
gen-1 refresh:  upsert 1,2,3 @g1 ; marker g1 → sweep <1 (no-op)
                sink: 1→g1, 2→g1, 3→g1
(row 2 deleted at the source; nothing observes it)
gen-2 refresh:  upsert 1,3   @g2 ; marker g2 → sweep <2
                sink: 1→g2, 3→g2         (row 2, still g1, is swept)
```

Two consequences worth internalizing:

- **The delete is a sink-side `DELETE`, never a log entry.** The commit log holds
  no delete for row 2 — it only ever recorded upserts. Row 2 leaves the sink
  because the marker's sweep removes what the refresh did not re-stamp.
- **No duplication despite re-emitting every row.** The log is append-only, so the
  re-snapshot appends a second copy of the surviving rows — but the sink is keyed,
  so re-upserting rows 1 and 3 overwrites them in place (`g1 → g2`), not adds them.

The watermark only holds if each refresh's `G` is **strictly above every
generation already on the sink**. committed keeps a delete-surviving, per-topic
generation high-water mark for exactly this, so a delete-and-recreate on the same
topic resumes *above* the rows the sink still holds instead of restarting at 1 and
sweeping nothing. It is also why a topic may have only one ingestable ([above](#one-writer-per-topic)):
two producers would stamp generations independently and sweep each other's rows.

For a **keyed SQL** syncable committed runs this sweep for you (`DELETE WHERE
generation < G`). For an **HTTP** syncable the same reconciliation is delivered
to your endpoint as an `op:"refresh"` carrying `G`, and the receiver runs the
sweep — see [writing a webhook receiver](../webhook-receiver.md).

#### Sinks that don't reconcile

A refresh boundary is a **no-op** for two sink shapes, because a generation sweep
has nothing to act on:

- **Keyless/append (history) tables** have no current-row identity — they record
  one row per event. A source-side delete lost in the gap was never captured, so
  it is simply **absent from the history**. A rebuild reconstructs the captured
  events; it cannot recover the uncaptured delete (this is the documented
  downtime-beyond-retention limitation, not a bug).
- **Projections** fan one source entity out to many/aggregated rows, so a
  topic-level sweep doesn't map onto their shape. After a gap, rows the source
  deleted **remain in the projection**, and — unlike a keyed sink — **a rebuild
  does NOT fix it** (the delete was never in the log, and the marker no-ops on
  replay too). Until projection reconciliation is implemented, recovery is
  **manual** (correct the stale rows, or re-derive the projection from a keyed
  sink that did reconcile).

Both cases log a `WARN` when a re-snapshot boundary reaches them (generation > 1).
For a **Postgres** source this log is the *only* signal — `reSnapshotRequired`
stays `false` on Postgres because the dialect auto-re-snapshots, which reconciles
*keyed* sinks but leaves these two shapes silently affected. Watch for that WARN
if you fan a Postgres topic into a projection or history table.

> **Compliance (RTBF/GDPR).** A source-side *erasure* — a subject deleted at the
> source for right-to-be-forgotten — lost in the gap is exactly what a keyed
> sink's sweep removes. On keyless/projection sinks it is **retained**: the
> subject's PII lingers with no delete. committed's own RTBF path (a delete
> proposal + event-log scrub) still erases these sinks when the erasure goes
> *through* committed; the exposure is specifically a source-side erasure
> committed never captured. Treat a re-snapshot `WARN` on a PII-bearing
> keyless/projection sink as a **manual-erasure** action item, not just stale data.

### What to watch

Every ingestable exposes its status:

```
GET /v1/ingestable/{id}/status
```

```json
{
  "phase": "streaming",
  "snapshotProgress": [{ "table": "ingress.movie", "complete": true }],
  "position": "0/1A2B3C8",
  "lag": 4096,
  "caughtUp": true,
  "reSnapshotRequired": false
}
```

- **`phase`** — `"snapshot"` while dumping existing rows, `"streaming"` once it is
  following the change stream.
- **`snapshotProgress`** — per watched table: the last key dumped and whether that
  table's snapshot is complete.
- **`position`** — the engine-native cursor: a Postgres LSN (`0/1A2B3C8`) or a
  MySQL binlog coordinate (`binlog.000007:4096`).
- **`lag`** — how far the source write head is ahead of what this ingest has
  durably consumed, in the engine's natural unit: **Postgres bytes**
  (`pg_current_wal_lsn − confirmed_flush_lsn`), **MySQL transactions** under GTID
  positioning (`@@gtid_executed − consumed`). `null` during snapshot, when the
  source is unreachable, on a MySQL source without GTID positioning, or when a
  re-snapshot is required.
- **`caughtUp`** — `true` only when the snapshot is complete **and** lag is a
  known `0`. Never `true` while `lag` is `null`.
- **`reSnapshotRequired`** — `true` when the source discarded change data this
  ingest never consumed and can never re-stream (MySQL: binlogs purged past the
  consumed GTID set). A distinct, loud state, not a lag number; recovery is a
  fresh snapshot. Always `false` for Postgres — not because a slot can't lose WAL
  (a reaped or dropped slot does), but because the dialect recovers in-band: it
  re-snapshots from the new slot's consistent point and sweeps the rows deleted
  in the lost window off **keyed** sinks, so for them the gap is reconciled rather
  than surfaced. Keyless/append and projection consumers of a Postgres topic are
  neither reconciled nor flagged here — only the sink-side `WARN` signals them
  (see [Sinks that don't reconcile](#sinks-that-dont-reconcile)).

The quickstart polls this endpoint to know when the initial snapshot has landed
(`"caughtUp": true`).

To answer "is my data showing up downstream?" in one call, ask for the whole
pipeline for a topic. It stitches the ingestable feeding the topic to every
syncable consuming it, so you don't have to call both endpoints and line them up
by hand. Pass only the topic (the type id):

```
GET /v1/type/{topic}/pipeline
```

```json
{
  "topic": "movie",
  "headIndex": 12044,
  "ingestable": "movie-ingest",
  "ingest": { "phase": "streaming", "lag": 0, "caughtUp": true },
  "syncables": [
    { "id": "movie-card", "checkpointIndex": 12044, "lag": 0, "caughtUp": true }
  ],
  "caughtUp": true
}
```

committed resolves the linkage server-side and reports the same numbers as the
per-resource endpoints. Top-level `caughtUp` is true only when every stage — the
producer (if any) and every consumer — is caught up.

A few edge cases:

- A topic fed by direct proposals has no producer, so the `ingestable` and
  `ingest` fields are simply absent.
- If a producer exists but its worker isn't running on the node that answered,
  the producer is still named, with an `ingestError` instead of being dropped.
- For alerting, the same lag is exported as metrics: `committed.sync.lag` and
  `committed.ingest.lag`.

---

## PostgreSQL

committed ingests Postgres via **logical replication** through the `pgoutput`
plugin.

### Prerequisites (operator)

1. **`wal_level = logical`.** This requires a server restart. Without it, the
   replication slot can't be created and ingest fails to start.

   ```ini
   # postgresql.conf
   wal_level = logical
   max_replication_slots = 10   # ≥ the number of ingestables you'll run
   max_wal_senders       = 10   # ≥ the number of concurrent slots
   ```

2. **A role with `REPLICATION`** that can also create a publication. committed
   creates the publication for you (see below), and `CREATE PUBLICATION` requires
   the role to **own the watched tables** (or be a superuser).

   ```sql
   CREATE ROLE committed WITH LOGIN REPLICATION PASSWORD '…';
   GRANT SELECT ON ALL TABLES IN SCHEMA ingress TO committed;
   -- and table ownership (or superuser) so committed can CREATE PUBLICATION
   ```

3. **`REPLICA IDENTITY` that carries the key.** On a `DELETE`, Postgres only puts
   the columns named by the table's replica identity into the change stream.
   committed needs the configured `primaryKey` to be among them so it can emit a
   keyed tombstone. This is **not** "always use FULL":

   | REPLICA IDENTITY | carries on DELETE | works for committed |
   |---|---|---|
   | `DEFAULT` (the default) | the table's PRIMARY KEY | ✅ **if** the PK covers your `primaryKey` |
   | `FULL` | every column | ✅ always |
   | `USING INDEX i` | that index's columns | ✅ if it covers your `primaryKey` |
   | `NOTHING` | nothing | ❌ |

   If your `primaryKey` is the table's real primary key, the default is fine and
   you need no `ALTER`. Otherwise set `REPLICA IDENTITY FULL`:

   ```sql
   ALTER TABLE ingress.movie REPLICA IDENTITY FULL;
   ```

   committed's preflight reads `pg_class.relreplident` for each watched table and
   refuses to start if the key isn't covered, naming the table and the fix.

### What committed creates for you

On its first streaming connection committed runs, idempotently:

- `CREATE PUBLICATION <publication> FOR TABLE <tables>` — only the watched tables
  are in the publication, so the ingest never sees writes to other tables
  (including a downstream projection's own sink table).
- the logical replication **slot** (`pgoutput`).

You don't create either by hand.

### TRUNCATE is not propagated (caveat)

committed replicates `INSERT`, `UPDATE`, and `DELETE`, but **not `TRUNCATE`**. It
has no "clear-all" primitive yet, so a `TRUNCATE` on a watched table empties the
source but leaves the sink's rows in place — the sink **diverges** from the source
until you reconcile it. committed does not swallow this silently: each dropped
truncate is logged at `Warn`, naming the tables, so you can alert on it:

```
TRUNCATE on a watched table is not propagated to the sink; the sink now
diverges from the source and must be re-snapshotted to reconcile   tables=[public.movie]
```

To reconcile after a truncate, **re-snapshot** the ingestable (rebuild it from
zero — see [rebuild.md](rebuild.md)). To avoid the divergence entirely, prefer
`DELETE FROM <table>` over `TRUNCATE` on watched tables: each row delete
replicates as a keyed tombstone and clears the sink row-by-row.

Full truncate propagation is planned (a clear-all signal applied downstream as
`DELETE FROM <sink>`); until then this caveat stands.

### Configuration

```toml
[ingestable]
name = "movie-ingest"
type = "sql"

[sql]
dialect          = "postgres"
topic            = "movie"
connectionString = "postgres://committed:…@db:5432/catalog?sslmode=disable"
primaryKey       = "movie_id"
tables           = ["ingress.movie"]   # schema-qualified
mapAllColumns    = true                # mirror every column 1:1

[sql.postgres]
slot_name   = "committed_movie_slot"   # optional; default "committed_slot"
publication = "committed_movie_pub"    # optional; default "committed_pub"
```

Give each ingestable its own `slot_name` and `publication` so they don't collide.
A runnable, end-to-end Postgres example lives in
[`examples/movies/`](../../examples/movies/) (`source.sql`, `ingest-*.toml`,
`compose.yml`).

### Lag and the slot's disk cost

For Postgres, `lag` is real: committed reads it in bytes from
`pg_replication_slots`, so `caughtUp` becomes `true` once the ingest has drained
the slot.

The flip side: **a replication slot retains WAL until its consumer acknowledges
it.** While committed is running this is bounded (it acks continuously), and
deleting an ingestable through the API drops its slot and publication for you —
best-effort, on the owning node, via the ingestable's teardown. The risk is a
**hard-stopped** committed: it acks nothing and never runs teardown, so its slot
keeps pinning WAL on the source and the source's disk grows without bound until
it fills. Two consequences:

- Don't leave a stopped committed pointed at a production database for long.
- Drop a slot **manually only as a fallback** — if committed was hard-stopped
  before you deleted the ingestable, or its teardown logged a failure (a wedged
  worker). A normal `DELETE /ingestable` has already dropped it:

  ```sql
  SELECT pg_drop_replication_slot('committed_movie_slot');  -- errors harmlessly if already gone
  DROP PUBLICATION committed_movie_pub;   -- optional cleanup
  ```

### Postgres troubleshooting

- **Ingest won't start, "replica identity" in the error.** A watched table's
  REPLICA IDENTITY doesn't carry your `primaryKey` — `ALTER TABLE … REPLICA
  IDENTITY FULL` or point `primaryKey` at the real PK.
- **"permission denied" creating the publication.** The role doesn't own the
  tables. Grant ownership or use a superuser for setup.
- **Source disk filling up.** A slot with no live consumer (committed stopped) is
  pinning WAL — restart committed or drop the slot.
- **`lag` not dropping to 0 on an idle source.** With no new writes the slot's
  `confirmed_flush_lsn` doesn't advance; this is normal. `caughtUp` reflects the
  last known position.

---

## MySQL

committed ingests MySQL via the **binary log** (row-based replication, the same
stream a replica reads).

### Prerequisites (operator)

1. **Binlog enabled, row format, full row image, full row metadata.** committed's
   MySQL CDC requires **MySQL 8.0.1+** (or MariaDB 10.5+) and the settings below. A
   stock MySQL 8/9 already has `log_bin` on, `binlog_format=ROW`,
   `binlog_row_image=FULL`, and a non-zero `server_id` — so in practice only
   `binlog_row_metadata` (which defaults to `MINIMAL`) needs changing.

   ```ini
   # my.cnf
   log_bin             = ON
   binlog_format       = ROW
   binlog_row_image    = FULL      # committed rejects MINIMAL and NOBLOB
   binlog_row_metadata = FULL      # MySQL 8.0.1+; default is MINIMAL
   server_id           = 1         # any unique non-zero id
   ```

   Preflight reads `@@global.binlog_row_image` and `@@global.binlog_row_metadata`
   and refuses to start unless **both are `FULL`**:

   - **`binlog_row_image=FULL`** — `MINIMAL` and `NOBLOB` omit unchanged columns
     from the UPDATE after-image, so a partial `UPDATE` would silently null those
     columns in the mirror. `FULL` always ships the complete before/after image
     (which also carries the key for a keyed `DELETE` tombstone).
   - **`binlog_row_metadata=FULL`** — a binlog row image is *positional* (values
     only, no column names). committed decodes each row against the column names
     and ENUM/SET labels carried in that row's own binlog `TableMapEvent` — the
     schema *as of the write* — so an online `ALTER` on the source cannot mis-join
     a still-replaying old row against the post-change columns. MySQL writes those
     names/labels into the event only under `FULL`; the default `MINIMAL` omits
     them. This is the same setting Debezium and other CDC tools require. It is a
     dynamic global (`SET GLOBAL binlog_row_metadata = 'FULL'` — no restart;
     persist it in `my.cnf`), and it exists only on MySQL 8.0.1+ / MariaDB 10.5+,
     which is therefore the minimum source version for committed's MySQL CDC.

2. **GTID mode on (strongly recommended).** With `gtid_mode=ON` committed resumes
   the binlog by **GTID set** rather than file:offset, which is what makes resume
   survive a source failover (a replica promoted to primary, where the file
   name and offset are server-local and meaningless on the new primary) and
   what lets it report a real transaction-count `lag` and `caughtUp`.

   ```ini
   # my.cnf
   gtid_mode                 = ON
   enforce_gtid_consistency  = ON   # required to enable gtid_mode
   ```

   It is **not required**: with `gtid_mode=OFF` committed falls back to file:offset
   positioning (the pre-0.7 behavior — single-server only, `lag`/`caughtUp` stay
   `null`). Preflight does not fail on this; it logs a warning so the degraded mode
   is visible rather than silent. Default MySQL ships `gtid_mode=OFF`, so set this
   explicitly for any production / failover-capable deployment.

3. **A replication grant.** The ingest user needs to read rows (snapshot), briefly
   lock to capture a consistent position, and stream the binlog:

   ```sql
   CREATE USER 'committed'@'%' IDENTIFIED BY '…';
   GRANT SELECT, RELOAD, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'committed'@'%';
   ```

   (`RELOAD` is for the brief `FLUSH TABLES WITH READ LOCK` at snapshot start;
   `REPLICATION SLAVE`/`CLIENT` are for the binlog stream; `SELECT` is for the
   snapshot and primary-key introspection.)

There is **no publication or slot** to manage on MySQL — committed connects as a
binlog replica using its own replica id.

### Unsupported column types

committed does **not** support MySQL **spatial** columns (`GEOMETRY` and its
subtypes: `POINT`, `LINESTRING`, `POLYGON`, `MULTIPOINT`, `MULTILINESTRING`,
`MULTIPOLYGON`, `GEOMETRYCOLLECTION`) or **`VECTOR`** (MySQL 9.0+). MySQL ships
these as raw bytes on both the binlog and snapshot paths, and they have no
lossless JSON representation — so rather than silently corrupt them, **preflight
rejects a config that maps one**, naming the column. Leave the column out of your
mappings (or `excludeColumns` it under map-all) to ingest the rest of the table;
its data is then simply not replicated.

Every other MySQL type is supported: numbers, `DECIMAL` (exact), `BIT`, `DATE`/
`TIME`/`DATETIME`/`TIMESTAMP`, `CHAR`/`VARCHAR`/`TEXT`/`ENUM`/`SET`, `JSON`, and
binary (`BLOB`/`BINARY`/`VARBINARY`, emitted as base64). (Postgres has no such
gap — PostGIS `geometry`/`geography` and `pgvector` come through as their
lossless `::text` form.)

### Configuration

```toml
[ingestable]
name = "widget-ingest"
type = "sql"

[sql]
dialect          = "mysql"
topic            = "widget"
connectionString = "mysql://committed:…@db:3306/shop"
primaryKey       = "wid"
tables           = ["widget"]

[[sql.mappings]]
jsonName = "wid"
column   = "wid"

[[sql.mappings]]
jsonName = "name"
column   = "name"
```

Note the connection string uses the `mysql://user:pass@host:port/db` URL form, and
there is no `[sql.mysql]` subsection — MySQL has nothing analogous to a slot or
publication to name. (`mapAllColumns = true` works here too, in place of the
explicit `[[sql.mappings]]` blocks.)

A complete worked MySQL setup — source DDL, the grant, an ingestable, and a
syncable projecting back into a MySQL sink table — is exercised end-to-end by the
`e2e/cdc` MySQL tests (`e2e/cdc/harness/mysql.go`, `e2e/cdc/mysql_test.go`); the
DDL and TOML there are copy-pasteable.

### MySQL lag, caughtUp, and the binlog-retention caveat

With `gtid_mode=ON`, committed reports a real `lag` and `caughtUp` for MySQL,
computed by GTID-set arithmetic: it diffs the consumed GTID set against the
source's `@@gtid_executed`. `lag` is the **number of transactions** the source is
ahead (not bytes — the units differ by engine), and `caughtUp` is `true` once the
consumed set covers `@@gtid_executed`. With `gtid_mode=OFF` there is no global
head to diff against, so MySQL `lag` stays `null` and `caughtUp` stays `false`
(use `phase == "streaming"` plus `snapshotProgress[*].complete` to know the
snapshot is done) — another reason to enable GTID mode.

**The retention caveat (the one property a slot gives that a binlog can't).** A
Postgres slot *holds* the WAL until committed acknowledges it; a MySQL binlog
dump holds nothing, so the source purges binlogs on its own schedule
(`binlog_expire_logs_seconds`). In steady state this is fine — committed drains
the binlog into its own log continuously, even while lagging. The exposure is
only **downtime longer than retention**: if committed is stopped long enough that
the source purges transactions committed never consumed, those changes are gone
from the source and can't be streamed. committed does **not** lose them silently —
it detects the hole (binlog error 1236 / `@@gtid_purged` ⊄ consumed), surfaces it
as **`reSnapshotRequired: true`** on the status endpoint, and recovers by
re-running the initial snapshot (the data re-applies idempotently). Preflight also
**warns** at config time when `binlog_expire_logs_seconds` is short. The contract:
**steady-state parity with Postgres; the weaker guarantee is only downtime beyond
retention, and it is detected loudly, never lost silently.** Keep retention longer
than your worst-case committed downtime (or set `binlog_expire_logs_seconds=0` to
never auto-purge).

### MySQL troubleshooting

- **Ingest won't start, "binlog_row_image" in the error.** The server is on
  `binlog_row_image=MINIMAL` or `NOBLOB`. Set `binlog_row_image=FULL` (global, no
  restart: `SET GLOBAL binlog_row_image = 'FULL'`, and persist it in `my.cnf`).
- **Ingest won't start, "binlog_row_metadata" in the error.** The server is on
  `binlog_row_metadata=MINIMAL` (the MySQL default). Set `binlog_row_metadata=FULL`
  (global, no restart: `SET GLOBAL binlog_row_metadata = 'FULL'`, and persist it in
  `my.cnf`). This needs MySQL 8.0.1+ / MariaDB 10.5+; on an older server committed's
  MySQL CDC can't run.
- **"Access denied" on connect or on the binlog dump.** The user is missing
  `REPLICATION SLAVE`/`REPLICATION CLIENT` (binlog) or `RELOAD` (snapshot lock).
- **`reSnapshotRequired: true` after a long outage.** The source purged the binlog
  past what committed had consumed (e.g. a short `binlog_expire_logs_seconds` and a
  downtime longer than it). committed detects this (binlog error 1236 /
  `@@gtid_purged`) and **automatically re-snapshots** to recover — the data
  re-applies idempotently — so you don't re-create the ingestable by hand. To avoid
  it, keep binlog retention longer than your worst-case downtime (see the retention
  caveat above). If it recurs, your retention is too short for your restart window.

---

## Restart behavior (both engines)

When committed restarts, each ingestable reads back its checkpointed stream
position and resumes from it — it does **not** re-snapshot. The requirement is
that the source still has the data after that position:

- **Postgres** retains it automatically (that's what the slot does — see the disk
  caveat above), so resume always succeeds while the slot exists.
- **MySQL** retains it only as long as the binlog isn't purged past the
  checkpoint; size your binlog retention accordingly. With `gtid_mode=ON` resume
  is by GTID set, so it also survives a **source failover** (a promoted replica —
  where the binlog file:offset would be meaningless); if the binlog was purged past
  the consumed point, committed re-snapshots rather than resuming (see
  `reSnapshotRequired` above).

The status endpoint goes back to `phase: "streaming"` once the resumed worker is
following the change stream again.
