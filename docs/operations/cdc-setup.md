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

### What to watch

Every ingestable exposes its status:

```
GET /v1/ingestable/{id}/status
```

```json
{
  "phase": "streaming",
  "snapshotProgress": [{ "table": "ingress.movie", "lastKey": "mv0000003", "complete": true }],
  "position": "0/1A2B3C8",
  "lag": 4096,
  "caughtUp": true
}
```

- **`phase`** — `"snapshot"` while dumping existing rows, `"streaming"` once it is
  following the change stream.
- **`snapshotProgress`** — per watched table: the last key dumped and whether that
  table's snapshot is complete.
- **`position`** — the engine-native cursor: a Postgres LSN (`0/1A2B3C8`) or a
  MySQL binlog coordinate (`binlog.000007:4096`).
- **`lag`** — bytes the source write head is ahead of what this ingest has
  durably consumed (Postgres only — see the MySQL note below). `null` during
  snapshot or when it can't be determined.
- **`caughtUp`** — `true` only when the snapshot is complete **and** lag is a
  known `0`.

The quickstart polls this endpoint to know when the initial snapshot has landed
(`"caughtUp": true`).

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
it.** While committed is running this is bounded (it acks continuously). But if
committed is **stopped** (or an ingestable is removed) while its slot still
exists, the slot pins WAL on the source and the source's disk grows without
bound until it fills. Two consequences:

- Don't leave a stopped committed pointed at a production database for long.
- When you **decommission** an ingestable, drop its slot on the source —
  committed does not drop it for you:

  ```sql
  SELECT pg_drop_replication_slot('committed_movie_slot');
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

1. **Binlog enabled, row format, full row image.** MySQL 8.0+ ships with all of
   these on by default (`log_bin` on, `binlog_format=ROW`,
   `binlog_row_image=FULL`, a non-zero `server_id`), so a stock MySQL 8/9 needs no
   changes. If your server has been customized, ensure:

   ```ini
   # my.cnf
   log_bin            = ON
   binlog_format      = ROW
   binlog_row_image   = FULL      # see the MINIMAL note below
   server_id          = 1         # any unique non-zero id
   ```

   committed's preflight reads `@@global.binlog_row_image` and, if it is
   `MINIMAL`, refuses to start **unless** every watched table has a PRIMARY KEY
   covering the configured `primaryKey`. The reason: a `MINIMAL` row image ships
   only the changed columns plus the key on a `DELETE`, so without a covering PK
   committed can't form a keyed tombstone. `FULL` (or `NOBLOB`) always works.

2. **A replication grant.** The ingest user needs to read rows (snapshot), briefly
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

### MySQL lag is `null` by design

Unlike a Postgres LSN, a MySQL binlog coordinate (`file:offset`) has no single
scalar "distance from the source write head" — the file rolls over, and there is
no cheap server-side measure of how far behind a given coordinate is. So
committed reports **`lag: null`** for MySQL, and because `caughtUp` requires a
known-zero lag, **`caughtUp` is never `true` for a MySQL ingestable.** This is
expected, not a fault: use `phase == "streaming"` plus `snapshotProgress[*].complete`
to know the snapshot is done and the ingest is live, and compare the reported
binlog `position` against the source's `SHOW BINARY LOG STATUS` if you need to
gauge how current it is. (A real numeric MySQL lag is tracked as a separate
follow-on.)

### MySQL troubleshooting

- **Ingest won't start, "binlog_row_image" in the error.** The server is on
  `binlog_row_image=MINIMAL` and a watched table lacks a PK covering your
  `primaryKey`. Set `binlog_row_image=FULL` (global, no restart:
  `SET GLOBAL binlog_row_image = 'FULL'`, and persist it in `my.cnf`) or add the
  PRIMARY KEY.
- **"Access denied" on connect or on the binlog dump.** The user is missing
  `REPLICATION SLAVE`/`REPLICATION CLIENT` (binlog) or `RELOAD` (snapshot lock).
- **Resume fails after a long outage.** If the source purged the binlog past the
  position committed checkpointed (e.g. a short `binlog_expire_logs_seconds`),
  the stream can't resume from there. Keep binlog retention longer than your
  worst-case committed downtime; recovering requires re-creating the ingestable
  to re-snapshot.

---

## Restart behavior (both engines)

When committed restarts, each ingestable reads back its checkpointed stream
position and resumes from it — it does **not** re-snapshot. The requirement is
that the source still has the data after that position:

- **Postgres** retains it automatically (that's what the slot does — see the disk
  caveat above), so resume always succeeds while the slot exists.
- **MySQL** retains it only as long as the binlog isn't purged past the
  checkpoint; size your binlog retention accordingly.

The status endpoint goes back to `phase: "streaming"` once the resumed worker is
following the change stream again.
