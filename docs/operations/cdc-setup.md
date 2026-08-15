# CDC setup: Postgres, MySQL, and SQL Server

This guide is for operators standing up **change data capture** — ingesting a
SQL database's inserts, updates, and deletes into a committed topic. It covers
what the source database needs, what committed sets up for you, the
snapshot→streaming lifecycle, what to watch, and how to fix the common failures.
All supported engines are here: PostgreSQL (logical replication), MySQL
(binlog), and SQL Server (Change Tracking).

For the end-to-end walkthrough (source → ingest → topic → projection → query),
see the [quickstart](../quickstart.md); this guide is the reference for the
ingest half. For the output half (projecting a topic to a SQL table), see the
README § SQL projections.

## How ingest works (all SQL engines)

An `ingestable` watches one or more source tables and turns every committed row
change into a proposal on a topic. It runs in two phases:

1. **Snapshot.** On first start the worker reads the current contents of each
   watched table in primary-key order, in bounded batches (keyset pagination),
   and proposes each row. It records a per-table cursor as it goes, so a restart
   mid-snapshot resumes where it left off rather than starting over.
2. **Streaming.** Once the snapshot completes, the worker follows the database's
   change stream (Postgres logical replication / MySQL binlog / SQL Server
   Change Tracking polls) from the position it captured at the start of the
   snapshot, proposing each insert, update, and delete as it commits at the
   source.

A source `DELETE` becomes a **keyed tombstone** (a delete entity with no
payload), not an upsert of the old row — that is what makes right-to-be-forgotten
flow all the way through to downstream projections. For that to work the change
stream has to carry the row's key on a delete; the per-engine sections below say
how to guarantee that, and committed's **preflight** check refuses to start an
ingestable whose source can't, so it fails loudly at config time instead of
silently dropping deletes.

Ingest is **effectively-once** for the change stream: committed checkpoints its
stream position into its own log, and on restart it resumes from that
checkpoint and de-duplicates any re-delivered changes by source sequence. You
do not get duplicate stream changes in the topic across a restart.

A change-stream transaction lands in the topic as **one atomic unit**: sinks
apply all of its rows in a single destination transaction, so consumers never
observe a partial source transaction — with one bounded exception. A source
transaction too large to fit a single committed proposal (larger than the
~12MiB soft-flush budget) is applied as ordered contiguous parts; a consumer
can transiently observe such a giant transaction partially applied, and
converges as the parts complete. This exception exists because the only
alternative is refusing to ingest oversized transactions.

Every change-stream proposal carries **capture provenance**: the timestamp at
which the source committed the change and an identity for the source
transaction that produced it (rows changed together in one source transaction
share the identity). Both are recorded into the log at ingest time because
they exist only in the change stream — once the source's binlog/WAL retention
expires they are unrecoverable. Fidelity varies by engine:

- **PostgreSQL** — commit time and xid, exactly as logical replication reports
  them per transaction.
- **MySQL** — the binlog event-header timestamp (whole-second resolution) and
  the transaction's GTID; a `gtid_mode=OFF` source falls back to a
  binlog-coordinate identity. For a transaction large enough to be applied in
  parts, the parts share one identity and non-final parts carry statement time
  rather than commit time (the commit time isn't known until the commit event).
- **SQL Server** — best-effort: Change Tracking is polled, so provenance is
  present only when a polled batch spans exactly one source transaction (the
  steady-state case; catch-up windows spanning transactions omit it). The
  identity is the change version, and the commit time comes from
  `sys.dm_tran_commit_table` when it is readable.

Snapshot-phase proposals carry **no provenance** — a snapshot row is a
re-observation, not a source transaction. Provenance is capture metadata in
the log, not payload: it never appears in a projected row or webhook delivery.
The source commit time is evidence for operators and interpretation tooling —
committed never orders, dedups, or resolves conflicts by it; the log's index
is the only ordering authority.

### Capture fidelity: what the log preserves, byte for byte

What lands in the log is a **canonical rendering** of the source row, and the
same source data always produces the same bytes — whichever path it arrived
by. The contract:

- **Snapshot and CDC render identically.** A row captured by the snapshot
  pass and the same row captured from the change stream produce
  byte-identical payloads (numbers included: a DECIMAL is never conflated
  with a DOUBLE, exact digits are preserved, and JSON-column leaf types are
  resolved so both paths agree). This parity is pinned by per-engine oracle
  tests.
- **Deliberately erased**: JSON object key order (keys are sorted — two
  writes differing only in key order capture identically) and duplicate keys
  (last wins). If key order carries meaning in your source, it is not
  preserved — encode it as data.
- **Deliberately preserved**: exact numeric representation (digits, not
  float round-trips), string bytes, null-vs-absent distinction, and the
  row's primary-key identity (the entity key).
- **Never transformed**: capture applies no semantic mapping — no renames,
  no computed fields, no filtering. Capture is unrepeatable (the source
  moves on); interpretation is revisable later, so anything lossy belongs
  downstream, never at ingest.

Snapshot rows are **convergent re-observations** rather than deduplicated
events: each snapshot row is its own single-row proposal, and the resume
checkpoint rides the final row of each read window — so a restart mid-window
re-emits that window and rows the crash had already committed appear in the
log again. That is the same semantics as a reconciling refresh (which
re-observes every row): keyed upserts, last write wins, consumers converge
identically. A keyless/append sink has no key to converge on, so it appends each
re-observation as another row — see [History tables vs. read
models](../read-models.md#history-tables-vs-read-models).

### One writer per topic

A topic is reconciled against a **single producer**, so it must have exactly one:

- **One ingestable per topic — rejected best-effort.** Creating a second
  ingestable on a topic another ingestable already produces is rejected at config
  time (`POST /v1/ingestable` returns `400`, naming the topic and the ingestable
  that already owns it). This closes the misconfiguration hole against the
  handling node's committed view; it is not a consensus-level lock, so two
  *simultaneous* creates racing the same topic on different nodes could both pass —
  resolve that by deleting one. The two would reconcile the topic independently,
  and one's reconciliation would delete the rows the other produced. If you need to
  move a topic to a different ingestable, delete the old one first.

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

> **Removing a table from a multi-table ingestable is rejected** (409,
> `ingestable_table_removal_requires_recreate`). A refresh re-stamps only the
> currently-configured tables, so an in-place removal would arm this sweep to
> silently delete the removed table's rows from keyed sinks at the *next*
> refresh event — possibly months after the config edit — while a syncable
> replay from the log would resurrect them. To drop a table **and** its sink
> rows, delete and recreate the ingestable (the recreate's snapshot + marker
> sweeps them as the explicit, immediate semantics of that operation); to keep
> the rows, keep the table listed. Adding a table remains allowed.

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

- **`phase`** — `"pending"` until anything has durably checkpointed (a
  just-created ingestable, or one still retrying its first snapshot batch — read
  `workerState` for whether it is running or recovering), `"snapshot"` while
  dumping existing rows, `"streaming"` once it is following the change stream.
- **`snapshotProgress`** — per watched table: the last key dumped and whether that
  table's snapshot is complete.
- **`position`** — the engine-native cursor: a Postgres LSN (`0/1A2B3C8`) or a
  MySQL binlog coordinate (`binlog.000007:4096`).
- **`lag`** — how far the source write head is ahead of what this ingest has
  durably consumed, in the unit **`lagUnit`** names: **Postgres bytes**
  (`pg_current_wal_lsn − confirmed_flush_lsn`), **MySQL transactions** under GTID
  positioning (`@@gtid_executed − consumed`), **MySQL bytes** under file:pos
  positioning (computed from the source's binlog inventory — every file at or
  after the consumed coordinate, minus the consumed offset), **SQL Server
  transactions** (current Change Tracking version − consumed). `null` during
  snapshot, when the source is unreachable, or when a re-snapshot is required.
  Check `lagUnit` before alarming on a threshold — the same number is wildly
  different sizes in bytes vs transactions.
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

### TRUNCATE is not propagated (caveat)

committed replicates `INSERT`, `UPDATE`, and `DELETE`, but **not `TRUNCATE`** — on
either engine. It has no "clear-all" primitive yet, so a `TRUNCATE` on a watched
table empties the source but leaves the sink's rows in place — the sink
**diverges** from the source until you reconcile it. committed does not swallow
this silently: each dropped truncate is logged at `Warn`, naming the affected
`schema.table`, so you can alert on it:

```
TRUNCATE on a watched table is not propagated to the sink; the sink now
diverges from the source and must be re-snapshotted to reconcile   tables=[public.movie]
```

This applies to both PostgreSQL and MySQL: on Postgres the truncate arrives as a
logical-replication `TRUNCATE` message, on MySQL as a binlog DDL statement, and
each is recognized, filtered to watched tables, and logged with the identical
message above (the `tables` field is the affected `schema.table`).

To reconcile after a truncate, **re-snapshot** the ingestable (rebuild it from
zero — see [rebuild.md](rebuild.md)). To avoid the divergence entirely, prefer
`DELETE FROM <table>` over `TRUNCATE` on watched tables: each row delete
replicates as a keyed tombstone and clears the sink row-by-row.

Full truncate propagation is planned (a clear-all signal applied downstream as
`DELETE FROM <sink>`); until then this caveat stands.

---

## PostgreSQL

committed ingests Postgres via **logical replication** through the `pgoutput`
plugin.

### Prerequisites (operator)

1. **`wal_level = logical`.** This requires a server restart. Preflight checks
   it, so `POST /v1/ingestable` fails with a clear 400 until it is set (without
   the check, the replication slot simply can't be created and ingest would
   never start).

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
connectionString = "postgres://committed:${PG_PASSWORD}@db:5432/catalog?sslmode=disable"
primaryKey       = "movie_id"
tables           = ["ingress.movie"]   # schema-qualified
mapAllColumns    = true                # mirror every column 1:1
# jsonColumns  = ["event_data"]        # string columns that HOLD JSON — see below

[sql.postgres]
slot_name   = "committed_movie_slot"   # optional; default "committed_slot"
publication = "committed_movie_pub"    # optional; default "committed_pub"
```

**`jsonColumns` — string columns that hold JSON.** A `JSON`/`JSONB` column
decodes as real JSON automatically, but JSON riding in a **string** column
(SQL Server `NVARCHAR`, MySQL `VARCHAR`/`TEXT` — the shape real CDC'd event
tables have) is undetectable from type metadata and would arrive as one
escaped string: projection jsonPaths can't traverse it, webhook consumers
get a string, every downstream double-parses. Listing the column under
`jsonColumns` (works with explicit mappings and `mapAllColumns` alike, and
per-entry in the `[[sql.topics]]` form) decodes it as a real, canonicalized
JSON value — the same sorted-keys/exact-numbers rendering as a native JSON
column, identical bytes on the snapshot and CDC paths. A value that isn't
valid JSON falls back to the plain string, so a malformed source row never
produces an invalid payload; a `jsonColumns` entry naming an unmapped
column is rejected at POST.

Best applied **when the ingestable is created**: adding the hint to an
existing ingestable changes that column's payload shape for NEW events only
(string → object), so a projection folding the topic sees both shapes
across history. Keyed sinks converge regardless (last write wins); to
reshape history too, pair the change with a re-snapshot (delete + recreate
the ingestable, or a slot recreate on Postgres).

> **Why ingest configs carry `connectionString` inline** (while syncables
> reference a shared `[database]` config by `sql.db`): a `[database]` config is
> a shared, long-lived **sink** pool that many syncables reuse — one place to
> rotate credentials for a destination. An ingest **source** is different: the
> worker owns its connections (the replication-protocol stream plus short-lived
> SQL sessions), opens them itself, and tears them down with the worker — there
> is no shared pool to reference. The two idioms are deliberate, not drift.
> Write the password as `${VAR}` (as above) — committed **rejects an inline
> connection-string password** (HTTP 400) so it is never stored in the replicated
> log or a snapshot. See [secrets.md](secrets.md).

Give each ingestable its own `slot_name` and `publication` so they don't collide.
A runnable, end-to-end Postgres example lives in
[`examples/movies/`](../../examples/movies/) (`source.sql`, `ingest-*.toml`,
`compose.yml`).

To feed **several topics** from this one ingestable — one slot and publication for
a whole database — see
[Multiple topics from one ingestable](#multiple-topics-from-one-ingestable-all-sql-engines).

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

   Preflight reads `@@global.binlog_format`, `@@global.binlog_row_image`, and
   `@@global.binlog_row_metadata` and refuses to start unless the format is
   `ROW` and **both row settings are `FULL`**:

   - **`binlog_format=ROW`** — `STATEMENT` (and `MIXED`'s statement-chosen
     paths) deliver DML as statement text rather than row events, so change
     capture would silently miss rows while the initial snapshot still works.
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
   positioning (the pre-0.7 behavior — single-server only; `lag` reports **bytes**
   behind the binlog write head instead of transactions, with `lagUnit` saying
   which). Preflight does not fail on this; it logs a warning so the degraded mode
   is visible rather than silent. Default MySQL ships `gtid_mode=OFF`, so set this
   explicitly for any production / failover-capable deployment.

   One transitional case looks degraded but is not: a server whose GTID mode was
   just enabled (online or at first boot) with **no transactions committed since**
   has `gtid_mode=ON` but an empty `@@gtid_executed`. committed then positions by
   file:offset for the initial snapshot — starting from an empty GTID set would
   replay every retained binlog — and **upgrades to GTID positioning automatically
   at the first streamed transaction** (visible in status as `lag` flipping from
   `null` to a number). The snapshot logs which positioning it captured either
   way, so neither case is silent.

   Resume is equally visible: every binlog (re)connect logs which positioning
   it **resumed by** and from exactly where (the GTID set, or the binlog
   file:pos), and every file rotation logs `from` → `to`. A re-delivery
   question — "what did the worker resume from, and where did the server
   start the dump?" — is answered by two adjacent log lines instead of by
   decoding the checkpoint out of bbolt. (Postgres logs the same on connect:
   the slot and the LSN it resumed from.)

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
connectionString = "mysql://committed:${MYSQL_PASSWORD}@db:3306/shop"
primaryKey       = "wid"
tables           = ["widget"]

[[sql.mappings]]
jsonName = "wid"
column   = "wid"

[[sql.mappings]]
jsonName = "name"
column   = "name"
```

Note the connection string uses the `mysql://user:${VAR}@host:port/db` URL form
(the password is a `${VAR}` reference — an inline password is rejected), and
there is no `[sql.mysql]` subsection — MySQL has nothing analogous to a slot or
publication to name. (`mapAllColumns = true` works here too, in place of the
explicit `[[sql.mappings]]` blocks.)

To feed **several topics** from this one ingestable — one binlog reader for a
whole database — see
[Multiple topics from one ingestable](#multiple-topics-from-one-ingestable-all-sql-engines).

**TLS.** A MySQL connection takes the same libpq-style TLS parameters as
PostgreSQL, so a MySQL source (or sink) is secured the same way. Use the
`mysqls://` scheme (shorthand for full verification) or an explicit `?sslmode=`:

- `sslmode=disable` — no TLS (the default for `mysql://`)
- `sslmode=require` — encrypt, but do not authenticate the server
- `sslmode=verify-ca` — verify the certificate chain (against `sslrootcert` if
  given, otherwise the system roots), but not the hostname
- `sslmode=verify-full` — verify the chain **and** the hostname (the default for
  `mysqls://`)

`sslrootcert` names a custom CA PEM; `sslcert` + `sslkey` add a client
certificate for mutual TLS. All three are node-local file paths that must exist
on every node. The same posture secures both the snapshot connection and the CDC
binlog stream, and any query parameter other than these four is rejected. Example:

```toml
connectionString = "mysqls://committed:${MYSQL_PASSWORD}@db:3306/shop?sslrootcert=/etc/committed/db-ca.pem"
```

A `tables` entry may be **schema-qualified** (`["otherdb.widget"]`) to read a table
outside the connection string's default database, exactly as on PostgreSQL; a bare
entry (`["widget"]`) resolves to the connection's database. The connection user
needs the usual read + `REPLICATION` grants on the qualified schema.

A complete worked MySQL setup — source DDL, the grant, an ingestable, and a
syncable projecting back into a MySQL sink table — is exercised end-to-end by the
`e2e/cdc` MySQL tests (`e2e/cdc/harness/mysql.go`, `e2e/cdc/mysql_test.go`); the
DDL and TOML there are copy-pasteable.

### Snapshot scale: resume and parallel readers (MySQL)

The snapshot is **restart-resumable at keyset granularity** out of the box: the
per-table cursor rides each batch's proposal, so a deploy or failover
mid-snapshot resumes from the last committed row — completed tables are never
re-read. Nothing to configure.

For very large tables, the snapshot can additionally read each table with
**parallel range readers**:

```toml
[sql.options]
batch_size       = "10000"   # rows per keyset batch (default 10000)
snapshot_readers = "4"       # parallel PK-range readers per table (default 1)
```

- `snapshot_readers` defaults to **1** (the single stream) on purpose: the
  snapshot target is usually a production replica, and every reader holds a
  connection running a range scan. Raise it deliberately, watching the
  source's load. Values are capped at 16.
- A table splits when its primary key is a **single integer column** (the
  auto-increment case) or a **BINARY/VARBINARY column** (UUID-as-BINARY(16)).
  Composite keys and CHAR/VARCHAR keys read on the single stream — a text
  key's ORDER BY follows its collation, which arithmetic range bounds cannot
  safely reproduce. Small tables also stay single-stream (splitting them
  gains nothing).
- The chunk plan is **frozen in the checkpoint**: a restart resumes the same
  ranges from their cursors even if `snapshot_readers` changed. Per-table
  progress shows as `chunksTotal` / `chunksDone` on
  `GET /v1/ingestable/{id}/status`.
- Ordering: rows from different ranges of one table interleave in the log
  (each range is still in key order). Keyed consumers are unaffected; a
  keyless history table records the interleaved order.

The CDC **stream** is unaffected — it stays a single ordered cursor.

### MySQL lag, caughtUp, and the binlog-retention caveat

With `gtid_mode=ON`, committed reports a real `lag` and `caughtUp` for MySQL,
computed by GTID-set arithmetic: it diffs the consumed GTID set against the
source's `@@gtid_executed`. `lag` is the **number of transactions** the source is
ahead (`lagUnit: "transactions"`), and `caughtUp` is `true` once the consumed set
covers `@@gtid_executed`. With `gtid_mode=OFF` there is no transaction head to
diff against, so committed falls back to **bytes behind** the binlog write head
(`lagUnit: "bytes"`), computed from the source's binlog inventory: every binlog
file at or after the consumed coordinate, minus the consumed offset. `caughtUp`
works in both modes. The GTID form is still the better signal — transaction
counts are stable under batch size, and GTID positioning is what makes resume
failover-safe — but file:pos deployments are no longer flying blind. If the
consumed binlog file is missing from the source's inventory, the source purged
past the consumed position and `reSnapshotRequired` is reported, exactly as the
GTID path does via `@@gtid_purged`.

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

### Very large compressed transactions

If the source runs with `binlog_transaction_compression=ON`, MySQL writes each
transaction as a single compressed event. committed's change dedup keys on the
transaction's binlog position, and a **single transaction of several gigabytes** in
one commit can advance that key far enough that the very next change — the one
committed immediately after it — is treated as already-seen and **not delivered
downstream**. Ordinary workloads never hit this: well-behaved bulk loads commit in
batches, not one multi-gigabyte transaction. If you do drive multi-gigabyte single
transactions into a compressed source, either **chunk the load into smaller commits**
(recommended anyway, to avoid replication lag and long lock holds) or **turn off
`binlog_transaction_compression`** on the source — the uncompressed path is
unaffected.

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
- **Lag growing while status says `streaming` (any engine).** The
  streaming/polling machinery believes it is connected but nothing arrives —
  historically a half-open TCP connection: the source (or a NAT/firewall/LB
  between) dropped the connection without a FIN reaching committed. As of
  0.7.8 this self-heals — the MySQL binlog stream runs server heartbeats
  under a rolling read deadline, and the SQL Server poll's per-cycle catalog
  queries carry their own deadline — so a dead connection surfaces as a
  reconnect (watch for the resume-positioning log line) within ~a minute or
  two. Growing-lag-while-streaming is still the right dashboard alert: it
  catches this whole class, including causes the deadlines can't see.
- **`reSnapshotRequired: true` after a long outage.** The source purged the binlog
  past what committed had consumed (e.g. a short `binlog_expire_logs_seconds` and a
  downtime longer than it). committed detects this (binlog error 1236 /
  `@@gtid_purged`) and **automatically re-snapshots** to recover — the data
  re-applies idempotently — so you don't re-create the ingestable by hand. To avoid
  it, keep binlog retention longer than your worst-case downtime (see the retention
  caveat above). If it recurs, your retention is too short for your restart window.

## SQL Server

SQL Server ingest captures changes via **Change Tracking (CT)** — available on
**every edition** (Web and Express included; there is no edition gate). CT
reports, per row, that it changed and how (insert/update/delete) since a
version; committed reads the row's current values through the same query path
the snapshot uses, so snapshot and change payloads are byte-identical by
construction. What CT does not carry is intermediate history: a row updated
five times between polls yields one upsert at the final value — the same
convergent last-write-wins contract as everywhere else in ingest. Do not use
it as a change-event historian.

### Prerequisites (operator)

1. **A `sqlserver://` connection string** naming the database in the query
   parameter (the URL path is the instance name):

   ```
   sqlserver://user:${SQLSERVER_PASSWORD}@host:1433?database=appdb
   ```

2. **Primary keys on every watched table.** Change Tracking requires one, and
   committed keys delete tombstones by it. The configured `primaryKey` must be
   columns of the table's real PRIMARY KEY (preflight verifies).

3. **Change Tracking enabled — or the rights to enable it.** committed turns
   CT on where it is off (database level with a 2-day auto-cleanup retention,
   and per table), which needs ALTER on the database/table. On a locked-down
   source, have a DBA pre-enable instead:

   ```sql
   ALTER DATABASE appdb SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);
   ALTER TABLE dbo.orders ENABLE CHANGE_TRACKING;
   ```

   **Ownership etiquette:** committed marks the tables whose CT it enabled
   (an extended property) and, when the ingestable is deleted, disables ONLY
   those. CT that pre-existed the ingestable is never touched, and
   database-level CT is never disabled.

### Configuration

```toml
[ingestable]
type = "sql"

[sql]
dialect = "sqlserver"
topic = "orders"
connectionString = "sqlserver://committed:${SQLSERVER_PASSWORD}@db:1433?database=appdb"
tables = ["orders"]           # bare names scope to dbo; "sales.orders" keeps its schema
primaryKey = "id"

[sql.options]
poll_interval = "3s"          # CT poll cadence (default 3s) — read models trail by ~this
batch_size = "1000"           # snapshot keyset batch
```

### Lag, retention, and the poll cadence

- `lag` reports **transactions** (`lagUnit: "transactions"`): the source's
  current CT version minus the consumed version — the version increments per
  committed transaction.
- Latency is the **poll cadence**: changes arrive within ~`poll_interval` of
  committing at the source, comparable to the sync workers' own cadence.
- **Retention** (`CHANGE_RETENTION`, default 2 days when committed enables
  CT) is the binlog-expiry analog: if the cleanup purges changes past the
  consumed version (downtime longer than retention), committed detects it
  and **automatically re-snapshots** — loud, and the data re-applies
  idempotently. Keep retention longer than your worst-case downtime.

### SQL Server troubleshooting

- **Preflight fails, "has no primary key."** Change Tracking (and delete
  tombstones) require one; add a PK or exclude the table.
- **Worker retries with "enable change tracking … ALTER" in the error.** The
  ingest user lacks ALTER rights and CT is off — pre-enable per the
  prerequisites, or grant the rights.
- **`reSnapshotRequired: true`.** The retention cleanup purged past the
  consumed version (or the consumed version predates CT's minimum valid
  version after a re-enable). committed re-snapshots automatically; if it
  recurs, raise `CHANGE_RETENTION`.

---

## Multiple topics from one ingestable (all SQL engines)

By default an ingestable feeds **one** topic: every table in `tables` merges into
that single topic. To ingest a source database's tables as **distinct** topics —
each with its own type, primary key, and column mapping — list them under
`[[sql.topics]]` instead of the flat `topic`/`tables`/`primaryKey`/`mappings`
fields. This works identically across committed's SQL engines; only the engine
bits of the config differ, exactly as in the flat form:

```toml
[ingestable]
name = "shop-ingest"
type = "sql"

[sql]
dialect          = "postgres"
connectionString = "postgres://committed:${PG_PASSWORD}@db:5432/shop?sslmode=disable"

[sql.postgres]
slot_name   = "committed_shop_slot"
publication = "committed_shop_pub"

[[sql.topics]]
topic      = "orders"
tables     = ["orders_us", "orders_eu"]   # several same-shape tables fan into one topic
primaryKey = "id"

[[sql.topics.mappings]]
jsonName = "id"
column   = "id"

[[sql.topics]]
topic      = "customers"
tables     = ["customers"]
primaryKey = "cust_id"

[[sql.topics.mappings]]
jsonName = "custId"
column   = "cust_id"
```

For a MySQL source, only the top-level engine bits change — `dialect = "mysql"`,
a `mysql://` connection string, and no `[sql.mysql]` subsection (nothing
analogous to a slot or publication to name); the `[[sql.topics]]` entries are
identical.

`dialect`, `connectionString`, and the `[sql.<dialect>]` options stay top-level:
every topic shares **one** source connection and **one** replication slot /
publication (Postgres) or **one** binlog reader (MySQL). That is the point — a
whole-database feed no longer needs one slot per table.

Each `[[sql.topics]]` entry is self-contained and carries the same fields as the
flat form (`topic`, `tables`, `primaryKey`, and `mappings` / `mapAllColumns` /
`excludeColumns`). committed enforces at config time (HTTP `400`):

- **The flat form and `[[sql.topics]]` are mutually exclusive** — don't set
  `sql.topic`/`sql.tables`/`sql.primaryKey`/`sql.mappings` alongside `[[sql.topics]]`.
- **Every table feeds exactly one topic** — a table listed under two topics is
  rejected (each row has a single destination topic).
- **Each topic id is claimed once**, and each topic must already exist as a type
  (`POST /v1/type/{id}`) before the ingestable, exactly like the flat form.

Because all the topics share one change stream, they share its **failure domain**:
a fatal drift in one topic — a `primaryKey` column dropped at the source — parks the
**whole** ingestable, so every topic stops together (the park message names which
topic). This is deliberate: silently pausing one topic while the shared cursor kept
advancing would drop that topic's changes. `GET /v1/ingestable/{id}/status` tags
each table in `snapshotProgress` with the topic it feeds, so per-topic snapshot
progress is readable at a glance. The [one-writer-per-topic](#one-writer-per-topic)
rule is unchanged — a multi-topic ingestable is the single producer of *each* of its
topics.

---

## Restart behavior (all SQL engines)

When committed restarts, each ingestable reads back its checkpointed stream
position and resumes from it — it does **not** re-snapshot. The requirement is
that the source still has the data after that position:

- **Postgres** retains it automatically (that's what the slot does — see the disk
  caveat above), so resume always succeeds while the slot exists.
- **MySQL** retains it only as long as the binlog isn't purged past the
  checkpoint; size your binlog retention accordingly. With `gtid_mode=ON` resume
  is by GTID set, so it follows the stream across a **source failover** (a
  promoted replica — where the binlog file:offset would be meaningless). One
  caveat in this release: a promoted replica's binlog file numbering can restart
  *below* the old primary's, and committed's effectively-once dedup is keyed on
  file:offset — so if the new coordinates would fall below the last-consumed
  position, committed **freezes the ingestable** as a fail-safe (it never
  silently drops the post-failover writes). Recover by re-POSTing the ingestable,
  which re-snapshots from the new source state. A future release removes this
  freeze by keying dedup on the GTID set directly. If the binlog was purged past
  the consumed point, committed re-snapshots rather than resuming (see
  `reSnapshotRequired` above).

The status endpoint goes back to `phase: "streaming"` once the resumed worker is
following the change stream again.

### An ingestable frozen on an oversized row or transaction

A single source row — or a whole transaction — whose committed proposal exceeds
`COMMITTED_MAX_PROPOSAL_BYTES` (default 16MiB) cannot be committed. committed
never silently drops it or advances past it: the worker **freezes** at that
position and the supervisor restarts it from the durable checkpoint, which
re-reads to the same row and freezes again. `committed.ingest.frozen` stays `1`
for that ingestable (it does not flap on restart), and after enough consecutive
freezes at the same position the supervisor gives up and emits
`committed.ingest.supervisor_giveups` plus an error log naming the stuck
position. Each freeze also logs a warning with the proposal's `sourceSeq`
(binlog coordinate) and `topic`, so you can identify the offending write.

To recover:

1. Raise `COMMITTED_MAX_PROPOSAL_BYTES` past the row/transaction size (it caps
   the marshaled proposal, so allow headroom over the raw row bytes), or reduce
   the source write so it fits.
2. Apply the fix — which action depends on the worker's state:
   - **Restart the node** to pick up a raised `COMMITTED_MAX_PROPOSAL_BYTES` (an
     env var, read only at startup). A worker still in the freeze→retry loop
     resumes from the durable checkpoint on restart and clears the row.
   - If the supervisor has already **given up** and the worker is **terminally
     parked** (`committed.worker.parked` = 1) — not merely freeze-retrying — a node
     restart does **not** revive it; it comes back parked. **Re-POST the
     ingestable** (its stored config, unchanged) to clear the parked state and
     resume from the durable checkpoint. A park persists until the operator
     re-POSTs or deletes the resource (see [metrics](metrics.md)) — so a parked
     worker whose cap you raised needs **both**: a restart to apply the new cap and
     a re-POST to clear the park.

Because the checkpoint never advanced past the frozen row, resume replays from
exactly that position — the oversized row is applied on the next attempt and no
data between the last checkpoint and the freeze is lost or duplicated. An
oversized *transaction* (not a single row) is instead split into ordered parts
under the ~12MiB soft-flush budget (see [How ingest works](#how-ingest-works-all-sql-engines)),
so only a single row larger than the cap, or a transaction whose one part still
exceeds it, reaches this freeze.
