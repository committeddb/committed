# Iceberg sink: a current-state table on S3

The `iceberg` syncable lands a topic in an Apache Iceberg table — the
warehouse/analytics landing zone. Anything that reads Iceberg through a REST
catalog (AWS S3 Tables, Athena, Redshift Spectrum, duckdb, Spark, Trino)
queries the table directly, with no committed in the read path.

The table is a **current-state materialization, not a fact log**: one row per
live entity, maintained by copy-on-write merge. A keyed upsert replaces the
row, a source DELETE removes it (right-to-be-forgotten flows through), and an
ingest refresh boundary sweeps rows whose generation predates the refresh —
the same reconciliation contract as the SQL sinks.

## Configuration

```toml
[syncable]
name = "warehouse-photos"
type = "iceberg"

[iceberg]
topic     = "photos"
catalog   = "https://catalog.example.com"   # Iceberg REST catalog
namespace = "committed"
table     = "photos"
# warehouse = "s3://lake/warehouse"         # if your catalog requires it
# flushRows = 10000                         # rows per commit (default 10000)
# flushInterval = "60s"                     # max buffer age (default 60s)

# [iceberg.props]                           # extra FileIO/catalog properties
# "s3.endpoint" = "http://minio:9000"       # private endpoints / minio
# "s3.region"   = "us-east-1"
```

**Authentication carries no config-borne secrets, structurally**: a catalog
URL with credentials in it is refused at POST. S3 and catalog auth come from
the node's environment — the standard AWS credential chain
(`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, instance/task roles) — so
nothing secret can land in the replicated config log.

## Table shape (v1)

A fixed envelope, one row per live entity:

| column            | type   | meaning |
|-------------------|--------|---------|
| `key`             | string | the entity key — the merge identity |
| `payload`         | string | the entity's JSON document, verbatim |
| `committed_index` | long   | the raft index that wrote this version |
| `generation`      | long   | the ingest refresh epoch (0 = unstamped) |

Query the JSON with your engine's functions (`json_extract` in Athena/duckdb),
or project to typed columns downstream (a CTAS/dbt model — or canonicalize
upstream with a `loopback` derived topic and land that instead).

## Semantics and delivery

- **Copy-on-write merge.** Each flush commits one atomic snapshot chain:
  delete every superseded row (the batch's keys, plus the sweep predicate on
  a refresh boundary), append the batch's live rows. Only data files the
  delete filter touches are rewritten — files are written in key order so
  their column stats prune. Readers always see plain data files: no
  merge-on-read cost. (Chosen over equality-delete merge-on-read: iceberg-go
  cannot write equality deletes and the Iceberg v4 spec discussion is moving
  away from them.)
- **Batched commits.** Rows buffer in memory and commit when `flushRows` is
  reached, when the buffer is older than `flushInterval` (checked on
  arrival — an idle topic flushes on its next delivery), or when a refresh
  boundary arrives. Watch commit cadence vs. your catalog's snapshot churn:
  bigger batches = fewer snapshots.
- **Exactly-once at the table.** The worker checkpoint advances only on a
  committed flush, and every commit stamps the flushed-through raft index
  into the snapshot summary (`committed.checkpoint-index`). A crash between
  commit and checkpoint replays the batch; the marker skips the re-commit. A
  replay that extends past a committed range re-merges it — idempotent by
  key, no duplicates.
- **A restart drops the in-memory buffer, never committed data** — the
  redelivery contract rebuilds it from the last checkpoint.

## Operator notes

- **Table maintenance is yours**: snapshot expiry and file compaction run on
  your schedule with your engine's tools (or S3 Tables' automatic
  maintenance). committed writes tight, key-sorted files but never expires
  snapshots it has committed.
- **Reshaping is blue-green**: the sink refuses the re-materialization verb
  (`POST /v1/syncable/{id}/rematerialize` → 409). To rebuild or reshape,
  create a second syncable into a new table and swap readers (or
  RenameTable) when it converges — the same pattern as projections.
- **One producer per table**: point exactly one syncable at a given catalog
  table. Two writers would interleave their checkpoint markers and each
  would treat the other's rows as its own to merge over.
