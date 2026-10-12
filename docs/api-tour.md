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

## Types

Define a type:

```sh
curl -X POST -H 'Content-Type: text/toml' \
  --data-binary @examples/movies/type-movie.toml \
  http://localhost:8080/v1/type/movie
```

A type declares a topic, its entity kind, its versions and migrations, and
an optional contract. See [types.md](types.md), which also covers
restatements (`POST /v1/restatement/{id}`).

## Databases

Configure a database to write into (a destination):

```sh
curl -X POST -H 'Content-Type: text/toml' \
  --data-binary @examples/movies/db-bff.toml \
  http://localhost:8080/v1/database/bff
```

See [databases.md](databases.md).

## Syncables

Configure a syncable that writes a topic out to that database. The simplest
shape is a plain `sql` mirror with `[[sql.mappings]]` extracting leaf values
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

A `projection` folds one or more topics into the current-state table you
query; the other kinds write to a webhook, an Iceberg table, or a derived
topic. See [syncables.md](syncables.md) and, for projections,
[read-models.md](read-models.md).

## Ingestables

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

See [ingestables.md](ingestables.md).

## Proposals

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

`/v1/proposal` is write-only: to read committed data, replicate it to a
queryable store with a syncable. See [proposals.md](proposals.md).

## Everything else

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
