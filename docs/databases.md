# Databases

A **database** is the connection config for an external SQL destination.
SQL [syncables](syncables.md) (`sql` and `projection`) reference one by name
(`db = "bff"`), so many syncables share one place to point at a server and
rotate its credentials. MySQL and PostgreSQL are supported.

[Ingestables](ingestables.md) don't use database configs: an ingest source
carries its own connection string inline, because the worker owns its
connections (a replication stream plus short-lived sessions) rather than
sharing a pool.

```toml
[database]
name = "bff"
type = "sql"

[sql]
dialect          = "postgres"    # or "mysql"
connectionString = "postgres://app:${BFF_DB_PASSWORD}@db:5432/catalog?search_path=read&sslmode=disable"
```

```sh
curl -X POST -H 'Content-Type: text/toml' \
  --data-binary @examples/movies/db-bff.toml \
  http://localhost:8080/v1/database/bff
```

- **`connectionString`** is a URL: `postgres://…` or `mysql://…`. Write the
  password as `${VAR}`: committed refuses an inline password (400), so the
  credential never enters the replicated log; each node resolves the variable
  from its own environment. See [secrets.md](operations/secrets.md).
- **`search_path`** (Postgres) chooses the schema committed creates and writes
  tables in.

| Endpoint | |
|---|---|
| `POST /v1/database/{id}` | Create or change a database config. |
| `GET /v1/database` | List them. |
| `GET /v1/database/{id}/versions[/{version}]`, `POST /v1/database/{id}/rollback` | Config history and rollback. |

A node that can't resolve a database's `${VAR}` keeps the config but can't
build it; `GET /v1/node/status` names the missing variable.
