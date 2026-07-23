# Movies quickstart assets

The runnable pieces behind **[docs/quickstart.md](../../docs/quickstart.md)** —
follow that walkthrough. The data is a small, entirely fictional movie catalog.

- `compose.yml` — committed + a Postgres preloaded with the catalog.
- `source.sql` — the normalized source (`ingress` schema) + an empty `read`
  schema for the read model.
- `db-bff.toml`, `type-*.toml`, `ingest-*.toml` — the source-side config you POST.
- `sync-movie-card.toml` — the projection that folds it all into `read.movie_card`.

```sh
docker compose up --build -d   # then follow docs/quickstart.md from step 2
```

The database/ingestable configs reference the DB password as `${CATALOG_DB_PASSWORD}`
rather than inlining it — committed rejects an inline connection-string password so a
secret is never written into its replicated log or snapshots. `compose.yml` sets
`CATALOG_DB_PASSWORD` on the committed service; if you run committed outside compose,
export it (here it's just `test`) before POSTing the configs.
