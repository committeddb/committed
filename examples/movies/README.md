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
