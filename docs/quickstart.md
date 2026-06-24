# Quickstart

Go from a **normalized** IMDb dataset in Postgres to **one denormalized table you
query with no joins** — actor and director *names* and all — in one
`docker compose up` and four `curl`s.

The payoff, at the end, is this single-table query:

```sql
SELECT primary_title FROM read.movie_card
WHERE top_cast @> '[{"name":"Morgan Freeman"}]';
--      primary_title
-- --------------------------
--  The Shawshank Redemption
```

No join. The cast names live right in the row — committed resolved them from a
separate `name` topic while folding four normalized tables into one.

A finished `read.movie_card` row:

```
 primary_title            | The Shawshank Redemption
 start_year               | 1994
 genres                   | Drama
 average_rating           | 9.3
 num_votes                | 2800000
 top_cast  | [{"ordering": 1, "nconst": "nm0000209", "name": "Tim Robbins"},
              {"ordering": 2, "nconst": "nm0000151", "name": "Morgan Freeman"}]
 directors | [{"nconst": "nm0001104", "name": "Frank Darabont"}]
```

**Prerequisites:** Docker (with Compose) and this repo cloned. committed itself
builds inside the compose — nothing else to install.

> committed is a commit log, not a query engine: you never query the source
> tables, you query the read model it maintains. This quickstart makes that split
> physical — an `ingress` schema you ingest *from* and a `read` schema you query.

---

## 1. Start everything

```sh
cd examples/imdb
docker compose up --build -d
```

One command brings up committed and a Postgres that's **preloaded** with the IMDb
slice. First run builds the committed image (a minute or two); after that it's
seconds.

<details><summary>What's running, and the two schemas</summary>

- **db** — Postgres with logical replication, preloaded on first boot from
  [`source.sql`](../examples/imdb/source.sql):
  - schema **`ingress`** — the normalized source tables (`title`, `rating`,
    `principal`, `name`). committed ingests *from* here; you never query it.
  - schema **`read`** — empty to start; committed creates `movie_card` here.
    This is what you query.
- **committed** — the database node, API on `:8080`.

Two schemas, one database, one connection. The split is the whole point: ingest
from `ingress`, query `read`.
</details>

## 2. Check it's healthy

```sh
curl localhost:8080/health
# {"status":"ok"}
```

## 3. Declare the source — database, types, ingestables

```sh
post() { curl -fsS -X POST -H 'Content-Type: text/toml' --data-binary @"$1" "localhost:8080$2"; echo " <- $1"; }

post db-bff.toml             /v1/database/bff
for t in title rating principal name; do post "type-$t.toml"   "/v1/type/$t"; done
for i in title rating principal name; do post "ingest-$i.toml" "/v1/ingestable/$i-ingest"; done
```

Each ingestable snapshots its `ingress` table into a topic, then streams live
changes. Watch the snapshots land:

```sh
docker compose logs committed | grep "snapshot: table complete"
```

<details><summary>What each config is</summary>

- **[`db-bff.toml`](../examples/imdb/db-bff.toml)** — the sync target. Its
  connection string sets `search_path=read`, so the read model is created and
  queried in the `read` schema.
- **`type-*.toml`** — declares the four topics as `snapshot` kind (current-state
  rows, the natural shape for a table mirror).
- **`ingest-*.toml`** — pulls each `ingress` table into its topic over Postgres
  logical replication: an initial snapshot of existing rows, then the live tail.
  `principal` has a **composite key** `["tconst", "ordering"]` — many cast/crew
  per movie — so the rows don't collide.

See [README § API tour](../README.md#api-tour) for the config shapes.
</details>

## 4. Build the read model — one projection

```sh
post sync-movie-card.toml /v1/syncable/movie-card
```

<details><summary>What this one syncable does</summary>

[`sync-movie-card.toml`](../examples/imdb/sync-movie-card.toml) folds **four
topics into one row**:

- **`title`** → the spine (descriptive columns; its delete drops the row).
- **`rating`** → a contributor (its columns; its delete clears them, row stays).
- **`name`** → a *dimension*: `nconst → primary_name`, joined into the arrays.
- **`principal`** → split by `category` into two array columns — `top_cast`
  (actors) and `directors` — and each element is **enriched** with the person's
  name from the `name` dimension.

That last part is why `top_cast` carries `"name": "Morgan Freeman"` and not just
a code. The full mechanics — multisource folds, split aggregates, enrichment —
are in [README § SQL projections](../README.md#sql-projections).
</details>

## 5. Query the read model

Give the sync a few seconds, then:

```sh
docker compose exec db psql -U test -d imdb -c \
  "SELECT primary_title, start_year, average_rating,
          top_cast->0->>'name'  AS lead_actor,
          directors->0->>'name' AS director
   FROM read.movie_card ORDER BY average_rating DESC;"
```

```
      primary_title       | start_year | average_rating |   lead_actor   |       director
--------------------------+------------+----------------+----------------+----------------------
 The Shawshank Redemption |       1994 |            9.3 | Tim Robbins    | Frank Darabont
 The Godfather            |       1972 |            9.2 | Marlon Brando  | Francis Ford Coppola
 The Dark Knight          |       2008 |            9.0 | Christian Bale | Christopher Nolan
```

And the headline — find movies by an actor's **name**, one table, no join:

```sh
docker compose exec db psql -U test -d imdb -c \
  "SELECT primary_title FROM read.movie_card
   WHERE top_cast @> '[{\"name\":\"Morgan Freeman\"}]';"
```

That's the whole point of committed: a denormalized read model you query
directly, kept in sync with a normalized source you never touch.

<details><summary>Optional: watch a live change flow through</summary>

The ingest is also streaming changes. Rename an actor in the source and re-query
— committed re-materializes every movie that cast them (the enrichment fan-out):

```sh
docker compose exec db psql -U test -d imdb -c \
  "UPDATE ingress.name SET primary_name = 'Morgan Freeman ⭐' WHERE nconst = 'nm0000151';"

# a moment later
docker compose exec db psql -U test -d imdb -c \
  "SELECT top_cast FROM read.movie_card WHERE tconst = 'tt0111161';"
```
</details>

## Clean up

```sh
docker compose down -v
```

## Next steps

- [README § SQL projections](../README.md#sql-projections) — the full
  projection / aggregate / enrichment reference.
- [docs/event-log-architecture.md](event-log-architecture.md) — why the log is
  the source of truth and the read model is disposable.
- [docs/consistency.md](consistency.md) — the consistency model.
