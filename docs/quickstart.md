# Quickstart

Go from a **normalized** movie catalog in Postgres to **one denormalized table
you query with no joins** — actor and director *names* and all — in one
`docker compose up` and four `curl`s.

The payoff, at the end, is this single-table query:

```sql
SELECT title FROM read.movie_card
WHERE top_cast @> '[{"name":"Organ Freeman"}]';
--           title
-- --------------------------
--  The Slawshank Redemption
```

No join. The cast names live right in the row — committed resolved them from a
separate `person` topic while folding four normalized tables into one.

A finished `read.movie_card` row:

```
 title     | The Slawshank Redemption
 year      | 1996
 genres    | Drama
 score     | 9.4
 votes     | 2600000
 top_cast  | [{"billing": 1, "person_id": "pn0000001", "name": "Tim Dobbins"},
              {"billing": 2, "person_id": "pn0000002", "name": "Organ Freeman"}]
 directors | [{"person_id": "pn0000003", "name": "Frank Marinara"}]
```

> The catalog is entirely fictional — the films and people are puns, not data.

**Prerequisites:** Docker (with Compose) and this repo cloned. committed itself
builds inside the compose — nothing else to install.

> committed is a commit log, not a query engine: you never query the source
> tables, you query the read model it maintains. This quickstart makes that split
> physical — an `ingress` schema you ingest *from* and a `read` schema you query.

---

## 1. Start everything

```sh
cd examples/movies
docker compose up --build -d
```

One command brings up committed and a Postgres that's **preloaded** with the
movie catalog. First run builds the committed image (a minute or two); after
that it's seconds.

<details><summary>What's running, and the two schemas</summary>

- **db** — Postgres with logical replication, preloaded on first boot from
  [`source.sql`](../examples/movies/source.sql):
  - schema **`ingress`** — the normalized source tables (`movie`, `rating`,
    `credit`, `person`). committed ingests *from* here; you never query it.
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
for t in movie rating credit person; do post "type-$t.toml"   "/v1/type/$t"; done
for i in movie rating credit person; do post "ingest-$i.toml" "/v1/ingestable/$i-ingest"; done
```

Each ingestable snapshots its `ingress` table into a topic, then streams live
changes. Ask any ingestable how it's doing — snapshot vs. streaming, and whether
it has caught up to the source:

```sh
curl -fsS localhost:8080/v1/ingestable/movie-ingest/status
# {"phase":"streaming","snapshotProgress":[{"table":"ingress.movie","complete":true}],
#  "position":"0/1A2B3C8","lag":0,"caughtUp":true}
```

When each ingestable reads `"caughtUp":true`, its snapshot is done and the topic
is tracking the source live. (`lag` is bytes behind the source; `phase` is
`snapshot` until the initial dump finishes.)

<details><summary>What each config is</summary>

- **[`db-bff.toml`](../examples/movies/db-bff.toml)** — the sync target. Its
  connection string sets `search_path=read`, so the read model is created and
  queried in the `read` schema.
- **`type-*.toml`** — declares the four topics as `snapshot` kind (current-state
  rows, the natural shape for a table mirror).
- **`ingest-*.toml`** — pulls each `ingress` table into its topic over Postgres
  logical replication: an initial snapshot of existing rows, then the live tail.
  Each is just a connection, a table, and `mapAllColumns = true` — that mirrors
  every column 1:1, no per-column boilerplate. `credit` has a **composite key**
  `["movie_id", "billing"]` — many cast/crew per movie — so the rows don't collide.

See [README § API tour](../README.md#api-tour) for the config shapes.
</details>

## 4. Build the read model — one projection

```sh
post sync-movie-card.toml /v1/syncable/movie-card
```

<details><summary>What this one syncable does</summary>

[`sync-movie-card.toml`](../examples/movies/sync-movie-card.toml) folds **four
topics into one row**:

- **`movie`** → the spine (descriptive columns; its delete drops the row).
- **`rating`** → a contributor (its columns; its delete clears them, row stays).
- **`person`** → a *dimension*: `person_id → name`, joined into the arrays.
- **`credit`** → split by `role` into two array columns — `top_cast` (actors)
  and `directors` — and each element is **enriched** with the person's name from
  the `person` dimension.

That last part is why `top_cast` carries `"name": "Organ Freeman"` and not just
an ID. The full mechanics — multisource folds, split aggregates, enrichment —
are in [README § SQL projections](../README.md#sql-projections).
</details>

## 5. Query the read model

Give the sync a few seconds, then:

```sh
docker compose exec db psql -U test -d catalog -c \
  "SELECT title, year, score,
          top_cast->0->>'name'  AS lead_actor,
          directors->0->>'name' AS director
   FROM read.movie_card ORDER BY score DESC;"
```

```
          title           | year | score |   lead_actor   |       director
--------------------------+------+-------+----------------+----------------------
 The Slawshank Redemption | 1996 |   9.4 | Tim Dobbins    | Frank Marinara
 The Codfather            | 1974 |   9.1 | Marlin Blando  | Francis Fjord Cupola
 The Bark Knight          | 2011 |   8.8 | Christian Kale | Christopher No-Land
```

And the headline — find movies by an actor's **name**, one table, no join:

```sh
docker compose exec db psql -U test -d catalog -c \
  "SELECT title FROM read.movie_card
   WHERE top_cast @> '[{\"name\":\"Organ Freeman\"}]';"
```

That's the whole point of committed: a denormalized read model you query
directly, kept in sync with a normalized source you never touch.

<details><summary>Optional: watch a live change flow through</summary>

The ingest is also streaming changes. Rename an actor in the source and re-query
— committed re-materializes every movie that cast them (the enrichment fan-out):

```sh
docker compose exec db psql -U test -d catalog -c \
  "UPDATE ingress.person SET name = 'Organ Freeman ⭐' WHERE person_id = 'pn0000002';"

# a moment later
docker compose exec db psql -U test -d catalog -c \
  "SELECT top_cast FROM read.movie_card WHERE movie_id = 'mv0000001';"
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
