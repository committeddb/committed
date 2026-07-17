# Writing a webhook receiver

The `http` syncable delivers a topic's committed data to an HTTP endpoint you
control: committed `POST`s each committed transaction to your URL and you build
whatever you want behind it — a read model in a datastore committed doesn't
speak, a cache, a message bus, a downstream service. This is the counterpart to
[SQL read models](read-models.md): there, committed writes the projection and
guarantees its shape; here, **you** own the projection, so a few of committed's
guarantees become contracts *you* implement. This page is that contract.

## Configuring the syncable

```toml
[syncable]
name = "movies-mirror"
type = "http"
topic = "<type-id>"
url = "https://example.internal/committed/movies"
method = "POST"          # POST (default) or PUT
timeoutMs = 5000
# Static headers on every request — e.g. auth. Use ${VAR} interpolation
# (see docs/operations/secrets.md) to keep tokens out of the config.
[[syncable.headers]]
name = "Authorization"
value = "Bearer ${MOVIES_WEBHOOK_TOKEN}"
```

## The request

One committed **Actual** (a transaction at a single raft index) is delivered as
**one request** — never one request per entity — so you see the transaction
boundary intact:

- **Method**: your configured `POST`/`PUT`.
- **`Content-Type: application/json`**.
- **`Idempotency-Key: <n>`** — the Actual's raft index, a stable id for *this
  transaction*. A redelivery (after a retry, a leader change, or a restart)
  carries the **same** key. This is your dedup key (see [Idempotency](#idempotency-is-yours)).
- Any static headers from `[[syncable.headers]]`.

### Body

```json
{
  "entities": [
    { "op": "upsert",  "key": "bW92aWU6NDI=", "type": {"id":"6f…","name":"movie","version":3}, "data": {"title":"Dune"}, "generation": 7 },
    { "op": "delete",  "key": "bW92aWU6OTk=", "type": {"id":"6f…","name":"movie","version":3}, "generation": 7 },
    { "op": "refresh", "type": {"id":"6f…","name":"movie","version":3}, "generation": 7 }
  ]
}
```

Each entity:

| Field | Present for | Meaning |
|-------|-------------|---------|
| `op` | always | `"upsert"`, `"delete"`, or `"refresh"` — branch on this explicitly. |
| `key` | upsert, delete | **base64** of the raw entity key bytes. Your record identity. |
| `type` | always | `{id, name, version}` of the committed type the entity belongs to. |
| `data` | upsert | the entity's JSON body. Binary columns (Postgres `bytea`, MySQL `BLOB`/`BINARY`/`VARBINARY`) are **base64** strings — decode them to recover the raw bytes. Omitted for delete/refresh. |
| `generation` | when known | committed's reconciling-refresh epoch (see [refresh](#op-refresh--the-sweep)). Omitted/`0` for sources predating the feature or for direct writes. |

A proposal can span several topics; committed sends you **only** the entities
for this syncable's `topic`, so every entity in the body is yours.

## The three ops

### `op: "upsert"`
Insert or replace the record identified by `key` with `data`. **Store
`generation` alongside the record** — you need it for the sweep. If you never
plan to honor `refresh`, you can ignore `generation` and keep plain
upsert/delete behavior.

### `op: "delete"`
Remove the record identified by `key`. **A delete for a record that doesn't
exist MUST be a harmless no-op** — deletes replay on recovery, and this is also
the downstream half of right-to-be-forgotten erasure, so it must actually remove
the record, not tombstone it in a way that keeps the data.

### `op: "refresh"` — the sweep
Carries no `key`/`data`, only a `generation` **G**. It closes a *full refresh*:
committed re-emitted every live row of the topic at epoch G (an initial snapshot,
or a re-snapshot after a CDC gap). On receiving it, a keyed receiver **MUST**:

```
DELETE every record whose stored generation < G
```

That removes rows that were **deleted at the source during a lost change-data
window** — the upsert-only refresh re-stamped every *live* row to G, so anything
still below G no longer exists at the source and must go. This is exactly what
the SQL syncable does internally (`DELETE WHERE generation < G`); over the
webhook, committed hands you the same primitive and you run it.

A receiver that ignores `refresh` **degrades gracefully**: you keep every upsert
and delete committed sent, and lose only the reconciliation of source-side
deletes that happened during a gap. Honor it when your read model must not
retain rows the source has dropped.

The generation model, end to end:

```
initial snapshot: upsert 1,2,3 @g1 ; refresh g1  → sweep < g1  (no-op)
source deletes 2, then a CDC gap forces a re-snapshot:
re-snapshot:      upsert 1,3   @g2 ; refresh g2  → sweep < g2  (removes 2)
```

## Idempotency is yours

Delivery is **at-least-once**. committed redelivers a transaction on retry,
leader change, or restart (bounded by `checkpointEvery`, default `1` for a
webhook — at most one duplicate). Unlike a SQL sink, whose `UPSERT`/`DELETE` are
idempotent by construction, committed **cannot** make an arbitrary endpoint
idempotent — so **you** must:

- Dedup on **`Idempotency-Key`** (the raft index): if you've already durably
  applied that key, ack and do nothing. Upserts and deletes keyed by `key` are
  naturally replay-safe (re-applying is a no-op); the key protects any
  *non-idempotent* side effect you trigger (sending an email, charging a card).

## Responding

Your status code tells committed what to do next:

| Status | committed's action |
|--------|--------------------|
| `2xx` | Success — checkpoint advances. Return this **only once you've durably applied** the transaction. |
| `408`, `429`, `5xx` | Transient — **retry** the same request later. Use `5xx`/`429` to apply backpressure. |
| other `4xx` (400, 404, 405, 422, …) | Permanent — the transaction is **dead-lettered** (see [stuck syncables](operations/stuck-syncables.md)) and the syncable moves on. Return this only for a genuinely un-processable payload. |

Because a permanent `4xx` skips the record, prefer `5xx` when in doubt: a retry
is safe (you dedup), and a persistently failing endpoint **wedges visibly** for
an operator instead of silently dead-lettering everything.

## A minimal receiver

```python
seen = set()  # persist this; Idempotency-Key = raft index

def handle(request):
    key = request.headers["Idempotency-Key"]
    if key in seen:                      # at-least-once → dedup
        return 200
    with db.transaction():               # apply the whole Actual atomically
        for e in request.json["entities"]:
            if e["op"] == "upsert":
                db.upsert(e["key"], e["data"], generation=e.get("generation", 0))
            elif e["op"] == "delete":
                db.delete(e["key"])                       # no-op if absent
            elif e["op"] == "refresh":
                db.delete_where("generation < %s", e["generation"])  # the sweep
        db.record_applied(key)
    seen.add(key)
    return 200                           # durably applied → 2xx
```

Ignore `refresh`/`generation` and drop the last branch if you don't need
source-delete reconciliation — everything else is unchanged.
