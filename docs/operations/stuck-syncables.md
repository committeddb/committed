# Stuck syncables and dead letters

This runbook is for operators. It explains how a syncable can stall, how
to tell when one has, how to unstick it, and what gets recorded when you
do. A syncable is the output-side projection that replays the log into an
external system (a SQL table, a webhook); when its destination misbehaves,
this is the loop you'll run.

## How a syncable fails

A syncable's worker walks the log and calls `Sync` for each proposal. Two
failure shapes are handled very differently:

- **Permanent error** — the syncable declares the proposal will never apply
  (a constraint violation, malformed data; `cluster.Permanent`). The worker
  **dead-letters it and moves on automatically.** No operator action needed.
- **Transient error** — anything else (a timeout, a deadlock, the
  destination is down). The worker **retries it forever.** This is
  deliberate: a transient failure is usually the *destination's* problem,
  not the proposal's, so the worker stalls *visibly* rather than skipping —
  and possibly losing — data on a guess.

The consequence: a syncable wedged against a downstream that will *never*
accept one particular proposal (a row the schema rejects, say) retries that
one proposal forever and never advances. It does not lose data, but it does
not make progress either. That's a **stuck** syncable, and clearing it is a
human decision — see [Unsticking it](#unsticking-it).

## Telling a syncable is stuck

The signal is replicated, so **any node** answers — you don't need to find
the one running the worker, and it survives a leader change.

- **Metric (alert on this).** `committed_sync_stuck{syncable_id}` is a gauge
  that goes to `1` once a worker has been blocked past the stuck threshold
  (~30 seconds), `0` otherwise. Every node pushes it via OTLP to your collector
  (see [metrics.md](metrics.md)), so an alert like
  `max by (syncable_id) (committed_sync_stuck) == 1 for 5m` fires regardless of
  which node holds the worker.
- **Status endpoint.** `GET /v1/syncable/{id}/status` reports it on demand:

  ```bash
  curl -H "Authorization: Bearer $TOKEN" \
    http://node:8080/v1/syncable/orders/status
  # {"stuck":true,"index":4123,"since":"2026-06-02T14:00:00Z",
  #  "message":"ERROR: value too long for type character varying(20)",
  #  "checkpointIndex":4122,"headIndex":4200,"lag":78,"caughtUp":false}
  ```

  `index` is the raft index it's wedged on; `message` is the last error.
  The `index`/`since`/`message` fields are present only when `stuck` is true.
- **Corroborating signals.** `committed_sync_errors_total{syncable_id,kind="transient"}`
  climbs with no progress, the syncable's persisted index stops advancing,
  and the worker logs `transient sync error, will retry` on each attempt.

The stuck threshold debounces the signal so a normal blip that recovers in a
few seconds never flags. It is currently fixed at **30 seconds** (tunable in
a future release).

## Telling how far behind a syncable is (lag)

The status endpoint also answers the everyday question — *is this syncable
caught up, and if not, by how much?* — with four fields that are **always
present**, stuck or not:

- `checkpointIndex` — how far the syncable has processed (its persisted
  resume cursor).
- `headIndex` — how far there is to process: the highest **user-topic-data**
  entry on this node. It deliberately excludes committed's internal entries —
  its configs and all coordination (index bumps, dead-letter records, ingest
  positions, …), which never reach a syncable — so it is **not** the cluster
  commit index.
- `lag` — `max(0, headIndex − checkpointIndex)`, i.e. how many data entries
  are still to go. Render "synced through X of Y" as `checkpointIndex` of
  `headIndex`.
- `caughtUp` — `true` exactly when `lag == 0`.

**The guarantee:** `lag == 0` ⇔ the syncable has nothing left to do, for both
SQL and webhook-style syncables. Computing lag against the raw commit index
would be wrong — it counts the syncable's own progress bumps and every other
syncable's traffic, so a perfectly caught-up syncable would show a permanent
non-zero "backlog". `headIndex` counts only what a syncable actually consumes,
which is what makes `0` mean `0`.

The read is O(1) and answerable from **any node** without a leader hop, so it
is safe to poll (e.g. every ~5s) for a dashboard "caught up / N behind"
indicator. Pass `?consistency=stale` to skip the quorum round-trip on a
follower if you can tolerate slightly stale numbers.

**One caveat for selective syncables.** A syncable that consumes only some
topics advances its checkpoint past entries on *other* topics that it reads
and cheaply skips (this keeps its restart cost low — it never re-scans a
firehose of other-topic entries). Under sustained cross-topic write load its
`lag` can therefore transiently overstate its own backlog while it reads
through (and skips) other topics to advance its consumed cursor. It returns to
`0` at rest. The number answers "is it caught up?" correctly; it is not a
per-topic backlog count.

## Unsticking it

When you've decided a proposal will genuinely never apply — the destination
is healthy, but *this* row won't go in — skip it:

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" \
  http://node:8080/v1/syncable/orders/deadletter
# 202 {"index":4123}   — the worker will skip raft index 4123 and advance
# 409                  — the syncable isn't currently blocked
```

It works from **any node** (the request goes through Raft to wherever the
worker runs), so it's safe behind a load balancer. It skips the proposal the
syncable is *currently* blocked on — you don't pass an index; the system
uses the one from the status above. The worker honors it on its next retry.

- **Single-proposal syncable:** the wedged proposal is dead-lettered and
  skipped.
- **Batch syncable** (the SQL syncables): a batch fails atomically, so the
  worker *isolates* it — re-runs the batch one proposal at a time,
  dead-letters only the proposal that actually fails, and lets the healthy
  proposals in the same batch through.

> **Use it when the destination is healthy but one proposal won't apply —
> not during an outage.** During an outage the syncable is *correctly*
> waiting; skipping would dead-letter good data one proposal at a time. Let
> it wait and fix the downstream instead.

## Verifying, and what gets recorded

A skip is recorded as a **dead letter** — a durable, replicated pointer back
to the dropped proposal (the proposal itself stays in the log). Confirm it
landed:

```bash
curl -H "Authorization: Bearer $TOKEN" \
  http://node:8080/v1/syncable/orders/errors
# [{"index":4123,"timestamp":"2026-06-02T14:05:00Z","kind":"manual",
#   "message":"ERROR: value too long for type character varying(20)"}]
```

`kind` says why the proposal was skipped:

- `permanent` — the syncable rejected it automatically.
- `manual` — you skipped it via `.../deadletter`.

Dead letters survive restart: the worker excludes an already-dead-lettered
proposal on re-read, so a stuck syncable you've cleared does not re-wedge
when its node restarts. Page forward through a long list with
`?since=<last index>&limit=<n>`.

## Re-driving a dead letter (replay)

Once you've fixed the destination — the column exists now, the outage is
over — re-drive a dead-lettered proposal and clear its record:

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" \
  http://node:8080/v1/syncable/orders/replay/4123
# 200                — re-synced; the dead-letter record was cleared
# 404                — raft index 4123 isn't a dead letter for this syncable
# 502 {... details}  — the syncable rejected it again; record left in place,
#                      the failure cause is in the error's details field
```

Replay re-runs the syncable's `Sync` once for that proposal against a fresh
build of its current config, and on success removes the dead letter. It is
node-agnostic (config, the proposal, and the dead-letter store are all
replicated) and safe to retry — `Sync` is idempotent, so replaying a
proposal that already applied is a no-op at the sink. A `502` means the
downstream *still* won't take it: read the `details`, fix the cause, and
replay again.

> **Replay applies the proposal out of order — mind superseding writes.**
> The worker skipped the dead letter and kept going, so by now the
> downstream reflects *every later proposal* but not this one. Replaying it
> re-applies it **last**, after those later proposals. For a key only this
> proposal touched, that's exactly right. But if a **newer** proposal for the
> **same key** has already applied — say this one was `insert K=old` and a
> later `update K=new` already landed — replaying the old one re-upserts
> `K=old` *on top of* `K=new` and reverts it.
>
> Two rules keep you safe:
>
> - **Replay in index order.** If several dead letters touch the same key,
>   replay the lowest index first so the newest lands last.
> - **Check for newer writes.** Before replaying an old dead letter, make
>   sure nothing newer has written the same key since — otherwise you'll
>   clobber it. (When in doubt, the safe replays are the most recent dead
>   letters, or ones for keys nothing else has touched.)

## Migration failures (always-current syncables)

A syncable in `always-current` mode runs each entity through its type's
migration chain (the jq programs registered on type versions) before `Sync`
sees it. The programs are compiled when the type version is proposed, so
syntax errors never reach production — but a program that works on 99% of
payloads can still fail at runtime on the edge case. That failure is a
**permanent** error: the worker dead-letters the proposal for the syncable
exactly as above, and additionally records it **against the type**, naming
the failing chain step:

```bash
curl -H "Authorization: Bearer $TOKEN" \
  http://node:8080/v1/type/person/migration-errors
# [{"index":4123,"fromVersion":1,"toVersion":2,
#   "timestamp":"2026-06-09T14:05:00Z",
#   "message":"jq runtime: cannot derive email for alice"}]
```

`toVersion` is the broken program. The same failure increments
`committed_type_migration_errors_total{type_id,from_version,to_version}` —
alert on that, not on log lines. (Successful chains feed
`committed_type_migration_duration_seconds{type_id}`.)

The recovery loop:

1. **Fix the program.** Re-`POST /v1/type/{id}` with the corrected
   `transform` and the schema unchanged — that updates the current
   version's migration in place, no version bump.
2. **Verify against the payload that broke it.**
   `POST /v1/type/{id}/migration-retry/{index}` re-runs the chain on the
   dead-lettered proposal: `200` clears the type-keyed record, `502` means
   the chain still fails (cause in `details`), `404` means the index isn't
   a migration dead letter for that type.
3. **Deliver.** The retry validates the chain only — the proposal is still
   dead-lettered for each syncable that skipped it. Replay those with
   `POST /v1/syncable/{id}/replay/{index}` (see above), which re-runs the
   now-fixed chain on the way to the destination. The out-of-order caveat
   above applies here too.

To catch the gremlin *before* it dead-letters anything, pre-flight the
program when you propose it: add `validateAgainst = '<sample JSON>'` (a
document in the previous version's shape) to the `[migration]` section and
the propose fails with `400` if the transform errors on it.

## What is *not* (yet) done

- **Bulk replay.** Replay is one index at a time; there is no "retry
  everything since X" yet. Page through `GET .../errors` and replay each.
- **Auto-classification.** Improving how syncables classify errors (so more
  genuinely-permanent failures dead-letter automatically and fewer need this
  loop) is ongoing.
- **Bulk migration retry.** Like replay, `migration-retry` is one index at
  a time. A fixed program usually clears a batch of identical failures —
  page through `GET .../migration-errors` and retry each.
