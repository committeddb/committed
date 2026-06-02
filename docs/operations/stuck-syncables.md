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
  (~30 seconds), `0` otherwise. Prometheus scrapes each node directly, so an
  alert like `max by (syncable_id) (committed_sync_stuck) == 1 for 5m`
  fires regardless of which node holds the worker.
- **Status endpoint.** `GET /syncable/{id}/status` reports it on demand:

  ```bash
  curl -H "Authorization: Bearer $TOKEN" \
    http://node:8080/syncable/orders/status
  # {"stuck":true,"index":4123,"since":"2026-06-02T14:00:00Z",
  #  "message":"ERROR: value too long for type character varying(20)"}
  ```

  `index` is the raft index it's wedged on; `message` is the last error.
- **Corroborating signals.** `committed_sync_errors_total{syncable_id,kind="transient"}`
  climbs with no progress, the syncable's persisted index stops advancing,
  and the worker logs `transient sync error, will retry` on each attempt.

The stuck threshold debounces the signal so a normal blip that recovers in a
few seconds never flags. It is currently fixed at **30 seconds** (tunable in
a future release).

## Unsticking it

When you've decided a proposal will genuinely never apply — the destination
is healthy, but *this* row won't go in — skip it:

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" \
  http://node:8080/syncable/orders/deadletter/
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
  http://node:8080/syncable/orders/errors
# [{"index":4123,"timestamp":"2026-06-02T14:05:00Z","kind":"manual",
#   "message":"ERROR: value too long for type character varying(20)"}]
```

`kind` says why the proposal was skipped:

- `permanent` — the syncable rejected it automatically.
- `manual` — you skipped it via `.../deadletter/`.

Dead letters survive restart: the worker excludes an already-dead-lettered
proposal on re-read, so a stuck syncable you've cleared does not re-wedge
when its node restarts. Page forward through a long list with
`?since=<last index>&limit=<n>`.

## Re-driving a dead letter (replay)

Once you've fixed the destination — the column exists now, the outage is
over — re-drive a dead-lettered proposal and clear its record:

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" \
  http://node:8080/syncable/orders/replay/4123
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

## What is *not* (yet) done

- **Bulk replay.** Replay is one index at a time; there is no "retry
  everything since X" yet. Page through `GET .../errors` and replay each.
- **Auto-classification.** Improving how syncables classify errors (so more
  genuinely-permanent failures dead-letter automatically and fewer need this
  loop) is ongoing.
