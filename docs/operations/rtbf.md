# Right-to-be-forgotten erasure

How to erase a data subject from committed and everything it feeds: issue the
deletes, let the machinery propagate and physically scrub, verify completion
with an end condition you can poll, and know what can delay it. This is the
operator how-to; the design (why the log can be both permanent and erasable)
is in [event-log-architecture.md](../event-log-architecture.md#right-to-be-forgotten--deletes).

Erasure is three stages, the first yours and the rest committed's:

1. **You issue logical deletes** — one per `(type, key)` the subject owns.
2. **The scrub physically removes the subject's data** from every node's
   permanent event log (the automatic scheduler, or your expedited request),
   while syncables propagate downstream `DELETE`s to the sinks they maintain.
3. **Delete-key erasure removes the last identifier**: the retained delete
   tombstone's key — kept raw only while some syncable still needs it to
   erase its downstream row — is rewritten to a fixed PII-free sentinel once
   every consumer has provably processed the delete.

## 1. Issue the deletes

Resolving a subject to its `(type, key)` pairs is domain-specific and lives
outside committed — the standard pattern is to query a projection you already
maintain for the relevant keys. Then, for each pair:

```sh
curl -X POST -H 'Content-Type: application/json' --data-binary @- \
  http://localhost:8080/v1/proposal <<'EOF'
{ "entities": [ { "typeId": "customer-event", "key": "subject-12345", "delete": true } ] }
EOF
```

A delete carries no payload and covers the key's whole history: one delete
per `(type, key)` is enough regardless of how many events the subject has.
Every syncable consuming the topic translates it downstream in log order — a
SQL sink executes `DELETE ... WHERE key = ...`, a webhook receiver gets
`op: "delete"` — so the read models you maintain through committed converge
on their own. After all deletes are committed, note any node's
`appliedIndex` (`GET /v1/node/status`): that index is your erasure watermark
for the verification loop below.

## 2. Physical scrub — automatic, or expedited

Each node's scrubber physically rewrites its permanent event log, removing
the subject's data (the delete tombstones are retained; their keys are
erased in stage 3). The **automatic scheduler** proposes a scrub whenever
there is erasure work outstanding, on the `COMMITTED_SCRUB_INTERVAL` cadence
(default `1h`; a duration string; `0` disables it — don't disable it if you
have RTBF obligations). For SLA-expedited erasure, trigger one now:

```sh
curl -X POST http://localhost:8080/v1/scrub
```

A scrub is an `O(log-size)` rewrite per node, run off the consensus path —
safe to trigger, but not free; expedite when an obligation clock is running,
and let the cadence handle routine housekeeping.

## 3. Verify: the erasure end condition

Each node rewrites its own log, so verification is **per node**. Poll
`GET /v1/node/status` on every node and read the `scrub` block:

```json
"scrub": { "pendingBound": 8123, "completedBound": 8123, "pendingDeleteKeyErasures": 0 }
```

- **Subject data erased on this node** once `completedBound >=` your erasure
  watermark (the `appliedIndex` you noted after the deletes committed). At
  that point no upsert of the subject survives in this node's log.
- **Erased-subject identifiers fully erased** once, additionally,
  `pendingDeleteKeyErasures` is `0` — every retained tombstone's raw key has
  been rewritten to the PII-free sentinel. (The count covers all pending
  erasures on the node, not one subject; it is the number that must reach
  zero for identifier erasure to be complete for anyone.)

The identifier half deliberately lags the data half: each raw key stays
exactly as long as some registered syncable still needs it to delete its
downstream row, and not a scrub longer. A nonzero count shortly after an
erasure is normal — watch it shrink across scrub passes; a count that stops
shrinking means a consumer is holding the gate (next section).

## What can delay erasure — and what to do

- **A lagging syncable** holds the delete-key erasure gate until its
  checkpoint passes the delete. Catch-up is fast (see
  [performance.md](performance.md)); this self-resolves.
- **A stuck syncable** holds the gate indefinitely — `pendingDeleteKeyErasures`
  stops shrinking, and erasure of every subject's identifier waits on it. Stuck syncables are already loud
  ([stuck-syncables.md](stuck-syncables.md)); fix it, or delete and recreate
  it — a syncable created after the covering scrub cannot have seen the
  erased data, so a recreate releases the gate immediately.
- **A rebuilding or freshly created syncable** pins its node's rewrite swap
  while it replays from index 0 (the log line says `scrub swap waiting on
  in-flight from-0 log reads`). The rewrite resumes when the replay catches
  up; a replay stalled on its sink delays that one node's scrub.
- **Deleting a syncable does not erase its sink.** A deleted syncable's
  downstream rows are yours: if it never processed the delete, run the
  downstream `DELETE` yourself. Deletion also removes the syncable from the
  erasure gate — committed will not hold every subject's erasure hostage to
  a sink nobody manages anymore.

## Adjacent copies and retention you own

Erasure in committed does not reach copies it doesn't manage:

- **The raft consensus log** holds a transient copy until its normal
  compaction passes — typically about an hour; bounded, never read by
  syncables, never in a snapshot.
- **Backups** taken before the erasure still contain the subject. Bound
  backup retention to your obligations, and after any restore, re-issue
  deletes committed after that backup was taken —
  [backup.md](backup.md#shared-responsibility-backups-and-right-to-be-forgotten).
- **Node logs** carry customer data by design — bound their retention
  ([logging.md](logging.md)).
- **Databases you point syncables at** got real `DELETE`s, but their own
  backups/replicas/logs are your domain.

## Why there is no per-subject ledger

committed keeps no durable record of which subjects were erased: such a
ledger would itself be a permanent store of erased-subject identifiers — the
exact artifact RTBF exists to remove. Track erasure requests in your own
compliance system (which you need anyway for the re-issue-after-restore
step), using the erasure watermark from step 1 as the completion reference.
