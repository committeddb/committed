# Memory sizing

A committed node's steady-state footprint is modest — a few hundred MB under
heavy ingest — and one pair of knobs matters as deployments grow: the
event-log caches.

## Event-log caches

The permanent event log is stored in segment files compressed in blocks, and
reading a record decodes its block. The node keeps recently used segments in
memory, in two tiers with separate byte budgets:

| Variable | Default | Purpose |
| --- | --- | --- |
| `COMMITTED_EVENT_CACHE_RECENT_BYTES` | `167772160` (160 MiB) | Retain recent segments, evicting older ones as new segments arrive. |
| `COMMITTED_EVENT_CACHE_HISTORICAL_BYTES` | `167772160` (160 MiB) | Retain historical segments using LRU on segment acquisition. |

Values are non-negative decimal byte counts; zero disables that tier. Invalid
values fail startup. The budgets do not preallocate memory. Entries shared by
readers remain immutable, and eviction does not invalidate an active reader.
The active tail, reader-owned copies, and evicted entries still used by readers
can consume memory beyond these budgets. Budgets are not persisted and can change
between restarts. Setting both to zero disables caching.

**Why it matters:** steady-state streaming (all syncables caught up) reads the
newest segments, which the recent tier holds. The historical tier earns its
memory during **replays** — initial destination builds, projection rebuilds,
and rebuilt nodes catching destinations back up — where several readers sit at
different positions in old history. Size it up with your RAM when you run many
replays at once.

The raft entry log is not tunable and needs no tuning: its reader is the
single sequential consensus loop, which cannot thrash.

## The rest of the footprint (for capacity planning)

Measured under the heaviest ingest workload to date (17 concurrent table
snapshots plus a scrub): ~290MB of Go heap, dominated by the two logs' write
buffers (~150MB) and the scrub's working set (~85MB, transient, scales with
log size), plus bbolt's file-backed pages in RSS. Pull a live profile from
your own workload with [`COMMITTED_PPROF`](logging.md) — heap profiles are the
authoritative answer for your data shape.
