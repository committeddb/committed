# Memory sizing

A committed node's steady-state footprint is modest — a few hundred MB under
heavy ingest, plus the projection store's budget — and two sets of knobs
matter as deployments grow: the event-log caches and the projection store.

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

## Projection store

Projections with stages keep their stage state in one database per node
(`<dataDir>/projections/`), shared by every projection on the node. Its
memory is a block cache, and the store takes about the cache's size: the
write buffers (memtables, up to two held while one flushes) are carved out of
the cache rather than added to it, and what they leave caches blocks:

| Variable | Default | Purpose |
| --- | --- | --- |
| `COMMITTED_PROJECTION_STORE_CACHE_BYTES` | `268435456` (256 MiB) | Cache of the store's blocks, shared by every projection on the node. |
| `COMMITTED_PROJECTION_STORE_MEMTABLE_BYTES` | `33554432` (32 MiB) | Size of one write buffer. Must be under 4 GiB. |

Values are positive decimal byte counts; invalid values fail startup. The
defaults total about 256 MiB, of which about 192 MiB caches blocks. Keep two
memtables well under the cache: when they take all of it, nothing is cached
and builds run at about a third of the speed.

**Why it matters:** a projection build looks up the stage state of nearly
every event it folds. While the store's index and hot state fit in the
cache, those lookups stay in memory; once they don't, each one reads from
disk and builds slow down sharply (with a 64 MiB cache, a 10M-event build ran
at half the speed of the default). Give the cache more memory when the node
builds large projections or hosts many: on a 16 GiB node, several GiB is
reasonable. The default memtable is enough: 128 MiB and 512 MiB memtables
built a 10M-event projection no faster.

The raft entry log is not tunable and needs no tuning: its reader is the
single sequential consensus loop, which cannot thrash.

## The rest of the footprint (for capacity planning)

Measured under the heaviest ingest workload to date (17 concurrent table
snapshots plus a scrub): ~290MB of Go heap, dominated by the two logs' write
buffers (~150MB) and the scrub's working set (~85MB, transient, scales with
log size), plus bbolt's file-backed pages in RSS. The projection store's
budget (above) comes on top. Pull a live profile from
your own workload with [`COMMITTED_PPROF`](logging.md) — heap profiles are the
authoritative answer for your data shape.
