# Memory sizing

A committed node's steady-state footprint is modest — a few hundred MB under
heavy ingest — and one knob matters as deployments grow: the event-log segment
cache.

## The event-log segment cache — `COMMITTED_EVENT_CACHE_SEGMENTS`

The permanent event log is stored in ~20MB segment files (compressed to a
few MB at rest once sealed — see disk-limits § Event-log compression; a
resident cache slot always holds the DECOMPRESSED ~20MB, so compression
changes nothing about memory sizing). Reading an entry
requires its segment's parsed index in memory, and the node keeps a bounded
number of segments resident at once:

| Variable | Default | Purpose |
| --- | --- | --- |
| `COMMITTED_EVENT_CACHE_SEGMENTS` | `16` | How many event-log segments may be held parsed in memory at once. Each **resident** segment costs ~21MB; unused capacity costs nothing. Invalid values warn and keep the default. |

**Why it matters: every syncable replaying history is a concurrent reader,
holding one segment's working set.** A newly created (or rebuilt) syncable
reads the log from the beginning; several of them sit at different positions,
each needing its own segment resident. When the cache is smaller than the
reader count, every read by one reader evicts another's segment, forcing a
~20MB re-read per access — replay throughput collapses from memory speed to
disk-parse speed.

**Sizing rule of thumb: at least your concurrent syncable count + 2.**

- Worst-case memory = `COMMITTED_EVENT_CACHE_SEGMENTS × ~21MB` — but only when
  that many readers are actually spread across distinct segments; capacity is
  not preallocation.
- The default (16 ≈ up to ~340MB) is sized to be comfortable on a small (2GB)
  box. It is deliberately conservative for production: on a 16GB machine,
  `64` (~1.3GB worst case) is cheap insurance; size up freely with your RAM.
- Steady-state streaming (all syncables caught up) concentrates reads in the
  newest segments and needs almost none of this — the cache earns its memory
  during **replays**: initial sink builds, projection rebuilds, and rebuilt
  nodes catching sinks back up.

The raft entry log is not tunable and needs no tuning: its reader is the
single sequential consensus loop, which cannot thrash.

## The rest of the footprint (for capacity planning)

Measured under the heaviest ingest workload to date (17 concurrent table
snapshots plus a scrub): ~290MB of Go heap, dominated by the two logs' write
buffers (~150MB) and the scrub's working set (~85MB, transient, scales with
log size), plus bbolt's file-backed pages in RSS. Pull a live profile from
your own workload with [`COMMITTED_PPROF`](logging.md) — heap profiles are the
authoritative answer for your data shape.
