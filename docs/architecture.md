# Architecture

Committed is a single binary. Each node runs Raft consensus, keeps the
permanent event log, and runs the ingest and sync workers that move data in
and out of it. Writes become [Proposals](proposals.md); consensus orders them
into [Actuals](actuals.md) on the log; [syncables](syncables.md) project
Actuals out to the systems you query. Configs (types, databases, syncables,
ingestables) are written through the same log, so every node holds the same
configuration.

## Storage layout

A Committed node's data directory looks like this:

```
<datadir>/
├── raft/
│   ├── log/         # raft consensus log; bounded (10GB / 1hr)
│   └── state/       # HardState + ConfState
├── events/          # permanent event log; infinite retention
├── metadata/
│   └── bbolt.db     # applied state: configs, positions, dead letters, applied index
└── projections/     # projection stage stores; node-local, rebuildable from the log
```

The permanent event log under `events/` is the canonical record of every
write; syncables read from it, and it keeps every entry except what
right-to-be-forgotten erasure scrubs and the superseded entries of
`snapshot`-kind topics ([Actuals](actuals.md#the-log-is-permanent)). `raft/log/` is
consensus transport only and is compacted aggressively. `projections/` holds
derived state a projection's internal stages keep, which can always be
rebuilt by replaying the log.

## Design references

- [Event-log architecture](event-log-architecture.md) — why the permanent log
  is separate from the raft log, entity kinds, apply determinism, and
  right-to-be-forgotten erasure.
- [Storage architecture](storage-architecture.md) — the four stores, why raft
  persistence is two logs, and the crash-recovery invariants that hold them
  together.
- [Read consistency](consistency.md) — what committed guarantees for reads and
  writes.
- [API compatibility](api-compatibility.md) — the on-disk and wire
  compatibility contract that makes rolling upgrades safe.
- [Experimental event storage](event-segment-design.md) — the segment-log
  backend and its current limits.
