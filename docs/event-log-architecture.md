# Event Log Architecture

**Status:** Proposed (2026-04-09)

This document describes how Committed stores, replicates, and manages its
application event log. It exists because the standard Raft model — log as
throwaway transport, snapshot-and-compact aggressively — does not fit
Committed's role as a CQRS event log where the events themselves are the
product.

This document is the source of truth for the architecture. Implementation
tickets reference it; if a ticket conflicts with this doc, this doc wins.
Update the doc first, then the tickets.

---

## Why this exists

Committed's value proposition is being a CQRS event log: applications write
events to a totally-ordered log, and downstream "syncables" project those
events into query-optimized databases. The two hard problems in any CQRS
system are:

1. **Keeping existing query databases up to date** — the steady-state
   replication problem.
2. **Bootstrapping new query databases** — when a new query need emerges,
   you need to replay the entire history of relevant events into a new
   projection.

Problem 2 is the central one. It means **the event log must have infinite
retention**. You cannot compact away events that a future query database
may need to bootstrap from.

This conflicts with Raft's standard storage model. Raft assumes the log is
a means to an end: get entries replicated and applied to a state machine,
then compact them away to bound disk usage. If we follow that model, we
lose the ability to bootstrap new query databases — i.e., we lose the
central feature.

The fix is to separate the two concerns:

- **Raft log**: short, bounded, just consensus transport.
- **Permanent event log**: append-only, infinite retention, the actual
  product output.

This document describes how that separation works.

---

## Goals

- **Infinite retention of application events.** Every event committed to
  the cluster lives forever (subject only to right-to-be-forgotten
  deletes — see below).
- **Bootstrap from index 1.** A newly-created syncable starts at the first
  event in the log and reads forward.
- **Cross-type total ordering preserved.** All events across all topics
  share one total order. This is the central differentiator from
  per-topic systems like Kafka, and it is non-negotiable for v1.
- **Single binary, no external dependencies.** No Kafka, no S3, no
  separate consensus service. Operating Committed should be operating one
  process per node.
- **Read fan-out to followers (eventual).** Once steady-state, syncable
  reads should be servable from any node, not just the leader.
- **Operationally tractable rebuild.** When a follower falls too far
  behind to catch up via normal raft replication, an operator can rsync
  from a healthy peer and restart.

## Non-goals (v1)

These are explicitly out of scope for the initial design. Some are deferred
to future work; others may never be needed.

- **Tiered storage** to object stores (S3 etc). AWS i7ie offers 120TB local
  NVMe per node; tiered storage is not necessary at the scale we are
  targeting.
- **Automated catch-up** of severely-behind followers. v1 is manual rsync
  triggered by a fatal-exit error message; automation is v2 future work.
- **Per-topic raft groups.** Cross-type ordering is preserved by having
  one raft group. Sharding is a v3+ decision and only if a single raft
  group hits a write-throughput ceiling.
- **Linearizable reads from followers.** Syncables read historical events,
  which do not need linearizability. Live state queries (e.g., "list
  current syncables") still go through the leader.
- **Custom permanent log format.** v1 uses tidwall/wal as a placeholder.
  A custom format will come once we know what the workload demands and
  measurements tell us tidwall/wal is the bottleneck.

---

## Architecture overview

### Three storage tiers

A Committed node has three storage tiers, each with very different
characteristics:

| Tier                  | Purpose                          | Retention                   | Size       | Compactable?       |
|-----------------------|----------------------------------|-----------------------------|------------|--------------------|
| Raft log              | Consensus transport              | 10GB or 1hr (whichever first) | Bounded    | Yes (aggressively) |
| Permanent event log   | Application events; product output | Infinite (modulo deletes) | TB scale   | No (only via deletes) |
| Metadata buckets (bbolt) | Type/database/syncable/ingestable definitions | Lifetime of cluster | MB scale | Snapshottable    |

Roughly: the raft log is the "now", the permanent event log is the
"forever", and the metadata buckets are the "control plane."

### Write paths

There are two distinct write paths depending on the entity type. Today
both share the same code; that is the bug this architecture fixes.

**Event proposals** (the 99.999% case):

1. Client posts a proposal to any node.
2. Proposal forwarded to leader if needed (raft handles forwarding).
3. Leader proposes through raft.
4. Raft replicates to followers.
5. On commit, each node's `ApplyCommitted` runs.
6. For event entities, `ApplyCommitted` appends to that node's local
   permanent event log. **Per-write `fsync` for durability** (matches raft
   semantics).
7. Once the entry is durably in the permanent event log, it becomes
   "graduated" — eligible for raft log compaction once a quorum has
   graduated.

**Metadata proposals** (~0.001%):

1. Same path through raft, but `ApplyCommitted` writes to bbolt buckets
   instead. Slow path is fine at this rate.

### Read path

**Syncables read exclusively from the local permanent event log.** They
never touch the raft log. Reads are addressed by raft index (the stable
identifier — see "Indexing" below). The raft log retention window is
irrelevant to syncables; they only care about the permanent event log.

Today in `internal/cluster/db/wal/reader.go`, syncables read from the raft
entry log via `Reader.Read()`. After this work lands, `Reader` reads from
the permanent event log instead. The interface stays the same; the
backing store changes.

### On-disk layout

```
<datadir>/
├── raft/
│   ├── log/         # raft consensus log; bounded ~10GB; tidwall/wal
│   └── state/       # HardState + ConfState (currently state-log/)
├── events/          # permanent event log; infinite retention; terabytes
├── metadata/
│   └── bbolt.db     # types, databases, ingestables, syncables, syncableIndex, appliedIndex
└── time-series/     # derived view; rsync'd for speed but regenerable from events
```

`events/` at the top level makes the rebuild story trivial to explain and
script: "rsync `events/`, `raft/`, `metadata/`, `time-series/` from a
peer." Different I/O profiles per tier mean operators can put `raft/` on a
smaller fast device if they want; not v1 work but the layout supports it.

### Indexing

**Syncable positions are tracked by raft index, never by file offset.**
Raft index is monotonic, stable across right-to-be-forgotten deletes (the
index sequence stays dense; deletes nullify content but never remove an
index slot), and authoritative.

The permanent event log's on-disk format will eventually be a `.log` file
plus a sparse `.index` file mapping `raft_index → byte_offset`. The sparse
index is a hint: if a lookup at the hinted offset doesn't match the
requested raft index (because of segment rewrites from delete sweeps), the
reader scans forward until it finds the right entry, then updates the
syncable's hint.

For v1, we use tidwall/wal as the substrate, with a thin wrapper that
maps raft index to wal sequence number. The custom format with sparse
index is a later optimization and will get its own ticket once we have
production data telling us what to optimize for.

---

## Compaction policy

The raft log is compacted when **all** of these are true:

- Size > 10GB **or** age > 1 hour (whichever fires first)
- The compact point ≤ the local permanent event log highwatermark
- The compact point ≤ the quorum-graduated index (the highest index a
  quorum of nodes has applied to their permanent logs)

The first constraint bounds disk usage. The second guarantees that this
node has the events safely in its permanent log before forgetting them
from the raft log. The third guarantees that even if this node crashes
mid-compact, a quorum exists that can continue serving from their
permanent logs.

**A node never compacts past its own permanent-log highwatermark, even if
a quorum has graduated higher.** This is the safety invariant for local
recovery — see "Storage invariant" below.

Compaction is triggered per-node, not coordinated globally. Each node
checks the constraints against its own state and the cluster's
quorum-graduated index (which it learns from raft heartbeats).

---

## The storage invariant

**After any Ready loop iteration, `P_local == R_local`** where:

- `P_local` = local permanent event log highwatermark (the highest raft
  index applied to events)
- `R_local` = local raft applied index (the highest raft index
  acknowledged to raft)

If this invariant is ever violated, the node fatal-exits with a clear
error message pointing at the rebuild runbook.

This invariant is the contract that makes everything else safe:

- Syncable reads only return data the node actually has.
- Compaction only forgets entries the node has graduated.
- Voting only happens from a node with a current event log (because
  non-current nodes have already exited).
- Leader-completeness implies event-completeness (a leader has all
  events, because non-current nodes can't be leaders).

The invariant can only be broken in one way: an `InstallSnapshot` from a
leader advances `R_local` past `P_local`. v1 catches this and exits; v2
backfills before allowing the advance.

### How a node decides whether to be a voting member

A Committed node never has a "joining mode" flag, a separate bootstrap
subcommand, or any operator hint about what state it's in. The node
determines its mode automatically from its on-disk state:

- On startup, load `P_local` and `R_local` from local storage. They
  must be equal (last shutdown left them consistent, or the rebuild
  procedure made them consistent).
- Start raft. Process Ready iterations as normal.
- After every Ready iteration, check `P_local == R_local`.
- If yes: continue normally. Vote, accept proposals, serve syncable
  reads.
- If no (i.e., raft just delivered an `InstallSnapshot` that advanced
  `R_local` past `P_local`): the node is "consensus-ready but
  reads-blocked." In v1, fatal-exit. In v2, enter the streaming
  catch-up mechanism.

The detection is automatic and uses only local state. The remediation in
v1 requires an operator (rsync), but the determination of "am I caught
up enough to vote" requires no operator input.

---

## Catch-up taxonomy

### Normal raft replication (the happy path)

A follower has been offline briefly, comes back, and the leader still has
the missing entries in its raft log (within the 10GB / 1hr window).
Standard raft `AppendEntries` fills the gap; the apply path writes the
missing entries to the follower's permanent event log. Done.

This handles the vast majority of real-world recovery cases (transient
network blip, brief restart, garbage-collection pause).

### Severe lag — v1 manual rebuild

A follower has been offline long enough that the leader's raft log no
longer covers the gap. From the leader's perspective, it would naturally
try to send `InstallSnapshot`. Our handling:

1. The follower receives the snapshot via `rd.Snapshot` in its Ready
   loop.
2. The snapshot contains the metadata bbolt content + the metadata
   `appliedIndex`. (No events — the events are on local disk on every
   node and are too big to ship through raft.)
3. Applying the snapshot would advance the follower's `appliedIndex` to
   `snap.Metadata.Index`, but its permanent event log is still at the
   old highwatermark — meaning a gap of millions of events.
4. The follower **detects this and fatal-exits** with a clear error
   message:

   ```
   FATAL: storage invariant violation. Local permanent event log is
   at index N; raft applied index is M (gap of M-N events). The
   cluster has compacted past my recovery point. Run the rebuild
   procedure documented at <link to runbook>.
   ```

5. An operator runs the rebuild script: copy `events/` + `metadata/` +
   `raft/` + `time-series/` from a healthy peer, restart the node.

After restart, the storage invariant (`P_local == R_local`) holds, the
node joins normally, and raft fills the small gap that accrued during
the rsync via standard `AppendEntries`.

This is intentionally fail-fast. A node that cannot satisfy the storage
invariant must not be running, because it could otherwise serve stale
syncable reads or (if elected leader, however briefly) confuse the
cluster.

### Severe lag — v2 automated catch-up

Same detection, but instead of fatal-exiting, the node:

1. Refuses to acknowledge the snapshot at the raft level until events
   catch up.
2. Initiates an internal "fetch missing event log segments" RPC against
   a peer.
3. Receives raw segment files via streaming bulk transfer (not per-entry
   — far too slow at TB scale).
4. Once `P_local == R_local` (or close enough that raft replication can
   finish the job), acknowledges the snapshot and resumes normal
   participation.

v2 is deferred to a future ticket. The v1 fail-fast behavior is
forward-compatible: switching from "fatal exit" to "enter catch-up mode"
is a localized change at the invariant check site.

### Brand-new node bootstrap

A newly-provisioned node with an empty data directory cannot in practice
catch up "the natural way" via raft replication, because the cluster is
almost certainly past the 10GB / 1hr retention window. So in v1, **the
bootstrap procedure for a new node is the same as the rebuild procedure**:

1. Provision the machine with an empty data directory.
2. rsync from a healthy peer.
3. Start the binary. The node will load the rsync'd state, find
   `P_local == R_local`, and behave like any other node.
4. From an existing cluster node, propose a conf change adding the new
   node.
5. Raft replicates the conf change. Normal AppendEntries fills the small
   gap accrued between rsync time and conf-change time.

The node does not need any special "I am new" flag. Whether it is a
member of the raft cluster is determined entirely by whether a conf
change has been proposed for it. Until then, it sits in "standalone"
mode receiving heartbeats but not participating in voting (this is
default raft behavior for an unrecognized peer).

---

## Right-to-be-forgotten / deletes

Deletes target entries by `(type, key)`, not by raft index. This matches
the existing entity model: every `cluster.Entity` already has a `Type`
and `Key`, and `NewDeleteEntity(type, key)` already exists in
`internal/cluster/type.go` and is used by the metadata handlers. The
right-to-be-forgotten machinery is the same primitive extended to
user-defined entities.

A delete proposal for `(type=user, key=12345)` covers every historical
entry with that `(type, key)` pair — every update, every state
transition, every event for that user. One delete proposal handles the
whole lifetime of the entity. The requester only needs to know what to
forget (the entity), not the raft indices of every historical event
involving it.

### Two-phase model

When a delete request arrives (e.g., GDPR right-to-be-forgotten):

**Phase 1: logical deletes flood the log.**
"Delete entity" proposals are written through the normal raft path. Each
delete carries `(type, key)` identifying what to forget. Existing
syncables receive these delete events as normal events and remove the
matching rows from their downstream projections — exactly the same way
the metadata handlers already process `NewDeleteEntity` for types,
databases, ingestables, etc. A syncable processing events in order sees
the original entity events, then later sees its delete event, and emits
the corresponding `DELETE FROM <table> WHERE key = '12345'` against its
downstream database.

For an entity with N historical update events, one delete proposal is
enough. The syncable's projection is keyed by entity, so a single
downstream `DELETE` removes everything; the historical events in the log
itself are handled by phase 2.

**Phase 2: physical sweep.**
A background process walks the permanent event log and maintains a
tombstone set of `(type, key)` pairs to delete. For each entry it
encounters, if the entry's `(type, key)` is in the tombstone set, the
entry's data field is zeroed out. The raft index slot stays in place so
the index sequence remains dense — only the data bytes are removed.

After enough time has passed that all live syncables have processed the
delete events themselves (they are in the log too, with their own raft
indices), the sweep also removes the delete events. From that point on,
a brand-new syncable bootstrapping from index 1 sees neither the
originals nor the deletes, and the right-to-be-forgotten request has
been fulfilled.

### Bootstrap timing edge cases

A new syncable that bootstraps from index 1 may catch the log mid-sweep.
Three sub-cases, all of which converge on the right answer:

- **Bootstrap before the sweep starts**: sees originals, then sees
  deletes, processes them in order. End state: rows inserted, then
  deleted. Correct.
- **Bootstrap during the sweep, before the originals are gone**: same
  as above.
- **Bootstrap after the originals are swept but before the deletes are
  swept**: sees only delete events for keys it never saw originals for.
  These translate to `DELETE FROM <table> WHERE key = X` against rows
  that don't exist — a no-op in any sane downstream database. Drop on
  the floor with no harm.
- **Bootstrap after both are swept**: sees neither. The deleted entity
  never existed from this syncable's perspective.

### Sweep cost

Type+key lookup means the sweep is **O(N)** in the size of the
permanent event log per cycle, because every entry has to be checked
against the tombstone set. At terabyte scale this matters. Two
mitigations make it manageable:

- **Sweeps run rarely.** Maybe once a day, or when the tombstone set
  reaches a threshold. Not every commit.
- **Bloom-filter the tombstone set.** Bloom-negative entries skip the
  precise comparison entirely; only the small fraction of bloom-positive
  entries get checked against the sorted tombstone set.

Even without those optimizations, walking 10TB on local NVMe is a few
minutes. A background job at that cadence is fine.

### Determinism of the sweep

The phase-2 sweep must produce byte-identical files on every node,
otherwise the rsync rebuild story breaks. Two requirements:

- **Deterministic trigger**. Every node sweeps at the same logical
  point. Drive it from a deterministic local condition, like "every
  time `appliedIndex` crosses a multiple of N, sweep delete events
  whose own raft index is older than M applied entries." Every node
  hits the same trigger at the same logical time.
- **Deterministic algorithm**. Walk the log in raft index order. Build
  the tombstone set in raft index order (so every node has the same
  set). Apply the same zero-out operation to the same matching entries.

If a node crashes mid-sweep, restart re-runs the sweep from the same
logical point. The sweep is idempotent (zeroing already-zeroed data is
a no-op), so files end up the same regardless of how many times the
sweep runs.

### Indexing impact

The raft index sequence stays dense throughout — sweeps zero data, they
do not remove index slots. Syncable positions remain raft-index-based.
A future segment-rewrite optimization could physically reclaim the
zeroed bytes by writing a new segment file and remapping the sparse
index, but that's an optimization for later, and the rewrite would
itself need to be deterministic across nodes.

---

## Determinism requirement

Apply must be deterministic across nodes. Specifically: given the same
raft entry, every node must produce identical state transitions and
identical permanent log writes. This is **load-bearing**, not
nice-to-have, because:

- **Byte-identical files across nodes enable O(diff) rsync rebuilds.**
  At terabyte scale, the difference between transferring "everything that
  changed" and "everything" is the difference between hours and days.
- **Hash comparison across nodes is a free correctness check.** A
  divergence indicates a determinism bug — exactly the kind of bug that
  is otherwise nearly impossible to find in distributed systems.
- **Future automated catch-up (v2) streams raw segment files**, which
  only makes sense if the segments match across nodes.

### Known sources of non-determinism

The apply-path audit lives at `.claude-scratch/tickets/determinism-audit.md`
and is **complete** as of the determinism-audit landing. Findings:

- **`time.Now()` in `wal/user_defined.go`** — fixed. Every propose path
  (HTTP `AddProposal`, the MySQL ingest `OnRow` handler) now stamps the
  proposal's `Entity.Timestamp` (unix milliseconds) once at propose time,
  and `handleUserDefined` reads that field directly with no fallback. The
  field is part of the `LogEntity` proto so it survives serialization. A
  proto path that forgets to stamp will land its row at unix epoch on
  every node — deterministic, and visible enough in time-series queries
  that the bug surfaces immediately.
- **Map iteration, random IDs, UUIDs, node-id fields, float math,
  goroutine ordering, OS-dependent calls** — none found in the apply
  path. `applyEntity` runs single-threaded inside the raft Ready loop,
  every per-handler bbolt write is a marshaled protobuf with scalar/bytes
  fields only, and channel notifies in `saveSyncable`/`saveIngestable`
  happen *after* the bucket write so they don't race the state.

A regression test, `wal.TestApplyDeterminism`
(`internal/cluster/db/wal/determinism_test.go`), constructs three fresh
`wal.Storage` instances on disjoint temp dirs, applies the same varied
burst of raft entries (multiple types, a database, syncable, ingestable,
syncable-index, stamped + unstamped user entities, mixed proposals, a
delete) to each, and compares three things across all three nodes:

1. **The raft entry log** — walked end-to-end via `EntryLog.Read`.
   Today this is mostly a check on raft's own replication contract
   ("every node sees the same committed entries in the same order"),
   but it's the stand-in for the future permanent-event-log directory
   hash. A `Save` bug that dropped or reordered entries on one node
   would diverge here even when the other checks looked fine.
2. **bbolt bucket contents** — every bucket walked in sorted name order
   with each bucket's keys walked lexicographically, snapshotted into a
   slice of `"bucket/hex(key)=hex(value)"` lines and compared via
   testify's `require.Equal` (per-line diff on failure). The snapshot
   helper lives in `wal/export_test.go` so it's only compiled into the
   test binary; production code does not carry a snapshot API.
3. **Raw `tstorage.Select()` points** for the user metric — ensures the
   time-series store also stays content-deterministic across nodes,
   which is the assertion that catches a re-introduced wall-clock read
   in `handleUserDefined` (the bucket walker deliberately excludes the
   time-series store because tstorage's on-disk format is not
   byte-stable today, so the raw-points comparison is what guards that
   handler).

The test is a unit test (no build tag) so it runs under both `make test`
and `make test-all`. It bypasses real raft because raft's only contract
is "every node sees the same committed entries in the same order" and
the test simulates exactly that — bypassing eliminates election timing
and HTTP transport flakiness without weakening the actual invariant.

---

## Read fan-out to followers (future work)

Eventually, syncable read traffic should fan out to all nodes, not just
the leader. Raft does not constrain this — historical syncable reads do
not need linearizability, and any node with the events in its permanent
log can serve them.

The blocker is **coordination**: if a syncable runs on multiple nodes,
the same raft index could be processed and written to a downstream
database twice. A future ticket will design the coordination mechanism
(likely a leader-elected "owner" per syncable, recorded in the metadata
buckets, with leases or session tracking for failover).

This is intentionally out of scope for the initial implementation. The
foundation needs to be in place first.

---

## PreVote and election timeout

These are small Raft tunings that should land independently of the
larger event-log work. They are tracked in
`prevote-and-election-timeout.md`.

### PreVote

Enabled via `Config.PreVote = true` in `internal/cluster/db/raft.go`.
Without it, a partitioned-then-rejoining node disrupts the cluster: it
increments its term repeatedly trying to start elections nobody responds
to, then on rejoin its high term forces the current leader to step down
even though the rejoiner has no chance of winning. PreVote runs an "am I
electable" round before bumping term. Recommended by Diego Ongaro's
thesis (§9.6).

### CheckQuorum

Enabled alongside PreVote via `Config.CheckQuorum = true`. CheckQuorum is
required for PreVote to fully protect against disruption under a
**directional** (one-way) partition: etcd raft's PreVote grant logic
allows peers to grant a pre-vote at `m.Term > r.Term` even when they're
hearing from a healthy leader — unless CheckQuorum is on, in which case
the leader-lease check (`electionElapsed < electionTimeout`) short-
circuits the grant.

Without CheckQuorum, the adversarial suite's asymmetric-partition
scenario reveals the gap: a follower that can't receive heartbeats from
the leader (but can still send) will campaign at a higher term, and
other followers — still receiving heartbeats from the current leader —
will grant the pre-vote anyway, forcing a spurious step-down. With
CheckQuorum, those peers correctly reject the pre-vote because their
leader lease hasn't expired.

CheckQuorum also lets a leader proactively step down if it loses quorum
(bounded by electionTimeout), which is the right posture for a
production database: a partitioned-away leader shouldn't keep accepting
reads indefinitely.

### Production election timeout

Production should default to `tickInterval = 30ms` with `ElectionTick =
10`, giving a 300ms election timeout — the upper end of the Raft paper's
recommended 150-300ms range (§5.2). Operators on a low-latency LAN can
reduce to `tickInterval = 15ms` (150ms timeout) as an optimization knob.

**Tests stay at their current fast values** — only the production
default changes.

---

## Rebuild procedure

The operator-facing runbook lives at
[`docs/operations/rebuild.md`](operations/rebuild.md) — that is the
canonical reference for anyone actually rebuilding a node, and it
covers the failure modes ("when to rebuild" / "when not to rebuild"),
the procedure, and post-restart verification. The summary below is
design-level only; treat the runbook as source of truth when the two
drift.

### Manual rebuild of an existing follower

Triggered when a node fatal-exits with the storage-invariant error.

```bash
# On the failed node:
sudo systemctl stop committed
sudo rm -rf /var/lib/committed/*

# Rsync from a healthy peer. --inplace makes the next rebuild
# of the same node much faster (only the diff transfers).
sudo rsync -av --inplace healthy-peer:/var/lib/committed/ /var/lib/committed/

# Restore ownership.
sudo chown -R committed:committed /var/lib/committed

# Restart.
sudo systemctl start committed
```

The node comes up, finds `P_local == R_local`, joins raft, catches up
the small gap from the rsync time via normal replication, and resumes
serving reads.

### Adding a new node to the cluster

Same procedure, with one extra step:

1. Provision the new machine with an empty data directory.
2. rsync from a healthy peer (same command as above).
3. Start the binary on the new node. The node loads the rsync'd state
   and waits for cluster membership.
4. From an existing cluster node, propose a conf change adding the new
   node ID.
5. Raft replicates the conf change. The new node receives it and
   transitions to a voting member.
6. Normal raft replication fills the gap that accrued between rsync time
   and conf-change time.

The new node never needs a `--joining` flag or a separate bootstrap
subcommand. Whether it is a member of the cluster is determined entirely
by whether a conf change has been propagated for it.

---

## Implementation phases

This is large work; the order matters because each phase depends on
the previous. Each phase is its own ticket.

### Phase 1: PreVote + production election timeout (small, independent)

Tunings only. No event-log changes. Can land first.
Ticket: `prevote-and-election-timeout.md`.

### Phase 2: Determinism audit (precondition)

Walk every code path reachable from `ApplyCommitted` and `applyEntity`.
Fix every source of non-determinism. Add a CI hash-comparison check.
Ticket: `determinism-audit.md`.

### Phase 3: Carve out the permanent event log

Add a second `wal.Log` at `events/`. `ApplyCommitted` writes events to
both stores during a transition period. Add the storage invariant check
at the end of every Ready loop iteration (fatal-exit on violation).
Ticket: `permanent-event-log.md`, phase 1.

### Phase 4: Migrate syncable reads to the permanent log

Change `Reader.Read()` to read from the permanent event log instead of
the raft entry log. Existing syncable tests must pass unchanged after
the swap. Same ticket, phase 2.

### Phase 5: Raft log compaction + metadata snapshot install

Implement the compaction trigger (10GB or 1hr), wire the safety
constraints, and implement metadata-only snapshot install for followers
within the catch-up window. Same ticket, phase 3.

### Phase 6: Document the rebuild procedure

Operator runbook in `docs/operations/rebuild.md`. Add the fatal-exit
error message that links to it. Same ticket, phase 4.

### Phase 7 (deferred): v2 automated catch-up

Streaming segment transfer RPC. Out of scope for the initial
implementation. Will get its own ticket once the foundation is in place.

### Phase 8 (deferred): Right-to-be-forgotten implementation

Two-phase deletes (logical proposals, then physical sweep). Will get its
own ticket; the architecture above describes the model so future work
can build on it.

---

## Decisions log

| Decision                          | Choice                                          | Rationale                                                                              |
|-----------------------------------|-------------------------------------------------|----------------------------------------------------------------------------------------|
| Compaction trigger                | 10GB or 1hr, whichever first                    | Operator-friendly, bounds disk                                                         |
| Indexing                          | Raft index                                      | Stable across right-to-be-forgotten sweeps; tidwall/wal positions are not              |
| New syncables start at            | Index 1                                         | CQRS bootstrap is the use case; no concrete need for other start points                |
| Compaction safety                 | Quorum-graduated AND local-graduated            | Survives any single-node loss                                                          |
| Read consistency for syncables    | Historical only                                 | No linearizability needed; cheap to fan out later                                      |
| Apply determinism                 | Required                                        | Load-bearing for rsync rebuilds and verification                                       |
| Catch-up v1                       | Manual rsync after fatal-exit                   | Operator-friendly; no silent stale reads                                               |
| Catch-up v2                       | Streaming segment files                         | Future work; per-entry streaming is too slow at TB scale                               |
| Permanent log format v1           | tidwall/wal                                     | Reuse existing dependency; custom format later when measurements demand it             |
| Tombstone model                   | Two-phase: delete events, then physical sweep   | Matches CQRS semantics and gives downstream syncables a chance to delete cleanly       |
| Delete addressing                 | `(type, key)`, not raft index                   | Matches existing `NewDeleteEntity` model; one delete covers all history for an entity  |
| Election timeout (production)     | 30ms tick / 300ms election                      | Upper end of Raft paper's recommendation; tests stay fast                              |
| Election timeout (operator knob)  | 15ms tick / 150ms election                      | Optional optimization for low-latency LANs                                             |
| PreVote                           | Enabled                                         | Eliminates rejoin-disruption failure mode (Ongaro thesis §9.6)                         |
| CheckQuorum                       | Enabled                                         | Required companion to PreVote for directional-partition protection; leader steps down on lost quorum |
| Joining-mode flag                 | None                                            | Node determines its state from on-disk data, not from operator input                   |
| Per-write fsync on permanent log  | Yes                                             | Matches raft semantics; safe against power loss                                        |
| Cross-type ordering               | Single raft group                               | Central differentiator from per-topic systems; sharding is v3+                         |

---

## References

- Ongaro & Ousterhout, "In Search of an Understandable Consensus
  Algorithm" (Raft paper) — https://raft.github.io/raft.pdf
- Ongaro PhD thesis, "Consensus: Bridging Theory and Practice" — covers
  PreVote (§9.6), joint consensus, single-server membership changes,
  leadership transfer, snapshotting tradeoffs
- etcd raft source — `go.etcd.io/etcd/raft/v3` — the implementation we use

---

## Open questions and out-of-scope decisions

These need future tickets to resolve. Listed here so future readers can
see what was deliberately deferred vs. accidentally forgotten.

- **Read fan-out coordination**: how to prevent multiple nodes from
  double-syncing the same syncable. Deferred until the foundation is
  in place.
- **Custom permanent log format**: when to write our own format with
  sparse indexing, segment GC, etc. Will be driven by performance metrics
  from production.
- **Tiered storage**: only if local NVMe ever stops scaling (not expected
  at the targeted workload size).
- **Per-topic raft groups**: only if write throughput on a single raft
  group hits a wall. Would compromise cross-type ordering.
- **Determinism CI check**: cross-node hash comparison after multi-node
  tests. Part of `determinism-audit.md`.
- **Right-to-be-forgotten implementation**: model is described above; the
  actual code is a future ticket.
- **Time-series storage in this picture**: today the time series store
  is populated as a side-effect of `handleUserDefined`. After the
  permanent event log lands, the time series can be regenerated from
  the permanent log on demand. v1 still rsyncs it for speed.
