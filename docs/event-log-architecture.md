# Event Log Architecture

**Status:** Proposed (2026-04-09)

This document describes how Committed stores, replicates, and manages its
application event log. It exists because the standard Raft model (log as
throwaway transport, snapshot-and-compact aggressively) does not fit
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
  the cluster lives forever, with two exceptions (both below):
  right-to-be-forgotten deletes, and topics declared `EntityKind =
  snapshot`, which the metadata GC compacts to the latest event **per key**
  (a later write supersedes earlier ones, so replay still yields the correct
  final state — but the intermediate history of a snapshot topic is not
  retained, so don't model a topic as `snapshot` if you need to replay its
  transitions).
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

### Proposals and Actuals

Consensus is a boundary, and the two sides have different names. A
**Proposal** is a write *request* — one or more entities offered to the log
together, with no place in the order yet. Once raft commits it, it is an
**Actual** (`cluster.Actual`): the same entities, now a committed fact at a
fixed `Index`. You *propose* Proposals; you *sync* Actuals.

The distinction is load-bearing on the read side. A `Reader` yields
`*cluster.Actual` in `Index` order, and a `Syncable` consumes Actuals — it
never sees a Proposal. The `Index` is the Actual's identity and its place in
the single total order (the only ordering authority in the system), so it is
also what a syncable's cursor, dead-letter records, and the HTTP webhook's
`Idempotency-Key` are keyed on. The write-side metadata a Proposal carries
(RequestID, IngestableID, SourceSeq) is plumbing that stops at the boundary
— an Actual carries only `Index` and the committed entities.

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

In `internal/cluster/db/wal/reader.go`, `Reader.Read()` returns the next
`*cluster.Actual` from the permanent event log in raft-index order (skipping
internal metadata entries like syncable-index bumps). It earlier scanned the
raft entry log and returned a raw proposal plus a separate index; the backing
store moved to the permanent log and the return type became the `Actual` that
folds the index into the value, but it stays a simple "read the next
committed entry" call.

### On-disk layout

```
<datadir>/
├── raft/
│   ├── log/         # raft consensus log; bounded ~10GB; tidwall/wal
│   └── state/       # HardState + ConfState (currently state-log/)
├── events/          # permanent event log; infinite retention; terabytes
└── metadata/
    └── bbolt.db     # types, databases, ingestables, syncables, syncableIndex, appliedIndex
```

`events/` at the top level makes the rebuild story trivial to explain and
script: "rsync `events/`, `raft/`, `metadata/` from a
peer." Different I/O profiles per tier mean operators can put `raft/` on a
smaller fast device if they want; not v1 work but the layout supports it.

### Indexing

**Syncable positions are tracked by raft index, never by file offset.**
Raft index is monotonic and authoritative, and a given entry's raft index
never changes. A right-to-be-forgotten scrub physically *removes* entries,
so the raft-index column becomes sparse (a removed index is absent) — but
it stays ascending and an index is never renumbered, so a syncable's
stored position remains valid and the binary-search Reader keeps working
across the gaps. (The wal sequence numbers, which are an internal
implementation detail, are re-densified on each rewrite.)

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

The raft log is compacted when **both** of these are true:

- Size > 10GB **or** age > 1 hour (whichever fires first), or disk pressure
  demands it
- The compact point ≤ the local permanent event log highwatermark
  (`min(applied − 8, EventIndex)`)

The first constraint bounds disk usage. The second guarantees this node has the
events safely in its own permanent log before forgetting them from the raft log:
**a node never compacts past its own permanent-log highwatermark.**

Compaction is a local, per-node decision — each node checks these constraints
against its own state, not a cluster-wide index. It does **not** consult a
"quorum-graduated index" (an earlier design not implemented in v1): raft's own
commit rule already guarantees every compacted entry was committed to a quorum's
raft logs, and each node graduates a committed entry to its permanent log on
apply, so durability holds without a separate quorum constraint. The storage
invariant (below) is the local-recovery backstop.

---

## The storage invariant

**After any Ready loop iteration, `P_local == R_local`** where:

- `P_local` = local permanent event log highwatermark (the highest raft
  index applied to events)
- `R_local` = local raft applied index (the highest raft index
  acknowledged to raft)

The two advance together within each Ready iteration: the batch apply
appends every committed entry's event-log record in one batched write,
applies the entities, then persists the applied index once — so `P > R`
transiently inside a batch (and across a crash, which restart replay
closes) and the invariant is checked only at iteration end.

The node **fatal-exits when `P_local < R_local`** — raft has acknowledged an
index the permanent log doesn't have (the dangerous direction; e.g. an
InstallSnapshot advanced `R_local` past the events), with a clear error pointing
at the rebuild runbook. The reverse, `P_local > R_local`, is the *benign*
crash-apply window (events the Ready loop wrote but hasn't re-acknowledged),
which restart replay closes — it is not fatal. In steady state, after each Ready
iteration, the two are equal.

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
   `raft/` from a healthy peer, restart the node.

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

**Mechanism: removal, not redaction.** An earlier version of this design
specified *redaction* — zero a deleted entry's payload in place and keep
its raft-index slot, so the index sequence stayed dense. That was
superseded by **physical removal**: the deleted entries are dropped from
the permanent event log entirely. Removal was chosen because it is the
only option that actually *shrinks* the log, so the same mechanism also
bounds the never-compacted tier's metadata growth (metadata GC reuses this
scrubber to compact superseded bookkeeping — see *Metadata GC (system
tombstones)* below) — which is why Committed stays single-raft instead of
introducing a separate compactable metadata raft.
The cost is that the raft-index column becomes **sparse (gapped)**: an
index that was removed is simply absent. The wal sequence numbers are
re-densified on rewrite and the raft index carries the gaps. The Reader
binary-searches by raft index (`resolveStartSeq`, `ActualAt`) and so
tolerates gaps natively, as long as the column stays ascending — which
removal preserves.

**A transient copy lives in the raft consensus log — by design, and bounded.**
The removal above is about the *permanent event log* — the tier syncables read,
and the system of record. Every committed entity is also written to the **raft
consensus log** (the replication/replay tier), which the scrubber does not
rewrite: it is trimmed by the normal compaction policy (size or age — see
*Compaction policy*), so a deleted subject's payload persists there only until
compaction passes its index — on the order of **an hour at most**, and usually
far less on a busy node. This is the same category as an OS **page cache**, a
database **WAL**, or a filesystem **journal**: a transient buffer that holds
recently-written data for a bounded window and self-clears. It is never read by a
syncable, never returned over the API, and never captured in a snapshot (the raft
snapshot serializes BoltDB, into which user topic entity *payloads* do not
materialize — the one identifier that does, the RTBF delete-tombstone key, is
pruned + compacted out of bbolt when its scrub runs, see *Phase 2* above).
The expectation is therefore that erased personal data may linger in this
consensus buffer for up to **~1 hour** after removal from the permanent log — a
bounded, non-exposed window, well within GDPR's "without undue delay." (Backups
are a separate concern: a backup that captures on-disk state can outlive this
window, and erasure from backups is handled by the backup-retention process, not
by the scrubber.)

### Scope boundaries

Two adjacent concerns are deliberately **not** part of the scrubber:

- **Subject resolution** — turning "customer 12345" into the *set* of
  `(type, key)` pairs that constitute their data — is domain-specific and
  lives outside the log. The client (or future tooling) computes the set
  and issues the deletes; the scrubber only ever consumes `(type, key)`
  tombstones. A workable pre-1.0 path is: hook up a syncable, query the
  projection for the relevant type/key pairs, and issue the deletes
  programmatically.
- **Downstream / logical-delete handling** — making a syncable translate a
  delete Actual into a downstream `DELETE` — is its own ticket. The model
  is described under Phase 1 below; honoring deletes is part of the
  Syncable contract, but the SQL/HTTP syncables wiring it up ships
  separately from the scrubber.

### Two-phase model

When a delete request arrives (e.g., GDPR right-to-be-forgotten):

**Phase 1: logical deletes flood the log.**
"Delete entity" proposals are written through the normal raft path. Each
delete carries `(type, key)` identifying what to forget. A syncable
processing events in order sees the original entity events, then later
sees its delete event, and emits the corresponding
`DELETE FROM <table> WHERE key = '12345'` against its downstream
database. For an entity with N historical update events, one delete
proposal is enough: the projection is keyed by entity, so a single
downstream `DELETE` removes everything. The historical events in the log
itself are handled by phase 2.

**Phase 2: physical scrub.**
As deletes apply, each node records an **event-log tombstone** — the
delete's raft index keyed by its `(type, key)` — in a replicated bbolt
bucket (`eventTombstones`). Physical removal is then driven by a committed
**`Scrub` command** carrying a single upper-bound raft index **B** (the
"freeze line"). When the command applies, each node records a pending
scrub and pokes a **background worker** (off the raft Ready loop — the
O(N) rewrite must never stall consensus). The worker rewrites the event
log: copy every entry, removing the entities whose `(type, key)` has a
tombstone with a delete index `D` such that `entry.Index < D <= B`
(i.e. the entity was written before a delete within the freeze line),
keeping the delete-tombstones, and copying entries at raft index `> B`
verbatim. It writes a fresh sibling directory and swaps it in under a
short lock (`eventMu`, mirroring how `kvMu` guards the bbolt handle during
`RestoreSnapshot`).

The same scrub also **erases the subject's identifier from bbolt**. The
`eventTombstones` bucket is keyed by `(type, key)`, so the raw subject key sits in
bbolt — and, because a raft snapshot serializes the whole bbolt file, it would
otherwise ride in every snapshot shipped to followers and written to disk. Once a
tombstone's scrub has run it is dead weight (log indices are monotonic, so nothing
can ever precede an already-scrubbed delete), so the worker **prunes** the
tombstone and then **compacts** bbolt. The compaction is load-bearing: bbolt's
logical `Delete` only frees the page, and `tx.WriteTo` copies free pages, so
without a live-data-only rewrite the raw key would linger in the file. After the
scrub, the erased subject's identifier is gone from bbolt and from every
subsequent snapshot. (Determinism is unaffected: the prune is a pure function of
the tombstone set and B; compaction only changes physical layout, which raft
never compares across nodes.)

Removal is **entity-granular**: a proposal that bundled several entities
is re-marshaled to keep its untombstoned siblings; the whole record is
dropped only when every entity in it is removed. (A referential constraint
from a *retained* entity to a *scrubbed* one can break a new syncable's
replay — inherent to time-traveled deletion, and the schema designer's
responsibility.)

The **delete-tombstone entries are retained** in the permanent event log.
They are erasure *instructions* and are tiny — but the entry's `key` is the
subject's natural identifier, so it **is** PII (an earlier version of this doc
wrongly called it "not PII"). Keeping them is required for correctness: a syncable
that has read the originals but not yet the delete must still receive the delete
to drop its row, and a fresh syncable replaying a scrubbed log sees only the
delete → a `DELETE` of a row that was never inserted → harmless no-op. That
retained identifier lives only in each node's **local event log** — it is *not* in
the bbolt snapshot (the snapshot's copy is pruned + compacted, above), so it is
never fanned out cluster-wide via `InstallSnapshot`. Erasing it from the local
event log too (rewriting the retained delete's key once every syncable has
consumed it) is tracked as a future item — see the RTBF erasure runbook/ledger.

**Triggers.** A `Scrub` command is proposed two ways: automatically by the
leader on a cadence (`COMMITTED_SCRUB_INTERVAL`, default 1h) whenever there
is unscrubbed RTBF backlog, and manually via `POST /v1/scrub` for
SLA-expedited erasure. Automatic scrubbing never *decides* to delete
anything — it only ever physically completes deletions a delete proposal
already requested.

### Bootstrap timing edge cases

A new syncable that bootstraps from index 1 may catch the log mid-scrub.
The sub-cases all converge on the right answer:

- **Bootstrap before the scrub**: sees originals, then the delete,
  processes them in order. End state: rows inserted, then deleted.
  Correct.
- **Bootstrap after the originals are removed (delete retained)**: sees
  only the delete event for a key it never saw an original for. Translates
  to `DELETE FROM <table> WHERE key = X` against a row that doesn't exist
  — a no-op in any sane downstream database. Harmless.

### Determinism of the scrub

The rewrite must produce byte-identical files on every node or the rsync
rebuild story breaks. The survivor set is a pure function of three frozen,
replicated inputs: the event-log bytes, the tombstone set **filtered to
delete index `<= B`**, and `B` itself (carried in the committed command).

- **Frozen bound.** `B` is pinned inside the committed `Scrub` command, so
  every node rewrites the same prefix. New commits during a background
  rewrite all land at index `> B` and are always survivors, so concurrent
  appends can't change the answer.
- **Frozen selection.** A tombstone stores the delete's raft index, and
  the predicate only considers indices `<= B`. All deletes `<= B` are
  guaranteed applied before the `Scrub` command applies (in-order apply;
  the command commits at an index `> B`), so the eligible set is identical
  on every node regardless of *when* each node's worker runs. Re-marshaled
  records use deterministic protobuf encoding.

If a node crashes mid-rewrite or mid-swap, the next `Open` rolls an
interrupted swap back to the pre-swap directory and re-runs from the
persisted pending bound. The rewrite is idempotent, so re-running yields
the identical result.

### EventIndex / invariant impact

`EventIndex()` (P_local — the max raft index in the event log) **never
regresses**, so the Ready loop's `P_local == R_local` invariant holds
across a scrub. The tail (highest index) is always `> B` and never a
removal candidate — at minimum the `Scrub` command's own mirrored entry
sits above `B` — so the rewritten log's tail index equals the old one.

The raft-index column becomes sparse but stays ascending; the wal
sequence column is re-densified (no gaps). Syncable positions remain
raft-index-based and the binary-search Reader is unaffected. The event log
is therefore no longer a 1:1 mirror of the committed index space — it is a
subset — so nothing may assume "every committed index is present in the
event log" (`ActualAt` returns `ErrActualNotFound` for a scrubbed index,
by design).

### Metadata GC (system tombstones)

The same scrubber serves a second job it was built to also support: bounding the
never-compacted tier's growth from **superseded last-writer-wins data**. On a
busy cluster the dominant source of event-log growth is often the steady stream
of LWW records — internal coordination (`SyncableIndex` checkpoint bumps,
ingestable positions, `SyncableStuck` / `SyncableSkipRequest` status) and
user-defined `EntityKindSnapshot` topics, where each write fully replaces the
previous one for its key. Only the latest value of each ever matters, yet every
superseded copy otherwise lives forever in the log.

**User tombstone vs system tombstone.** Removal has two predicates that run in
the *one* rewrite pass:

- A **user tombstone** is the RTBF delete above — an explicit erasure of a
  subject. It removes a subject's entries regardless of entity kind (events
  included) and always *spares* the delete-tombstone itself.
- A **system tombstone** is metadata GC — the scrubber compacting an
  `EntityKindSnapshot` key (internal bookkeeping or a user Snapshot topic) to its
  latest value. It removes superseded entries (upserts *and* deletes), keeping
  only the newest `<= B`. So it *can* drop a superseded delete, exactly where the
  RTBF predicate spares one.

Both predicates run in the *one* rewrite pass: an entry is dropped if **either**
fires (a single OR), so they never double-remove. Where they can overlap — a user
Snapshot key that also carries an RTBF delete — they agree, since both keep only
the latest entry per key. RTBF additionally guarantees a delete-tombstone
survives (it spares deletes); metadata GC only drops a delete that a *later*
entry for the same key supersedes, so the latest delete is always kept.

**Keep-latest, never drop-all — the source-of-truth invariant.** Metadata GC
keeps the *latest* committed entry per `(type, key)` and drops only entries a
later surviving entry makes redundant. It never drops the latest value of
anything. This preserves the invariant that the permanent event log is a
complete source of truth from which bbolt (a derived materialized view) can be
reconstructed by replay: the live value of every key stays in the log. Dropping
all metadata `<= B` would reclaim only one extra entry per key while forfeiting
that invariant permanently, a bad trade. The rule, stated as an invariant:
*metadata GC may drop only an entry a later surviving entry makes redundant —
never the latest value of anything.*

**Gating: last-writer-wins only (`EntityKindSnapshot`).** Compacting a key to its
latest is sound only when a later write fully *implies* the earlier one — i.e.
snapshot (LWW) semantics, where superseded copies are disposable. So the gate is
exactly **`EntityKind == EntityKindSnapshot`**, for internal *and* user-defined
types alike: an internal Snapshot built-in (`SyncableIndex`, ingestable position,
stuck, skip) or a user Snapshot topic is compacted to its latest-per-key.
Everything else is retained, by kind — and the held-back built-ins are
deliberately classified, not special-cased. The one built-in category held back
is the version-stored configs (`type` / `database` / `syncable` / `ingestable`):
they are **`EntityKindRevision`** — full states in a retained, addressable
version series (the rollback endpoints) — so compacting them would discard
history the log must keep to stay self-describing and replayable into bbolt's
versioned stores. Every other built-in compacts, including the dead-letter logs:
their upsert and clearing-delete entities are both keyed by the record's full
`id + index` identity, so each is a distinct, symmetric key that keep-latest
compacts correctly (distinct dead letters survive; a re-propose or clear of the
same record collapses to its latest).

**Resolving the kind deterministically.** An internal type's kind is a constant.
A *user* type's kind lives in mutable, deletable state, so reading the live type
bucket at scrub time would be non-deterministic — a type delete at index `> B`
could race the async worker, and two nodes would disagree. Instead the kind is
harvested from the type's registration entries **in the log prefix `<= B`**
(latest registration wins, in index order) — a pure function of the prefix, like
the rest of the selection. `typeType` being `EntityKindRevision` (retained)
guarantees a type's registration is always present in the log before the data
that references it, so the harvest always resolves.

**Determinism.** Like the RTBF selection, the survivor set is a pure function of
the log prefix `<= B`: a pre-pass computes, per Snapshot `(type, key)`, the max
raft index `<= B`, and an entry is dropped iff its index is below that max.
Identical inputs on every replica yield byte-identical rewritten logs.
`EventIndex` is preserved by the same tail-is-always-`> B` argument as RTBF.

**Trigger.** `HasScrubBacklog` is generalized from "is there RTBF erasure?" to
"is there *any* scrubbable work — RTBF erasure **or** enough accumulated
superseded metadata?", so a metadata-heavy, RTBF-free cluster still triggers the
automatic scrubber and the one rewrite pass does both jobs. Because metadata GC
only reclaims space (unlike legally-urgent erasure), it batches behind a small
threshold rather than firing on every superseded write.

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

The apply-path determinism audit is **complete**. Findings:

- **`time.Now()` in `wal/user_defined.go`** — moot. The original finding
  was a wall-clock read in the user-defined apply handler that fed the
  derived time-series store. That store (and the handler) have since been
  removed entirely; user-defined (topic) entities now apply as a no-op,
  their durable record being the permanent event log. The per-entity
  `Timestamp` field that the handler consumed was removed too — a
  server-stamped clock has no place in a consensus log, where the raft
  index is the only ordering authority. No wall-clock is read at apply.
- **Map iteration, random IDs, UUIDs, node-id fields, float math,
  goroutine ordering, OS-dependent calls** — none found in the apply
  path. `applyEntity` runs single-threaded inside the raft Ready loop,
  every per-handler bbolt write is a marshaled protobuf with scalar/bytes
  fields only, and channel notifies in `saveSyncable`/`saveIngestable`
  happen *after* the bucket write so they don't race the state.

**The rule, stated positively — operation parameters ride the committed
command.** Anything that affects the *outcome* of applying a committed entry
must itself be carried by that entry (or by prior committed state): never by a
node-local side channel keyed to the operation. A parameterized operation
therefore puts its parameters in the command it commits — the scrub commits
its `Scrub{Bound}`, a syncable delete carries its `keepData` intent on the
tombstone entity — so every replica, whichever holds leadership when the entry
applies, decides identically. (A node-local `keepData` intent map once
violated this: a leadership change between propose and apply made the new
leader decide differently than the proposing node intended.) The one sanctioned
exception is *execution* of destructive external side effects — a destination
teardown, a source-slot drop — which is owner-gated and live-only precisely so
it never replays; the *decision* to perform it must still be a pure function of
committed state. Node-local state that feeds only observability (a stuck
tracker's debounce, metrics) is exempt: its staleness can misreport, but it can
never mis-apply.

A regression test, `wal.TestApplyDeterminism`
(`internal/cluster/db/wal/determinism_test.go`), constructs three fresh
`wal.Storage` instances on disjoint temp dirs, applies the same varied
burst of raft entries (multiple types, a database, syncable, ingestable,
syncable-index, stamped + unstamped user entities, mixed proposals, a
delete) to each, and compares two things across all three nodes:

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

These are small Raft tunings, independent of the larger event-log work.

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
other followers (still receiving heartbeats from the current leader)
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

## Commit ambiguity on leadership change (`ErrProposalUnknown`)

When a node calls `db.Propose`, it registers a waiter stamped with the
leader it currently believes in (`proposeAsync` in
`internal/cluster/db/db.go`). A background watcher
(`watchLeaderTransitions`) notices when that leader changes and, after a
short grace period (`WithLeaderChangeGracePeriod`, default 3× tick —
during which apply may still land) signals any still-unresolved waiter
with **`ErrProposalUnknown`**: literally "proposal status unknown after
leader change." The proposal *might* have committed (and will apply) or
*might* have been dropped by the transition. The proposer cannot tell.

Every in-flight propose has to decide what to do with that ambiguity. The
governing principle is:

> **The reaction to commit ambiguity is proportional to what is lost if
> you guess wrong.**

Guessing wrong about a data-carrying proposal corrupts the log
(duplication or loss); guessing wrong about a diagnostic costs at most a
missing diagnostic line. So the write paths react differently — by what is
at stake, not by which subsystem they live in:

| In-flight proposal | What's at stake on a wrong guess | Reaction | Recovery mechanism |
|--------------------|----------------------------------|----------|--------------------|
| Ingest data (a row from the source) | duplicate in the log, or silent data loss | **Freeze** the worker | Supervisor restarts the ingestable from the last *durable* position; the dialect replays from there; effectively-once dedup (`SourceSeq` highwater) drops any re-emit that already committed |
| Ingest position checkpoint | resume point diverges → loss or re-emit storm | **Freeze** the worker | The unacknowledged position is discarded; restart re-reads `storage.Position`, still the last durable checkpoint |
| Sync index bump (`proposeSyncableIndex`) | durable resume point advances past unconfirmed work → missed re-sync | **Don't advance**; keep the proposal and re-sync it next iteration | Bounded to at most one duplicate; safe because `Sync` is contractually idempotent (SQL UPSERT) |
| Sync dead-letter record (`proposeSyncableDeadLetter`) | a row missing from `GET /v1/syncable/{id}/errors` | **Log and continue** | Best-effort; the error counter and ERROR log already fired, and the skip stands regardless. Often self-healed on a later replay, but losing it costs nothing |

The reactions form a ladder of severity, deepest cost first:

- **Freeze** (ingest data + position). Continuing past an ambiguous
  *data* proposal is the dangerous option — it risks unrecoverable loss or
  duplication — so the worker stops entirely and rewinds to a known-good
  point. Freezing halts that ingestable, a real availability cost, but it
  is the lesser evil when the alternative is corrupting the event log, and
  it is only safe because the rewind + replay + dedup machinery absorbs the
  overlap on restart. See the `ingestExitFreeze` branch in
  `internal/cluster/db/ingest.go`.

- **Don't-advance-and-retry** (sync index bump). The bump *is* the durable
  resume point, so advancing it past work whose commit is unconfirmed would
  silently skip re-syncing that work on recovery. The worker refuses to
  advance and re-syncs the same proposal next iteration. This caps recovery
  at one duplicate rather than freezing, because `Sync` is idempotent and a
  single re-delivery is harmless. See `proposeSyncableIndex` in
  `internal/cluster/db/db.go`.

- **Log-and-continue** (sync dead letter). The dead-letter record is
  observability *about* a decision (the permanent skip) that has already
  been made and is independent of whether the record persists. Nothing is
  at stake in a wrong guess: the counter and ERROR log already captured the
  event, the proposal is skipped regardless, and because a permanent skip
  does not advance the durable `SyncableIndex`, a later replay frequently
  re-derives and re-records the dead letter idempotently. Freezing here
  would be actively wrong — it would halt the whole syncable over a
  diagnostic for an *expected* condition (bad data), defeating the entire
  point of skip-and-dead-letter, which is to keep good data flowing past
  bad rows. See `proposeSyncableDeadLetter` in `internal/cluster/db/db.go`.

The same ambiguity, three reactions — and the deciding question is always
"if I'm wrong about whether this committed, what breaks, and can I
recover?"

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

### Phase 2: Determinism audit (precondition)

Walk every code path reachable from `ApplyCommitted` and `applyEntity`.
Fix every source of non-determinism. Add a CI hash-comparison check.

### Phase 3: Carve out the permanent event log

Add a second `wal.Log` at `events/`. `ApplyCommitted` writes events to
both stores during a transition period. Add the storage invariant check
at the end of every Ready loop iteration (fatal-exit on violation).

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

### Phase 8: Right-to-be-forgotten implementation

The physical scrubber — a committed `Scrub{B}` command driving a
deterministic background segment-rewrite that removes already-delete-
proposed entities — has landed. It uses
**removal**, not the redaction sketch earlier drafts described; see
§ "Right-to-be-forgotten / deletes". **Metadata GC** — reusing the same
scrub engine to bound never-compacted bookkeeping growth — has since landed;
see § "Metadata GC (system tombstones)".

---

## Decisions log

| Decision                          | Choice                                          | Rationale                                                                              |
|-----------------------------------|-------------------------------------------------|----------------------------------------------------------------------------------------|
| Compaction trigger                | 10GB or 1hr, whichever first                    | Operator-friendly, bounds disk                                                         |
| Indexing                          | Raft index                                      | Never renumbered (only made sparse) by a scrub; tidwall/wal positions are not stable   |
| New syncables start at            | Index 1                                         | CQRS bootstrap is the use case; no concrete need for other start points                |
| Compaction safety                 | Local-graduated only (never past own permanent log) | Raft commit already places each entry in a quorum; the quorum-graduated constraint is unimplemented in v1 |
| Read consistency for syncables    | Historical only                                 | No linearizability needed; cheap to fan out later                                      |
| Apply determinism                 | Required                                        | Load-bearing for rsync rebuilds and verification                                       |
| Catch-up v1                       | Manual rsync after fatal-exit                   | Operator-friendly; no silent stale reads                                               |
| Catch-up v2                       | Streaming segment files                         | Future work; per-entry streaming is too slow at TB scale                               |
| Permanent log format v1           | tidwall/wal                                     | Reuse existing dependency; custom format later when measurements demand it             |
| RTBF mechanism                    | Physical removal (not redaction/crypto-shred)   | Only option that shrinks the log → serves metadata-GC too, keeping us single-raft      |
| Scrub trigger                     | Committed `Scrub{B}` command; auto + manual     | One committed bound makes the background rewrite deterministic across replicas         |
| Delete-tombstone retention        | RTBF-spared (but a superseded snapshot-topic delete may still be metadata-GC'd) | In-flight/fresh syncables still need the delete. The tombstone key **is** PII (the subject's identifier); RTBF scrub spares it deliberately, it is not inherently non-PII |
| Delete addressing                 | `(type, key)`, not raft index                   | Matches existing `NewDeleteEntity` model; one delete covers all history for an entity  |
| Metadata GC predicate             | Keep-latest per `(type, key)`; never drop-all   | Event log stays a replayable source of truth for the derived bbolt view                |
| Metadata GC gate                  | `EntityKind == Snapshot` (internal + user)      | Keep-latest is sound only for LWW with a record-unique key; version-stored configs are Revision (history retained) → excluded. Dead-letter keys reshaped to `id+index` so they compact too. User kinds harvested from the log prefix for determinism |
| Scrub backlog signal              | Generalized: RTBF OR superseded-metadata work   | A metadata-heavy, RTBF-free cluster must still trigger the automatic scrubber          |
| Election timeout (production)     | 30ms tick / 300ms election                      | Upper end of Raft paper's recommendation; tests stay fast                              |
| Election timeout (operator knob)  | 15ms tick / 150ms election                      | Optional optimization for low-latency LANs                                             |
| PreVote                           | Enabled                                         | Eliminates rejoin-disruption failure mode (Ongaro thesis §9.6)                         |
| CheckQuorum                       | Enabled                                         | Required companion to PreVote for directional-partition protection; leader steps down on lost quorum |
| Joining-mode flag                 | None                                            | Node determines its state from on-disk data, not from operator input                   |
| Per-write fsync on permanent log  | Yes                                             | Matches raft semantics; safe against power loss                                        |
| Cross-type ordering               | Single raft group                               | Central differentiator from per-topic systems; sharding is v3+                         |
| In-flight proposal on leader change | Reaction scaled to stakes: freeze (ingest data/position) / don't-advance + re-sync (sync index bump) / log + continue (dead letter) | Data proposes can't risk loss or duplication; a diagnostic can — `ErrProposalUnknown` is handled by what's lost on a wrong guess |

---

## References

- Ongaro & Ousterhout, "In Search of an Understandable Consensus
  Algorithm" (Raft paper) — https://raft.github.io/raft.pdf
- Ongaro PhD thesis, "Consensus: Bridging Theory and Practice" — covers
  PreVote (§9.6), joint consensus, single-server membership changes,
  leadership transfer, snapshotting tradeoffs
- etcd raft source `go.etcd.io/etcd/raft/v3` — the implementation we use

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
  tests.
- **Right-to-be-forgotten implementation**: model is described above; the
  actual code is a future ticket.
