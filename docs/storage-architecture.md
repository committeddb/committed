# On-disk storage architecture

How committed persists data on disk, why it is split across four stores, and the
design decisions behind that split. This is background for anyone reading the
storage internals — not a how-to. For the *semantics* of the log (entity kinds,
apply determinism) see [event-log-architecture.md](event-log-architecture.md);
for recovery procedures see [operations/rebuild.md](operations/rebuild.md) and
[operations/backup.md](operations/backup.md).

## The four stores

Under a node's data directory, committed keeps **three append-only write-ahead
logs** (tidwall/wal) and **one key/value store** (BoltDB):

| Path | Store | Holds | Index space | Lifecycle |
|------|-------|-------|-------------|-----------|
| `raft/log` | EntryLog (tidwall) | raft log entries | raft index | append; tail-truncate on conflict; front-truncate on compaction |
| `raft/state` | StateLog (tidwall) | raft `HardState` (term/vote/commit) + snapshot metadata | a private sequence | append latest-wins; periodic re-anchor |
| `events` | eventLog (tidwall) | the permanent commit log (committed Actuals) | event index | append-only; rewritten only by the RTBF scrubber |
| `metadata` | BoltDB | applied state: applied index, versioned configs, ingest/sync positions, dead-letters | key/value | random-access read/write per apply |

Two of these are raft/consensus plumbing (`raft/log`, `raft/state`), one is
durable applied state (`metadata`), and the fourth — `events` — is the product
itself: the permanent, replayable log that syncables project out to external
systems.

Why four and not one? Because their access patterns and lifecycles genuinely
differ, and merging any two would force a mismatch:

- **EntryLog** is raft's short-lived scratch — entries are appended, sometimes
  truncated at the tail (a leader change overwrites uncommitted suffixes), and
  compacted away once applied. It is read by *raft index*.
- **StateLog** is a single mutable value (the latest `HardState`) plus snapshot
  metadata, not a growing history — "the last record wins."
- **eventLog** is the opposite of EntryLog: append-only and **kept forever** (it
  is the source of truth downstream systems replay), rewritten only when the RTBF
  scrubber removes erased data. Compacting it would destroy the product.
- **metadata** (BoltDB) is random-access key/value — applied index, config
  version history, resume positions, dead-letters — none of which fit an
  append-log's sequential model.

## The foundational choice: tidwall as a random-access store

committed uses tidwall/wal not merely as a durable record stream but as a
**random-access, index-keyed store**. `Storage.Entries(lo, hi)` and `Term(i)`
read raft entries by computing `tidwall seq = raftIndex − firstIndex + 1` and
reading that record directly — the log *is* the entry store.

This is a deliberate divergence from the etcd raft library, whose own WAL is a
**sequential record stream replayed into an in-memory `MemoryStorage` on boot**:
etcd serves `Entries`/`Term` from RAM and the WAL is only the durable journal.
committed's choice trades that in-RAM index for **durable indexed reads without
holding the uncompacted log in memory** — a reasonable fit for a system whose
whole purpose is a large, long-lived log.

That choice has one important consequence, which explains the next section.

## Why raft persistence is two logs, not one

A natural question: etcd puts `HardState` and entries in **one** WAL with **one
fsync per `Ready`**, making them crash-atomic by construction. Why does committed
split raft persistence into `raft/log` and `raft/state`?

Because committed uses tidwall as a raft-index store (above), and **`HardState`
has no raft index**. The two cannot share one tidwall index space:

1. **Index-space conflict.** EntryLog's index space *is* the raft-index space. A
   `HardState` record placed in it would either collide with a real entry index
   or consume an interleaved slot that breaks the `raftIndex → seq` arithmetic
   every read depends on. Keeping random access while interleaving `HardState`
   would require an in-memory `raftIndex → seq` map rebuilt on every Open — i.e.
   re-implementing etcd's `MemoryStorage`, a different architecture, not a merge.
2. **Truncation conflict.** raft truncates the entry tail on a log conflict
   (`TruncateBack` to a raft index). The latest `HardState` is written last, so it
   sits at the tail — a single merged log would delete it on every
   conflict-truncate, forcing a re-append and fighting raft's truncation model.

So two logs, with two index spaces (raft-index vs a private sequence) and two
lifecycles (append / tail-truncate / compact vs latest-wins / re-anchor), is the
**correct** decomposition given the tidwall-as-store choice. Merging the two was
evaluated and rejected: the only coherent "one log, one fsync" design is etcd's
WAL-plus-`MemoryStorage`, a foundational rewrite with a real RAM tradeoff — not a
consolidation of the two tidwall logs.

## The cost of multiple stores: crash-recovery invariants

Splitting durable state across independently-fsync'd stores buys clean lifecycles
but owes a debt: **any two stores that must be mutually consistent need an
explicit invariant that recovery restores after a crash lands between their
fsyncs.** committed cannot fsync two tidwall logs, or a log and BoltDB, as one
atomic unit, so the discipline is instead "keep the inconsistent-but-recoverable
state recoverable."

The store-pair invariants today:

- **eventLog vs `metadata` applied-index.** The apply path fsyncs the event log
  *before* bumping the applied index in BoltDB, so a crash leaves
  `EventIndex ≥ AppliedIndex`. Recovery re-applies the gap (`checkStorageInvariant`
  treats `EventIndex > AppliedIndex` as the benign replay window and
  `EventIndex < AppliedIndex` as fatal corruption). This is why every apply handler
  must be **replay-idempotent**.
- **EntryLog vs StateLog.** A crash between the entry-log fsync and the state-log
  fsync can leave `HardState.Term` below the last entry's term. Recovery must
  **truncate the entry tail down to the highest entry whose term ≤ `HardState.Term`**
  and let raft re-replicate: those entries were never durably acked at that term
  (the node crashed before persisting `HardState`, hence before signalling the
  ack), and anything genuinely committed elsewhere returns on catch-up. Reversing
  the write order does not help — it exposes `Commit > lastIndex` instead — so the
  recovery invariant is the fix, not the ordering.
- **Config-apply idempotency.** Because apply can replay after a crash (first
  invariant above), config writes must dedupe byte-identical replays rather than
  append a duplicate version; otherwise nodes that crashed at different points
  diverge on version history and rollback-by-number.

Genuine corruption (a checksum failure or a torn mid-log record, distinct from a
plausible one-fsync-behind crash) is *not* recoverable in place: it fails loudly
and the node is rebuilt from a healthy peer — see
[operations/rebuild.md](operations/rebuild.md).

The lesson generalizes: the fix for a cross-store crash window is an **explicit,
tested recovery invariant**, not merging the stores. The most robust way to keep
these honest is crash-injection testing that kills the process at each fsync
boundary and asserts recovery — the natural next step beyond the per-bug tests
that established each invariant above.

## Filesystem and platform requirements

Everything above assumes an `fsync` that is honored and a filesystem that makes a
file's — and its directory entry's — durability survive power loss. committed
therefore **requires a crash-consistent filesystem** for its durability
guarantee: a journaling filesystem (ext4, xfs, NTFS) or a copy-on-write one
(btrfs, zfs, ReFS, APFS). This is the same requirement every durable database
has; a non-journaling filesystem (ext2, FAT) is not supported for a data
directory.

One consequence, made explicit so it isn't mistaken for a gap: committed does not
`fsync` a directory on every internal WAL **segment cut** (`tidwall/wal` doesn't
expose the hook). On a crash-consistent filesystem that is covered two ways — the
filesystem commits the new segment's directory entry through its journal or
transaction, and if a just-cut segment were nonetheless lost, the fail-loud
storage-invariant / checksum check turns it into a *fail-to-start* (a rebuild),
never a silent gap. (A plain file `fsync` does **not** by itself persist the
parent directory entry on ext4/xfs — that is the FS journal's job — so the
durability here rests on the journal plus the fail-loud backstop, not on the
per-`Ready` `fsync`.) A segment cut is therefore durable within a bounded window
on every filesystem committed supports; it is another reason the crash-consistent
requirement is a hard one rather than a suggestion.
(committed *does* explicitly directory-`fsync` its own swap/reset paths — the
event-log scrub swap and the entry-log snapshot reset — so those do not depend on
this.)

**Platform.** committed's production target is **Linux**. It cross-compiles to
Windows and macOS for development and the CLI, but **Windows is not a supported
production node** at this point — for example, the disk-usage watcher is
unavailable there (no `statfs`; see
[disk-limits.md](operations/disk-limits.md)). Run production nodes on Linux with
a crash-consistent filesystem.
