# Actuals

An **Actual** is a committed fact: the [proposal](proposals.md) that
consensus ordered and wrote to the log at a fixed **index**. You propose
proposals; you *sync* Actuals. A [syncable](syncables.md) is handed Actuals,
in index order, and never sees a proposal.

Each Actual carries:

- its **index**, its position in the log's single total order. Every node
  holds the same Actuals at the same indexes.
- its **entities**, as proposed: each with its [type](types.md) and the type
  **version** stamped at write time, its key, and its payload — or, for a
  delete, a tombstone with no payload.
- for Actuals an [ingestable](ingestables.md) wrote, the source's
  **provenance**: the source commit time and transaction id.

## The log is permanent

Actuals are kept forever: a new syncable replays from index 0 and builds its
destination from the whole history, and any destination can be rebuilt the
same way. Two things remove history:

- a **right-to-be-forgotten** delete scrubs a key's entries (see
  [rtbf.md](operations/rtbf.md));
- a topic whose type is `entityKind = "snapshot"` keeps only the latest entry
  per key (see [entity kinds](types.md#entity-kinds)).

The bytes of an Actual never change. How it is *read* can: a
[restatement](types.md#restatements-changing-how-history-is-read) rebinds
the version a range of Actuals is read as, and syncables in the
always-current stance read older versions through the type's migration chain.

## Delivery to syncables

A syncable applies Actuals in index order and checkpoints its progress
through consensus. After a crash it resumes from its checkpoint and may
receive Actuals it already applied, at most its
[checkpoint cadence](syncables.md#checkpoint-cadence) worth. SQL
destinations apply idempotently, so a re-delivered Actual's effect lands
once; an HTTP receiver deduplicates on its side (see
[webhook-receiver.md](webhook-receiver.md#idempotency-is-yours)).

Entities in one Actual are applied together: a SQL syncable applies them in
one destination transaction.

For how the log is stored and replicated, see
[event-log-architecture.md](event-log-architecture.md).
