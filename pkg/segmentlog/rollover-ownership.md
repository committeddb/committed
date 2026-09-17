# Rollover ownership

Managed rollover separates physical preparation from authoritative layout
publication. These are private components within `pkg/segmentlog`; they do not
introduce an application dependency or another persisted receipt.

| Component | Responsibility | Evidence passed onward |
| --- | --- | --- |
| `Log` | Hold exclusive ownership and its operation mutex; stop writes to the old tail; switch appenders after publication | Current catalog and synchronized appender state |
| `segmentStorage` | Retain the old file, calculate its digest, install the new empty tail, construct its appender | Private `preparedRollover` bound to the source layout |
| `CatalogStore.publishRollover` | Consume preparation once, validate the successor layout, publish catalog and CURRENT | Successful authoritative layout selection |
| `durablefs` | Install and sync files and directory entries; durably replace CURRENT | Success or an error, including uncertain publication |
| Recovery | Follow CURRENT and validate the selected files from disk | Recovered state independent of any previous process |

## Preparation

The managed appender reports only successfully synchronized state. The old active
file's name is already durable. Holding the Log mutex prevents further writes to
that file while it becomes immutable. Preparation reads its bytes once for the
catalog digest; it does not rewrite, revalidate, or resync those bytes. A fully
erased range needs no file reference.

The installer writes the new tail header and makes both file contents and its name
durable. Segment storage constructs an empty appender directly from that known
state. `OpenTail` remains a recovery operation and is not used on this path.

The private handle contains the closed reference, new tail, source directory,
history, revision, and source filename. It is not a public mechanism for asserting
that arbitrary files are safe. Its fields and files must remain unchanged until
publication. The Log mutex covers preparation, publication, and handle transfer.

## Publication and failure

The publisher checks the preparation belongs to the current layout and consumes
it once. It builds the successor itself, preserving history, generation, rotation
target, and previous ranges. It validates and serializes that metadata, installs
the catalog durably, then replaces CURRENT durably. It does not repeat payload
file checks or the installer's directory sync.

The Log switches appenders only after publication succeeds. Only then can it
append and acknowledge new records. On failure it closes the prepared file handle
and poisons the Log. It does not delete prepared files or roll back CURRENT:
publication may have succeeded without durability confirmation. Reopening follows
CURRENT; a catalog artifact by itself does not select a layout.

## Scope

This preparation contract covers managed rollover. Initial creation, standalone
catalog publication, rewriting, and recovery retain their existing verification
paths. Recovery verifies all selected payload files. Rollover still hashes the
closed append file, serializes a complete catalog, and synchronously publishes
the new tail, catalog, and CURRENT before appending the next records.

Tests cover publication without reopening prepared payload files, rejection of
stale or foreign preparation, single consumption, and recovery before/after
CURRENT replacement failures. Existing installer failure and concurrent-operation
tests exercise preparation failures and serialization through the managed Log.
