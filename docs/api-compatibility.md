# API compatibility and versioning

This document is the contract for how the Committed HTTP API evolves. It
is for client authors and operators who need to know what an upgrade can
and cannot break.

## URL-prefix versioning

Every API endpoint is served under a major-version URL prefix:

```
/v1/proposal
/v1/database/{id}
/v1/syncable/{id}/status
...
```

The prefix is the *only* version signal. There is no version header,
no media-type parameter, and no query flag. A client that hardcodes
`/v1` in its base URL has pinned itself to a major version.

## What is allowed within a major version

Changes that **do not** bump the prefix (a client written against `/v1`
keeps working):

- Adding a new endpoint.
- Adding a new optional request field. Omitting it must preserve the
  prior behaviour.
- Adding a new field to a response body. Clients must ignore unknown
  fields.
- Adding a new optional query parameter with a backward-compatible
  default.
- Adding a new enum value or error `code` (clients must treat an
  unrecognised `code` as a generic failure for that HTTP status).
- Relaxing a validation rule so that previously-rejected requests now
  succeed.

Clients **must** tolerate all of the above to be considered compatible.

## What bumps the major version

Changes that **do** bump the prefix (e.g. introduce `/v2`):

- Removing or renaming an endpoint, field, or parameter.
- Changing the type or meaning of an existing field.
- Making a previously-optional request field required.
- Tightening validation so previously-valid requests start failing.
- Changing a success status code or the default behaviour of an
  endpoint.

When `/v2` ships, `/v1` continues to be served for **at least one
further release** so clients have an overlap window to migrate.

## Deprecation policy

A deprecation always gives at least one release of overlap between the
old and new behaviour. While something is deprecated:

- It keeps working unchanged.
- Responses carry a `Deprecation: true` header and a `Warning` header
  naming the replacement, so deprecated usage is visible in client and
  proxy logs.
- The removal is called out in the release notes for the release that
  introduces the deprecation and again for the release that removes it.

There are currently no deprecated endpoints. `/v1` is the first and only
major version; there is no pre-`/v1` surface to keep alive.

## Operational endpoints are not API surface

These endpoints are infrastructure for orchestrators and humans, not
part of the versioned API. They are **never** prefixed and are exempt
from this contract (and from authentication):

- `/health` — liveness probe
- `/ready` — readiness probe
- `/version` — build information
- `/openapi.yaml` — the machine-readable spec
- `/docs` — Swagger UI

The OpenAPI document at `/openapi.yaml` is the authoritative description
of the versioned surface; its `info.version` tracks the spec revision,
which is independent of the `/v1` URL major version.

## On-disk and wire compatibility

The HTTP contract above is for clients. This section is the contract for
**operators**: what a node's persisted state and a peer's wire format
guarantee across an upgrade, which is what makes the [rolling-upgrade
procedure](operations/upgrade.md) and its rollback safe.

The guarantee, within a major line: **a newer binary reads everything an
older binary wrote** (forward compatibility), and — except across the
one-way transitions listed at the end — an older binary reads what a
newer one wrote (backward compatibility, i.e. rollback). A node that
cannot read its on-disk state does not run on it; it fails to start with
`ErrCorruptEntry` (see [operations/rebuild.md](operations/rebuild.md)).

### Log entities (protobuf)

Every committed proposal, type, syncable record, etc. is a protobuf
message on the permanent event log (`internal/cluster/clusterpb`):
`LogProposal`, `LogEntity`, `LogType`, `LogConfiguration`,
`LogSyncableIndex`, `LogIngestablePosition`, `LogSyncableDeadLetter`,
`LogSyncableStuck`, `LogSyncableSkipRequest`, `LogTypeMigrationDeadLetter`,
`LogScrub`, `LogNodeAPIURL`. The rule is **add-only**:

- A new field gets a new, never-before-used tag number. Old binaries
  ignore unknown fields (proto3); new binaries treat an absent field as
  its zero value.
- A field tag is **never** removed, renumbered, or repurposed for a
  different meaning — that silently corrupts the reading of every old
  entry that used it. Retire a field by leaving it unused, not by reusing
  its number.

New entity *types* (a new system type ID) are additive: an old binary
that meets one it doesn't recognize must not be in the cluster yet — which
is why upgrades go one major line at a time and new entity types ship
disabled until the whole cluster is upgraded.

### WAL / event-log framing

Entries on disk are wrapped in a self-describing frame
(`internal/cluster/db/wal/checksum.go`):

```
[magic 0xC0 'C' 'L'][version 0x01][crc32c, 4 bytes BE][payload…]
```

The leading magic byte is a discriminator: pre-checksum (legacy) entries
begin with the raw protobuf/gob bytes (`0x08`/`0x24`), so a current
binary reads **both** legacy and framed entries without a persisted
format flag. A frame whose CRC32C doesn't match fails the read with
`ErrCorruptEntry` (a torn or bit-rotted entry is surfaced, never applied).
A future frame-version bump (`0x02`) would be introduced the same way —
new binaries read old versions; the bump itself is a one-way transition.

### BoltDB metadata buckets

Replicated metadata lives in named BoltDB buckets: `types`, `databases`,
`ingestables`, `ingestablePositions`, `ingestSourceSeq`, `eventTombstones`,
`memberAPIURLs`, `syncables`, `syncableIndexes`, `syncableDeadLetters`,
`syncableStuck`, `syncableSkipRequests`, `typeMigrationDeadLetters`,
`appliedIndex`, `pendingScrub`. Adding a bucket is additive (a new binary
creates it on open; an old binary ignores it). **Renaming or removing** a
bucket, or changing the encoding of the values within one, requires an
explicit migration step at open time and is a release-noted change.

### Snapshots and the state log

A raft snapshot's `Data` is the serialized BoltDB database; the state log
records (HardState, snapshot metadata) are gob-encoded and wrapped in the
same checksummed frame as the entry log. Both follow the same rules as
above — the BoltDB snapshot inherits the bucket contract, the state log
inherits the framing contract.

### One-way transitions (no rollback past these)

Some upgrades cannot be rolled back, because the new binary writes state
the old binary cannot read:

- **Enabling per-entry WAL checksums.** Once a checksum-aware binary
  writes a framed (`0xC0…`) entry, a pre-checksum binary — which doesn't
  know the magic byte — can't read it. Upgrading *to* a checksum-aware
  release is therefore one-way; rolling a node back to a pre-checksum
  binary means a [rebuild](operations/rebuild.md), not a binary swap.
- **A frame-version or snapshot-format bump**, when one ships, is called
  out as one-way in that release's notes.

Routine upgrades *between* two releases that already share these formats
have no one-way transition and roll back with a plain binary swap. When in
doubt, the release notes for each version state whether it introduces a
one-way transition.
