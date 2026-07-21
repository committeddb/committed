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
/v1/ingestable/{id}/status
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
of the versioned surface; its `info.version` tracks the release that last
revised the spec (bumped as part of cutting a release), which is
independent of the `/v1` URL major version.

List endpoints return bare JSON arrays. When pagination is needed it
arrives additively as query parameters (the dead-letter and error lists
already paginate via `since`/`limit`) — never as a breaking envelope
change around the array.

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

**Wire-decodable is not the same as semantically compatible.** "An older
binary can *decode* the bytes" (proto3 ignores unknown fields) does not
imply "an older binary *applies* them correctly." An additive field or a
new entity type can be perfectly decodable yet change what an older
consumer must *do* — and a consumer that decodes but mishandles it (a
crash, a dead-letter, a wrong projection) is a backward-incompatibility
just as real as an unreadable byte. Committed closes that gap with a
**cluster feature level** (below): anything an older peer would mishandle
is not *emitted* until every member advertises that it can apply it, so
the incompatible entry is never committed while a node that would mishandle
it is present. The one-way-transitions list must grow whenever an additive
change alters how an *older* consumer has to behave — not only when it
changes the bytes.

### Cluster feature level (semantic compatibility gate)

`version.FeatureLevel` is a monotonic integer each binary supports and
**self-announces** into the replicated `memberVersions` bucket on startup
(`LogNodeVersion`, applied like `LogNodeAPIURL`; survives restart and rides
snapshots). Any node can therefore compute the **cluster-agreed minimum**
feature level — the lowest level across current members, with an
un-announced member (one still starting, or a binary predating the
mechanism) counted as level 0.

A feature whose entries an older peer would mishandle gates its *emission*
on that minimum: the entry is proposed only once every member is at the
required level. During a rolling upgrade the minimum is held down by the
not-yet-upgraded nodes, so the feature stays dormant; it activates
cluster-wide the moment the last node upgrades and announces. This is how
Once every node carries the mechanism, a *gated* feature ships disabled until
the whole cluster is upgraded, keeping a rolling upgrade safe in both directions
between those releases: an old node is never handed gated state it can't apply.
This is a discipline the emitter must follow (bump + gate, below), not a
framework-enforced invariant — an ungated new entity type would ship anyway.

To introduce such a feature: bump `version.FeatureLevel`, add a
`featureLevel*` requirement constant at the emitting site, and gate the
emission on `featureEnabled(that level)`. Never renumber or reuse a level.

Two boundaries the gate cannot cover:

- **Introducing the mechanism itself is a forward full-stop.** The mechanism
  bootstraps by announcing a new system entity type (`LogNodeVersion`) that
  *cannot* be gated — it is the bootstrap. A node still on a pre-mechanism binary
  **fatal-exits** when it applies that announcement, so upgrading a multi-node
  cluster *into* the first mechanism-carrying release (0.7.2) must be done
  all-nodes-at-once, not rolling. See [upgrade.md](operations/upgrade.md). ("Safe
  in both directions" holds only *between* releases that already carry the
  mechanism.)
- **Rolling back past the mechanism is one-way** — a pre-mechanism binary meets
  entries it can't apply (see the list at the end).

### Log entities (protobuf)

Every committed proposal, type, syncable record, etc. is a protobuf
message on the permanent event log (`internal/cluster/clusterpb`):
`LogProposal`, `LogEntity`, `LogType`, `LogConfiguration`,
`LogSyncableIndex`, `LogIngestablePosition`, `LogSyncableDeadLetter`,
`LogSyncableStuck`, `LogSyncableSkipRequest`, `LogTypeMigrationDeadLetter`,
`LogScrub`, `LogNodeAPIURL`, `LogNodeVersion`. The rule is **add-only**:

- A new field gets a new, never-before-used tag number. Old binaries
  ignore unknown fields (proto3); new binaries treat an absent field as
  its zero value.
- A field tag is **never** removed, renumbered, or repurposed for a
  different meaning — that silently corrupts the reading of every old
  entry that used it. Retire a field by leaving it unused, not by reusing
  its number.

**The typed control envelope (0.7.3-beta).** A `LogEntity`'s payload is a
`oneof body` — exactly one of `LogRow` (an upsert), `LogDelete` (a keyed
tombstone, carrying `keep_data`), or `LogRefresh` (a reconciling-refresh
boundary marker) — so an entity's role is explicit on the wire and impossible
states (a refresh with a key, a row with `keep_data`) are unrepresentable. The
flat `Key`/`Data`/`generation`/`refresh_boundary` fields on `LogEntity` are
**legacy, decode-only**: logs written by ≤ 0.7.2-beta carry them, and readers
map both encodings into one view at a single chokepoint
(`cluster.logEntityView`) — writers never emit them again. The delete sentinel
value exists only in memory; on the wire a delete is the explicit variant.

Extending the envelope with a **new variant** is a fixed recipe:

1. Add a new `oneof body` message with a fresh tag number — wire-level
   add-only (an old reader decodes `body` as unset).
2. **Feature-gate its emission** (`db.featureEnabled`, see cluster feature
   level above) so the variant is only committed once every member can apply
   it. "Decodes as unset" is *not* compatible-enough for the apply path — an
   old binary would misapply the entity as empty, so the gate is mandatory.
3. Handle it in `logEntityView` — every reader (unmarshal, scrub traversals)
   goes through that one switch. An entity carrying `LogEntity`-level wire
   tags the binary does not know fails decode loudly — so a feature-gate
   bypass surfaces as an apply failure, never a silent empty-entity misapply.
   Unknown tags *inside* a known variant's message stay ordinary add-only
   evolution. The same guard defines the **data-dir support floor**: 0.7.3+
   reads data dirs written by **0.7.2-beta and later**. Dirs older than that
   are unsupported — pre-v0.5-beta logs carry the long-removed `Timestamp`
   field 4, which trips the guard — and must be recreated, not upgraded.
4. Give it a `cluster.EntityVariant` constant and handle it in every consumer
   switch. Consumers apply an entity by switching on `Entity.Variant()`
   (sinks; the wal apply dispatch admits only row/delete to internal
   handlers; the migration chain migrates only rows) — a variant a consumer
   does not handle lands in its `default` case and dead-letters/errors
   explicitly.

The envelope's own introduction is the one deliberate exception to step 2: at
0.7.3-beta every entity — including plain rows — switched to the envelope
without a feature gate (gating would have threaded cluster state into
`Proposal.Marshal`, which is a pure function used far from the db), making
0.7.2 → 0.7.3 a **full-stop upgrade** (see
[upgrade.md](operations/upgrade.md)). Every later variant follows the recipe
and stays rolling-safe.

New entity *types* (a new system type ID) are additive on the wire, but an
old binary that meets one it doesn't recognize can't *resolve* it (a system
type UUID is indistinguishable from an unknown user type) and would
fatal-exit applying it. This is exactly a "wire-decodable but not
semantically compatible" case, so a new system type MUST gate its emission
on a **cluster feature level** (above): it is not proposed until every
member advertises support, so an old node never receives it. That is what
makes "new entity types ship disabled until the whole cluster is upgraded"
an enforced invariant rather than an operator's promise.

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
`ingestables`, `ingestablePositions`, `ingestSourceSeq`, `topicRefreshEpoch`,
`eventTombstones`, `memberAPIURLs`, `memberPeerURLs`, `memberVersions`,
`syncables`, `syncableIndexes`, `syncableDeadLetters`, `syncableStuck`,
`syncableSkipRequests`, `typeMigrationDeadLetters`, `appliedIndex`,
`pendingScrub`. Adding a bucket is additive (a new binary
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
- **Rolling back below a feature whose entries are already on your log.**
  The cluster feature level keeps a feature dormant until every member can
  apply it, but once it activates its entries are committed permanently.
  Rolling a node back to a binary that *predates that feature* means it meets
  entries it can't apply — a rebuild, not a binary swap. Concretely, rolling
  back past **0.7.2** to a pre-0.7.2 binary is barred: the `LogNodeVersion`
  announcement is a system type the old binary can't resolve, so it
  **fatal-exits** on apply; and the **ingest refresh-boundary marker**
  (`RefreshBoundary`/`Generation` on `LogEntity`) decodes but dead-letters on a
  pre-marker binary. Separately, 0.7.2 changed **binary-column payloads to
  base64** (was Postgres `\x` hex / MySQL raw text); a pre-0.7.2 syncable would
  mishandle those, but it can't run this far anyway, so the change needs no
  separate gate — it rides this same one-way boundary. External payload consumers
  (webhooks, tools) that decode binary fields *were* release-noted and must
  base64-decode.
- **Rolling back past 0.7.3 (the typed control envelope).** From 0.7.3-beta
  every committed entity is envelope-encoded (`LogEntity.body`, above). A
  ≤ 0.7.2 binary decodes such an entry *without error* but sees it as an
  **empty entity** (proto3 ignores the unknown `body` tags and the legacy flat
  fields are unset) and silently misapplies it — worse than a fatal-exit.
  Rolling back means a rebuild; and the forward upgrade must be **full-stop**,
  not rolling, for the same reason (see
  [upgrade.md](operations/upgrade.md)).
- **A raft transport `protocolVersion` bump.** The peer transport
  currently accepts only an exact protocol-version match, so a bump is a
  flag-day: it partitions a half-upgraded cluster until every node is on
  the new version. Ship such a bump stop-the-world, or behind a future
  negotiated dual-accept built on the same cluster-feature-level mechanism.

Routine upgrades *between* two releases that already share these formats
have no one-way transition and roll back with a plain binary swap. When in
doubt, the release notes for each version state whether it introduces a
one-way transition.
