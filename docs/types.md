# Types

A **type** is the schema and metadata for a topic's payload. Every entity
written to the log is tagged with a type ID, and the type ID names the topic:
`POST /v1/type/movie` declares the `movie` topic. A type is versioned — each
schema change is a new version, and every entity carries the version that was
current when it was written — and it can carry a contract that committed
checks payloads against.

```sh
curl -X POST -H 'Content-Type: text/toml' \
  --data-binary @examples/movies/type-movie.toml \
  http://localhost:8080/v1/type/movie
```

```toml
[type]
name          = "TenantEvents"
entityKind    = "event"            # what the entities are; see Entity kinds
discriminator = "$.event_type"     # event kinds only: the field naming the variant
schemaType    = "JSONSchema"       # JSONSchema or Protobuf
schema        = '{"type":"object"}'
validate      = "none"             # none | schema (the gate) | announce (the tripwire)
# schemaChangeTopic = "schema-changes"   # required with validate = "announce"

[migration]                        # required on every version after the first
transform = '.'                    # jq: the previous version's payload → this one
# none = true                      # or: old data already fits, no rewrite
# nonConvertible = true            # or: this version needs data old entities never had
```

| Endpoint | |
|---|---|
| `POST /v1/type/{id}` | Declare a type, or a new version of it. |
| `GET /v1/type`, `GET /v1/type/{id}/versions`, `GET /v1/type/{id}/versions/{version}` | Read types and their version history back. |
| `GET /v1/type/{id}/migration-errors`, `POST /v1/type/{id}/migration-retry/{index}` | Entities the migration chain could not convert; see [stuck-syncables.md](operations/stuck-syncables.md). |
| `POST /v1/restatement/{id}`, `POST /v1/restatement/dryrun`, `GET /v1/restatement` | [Restatements](#restatements-changing-how-history-is-read). |

Types cannot be deleted: log entries reference their type permanently, so
deleting one would break replay. `DELETE /v1/type/{id}` answers 405; POST a
new version to change a type, and leave an unused one as harmless residue.

## Entity kinds

A type can declare what the entities written under it are, ordered here
by how much interpretation a consumer needs to apply one:

```toml
[type]
name          = "TenantEvents"
entityKind    = "event"
discriminator = "$.event_type"
```

- **`snapshot`** — full objects, "tenant X is now {…}". Apply =
  overwrite (last-write-wins per key). Kafka compacted-topic semantics.
- **`delta`** — state-relative patches, "set tier=prod / add 3".
  **Rejected at type creation**: syncables deliver at-least-once, and a
  redelivered non-idempotent patch corrupts. Model the changes as
  events instead.
- **`event`** — domain facts, "tenant.provisioned happened". Apply =
  fold via domain rules; partial and implicative by design. An event
  type may also declare a `discriminator` — the jsonpath of the field
  that distinguishes its variants — for projection tooling to consume.
- **`command`** — requests, "please provision X". Apply = execute side
  effects; replay is dangerous, and the lifecycle (dedup, acks)
  belongs to the consumer. Committed carries commands; it does not
  execute them.
- **`standalone`** — facts with no aggregate to converge on (audit,
  telemetry). Apply = append; never folded.
- **`revision`** — full states in a retained, ordered series. Like
  `snapshot` (self-contained, no folding) but every prior version is kept
  and individually addressable — roll back, or read "as of" a version.
  The latest is current; the history is part of the data. This is the
  shape of committed's own versioned configs (type/database/syncable/
  ingestable, with their rollback endpoints) and of any record you keep
  as a revision history rather than just a current value.

The snapshot-vs-event fork is the one that matters in practice. Choose
**snapshot** when one writer owns the object and no read model needs
history — the topic carries current state and is key-compactable by
definition. Choose **event** when there are multiple writers, the
entities are audit or intent, or you want many read models folded from
the same history.

The entity kind is advisory metadata, validated at config time.
Declaring none (`unspecified`, the default for every type written
before the field existed) changes nothing — no enforcement ever applies
to it. A declared kind is immutable: a version bump must restate it,
and changing it means a new type/topic. Today the validation matrix
warns (log + `committed_entity_kind_misuse` metric) when a leaf-mapped
`sql` syncable targets an `event`-kind topic — events are partial by
design, so each variant either dead-letters on the jsonpaths it doesn't
carry or clobbers columns it didn't mean to write. The
[whole-payload `"$"` mapping](syncables.md#landing-a-whole-payload-in-one-column) is the right way to land an event topic in
SQL and is exempt from the warning.

## Superseded versions (`onSupersede`)

A later entity with the same key as an earlier one supersedes it. By
default the log keeps both. `onSupersede` declares whether the scrub
removes the superseded versions instead, keeping only each key's latest:

```toml
[type]
name          = "order-events"
entityKind    = "event"
discriminator = "$.type"
onSupersede   = "scrub"
```

| Entity kind | Default | The other value |
|---|---|---|
| `snapshot` | `"scrub"` | `"keep"` is refused: a snapshot that keeps every prior state is a `revision` |
| `revision` | `"keep"` | `"scrub"` is refused: a revision's prior versions are its data |
| `event`, `standalone`, `command`, unspecified | `"keep"` | `"scrub"` |

Declare `"scrub"` on a type whose key names one entity, so that a later
write to the key is a correction of that entity rather than a new one:
an events table ingested with its event id as the key, where the source
masks or replaces an event in place. The scrub then removes the original
from the log, which is how field-level erasure at the source reaches
committed (see [rtbf.md](operations/rtbf.md#masking-or-correcting-at-the-source)).
Do not declare it on a type whose key groups many entities, such as
events proposed under their stream's id: every event but the stream's
last would be scrubbed.

- **Changing it.** `onSupersede` can change in either direction on a
  re-POST, in place, with no version bump. Switching to `"scrub"` is
  one-way in effect: the next scrub removes every superseded version
  already in the log, including those written before the switch, and
  switching back to `"keep"` restores none of them.
- **When versions leave the log.** Every scrub removes them, and the
  automatic scheduler runs one at least weekly while a type declared
  `"scrub"` has been written since the last scrub.
- **Cluster version.** A non-default value is accepted once every node
  runs 0.8.2 or later (feature level 3).

## Versions and migrations

The log is permanently heterogeneous: there is no ALTER TABLE, only new writes
under new versions, and each entity keeps the version it was stamped with.

- **A schema change is a new version.** Re-POSTing a type with a different
  `schema` bumps its version, and the POST must carry a `[migration]` section
  saying how data written under the previous version relates to the new one:
  - `transform = "<jq>"` — a jq program that rewrites a previous-version
    payload into this version's shape;
  - `none = true` — the change is additive enough that old data needs no
    rewriting;
  - `nonConvertible = true` — this version needs information old entities
    never contained, so no program can upgrade them. The break is declared
    with the schema bump that causes it; a type's first version cannot
    declare one.
- **Other changes apply in place**, at the same version: a migration fix, an
  entity kind adopted by a type that had none, a discriminator, or a new
  `schemaChangeTopic`. An unchanged re-POST is a no-op.
- **The migration chain** applies these transforms step by step, upgrading an
  older entity to the current version. Only syncables in the always-current
  stance read through it; see
  [consumer stances](syncables.md#consumer-stances-how-a-syncable-meets-a-versioned-topic).
  A `nonConvertible` bump that would strand an existing always-current
  syncable is refused at POST, naming the syncables; re-POST with
  `?force=true` to strand them deliberately.

**Fixing a type's migration — the fan-out case.** A type's `[migration]` jq (the
transform that brings older-version Actuals up to the current schema) is read by
**every** projection on that topic. Committed lets you correct a buggy migration
in place, at the same version — but that edit, too, changes only how *future*
reads transform the data. Every row already synced through the old migration
stays as it was, on **all** dependent projections, and committed does not
currently tell you which projections are now stale or rebuild them for you.

**After an in-place migration fix, rebuilding the dependent projections is your
responsibility** — blue-green each one (see [read-models.md](read-models.md#changing-the-rules-after-a-projection-is-live)) — for the correction to reach
already-synced history. Treat a migration edit as "applies going forward;
rebuild the dependents to fix the past." If you only need the correction for new
data and accept the historical rows as-is, that is a valid choice — just make it
knowingly, because nothing rebuilds them on your behalf.

## Schema contracts: the census, the gate, and the tripwire

A type may carry a schema. committed does three things with one: take a
**census** of an existing table's payload shapes so you can draft a contract
from evidence; **gate** direct proposals against the contract; or run a
**tripwire** that lets divergent data commit and announces each new divergent
shape to a topic you watch.

### The shape census

The hardest part of blessing a contract on an existing table is that nobody
knows what's in it — JSON columns whose shape lives only in what developers
happened to write, evolved over years, often with several shapes interleaved.
committed takes that census for free: the ingest snapshot pass already streams
every historical row, and the worker folds each row's payload **shape** — its
JSON paths and their types, never its values — into a per-topic census,
publishes it as replicated state, and any node serves it on
`GET /v1/ingestable/{id}/status`. The census drafts a JSON Schema you review
and bless; inference is bootstrap-only, so the draft is never auto-blessed and
the runtime never infers.

**Configuration.** Three keys in the `[ingestable]` envelope:

| Key | Default | Meaning |
|---|---|---|
| `census` | `true` | Take the census during the snapshot pass. Costs about a microsecond per row. |
| `censusValues` | `false` | Also keep a bounded set of distinct values per string path, the input for `enum` drafting. Opt-in because it puts source **values** into replicated state. |
| `censusValueLimit` | `16` | How many distinct values to keep per path before the path is marked overflowed and stops accumulating (an overflowed path gets no `enum`). |

The census happens only while a snapshot streams the table. A table that
was ingested with `census = false` gets a census only from a fresh snapshot
(see [re-snapshot](ingestables.md#reconciling-refresh-how-a-re-snapshot-removes-rows-deleted-at-the-source)).

**What the status reports.** The `census` object is keyed by topic and is
omitted until a census has been published (census opted out, or no snapshot
yet). Per topic:

- `refreshEpoch` — the snapshot pass the census describes. A resumed worker
  continues the same epoch; a fresh full snapshot starts a higher one and
  resets the census instead of double-counting.
- `rows` — rows folded so far.
- `shapes[]` — each distinct payload shape: its `fingerprint`, the `shape`
  (paths with types, such as `$.caption:string`, `$.tags[]:string`,
  `$.meta.size:number`), its row `count`, and `firstRow` / `lastRow`, the
  1-based row ordinals of its first and latest sighting within the pass. Two
  shapes with overlapping ranges are the interleaved-shapes evidence.
- `paths[]` — the per-path view derived from the shapes: every path with the
  types observed at it, how many rows carry it, and its row range. Paths
  nested inside JSON columns are included.
- `draftSchema` — a JSON Schema mirroring the observed structure, with
  `additionalProperties: false` at every level, ready to review and POST as a
  type. With `censusValues` on, a string path whose distinct values stayed
  within the limit is drafted as an `enum`, so a brand-new value later trips
  the tripwire as an ordinary violation.

The census publishes at most once every few seconds during the pass and once
more when the pass closes, so a short snapshot may show only the final
census.

**Limits.** The census describes only what was written: it is first-row
detection over history, not enforcement on the producer's write path. And a
discriminant the producer never recorded cannot be recovered by any tool: if
two shapes interleave with no field telling them apart, the census shows the
interleaving and nothing more. Fix that at the producer, or bind readings by
row range with [restatements](#restatements-changing-how-history-is-read).

**Personal data.** By default the census carries types and paths only.
`censusValues = true` puts source values into the replicated census record,
and right-to-be-forgotten erasure does not scrub it — see
[rtbf.md](operations/rtbf.md#adjacent-copies-and-retention-you-own).

### Declaring the contract

A type's contract is its `schemaType` (`JSONSchema` or `Protobuf`, the two
languages the binary can check) and its `schema`. A type that validates must
name both; a type without a schema validates nothing. What happens when a
payload does not match is chosen by `validate`:

| `validate` | Behaviour |
|---|---|
| `"none"` (default) | Payloads are never checked. |
| `"schema"` | The **gate**: a direct proposal whose payload violates the schema is refused. |
| `"announce"` | The **tripwire**: the payload commits, and the first occurrence of each distinct divergent shape is announced on `schemaChangeTopic`. |

The strategy is a word; the integers of releases before 0.8.0 are refused
at POST naming the word each became.

### The gate: `validate = "schema"`

`POST /v1/proposal` checks each entity of a gated type against the schema
and refuses the proposal with `400 schema_validation_failed`, naming the
type, when one does not conform. Nothing commits.

The gate applies to direct proposals only. Rows arriving through CDC ingest
are never refused — a non-conformant source row is a true fact about the
source, and refusing to record it would make the log less true. A CDC-fed
topic that needs to know when its contract stops matching reality uses the
tripwire.

### The tripwire: `validate = "announce"`

An announce-typed type names an events topic in `schemaChangeTopic`. Every
write path runs the tripwire — direct proposals, CDC streaming, and the CDC
snapshot pass — and it never blocks or fails a write. When a payload
diverges from the schema, the data commits as it would have anyway, and the
first time each distinct divergent **shape** is seen, a `ContractExtension`
event is proposed to the events topic.

**Rules on the destination.** `schemaChangeTopic` is only valid with
`validate = "announce"`, and announce requires it. The destination must be an
existing type — declare the events topic first — and must not itself be
announce-typed (an events topic cannot announce its own divergences), nor the
type itself. Any other type works; `entityKind = "standalone"` fits an events
topic. The destination is routing, not shape: re-POSTing the type with a
different `schemaChangeTopic` re-points it in place, without a version bump.
For the schema to catch *added* fields, declare `additionalProperties: false`.

**The event.** A JSON payload with:

| Field | Meaning |
|---|---|
| `typeID`, `typeName`, `version` | The contract diverged from — the version the payload was validated against, which is its stamp, not necessarily the latest. |
| `fingerprint` | The divergent shape's signature. |
| `observedShape` | The shape as a path list, types and paths only. |
| `violations` | The structured validation failures — where and why, never sample values. |
| `ingestableID`, `sourceSeq` | Where the first occurrence came from when it arrived through CDC; absent for a direct proposal. |

The event's key is `typeID:version:fingerprint`. Delivery is at-least-once:
one event per distinct shape, with the dedupe mark replicated so it survives
restarts and failover, and the rare concurrent detection on two nodes
converging in a keyed destination. The announcement is committed before the
data it describes, so a crash between the two cannot lose the only
announcement of a shape whose rows are already committed.

**Which contract a writer checks.** A direct proposal is checked against the
type's current version. A running CDC worker checks against the contract
that was current when the worker was built — the same point at which the
version it stamps on rows is fixed. After blessing a new contract on a topic
that is already flowing, re-POST the ingestable, or restart the node, so its
worker picks the contract up.

**Consuming the events.** Attach any ordinary syncable to the events topic:
a SQL projection into a table your data tests watch, an `http` syncable
posting to a webhook, an Iceberg table. The consumer learns the day reality
diverges from the contract, with the diff in hand.

### From evidence to contract

1. Ingest the table with the census on (the default). Read the census from
   `GET /v1/ingestable/{id}/status` and review `draftSchema` and the shapes.
2. Declare the events topic, then the contract with `validate = "announce"`
   pointing at it:

```toml
# 1. POST /v1/type/schema-changes — any non-announce type works
[type]
name = "SchemaChanges"
entityKind = "standalone"

# 2. POST /v1/type/photo-meta — the contract, drafted from the census
[type]
name = "PhotoMeta"
schemaType = "JSONSchema"
schema = '{"type":"object","properties":{"caption":{"type":"string"}},"additionalProperties":false}'
validate = "announce"
schemaChangeTopic = "schema-changes"
```

3. Re-POST the ingestable so its worker validates against the new contract,
   and attach a syncable to `schema-changes`.
4. When the census or the tripwire shows that history was written in a shape
   the contract did not anticipate, the bytes are still true and the
   *reading* is what changes: see
   [restatements](#restatements-changing-how-history-is-read).

## Restatements: changing how history is read

An entity's stamped version is a *cache of the default reading in force at
write time* — for CDC data, committed classified it; nobody asserted anything.
When the census or the tripwire reveals that months of entities are stamped v1
but were really v2-shaped (the unannounced writer), the log's bytes are still
true; the *reading* is wrong. There is no ALTER TABLE and no rewrite:
**restatements** are append-only, consensus-ordered statements that rebind
readings. Entities of a type committed in an index range (optionally narrowed
to one stamped version and a deterministic jq predicate) read as a different
declared version from the moment the restatement commits. The same mechanism
covers a `nonConvertible` break whose below-break data turns out readable
after all, and understanding that simply evolved.

```toml
# POST /v1/restatement/photos-backfill-v2
[restatement]
topic = "photo-meta"        # the type (topic) whose readings rebind
fromIndex = 1200           # inclusive raft-index range of existing entities
toIndex = 8400
readAsVersion = 2        # read these as v2
fromVersion = 1            # optional: only entities STAMPED v1
# predicate = '.writer == "app-b"'   # optional: the interleaved-writers case,
                                     # a deterministic jq program (no now(), no env)
```

Every syncable receives the authoritative reading: stamp ⊕ restatement fold.
A rebound entity dispatches, and enters the migration chain, at its
*effective* version, so a v2-shaped row wrongly stamped v1 is never mangled by
the v1→v2 transform. Among matching restatements, later in the log wins,
matching always against the stamp, and replaying data plus restatements is
deterministic. Topics with no restatements pay nothing on the read path.

A restatement marks dependent syncables **stale**
(`GET /v1/syncable/{id}/status` → `interpretationStale`): rows synced before
it landed were derived under the superseded reading. Staleness is loud and
queryable, never auto-healed — re-derivation is yours to trigger:
`POST /v1/syncable/{id}/rematerialize` replays a keyed `sql` mirror in place,
and a projection is rebuilt blue-green (see
[read-models.md](read-models.md#changing-the-rules-after-a-projection-is-live)).

Restatements are the highest-blast-radius config in committed: they are
**append-only** (a wrong one cannot be edited, only corrected by another —
later in the log wins), and admitting one instantly marks every consumer of
the topic `interpretationStale` — rows materialized before it keep the
superseded reading until you re-materialize each syncable. So the workflow is
**rehearse, then author**:

1. `POST /v1/restatement/dryrun` with the exact body you intend to admit. Nothing
   is admitted or stored; the rehearsal runs the restatement's own index range
   through the *real* interpretation fold and reports what the selectors
   actually catch (`stampEligible`, `matched`, broken down `byStampedVersion`),
   what the restatement really *changes* (`rebound`, with before/after reading
   `samples`), rows its predicate cannot evaluate (`predicateErrors` — each of
   these would dead-letter or wedge a live consumer), already-applied restatements it
   composes with (`overlaps`), and the re-materialization bill
   (`affectedSyncables`, with each consumer's current interpretation pin).
   Auto-generated `findings` name the authoring signatures: a restatement that
   matches nothing, one that changes no readings, a predicate that filtered
   nothing. Admission-level validation runs in full, with the same words the
   real POST would refuse with — and a rehearsal works even mid-rolling-upgrade,
   when the real POST would still be refused by the feature gate (the report
   says so).
2. Iterate until the findings list is empty and `matched`/`rebound` are the
   numbers you expected.
3. `POST /v1/restatement/{id}` with the same body — then re-materialize the
   affected syncables (`POST /v1/syncable/{id}/rematerialize`) when you want
   the corrected reading to reach their already-synced rows.

Like the syncable dry-run, the scan is budgeted (`?maxEntries`, default 100k)
and time-bounded (`?timeoutSeconds`); an exhausted budget yields a **partial**
report that says so (`coverage`, `truncated`) — never a silently-complete-
looking one. Sample rows quote entity keys verbatim (that precision is the
diagnostic point; the trusted-appliance caveat from the syncable dry-run
applies).
