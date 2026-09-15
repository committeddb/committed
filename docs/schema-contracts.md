# Schema contracts: the census, the gate, and the tripwire

A type may carry a schema. This page is the reference for the three things
committed does with one: take a **census** of an existing table's payload
shapes so you can draft a contract from evidence; **gate** direct proposals
against the contract; or run a **tripwire** that lets divergent data commit
and announces each new divergent shape to a topic you watch. The narrative
of why these exist is in the [README](../README.md#the-shape-census-drafting-a-contract-from-the-data);
this page is what each knob does.

## The shape census

While an ingestable's snapshot pass streams a table, the worker folds each
row's payload **shape** — its JSON paths and their types, never its values —
into a per-topic census, publishes the census as replicated state, and any
node serves it on `GET /v1/ingestable/{id}/status`.

**Configuration.** Three keys in the `[ingestable]` envelope:

| Key | Default | Meaning |
|---|---|---|
| `census` | `true` | Take the census during the snapshot pass. Costs about a microsecond per row. |
| `censusValues` | `false` | Also keep a bounded set of distinct values per string path, the input for `enum` drafting. Opt-in because it puts source **values** into replicated state. |
| `censusValueLimit` | `16` | How many distinct values to keep per path before the path is marked overflowed and stops accumulating (an overflowed path gets no `enum`). |

The census happens only while a snapshot streams the table. A table that
was ingested with `census = false` gets a census only from a fresh snapshot
(see [re-snapshot](operations/cdc-setup.md#reconciling-refresh-how-a-re-snapshot-removes-rows-deleted-at-the-source)).

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
row range with [restatements](read-models.md#restatements-changing-how-history-is-read--rehearse-first).

**Personal data.** By default the census carries types and paths only.
`censusValues = true` puts source values into the replicated census record,
and right-to-be-forgotten erasure does not scrub it — see
[rtbf.md](operations/rtbf.md#adjacent-copies-and-retention-you-own).

## Declaring the contract

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

## The gate: `validate = "schema"`

`POST /v1/proposal` checks each entity of a gated type against the schema
and refuses the proposal with `400 schema_validation_failed`, naming the
type, when one does not conform. Nothing commits.

The gate applies to direct proposals only. Rows arriving through CDC ingest
are never refused — a non-conformant source row is a true fact about the
source, and refusing to record it would make the log less true. A CDC-fed
topic that needs to know when its contract stops matching reality uses the
tripwire.

## The tripwire: `validate = "announce"`

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

## From evidence to contract

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
   [restatements](read-models.md#restatements-changing-how-history-is-read--rehearse-first).
