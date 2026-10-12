# Loopback syncables: derived topics


When several syncables need the same cleanup — a jsonPath projection to a
canonical shape, version normalization via `mode = "always-current"` — the
transform logic is repeated N times and drifts. A `loopback` syncable applies
it ONCE, inside the cluster: it consumes a source topic, transforms each
entity, and proposes the result to a **derived topic** under that topic's own
type. Downstream syncables then consume the derived topic with dumb mappings.

```toml
[syncable]
name = "canonical-photos"
type = "loopback"
mode = "always-current"        # normalize versions before deriving (optional)

[loopback]
topic  = "photos-raw"          # source topic
target = "photos"              # derived topic (its type must exist)

[[loopback.mappings]]
jsonPath = "$.id"
jsonName    = "photo_id"

[[loopback.mappings]]
jsonPath = "$.meta.title"
jsonName    = "title"
```

No mappings means whole-payload passthrough — with `always-current`, that is
the version-normalization shape: the derived topic carries every entity
re-written at the current type version. Because the loopback sits behind the
same interpretation machinery as every syncable, restatements are folded in too —
**the derived topic is the cache of the source's current interpretation**,
and its status (`GET /v1/syncable/{id}/status`) shows `derivedFrom` /
`derivesInto` plus `interpretationStale` when a later restatement supersedes the
reading it was materialized under. Re-derivation is operator-triggered, never
automatic: `POST /v1/syncable/{id}/rematerialize` replays the source from
index 0 (a snapshot-kind derived topic converges by key).

The rules that keep the chain safe:

- **The derived topic is a materialization, never a source of truth.** The raw
  topic stays sacred; the derived one is rebuildable from it forever.
- **Keys are preserved** (re-keying is refused at POST): a delete proposed to
  the source — including an RTBF delete — forwards as a tombstone with the
  same key, so erasure chases the derivation chain. Generations and
  refresh-boundary markers forward verbatim too, so an ingest full-refresh of
  the source reconciles all the way through to the derived topic's destinations.
- **The derivation graph is a DAG with one producer per derived topic** —
  checked at POST and re-checked deterministically at apply (a config that
  races past admission is persisted but loudly degraded, never run). A cycle
  would re-derive its own output forever; a second producer would interleave
  two refresh-epoch spaces on one topic.
- **Producer handover must not regress the epoch space.** A loopback targeting
  a topic whose committed refresh epochs exceed its source's is refused at
  POST: forwarded sweeps could never reconcile the previous producer's
  higher-stamped rows, which would linger stale on downstream keyed syncables.
  Derive into a fresh topic instead — the old topic's log carries its history
  permanently. A source at or *above* the target's highwater is fine (its
  first refresh reconciles the handover), and replacing any producer with an
  **ingestable** is always safe: the ingest epoch floor is topic-keyed and
  sees every committed generation, forwarded ones included.
- **Declare the derived topic's type `entityKind = "snapshot"`** (recommended):
  replays then converge by key. Any other kind appends on replay and requires
  an explicit `acknowledgeAppendSemantics = true`.
- **Transforms are stateless per-entity** — no cross-entity aggregation,
  no filtering. Sequence-context analysis belongs offline; its output lands
  as restatements or as proposals to a topic of its own.

Chains (`raw → canonical → per-team`) are fine — each hop is its own loopback.
