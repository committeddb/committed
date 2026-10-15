# Proposals

A **proposal** is a write *request*: one or more entities, each tagged with a
type ID, offered to the log together. You propose a proposal; consensus
decides its fate. Until it commits it has no place in the order. Once
consensus orders it and writes it at a fixed index, it is an
[Actual](actuals.md).

Proposals arrive two ways: from an [ingestable](ingestables.md), which turns
a source database's changes into proposals, or directly over HTTP.

## Proposing over HTTP

```sh
curl -X POST -H 'Content-Type: application/json' --data-binary @- \
  http://localhost:8080/v1/proposal <<'EOF2'
{
  "entities": [
    {
      "typeId": "movie",
      "key": "mv0000099",
      "data": { "movie_id": "mv0000099", "title": "Plan 9 from Inner Join", "year": 1959 }
    }
  ]
}
EOF2
```

Each entity is `{ typeId, key, data }`:

- **`typeId`** names the [type](types.md), and so the topic. It must exist
  (`400 type_not_found` otherwise); committed's own system types are refused
  (`400 type_reserved`).
- **`key`** identifies the entity within its type. Syncables converge on it:
  a keyed destination upserts by it, and a delete removes by it.
- **`data`** is the JSON payload.

The request returns once the proposal has committed and been applied on the
node that answered, so a write you've just made is visible to the configs
that depend on it. All entities in one proposal commit together, as one
entry in the log, or none do.

## Deletes

`"delete": true` on an entity issues a **tombstone** for `(typeId, key)`
instead of an upsert. It carries no payload — `data` is ignored and no schema
check runs — and every syncable consuming the topic removes the record keyed
by `key`. A delete is also the first step of right-to-be-forgotten erasure,
which goes on to scrub the key's whole history from the log; see
[rtbf.md](operations/rtbf.md).

```json
{ "entities": [ { "typeId": "customer-event", "key": "subject-12345", "delete": true } ] }
```

## What can refuse a proposal

| Status | Code | Why |
|---|---|---|
| 400 | `invalid_json`, `empty_proposal` | The body isn't valid JSON, or carries no entities. |
| 400 | `type_not_found`, `type_reserved` | An entity names a type that doesn't exist, or a system type. |
| 400 | `schema_validation_failed` | An entity of a [gated type](types.md#the-gate-validate--schema) doesn't match its schema. Nothing in the proposal commits. |
| 413 | `proposal_too_large` | The proposal exceeds `COMMITTED_MAX_PROPOSAL_BYTES`; see [http-limits.md](operations/http-limits.md). |
| 507 | `insufficient_storage` | The cluster is refusing writes for disk space; see [disk-limits.md](operations/disk-limits.md). |
| 503 | `request_unconfirmed` | The commit wasn't confirmed before the deadline (or a leader change). The proposal **may still commit**, so a retry can commit it twice — harmless to keyed destinations, a duplicate row in a keyless one. |

## Properties to know

- **Write-only.** There is no endpoint to read the log back over HTTP; that
  is by design. To read committed data, replicate it to a queryable store
  with a [syncable](syncables.md) and query there.
- **No deduplication of your writes.** Proposing the same data twice commits
  it twice. Keyed destinations converge on the key regardless; a keyless
  history table records both (see
  [history tables](syncables.md#history-tables)).
- **Validation by type.** A type with `validate = "announce"` never refuses a
  proposal: the payload commits and a divergent shape is announced instead
  (see [the tripwire](types.md#the-tripwire-validate--announce)).
