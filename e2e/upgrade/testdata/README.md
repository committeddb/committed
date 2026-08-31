# Old-binary data-dir fixtures

Each `<tag>/datadir/` here is a complete committed data directory **written by
the released binary at that tag** — real event-log segments, bbolt metadata,
and raft state. The data dirs are **gitignored, never committed**: the upgrade
e2e (`old_datadir_test.go`, `make test/upgrade`) captures a missing one on
demand — `capture_fixture_test.go` builds the pinned release tag in a temp git
worktree, boots it, seeds it through its own HTTP API, and caches the result
here for later runs. Everything is plain Go test code on the same harness the
suite already uses (no shell, no extra tooling). The bytes are therefore
always genuinely old-binary-written — the pinned thing is the TAG and the
seed, not a checked-in blob. The test then opens each dir with the CURRENT
binary and asserts the full read contract: boot to `/ready`, syncables replay
every entry (decode of every era's bytes), a scrub rewrites the old log
(verified by a fresh syncable's replay), and the node restarts over the
rewritten dir.

Capture needs the release tags (`git fetch --tags` on a shallow clone) and,
for a CDC-seeded era, docker — at capture time only; replaying a cached
fixture needs neither.

Why captured, not synthesized: bytes generated with the current `clusterpb`
package can never reproduce a removed field or a superseded encoding, so a
guard against genuinely-old bytes would pass every synthetic test while
failing in the field. Only a dir the old binary actually wrote proves the
contract (docs/api-compatibility.md § on-disk compatibility).

## Fixtures

| Fixture | Generating tag | Seed | Why this era |
|---|---|---|---|
| `v0.7.3-beta/` | `v0.7.3-beta` | standard | The data-dir support floor: the first envelope-era release. (0.7.2-beta's flat encoding never had a deployment; its bytes are below the floor and fail decode by design — pinned by `TestLegacyFlatEntitiesFailLoudly`, not by a fixture.) |
| `v0.7.10-beta/` | `v0.7.10-beta` | standard + `--cdc` | The last pre-0.8.0 era — the upgrade path into this release. CDC-seeded, so it carries the ingest-written byte surface too: the refresh-boundary marker, generation-stamped rows, SourceSeq/IngestableID-stamped proposals, and old-regime ingest-dedup records in bbolt. |

The refresh-marker wire shape (`LogRefresh`) is one CDC-seeded era's worth of
coverage: the variant has been encoding-stable since the envelope introduced
it, so pinning it once (in the newest pre-0.8.0 era) covers the range.

## Adding an era

Append the era to `fixtureEras` in `old_datadir_test.go` with its expected
keys (set `cdc: true` for a CDC-seeded era) and run `make test/upgrade` — the
capture happens automatically. The standard seed is type `movie`, upserts
`mv1..mv3`, an upsert + RTBF delete for `subject-erased`, and a post-delete
`mv4`; a CDC era adds a 3-row MySQL snapshot plus one streamed row into topic
`cdcrow`, and the capture refuses to complete until its webhook guard has
observed the refresh-boundary marker arrive — a fixture can never silently
ship without the bytes it claims to pin.

To force a re-capture (say, after changing the seed), delete the cached
`<tag>/datadir/` and rerun. Never edit a captured `datadir/` by hand — the
bytes are the contract.
