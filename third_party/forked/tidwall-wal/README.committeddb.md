# Forked `tidwall/wal` (committeddb)

This is a **patched** copy of [`github.com/tidwall/wal`](https://github.com/tidwall/wal),
consumed via a `replace` directive in the repo-root `go.mod`:

```
replace github.com/tidwall/wal => ./third_party/forked/tidwall-wal
```

The module path inside this copy is unchanged (`module github.com/tidwall/wal`),
so every `import` in committed resolves here with no source changes. License: MIT
(see `LICENSE`, retained verbatim).

## Upstream baseline

- **Version:** `v1.2.1`
- **Re-sync:** bump the tag in the root `go.mod`'s `require`, re-drop the upstream
  tree, re-apply committed's change (see below).

## Why it's forked

The WAL is committed's durability floor: a proposal is only acked after its entry
is fsync'd. Upstream `wal` fsyncs each segment's **content** (`sfile.Sync()` in
`writeBatch`/`cycle`) but **never fsyncs the parent directory** when it creates or
renames a segment file. On POSIX a file's directory entry is not durable until the
*directory* is fsync'd, so a power loss right after a `cycle()` (a new ~20 MB
segment) keeps the content-fsync'd new segment on disk while its unpersisted
directory entry vanishes on restart — silently dropping the just-acked entries it
held (surfacing as `P_local < R_local`, a fatal storage-invariant trip / raft
`appliedTo` panic, or, single-node or under a correlated power event, genuine
acked-data loss).

committed already fsyncs the directory on every swap it performs *itself*
(`internal/cluster/db/wal/fsync.go` → `fsutil.SyncDir`, used by the snapshot/
compaction/scrub/entry-log-reset swaps), but tidwall's segment lifecycle
(`cycle`, genesis create, `truncateFront`/`truncateBack`) is internal to the
library and exposes no cycle hook, no segment-count getter, and no dir-sync
option — so the invariant can only be enforced inside the library. etcd's own WAL
does exactly this (`fileutil.Fsync` on the dir in `cut()`); this fork brings
`tidwall/wal` to the same standard.

## What changed

One change, marked `committeddb fork patch` in `wal.go`:

- A `syncDir(path)` helper (fsync a directory fd; return the error), and a call to
  it — **gated on `!opts.NoSync`, error returned pre-ack** — at each site that
  creates or renames a segment file:
  - `cycle()` — after the new segment is `O_CREATE`'d (the load-bearing site: the
    hot append path for the permanent event log).
  - genesis `load()` — after the first segment is `O_CREATE`'d.
  - `truncateFront` / `truncateBack` — after the removals + `START`/`END`→final
    rename (raft/entry-log paths; folded in so the directory-durability invariant
    is uniform, matching etcd).

The dir-fsync is gated on `!NoSync` (a directory fsync is a fsync, which the
`NoSync` contract opts out of) and its error is **returned**, not tolerated —
unlike committed's own `syncDirBestEffort`, which runs *after* a swap already
committed and only logs. Here the fsync happens *before* the write is acked, so a
failure must abort the write.

**Deliberately not patched:** the `.START`/`.END` cleanup renames inside
`load()`'s recovery branch. Those run at `Open` (before any ack) and are
idempotent — a crash mid-cleanup re-runs the same cleanup on the next `Open` — so
they self-heal without a dir-fsync. Only the pre-ack write paths above need it.

This is an **upstream candidate** (a strict durability improvement with no API or
behavior change under `NoSync=false`); once merged, delete this directory and the
`replace` line.

## What was kept

Unlike the go-mysql fork, nothing was stripped — `tidwall/wal` is a single-package
module. Upstream's `wal_test.go` is **retained** as the patch-regression guard: it
exercises segment cycling and both truncate paths (via a small `SegmentSize`), so
it proves the dir-fsync insertions didn't break the segment mechanics. Upstream's
`io/ioutil` usage is left verbatim (not modernized) to keep the re-sync diff
minimal; the fork is excluded from committed's linters.

## Re-sync procedure

1. `TW=$(go list -m -f '{{.Dir}}' github.com/tidwall/wal)` (after bumping the
   version in the root `go.mod`'s `require`).
2. Copy `wal.go wal_test.go go.mod go.sum LICENSE README.md` over this directory;
   `chmod -R u+w`.
3. Re-apply the change — `git diff` the pre-bump tree against the new upstream is
   the authoritative list. The spots carry a `committeddb fork patch` comment and
   the `syncDir` identifier (absent upstream), all at the `os.OpenFile(O_CREATE)`
   and `os.Rename` segment sites, so they're easy to find and conflicts are rare.
4. `go -C third_party/forked/tidwall-wal build ./... && go -C third_party/forked/tidwall-wal test ./...`
   (the retained upstream test suite must pass — it covers cycle + truncate).
5. From the repo root: `go mod tidy` and `make test/ci` (committed's WAL storage
   suite exercises the event/entry/state logs end to end over this fork).
