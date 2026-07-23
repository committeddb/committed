# Forked & stripped `go-mysql` (committeddb)

This is a **patched, stripped** copy of
[`github.com/go-mysql-org/go-mysql`](https://github.com/go-mysql-org/go-mysql),
consumed via a `replace` directive in the repo-root `go.mod`:

```
replace github.com/go-mysql-org/go-mysql => ./third_party/forked/go-mysql
```

The module path inside this copy is unchanged (`module
github.com/go-mysql-org/go-mysql`), so every `import` in committed resolves here
with no source changes — see `go.mod`'s `replace`. License: MIT (see `LICENSE`
and `vitess_license`, retained verbatim).

## Upstream baseline

- **Version:** `v1.15.0`
- **Re-sync:** bump the tag, re-drop the upstream tree, re-apply committed's
  changes (see below).

## Why it's forked

committed's CDC (binlog) path must emit bytes identical to the initial-snapshot
path for every value, or replay/dedup breaks on the byte-compare. Upstream
go-mysql diverges on JSON-embedded scalars, with no upstream option or non-fork
hook to fix it (`RowsEventDecodeFunc` replaces the whole row decode):

- an opaque **DECIMAL** marshals as a quoted string (`{"d":"1.50"}`); the
  snapshot renders an exact, unquoted number (`{"d":1.50}`).
- an opaque **DATE** marshals as a full DATETIME (`"2021-06-15 00:00:00.000000"`);
  the snapshot renders it date-only (`"2021-06-15"`).

committed's changes are marked `committeddb fork patch` in the source (mostly
`replication/json_binary.go`, plus the `UseNumberForJSONDecimal` decode option
plumbed through `binlogsyncer`). They're all **upstream candidates** — once
merged, delete this directory and the `replace` line. The snapshot↔CDC parity is
pinned by committed's own docker tests (`TestMysqlSnapshotStreamJSON*`).

A second, unrelated patch **bounds transaction-payload decompression**
(`replication/transaction_payload_event.go`, plumbed as
`PayloadDecoderMaxDecompressedSize` through `parser.go`/`binlogsyncer.go`): a
compressed transaction (source `binlog_transaction_compression=ON`) decompressed
unbounded — a large or zstd-bomb transaction OOM-crash-loops the node. Also an
upstream candidate. Pinned by `replication/transaction_payload_bound_test.go`
(committed-added, in this fork — see re-sync note) and, on the committed side, by
`TestBinlogSyncerConfig` asserting the bound is plumbed non-zero.

## What was stripped

Only the 8 packages committed's build closure actually compiles are kept:
`client compress mysql packet replication serialization stmt utils`. Removed:
the other 11 packages (`canal server dump driver …`), the ~3.7 MB of mascot/logo
PNGs, the docs, and go-mysql's own `*_test.go`/`testdata` (committed covers the
patched behavior with its own snapshot↔CDC parity test). `go.mod` was
`go mod tidy`'d against the kept set.

## Re-sync procedure

1. `GM=$(go list -m -f '{{.Dir}}' github.com/go-mysql-org/go-mysql)` (after
   bumping the version in the root `go.mod`'s `require`).
2. Copy the 8 kept packages + `go.mod go.sum LICENSE vitess_license` over this
   directory; `chmod -R u+w`; delete `*_test.go` and `testdata/`. Exception:
   `replication/transaction_payload_bound_test.go` is committed-added (not
   upstream), so it comes back with the changes in step 3 — don't leave it out.
3. Re-apply committed's changes — `git diff` the pre-bump tree against the new
   upstream is the authoritative list. The spots carry a `committeddb fork patch`
   comment or the `UseNumberForJSONDecimal` / `PayloadDecoderMaxDecompressedSize`
   identifiers (absent upstream) and sit next to the `useFloatWithTrailingZero`
   lines and the temporal switch, so they're easy to find and conflicts are rare.
4. `go -C third_party/forked/go-mysql mod tidy && go -C third_party/forked/go-mysql build ./...`
5. From the repo root: `make test/ci`, and run the MySQL docker parity tests
   (`TestMysqlSnapshotStreamJSONDecimalByteIdentity`,
   `TestMysqlSnapshotStreamJSONTemporalByteIdentity`) — a missed change fails the
   byte-compare there. Also run the decompression-bound unit test in the fork:
   `go -C third_party/forked/go-mysql test ./replication/ -run TestTransactionPayload_DecompressionBounded`
   — it catches a re-sync that re-adds the field but drops the enforcement (which
   the committed-side wiring test cannot see).
