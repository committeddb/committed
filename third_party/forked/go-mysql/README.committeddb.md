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
- **Re-sync:** bump the tag, re-drop the upstream tree (see below), reapply the
  patch.

## Why it's forked

go-mysql marshals a JSON-embedded MySQL **opaque DECIMAL** leaf as a *quoted
string* (`{"d":"1.50"}`). committed's initial-snapshot path renders the same
leaf as an *exact, unquoted number* (`{"d":1.50}`), so the CDC and snapshot
payloads diverged byte-for-byte, breaking the byte-compare that replay/dedup
relies on. There is no upstream option and no non-fork hook that fixes this
(`RowsEventDecodeFunc` replaces the whole row decode; `useDecimal=true` +
shopspring trims trailing zeros and corrupts top-level decimals).

The patch adds one decode option, `UseNumberForJSONDecimal`, mirroring the
existing `UseFloatWithTrailingZero` plumbing 1:1. When set, a JSON DECIMAL leaf
is emitted as a scale-preserving `json.Number` (unquoted, exact, >2^53-safe).
committed sets it in `binlogSyncerConfig`. **Upstream candidate** — once merged,
delete this directory and the `replace` line.

The full patch is `committeddb-json-decimal.patch` (5 hunks across
`replication/{binlogsyncer,parser,row_event,json_binary}.go`).

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
   directory; `chmod -R u+w`; delete `*_test.go` and `testdata/`.
3. `git apply third_party/forked/go-mysql/committeddb-json-decimal.patch`
   (the hunks sit next to the `useFloatWithTrailingZero` lines, so conflicts are
   rare and obvious). Regenerate the patch afterward if line numbers shifted.
4. `go -C third_party/forked/go-mysql mod tidy && go -C third_party/forked/go-mysql build ./...`
5. From the repo root: `make test/ci`, and run the MySQL docker parity test
   (`TestMysqlSnapshotStreamJSONDecimalByteIdentity`) to confirm parity holds.
