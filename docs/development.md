# Developing Committed

How to build Committed from source, find your way around the repository, run
the test suites, and keep a change passing CI.

## Prerequisites

- **Go**, at the version in `go.mod`.
- **Docker**, for the suites that start database containers
  (testcontainers). If Docker listens somewhere other than the default socket,
  set `DOCKER_HOST`.
- **golangci-lint**, at the version CI pins (see `.github/workflows/ci.yml`),
  so local lint flags what CI flags.
- **protoc** and **protoc-gen-go**, only to regenerate the protobuf code.

## Building

```sh
make build          # ./committed, stamped with version, commit, and build date
make docker/build   # the runtime image (IMAGE=committed:latest by default)
make crosscompile   # release binaries for darwin, linux, and windows
```

`make build` stamps `git describe` into the binary, so `committed --version`
says which commit built it and whether the tree was dirty. To run what you
built, as a single node or a three-node cluster with the included `Procfile`,
see [Running Committed](running.md).

## Repository layout

```
cmd/                     The CLI: node, member, backup/restore, wal repair/decompress, healthcheck
internal/cluster/        The domain vocabulary (Proposal, Actual, Entity, Type) and the plugin contracts
  db/                    The engine: Raft consensus, WAL storage, sync and ingest processing
  db/wal/                The log layer
  db/http/               The REST API handlers
  db/parser/             Config document parsing
  syncable/              Syncable kinds: sql/ (mirrors and projections; dialects/ per engine),
                         iceberg/, loopback/, http/
  syncable/stages/       The projection stage runtime; stagestore/ is its node-local store
  ingestable/sql/        SQL ingest: mysql/, postgres/, sqlserver/
  interpretation/,
  migration/             The read-path wrappers: restatements and type migrations
  clusterpb/             Protobuf definitions
  clusterfakes/          Generated test fakes
internal/lint/redaction/ The taint analyzer `go test` runs (see below)
pkg/segmentlog/          Experimental event storage, with its design notes
api/openapi.yaml         The OpenAPI spec, served at /openapi.yaml
e2e/                     End-to-end suites that build and run the real binary
examples/                Runnable configs; examples/movies backs the quickstart
third_party/forked/      Patched dependencies, wired in by replace directives in go.mod
```

There is no aggregated service interface: the engine is `*db.DB`, and the HTTP
handlers hold it directly. The interfaces in `internal/cluster` are the plugin
seams: `Syncable`, `Ingestable`, `Database`, and their parsers.

## Testing

| Target | Scope |
|---|---|
| `make test` | Unit tests (`-short`), plus the forked dependencies' tests. The quick loop. |
| `make test/ci` | Build, then the full unit suite with `-race` and coverage. |
| `make test/integration` | Every file tagged `integration`: the HTTP server and the MySQL, Postgres, and SQL Server dialects against real containers, with `-race`. |
| `make test/cdc` | The end-to-end CDC harness (`e2e/cdc`): a real Postgres and a real committed binary, asserting the exact proposal stream for scripted source changes. |
| `make test/upgrade` | Rolling-upgrade checks against previously released binaries (`e2e/upgrade`); see [how its fixtures are captured](../e2e/upgrade/testdata/README.md). |
| `make test/multinode` | A real three-node cluster: crash recovery and catch-up (`e2e/multinode`). |
| `make test/backup` | Backup and restore round-trip with the real binary (`e2e/backup`). |
| `make test/adversarial` | The Raft adversarial suite (partitions, leader flaps, slow followers, full disks), `-race -count=20`. |
| `make test-all` | Every `docker` and `integration` test in one run. |
| `make bench` | Hot-path benchmarks with real fsyncs; compare before and after with `benchstat`. |

**Build tags decide where a test runs.** A test that needs a container is
tagged `//go:build docker || integration`, which is what `make
test/integration` and CI run. The `docker` tag alone is for the `e2e/cdc`
harness only; a file tagged just `docker` anywhere else never runs in CI. The
`adversarial`, `upgrade`, `multinode`, and `backup` tags belong to their own
suites and targets.

**CI** (`.github/workflows/ci.yml`) runs on every pull request and every push
to `main`: lint, gosec, the race, integration, CDC, upgrade, backup, and
multinode suites, benchmarks, an image vulnerability scan, and the
adversarial suite (one iteration per shard on a pull request, five on a push).

## Lint

```sh
make lint          # golangci-lint, including docker and integration test files
make lint/gosec    # the full gosec security scan, a separate config
make lint/openapi  # lint api/openapi.yaml
```

Run both `make lint` and `make lint/gosec` before you push: CI runs them as
separate jobs, and `make lint` does not include gosec's taint analyzers.
Formatting is gofumpt, enforced by the linter.

## Generated code

```sh
go generate ./...
```

regenerates the counterfeiter fakes (`internal/cluster/clusterfakes`,
`internal/cluster/db/dbfakes`, `internal/cluster/ingestable/sql/sqlfakes`) and
the protobuf code (`clusterpb`, `dialectpb`). Never edit generated files by
hand; change the interface or the `.proto` and regenerate.

## Rules a change has to keep

- **Redaction.** Error text that can echo entity values reaches a persisted or
  exposed surface (a dead-letter record, an HTTP body, a config-build error)
  only through `cluster.RedactedMessage`. `go test` runs a taint analyzer
  (`internal/lint/redaction`) that fails on a bypass; route the text through
  that choke point rather than working around a finding.
- **Dependencies.** After changing dependencies, run `make
  third-party-notices` to regenerate [`THIRD_PARTY_NOTICES.md`](../THIRD_PARTY_NOTICES.md); a test fails
  when it drifts.
- **Forked dependencies.** `go-mysql` and `tidwall/wal` are patched forks under
  `third_party/forked/`. Each carries a README describing the patch
  ([go-mysql](../third_party/forked/go-mysql/README.committeddb.md),
  [tidwall/wal](../third_party/forked/tidwall-wal/README.committeddb.md)); bumping one means re-applying the patch, not just changing the
  version.
- **Settings are documented.** Every `COMMITTED_*` variable the binary reads
  must be named in the README or under `docs/`; a test enforces it.
