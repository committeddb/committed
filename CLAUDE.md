# CLAUDE.md

## Project Overview

Committed is a distributed commit log database built on etcd Raft consensus. It stores data long-term in a log structure with two primitives: **write** (append proposals to topics) and **sync** (replicate/transform data to external systems like SQL databases). It is designed for building distributed CQRS systems, not for direct querying.

## Tech Stack

- **Backend**: Go 1.26 (Raft consensus, Chi HTTP router, Protobuf serialization, Zap logging) — the exact toolchain is in go.mod
- **Storage**: Write-ahead log (tidwall/wal), BoltDB
- **Databases**: MySQL (go-sql-driver), PostgreSQL (pgx), SQL Server (go-mssqldb)

## Project Structure

```
cmd/                 CLI commands (Cobra) - node, member, backup/restore, wal repair/decompress, healthcheck
internal/cluster/    Core domain: the vocabulary and the plugin contracts
  db/                Raft consensus, WAL storage, sync/ingest processing (db/wal/ the log layer)
  db/http/           REST API handlers (Chi router) — the engine's transport subpackage
  db/parser/         Config document parsing and the removed-spelling ledger
  syncable/          sql/ (mirrors + projections; dialects/ per engine), iceberg/, loopback/, http/
  syncable/stages/   Projection stage runtime; stagestore/ its node-local store
  ingestable/sql/    SQL ingest: mysql/, postgres/, sqlserver/
  interpretation/, migration/  The read-path wrappers (restatements, type migrations)
  clusterpb/         Protobuf definitions
  clusterfakes/      Generated test fakes (counterfeiter)
internal/lint/redaction/  The taint analyzer run by go test
```

## Key Concepts

- **Topics**: Where data is appended
- **Proposals** (`cluster.Proposal`): A write *request* — the entities offered to the log, pre-consensus, with no Index yet. You propose Proposals.
- **Actuals** (`cluster.Actual`): A committed fact — the Proposal consensus ordered and wrote at a fixed `Index`. Readers yield Actuals in Index order and Syncables consume them; a Syncable never sees a Proposal. The consensus boundary turns a Proposal into an Actual.
- **Syncables**: Configs for syncing committed Actuals to external systems (SQL, HTTP webhook, Iceberg, loopback)
- **Destination**: The external table or system a syncable writes to. Customer-facing text (docs, API text, status and log messages) uses exactly these two words — "syncable" for the config and its worker, "destination" for what it writes to — never "sink", which collided with both.
- **Ingestables**: Configs for ingesting data from external sources
- **Types**: Schemas/metadata for topic data
- **Databases**: External database connection configurations (TOML format)

## Build & Run

```bash
go build                    # Build the binary
make test                   # Run short tests with coverage
make test/ci                # Build + the race job (no build tags)
make test/integration       # `integration`-tagged tests (docker || integration files)
make test/cdc               # CDC end-to-end (docker, -p=1); also test/upgrade, test/backup, test/multinode, test/adversarial
make lint                   # golangci-lint (+ lint/gosec for the security config)
make crosscompile           # Build for darwin/linux/windows amd64
```

### Running a Local Cluster

Use goreman (`go get github.com/mattn/goreman`) with the Procfile to start a 3-node cluster:

```bash
goreman start               # Starts nodes on ports 12380, 22380, 32380
```

Node config is environment-only (`COMMITTED_*` variables, no flags): see the Procfile for a working single-node set.

## Testing

- Go tests use the standard `testing` package with `counterfeiter/v6` for generating interface fakes
- Fakes are in `clusterfakes/` and `db/dbfakes/` directories
- Run `make test` for quick iteration; `make test/ci` for the race job. Docker-backed tests need the `docker` or `integration` build tag (see the Makefile targets above).

## Configuration Format

Databases, syncables, and ingestables use TOML configuration. See README.md for example payloads.

## API Endpoints

All served via Chi router in `internal/cluster/db/http/`:

- `POST /database/{id}` - Database configurations (list via `GET /database`; history via `/versions`)
- `POST /proposal` - Append proposals (write-only; the log is not queried over HTTP — sync it out and query there)
- `POST /syncable/{id}` - Syncable configurations (list via `GET /syncable`; `DELETE`, `/status`, `/rebuild`, `/rematerialize`)
- `POST /syncable/dryrun` - Rehearse a syncable config against a log sample (diagnostic report; nothing admitted)
- `POST /ingestable/{id}` - Ingestable configurations (list via `GET /ingestable`; `DELETE`, `/status`)
- `POST /type/{id}` - Type configurations (read back via `GET /type` and the version endpoints)

## Code Generation

- **Protobuf**: Definitions in `internal/cluster/clusterpb/`
- **Counterfeiter fakes**: Generated from interfaces in `internal/cluster/`

## Scratch / Throwaway Files

Put any temporary files (race-detector logs, throwaway test scripts, captured command output, intermediate analysis, etc.) under `.claude-scratch/` at the project root. The directory is gitignored and pre-approved in `.claude/settings.local.json` so reads/writes don't prompt.

Do **not** write to `/tmp` or other system temp directories. Do **not** write to anything under `.claude/` — Claude Code hardcodes a sensitive-file protection on that directory (because it holds `settings.local.json`), so writes there always prompt regardless of allowlist rules. `.claude-scratch/` exists specifically to avoid that protection.
