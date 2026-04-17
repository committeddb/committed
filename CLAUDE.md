# CLAUDE.md

## Project Overview

Committed is a distributed commit log database built on etcd Raft consensus. It stores data long-term in a log structure with two primitives: **write** (append proposals to topics) and **sync** (replicate/transform data to external systems like SQL databases). It is designed for building distributed CQRS systems, not for direct querying.

## Tech Stack

- **Backend**: Go 1.26.2 (Raft consensus, Chi HTTP router, Protobuf serialization, Zap logging)
- **Storage**: Write-ahead log (tidwall/wal), BoltDB, in-memory time series (tstorage)
- **Databases**: MySQL (go-sql-driver), PostgreSQL (pgx)

## Project Structure

```
cmd/               CLI commands (Cobra) - node, demo-seeder, proposals
internal/cluster/  Core domain
  db/              Raft consensus, WAL storage, sync/ingest processing
  http/            REST API handlers (Chi router)
  syncable/sql/    SQL sync implementations
  ingestable/sql/  SQL ingest implementations
  clusterpb/       Protobuf definitions
  clusterfakes/    Generated test fakes (counterfeiter)
```

## Key Concepts

- **Topics**: Where data is appended
- **Proposals**: Atomic units of data written to topics
- **Syncables**: Configs for syncing data to external databases
- **Ingestables**: Configs for ingesting data from external sources
- **Types**: Schemas/metadata for topic data
- **Databases**: External database connection configurations (TOML format)

## Build & Run

```bash
go build                    # Build the binary
make test                   # Run short tests with coverage
make test/ci                # Full test suite (build + test)
make test/e2e               # End-to-end tests (sequential: -p=1)
make crosscompile           # Build for darwin/linux/windows amd64
```

### Running a Local Cluster

Use goreman (`go get github.com/mattn/goreman`) with the Procfile to start a 3-node cluster:

```bash
goreman start               # Starts nodes on ports 12380, 22380, 32380
```

Single node: `./committed --id 1 --cluster http://127.0.0.1:12379 --port 12380`

## Testing

- Go tests use the standard `testing` package with `counterfeiter/v6` for generating interface fakes
- Fakes are in `clusterfakes/` and `db/dbfakes/` directories
- Run `make test` for quick iteration; `make test/ci` for full suite
- Package-specific test targets: `make test/topic`, `make test/cluster`, `make test/sync`

## Configuration Format

Databases, syncables, and ingestables use TOML configuration. See README.md for example payloads.

## API Endpoints

All served via Chi router in `internal/cluster/http/`:

- `GET/POST /database/{id}` - Database configurations
- `GET/POST /proposal` - Proposals (log entries)
- `GET/POST /syncable/{id}` - Syncable configurations
- `GET/POST /ingestable/{id}` - Ingestable configurations
- `GET/POST /type/{id}` - Type definitions

## Code Generation

- **Protobuf**: Definitions in `internal/cluster/clusterpb/`
- **Counterfeiter fakes**: Generated from interfaces in `internal/cluster/`

## Scratch / Throwaway Files

Put any temporary files (race-detector logs, throwaway test scripts, captured command output, intermediate analysis, etc.) under `.claude-scratch/` at the project root. The directory is gitignored and pre-approved in `.claude/settings.local.json` so reads/writes don't prompt.

Do **not** write to `/tmp` or other system temp directories. Do **not** write to anything under `.claude/` — Claude Code hardcodes a sensitive-file protection on that directory (because it holds `settings.local.json`), so writes there always prompt regardless of allowlist rules. `.claude-scratch/` exists specifically to avoid that protection.
