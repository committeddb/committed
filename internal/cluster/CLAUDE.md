# internal/cluster

Core domain package. All key interfaces are defined in `cluster.go`.

## Key interfaces (cluster.go)

- **Cluster**: Main interface — propose writes, manage configs, sync/ingest
- **Syncable** (syncable.go): Consumes committed Actuals (`cluster.Actual`, in Index order) and applies them to an external system. A Syncable is handed Actuals, never Proposals — propose a `Proposal`, sync an `Actual`.
- **Ingestable** (ingestable.go): Ingests data from an external source into topics
- **Database** (database.go): External database connection config
- **SyncableParser / IngestableParser / DatabaseParser** (syncable.go, ingestable.go, database.go): Parse config documents into typed structs. They receive a `*cluster.ParsedConfig` (parsed_config.go) — committed's own decode seam (go-toml/v2 + mapstructure, no third-party type in the contract). Committed's field names match case-insensitively (`Topic =` works; load-bearing compat, pinned by the tolerance_test.go corpus); user data — including map keys like jsonpaths — is preserved byte-exact. `${VAR}` secret interpolation runs at the parse boundary in db/parser, not here; type configs deliberately skip it.

## Package layout

- **db/**: Raft consensus, WAL storage, sync/ingest processing. `db.go` is the main Cluster implementation. `raft.go` handles Raft node lifecycle. `sync.go` handles syncable processing. `ingest.go` handles ingestable processing.
- **db/wal/**: Write-ahead log storage layer (tidwall/wal wrapper)
- **http/**: REST API handlers (Chi router). `handler.go` defines all routes and handlers. `versions.go` handles config version history endpoints.
- **syncable/sql/**: SQL sync implementations — `mysql/` and `postgres/` subdirectories
- **ingestable/sql/**: SQL ingest implementations — `mysql/` and `postgres/` subdirectories
- **clusterpb/**: Protobuf definitions (generated — do not edit, regenerate with `go generate ./...`)
- **clusterfakes/**, **db/dbfakes/**, **ingestable/sql/sqlfakes/**: Generated counterfeiter fakes (do not edit, regenerate with `go generate ./...`)

## Other files in this package

- `config_error.go`: ConfigError type for configuration validation
- `type.go`, `proposal.go`, `actual.go`, `time_point.go`, `version_info.go`: Domain types (`proposal.go` = the write request; `actual.go` = the committed fact a Syncable consumes)
