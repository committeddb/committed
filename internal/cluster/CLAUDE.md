# internal/cluster

Core domain package. All key interfaces are defined in `cluster.go`.

## Key interfaces (cluster.go)

- **Cluster**: Main interface — propose writes, read proposals, manage configs, sync/ingest
- **Syncable** (syncable.go): Syncs topic data to an external system
- **Ingestable** (ingestable.go): Ingests data from an external source into topics
- **Database** (database.go): External database connection config
- **SyncableParser / IngestableParser / DatabaseParser** (configuration.go): Parse TOML configs into typed structs

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
- `type.go`, `proposal.go`, `time_point.go`, `version_info.go`: Domain types
