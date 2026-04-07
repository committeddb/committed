# Plan: Comprehensive Go Backend Tests

## Context

The Go backend has significant test coverage gaps identified through a full audit. ~28% of source files have corresponding tests. The most critical gaps are: HTTP API handlers (no unit tests), PostgreSQL dialect (zero tests with a known bug), parser implementations (mocked but not directly tested), and incomplete error path coverage in sync/ingest orchestration. The existing E2E test (`http_test.go:TestEndToEnd`) is currently **broken** — it calls handlers directly without chi routing, so `r.PathValue("id")` returns `""` and all Add* handlers return 400.

## Bugs Discovered

1. **E2E test broken** — `http_test.go` calls handlers directly (e.g., `h.AddType(w, req)`) without going through chi router, so `PathValue("id")` is empty. Fix: route through chi or use `req.SetPathValue("id", value)`.
2. **`GetProposals` missing `return`** — `proposal.go:81-82` calls `badRequest` on invalid `number` param but doesn't `return`, falls through to call `h.c.Proposals()`. Same at line 88-89 after `internalServerError`.
3. **PostgreSQL uses MySQL syntax** — `dialects/postgres.go:40` generates `ON DUPLICATE KEY UPDATE` (MySQL) instead of `ON CONFLICT ... DO UPDATE SET` (PostgreSQL).
4. **Stale `MemorySyncable` in `testing/db.go:85`** — Returns `error` instead of `(ShouldSnapshot, error)`. The `db_test.go:268` version is correct. The `testing/db.go` version doesn't implement `cluster.Syncable`.

## Implementation Plan

### Phase 1: Fix Existing Issues + HTTP Handler Tests

#### 1a. Fix broken E2E test
**File:** `internal/cluster/http/http_test.go`

All helper functions (`addType`, `addDatabase`, `addSyncable`, `addIngestable`) need to route requests through chi. Add a `ServeHTTP` method to `HTTP` struct or use `req.SetPathValue("id", value)` on test requests.

Simplest fix: add to `internal/cluster/http/http.go`:
```go
func (h *HTTP) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    h.r.ServeHTTP(w, r)
}
```

Then update test helpers to use `h.ServeHTTP(w, req)` with proper URLs like `http://localhost/database/mydb-id` instead of calling `h.AddDatabase(w, req)` directly.

#### 1b. Fix stale MemorySyncable
**File:** `internal/cluster/db/testing/db.go`

Update `MemorySyncable.Sync` signature from `error` to `(cluster.ShouldSnapshot, error)` to match `cluster.Syncable` interface, mirroring the correct version in `db_test.go:268`.

#### 1c. HTTP handler unit tests
**File:** `internal/cluster/http/handler_test.go` (new)

Uses `FakeCluster` from `clusterfakes` + `httptest`. Tests go through chi router via the new `ServeHTTP` method so path params work correctly.

**Test functions:**

Configuration endpoints (Database, Syncable, Ingestable, Type share the same Add/Get pattern — use table-driven tests):

| Test | What it verifies |
|------|-----------------|
| `TestAddConfiguration_Success` | Table-driven: POST /database/{id}, /syncable/{id}, /ingestable/{id}, /type/{id} with TOML body → 200, correct ID returned, correct ProposeX called with right Configuration |
| `TestAddConfiguration_ClusterError` | Table-driven: ProposeX returns error → 500 |
| `TestAddConfiguration_MissingID` | POST to base path (no {id}) → 404 from chi (route not matched) |
| `TestAddConfiguration_EmptyBody` | POST with empty body → 200 (empty data is valid) |
| `TestAddConfiguration_ContentTypeHandling` | Default mime is `text/toml`; explicit `application/json` is passed through |
| `TestGetConfigurations_Success` | Table-driven: GET /database, /syncable, /ingestable, /type → 200, JSON array of ConfigurationResponse |
| `TestGetConfigurations_Empty` | Returns `[]` not `null` when no configs |
| `TestGetConfigurations_Error` | Cluster method returns error → 400 (or 500 for GetTypes) |

Proposal endpoints:

| Test | What it verifies |
|------|-----------------|
| `TestAddProposal_Success` | POST /proposal with valid JSON → 200, Type() called per entity, Propose() called with correct Proposal |
| `TestAddProposal_BadJSON` | Malformed JSON body → 400 |
| `TestAddProposal_TypeNotFound` | Type() returns error → 400 |
| `TestAddProposal_ProposeError` | Propose() returns error → 500 |
| `TestAddProposal_MultipleEntities` | Multiple entities in single proposal, each Type() call verified |
| `TestGetProposals_Success` | GET /proposal → 200, JSON array of GetProposalResponse |
| `TestGetProposals_DefaultNumber` | No `number` param → Proposals called with 10 |
| `TestGetProposals_WithTypeFilter` | `?type=foo` → Proposals called with type filter |
| `TestGetProposals_InvalidNumber` | `?number=abc` → 400 (documents missing-return bug: Proposals is still called) |
| `TestGetProposals_ClusterError` | Proposals() returns error → 500 (documents missing-return bug) |

Type graph endpoint:

| Test | What it verifies |
|------|-----------------|
| `TestGetType_Success` | GET /type/{id}?start=...&end=... → 200, JSON array of {x: time, y: value} |
| `TestGetType_MissingStartParam` | Missing `start` → 400 |
| `TestGetType_BadEndFormat` | Invalid `end` date → 400 |
| `TestGetType_ClusterError` | TypeGraph() returns error → 500 |
| `TestGetType_EmptyResult` | TypeGraph returns nil → 200, `[]` |

**Setup helper:**
```go
func setupTest() (*http.HTTP, *clusterfakes.FakeCluster) {
    fake := &clusterfakes.FakeCluster{}
    h := http.New(fake)
    return h, fake
}
```

---

### Phase 2: PostgreSQL Dialect + Parser Tests

#### 2a. PostgreSQL dialect tests
**File:** `internal/cluster/syncable/sql/dialects/postgres_test.go` (new)

| Test | What it verifies |
|------|-----------------|
| `TestPostgreSQLDialect_CreateDDL` | Generates valid CREATE TABLE with columns, PRIMARY KEY, INDEX |
| `TestPostgreSQLDialect_CreateDDL_NoIndexes` | DDL without indexes |
| `TestPostgreSQLDialect_CreateSQL` | INSERT with `$1,$2` placeholders (not `?`). Documents bug: uses `ON DUPLICATE KEY UPDATE` instead of `ON CONFLICT ... DO UPDATE SET` |
| `TestPostgreSQLDialect_CreateSQL_SingleColumn` | Single mapping generates `$1` |
| `TestDialect_CreateDDL_Identical` | Both MySQL and PostgreSQL produce identical DDL (shared `createDDL()`) |
| `TestDialect_PlaceholderDifference` | MySQL uses `?`, PostgreSQL uses `$N` |

**Dependencies:** `sql.Config`, `sql.Mapping`, `sql.Index` structs, `strings.Contains` assertions.

#### 2b. Parser implementation tests
**File:** `internal/cluster/db/parser/parser_test.go` (new)

| Test | What it verifies |
|------|-----------------|
| `TestParseDatabase_TOML_Success` | Register FakeDatabaseParser, parse TOML → correct name, Parse() called |
| `TestParseDatabase_JSON_Success` | Same with `application/json` mimeType |
| `TestParseDatabase_UnknownType` | Unregistered type → error "cannot parse database of type: ..." |
| `TestParseDatabase_SubParserError` | Sub-parser returns error → propagated |
| `TestParseIngestable_Success` | Same pattern for ingestable |
| `TestParseIngestable_UnknownType` | Error for unknown type |
| `TestParseSyncable_Success` | Same pattern, includes DatabaseStorage arg |
| `TestParseSyncable_UnknownType` | Error for unknown type |
| `TestParser_MultipleParsers` | Register multiple parsers, correct one is selected by type |

**Dependencies:** `clusterfakes.FakeDatabaseParser`, `FakeIngestableParser`, `FakeSyncableParser`. TOML test data as string literals.

---

### Phase 3: WAL Entity Handler Tests

#### 3a. Save entity dispatch tests
**File:** `internal/cluster/db/wal/entity_handler_test.go` (new)

Uses existing `StorageWrapper` pattern from `wal_storage_test.go` — creates temp dir, opens Storage, defers cleanup.

Need a test parser that returns fake Database/Syncable/Ingestable objects. Create minimal fakes inline in the test file.

| Test | What it verifies |
|------|-----------------|
| `TestSave_TypeEntity_Upsert` | Save type entity → `Type(id)` returns it, `Types()` includes it |
| `TestSave_TypeEntity_Delete` | Save then delete → `Type(id)` returns error |
| `TestSave_DatabaseEntity` | Save database config → `Database(id)` returns parsed result, `Databases()` includes config |
| `TestSave_SyncableEntity_SignalsChannel` | Save syncable config → receive `SyncableWithID` on sync channel |
| `TestSave_IngestableEntity_SignalsChannel` | Save ingestable config → receive `IngestableWithID` on ingest channel |
| `TestSave_SyncableIndexEntity` | Save syncable index → `Reader(id)` starts from saved index |
| `TestSave_MixedEntities` | Proposal with type + user-defined entity → both handled |
| `TestSave_ConfChangeEntry` | `EntryConfChange` type → entity handlers NOT called |
| `TestSave_MultipleEntries` | Multiple entries in single Save → all processed in order |

**Setup:** Will need to construct `raftpb.Entry` with marshaled proposals containing the right entity types. Use `cluster.NewUpsertTypeEntity()`, `cluster.NewUpsertSyncableEntity()`, etc. to create proper entities, marshal into proposals, then wrap in `raftpb.Entry`.

#### 3b. Reader tests
**File:** `internal/cluster/db/wal/reader_test.go` (new)

| Test | What it verifies |
|------|-----------------|
| `TestReader_ReadSequential` | Reader reads entries in order, returns correct index + proposal |
| `TestReader_EOF` | Reader past last entry → `io.EOF` |
| `TestReader_ResumeFromCheckpoint` | Reader created with syncable index → starts from checkpoint |
| `TestReader_SkipsSyncableIndex` | Reader skips SyncableIndex entries, only returns user data |
| `TestReader_ConcurrentReads` | Multiple goroutines reading same Reader → no race (run with -race) |

---

### Phase 4: Sync/Ingest Error Path Tests

#### 4a. Sync error paths
**File:** `internal/cluster/db/sync_error_test.go` (new, in `db_test` package)

Need an `ErrorSyncable` that can return configurable errors:
```go
type ErrorSyncable struct {
    syncErr         error
    shouldSnapshot  cluster.ShouldSnapshot
    count           int
    maxCount        int
    cancel          func()
}
```

| Test | What it verifies |
|------|-----------------|
| `TestSync_SyncError_Continues` | Syncable.Sync returns error → loop continues, next proposal still synced |
| `TestSync_EOF_Continues` | Reader returns io.EOF → loop continues (busy-wait, no crash) |
| `TestSync_ContextCancel` | Cancel context → sync goroutine terminates cleanly |

#### 4b. Ingest error paths
**File:** `internal/cluster/db/ingest_error_test.go` (new, in `db_test` package)

Need an `ErrorIngestable`:
```go
type ErrorIngestable struct {
    proposals  []*cluster.Proposal
    positions  []cluster.Position
    ingestErr  error
}
```

| Test | What it verifies |
|------|-----------------|
| `TestIngest_ContextCancel` | Cancel context → ingest goroutine terminates |
| `TestIngest_LeaderChangeStopsIngestGoroutine` | Leader → not-leader → inner context cancelled, ingest goroutine stops |

---

### Phase 5: Transport Tests

#### 5a. Stoppable listener tests
**File:** `internal/cluster/db/httptransport/listener_test.go` (new)

| Test | What it verifies |
|------|-----------------|
| `TestStoppableListener_Accept` | Create listener on `:0`, dial it → Accept returns connection |
| `TestStoppableListener_Stop` | Close stop channel during Accept → returns "server stopped" error |
| `TestStoppableListener_InvalidAddress` | Bad address → error from `net.Listen` |

#### 5b. Transport unit tests
**File:** `internal/cluster/db/httptransport/transport_test.go` (new)

Transport heavily wraps etcd's `rafthttp.Transport`, limiting what can be unit tested without a full Raft cluster. Focus on the testable surface:

| Test | What it verifies |
|------|-----------------|
| `TestNew_CreatesTransport` | Constructor doesn't panic, ErrorC is non-nil |
| `TestAddPeer_InvalidURL` | `AddPeer` with `"://bad"` → error |

---

## File Summary

| File | Status | Tests |
|------|--------|-------|
| `internal/cluster/http/http.go` | Modify (add ServeHTTP) | — |
| `internal/cluster/http/handler_test.go` | **New** | ~25 test functions |
| `internal/cluster/http/http_test.go` | Modify (fix E2E) | Fix existing |
| `internal/cluster/db/testing/db.go` | Modify (fix Sync signature) | — |
| `internal/cluster/syncable/sql/dialects/postgres_test.go` | **New** | 6 test functions |
| `internal/cluster/db/parser/parser_test.go` | **New** | 9 test functions |
| `internal/cluster/db/wal/entity_handler_test.go` | **New** | 9 test functions |
| `internal/cluster/db/wal/reader_test.go` | **New** | 5 test functions |
| `internal/cluster/db/sync_error_test.go` | **New** | 3 test functions |
| `internal/cluster/db/ingest_error_test.go` | **New** | 2 test functions |
| `internal/cluster/db/httptransport/listener_test.go` | **New** | 3 test functions |
| `internal/cluster/db/httptransport/transport_test.go` | **New** | 2 test functions |

## Verification

After each phase, run:
```bash
go test -race ./internal/cluster/...
```

After all phases:
```bash
make test       # Quick coverage check
make test/ci    # Full suite
```
