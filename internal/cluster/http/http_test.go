//go:build docker || integration

package http_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	networktypes "github.com/moby/moby/api/types/network"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	"github.com/committeddb/committed/internal/cluster/db/parser"
	test "github.com/committeddb/committed/internal/cluster/db/testing"
	"github.com/committeddb/committed/internal/cluster/db/wal"
	"github.com/committeddb/committed/internal/cluster/http"
	"github.com/committeddb/committed/internal/cluster/ingestable"
	"github.com/committeddb/committed/internal/cluster/syncable/sql"
	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects"
)

const (
	mysqlImage = "mysql:9"
	mysqlDB    = "committed"
	mysqlUser  = "root"
	mysqlPass  = "secret"
)

// TestEndToEnd verifies the full HTTP → raft → syncable → destination database
// pipeline: data submitted via the HTTP /proposal endpoint must eventually
// appear in a configured destination database after a syncable is wired up.
//
// The destination is a real MySQL container (testcontainers), not the in-memory
// go-mysql-server emulator this test used to drive. The emulator's SQL semantics
// diverged from real MySQL — it rejected a TEXT primary key and mis-resolved a
// schema-qualified ALTER — which silently wedged the syncable's Init and
// surfaced only as a sync-never-caught-up timeout. Running against the engine
// production targets makes a green here mean the pipeline works on MySQL, and
// costs a container the CI integration job already pays for other packages.
//
// Intermediate raft-log inspection that this test used to do has moved to
// dedicated unit tests:
//
//   - HTTP propose round-trip (TypeID/Key/Data passing through the handler) is
//     covered by handler_test.go:TestAddProposal_Success.
//   - Raft proposal byte-equality is covered by db_test.go:TestDBPropose.
//   - The "ProposeIngestable wires up the ingest goroutine and its proposals
//     reach raft" wiring is covered by
//     wal/ingestable_test.go:TestProposeIngestable_StartsIngestionWiring.
//
// Read-after-write between AddType/AddDatabase and the next handler is
// guaranteed by db.Propose's synchronous-apply contract (PR2): when the
// AddType/AddDatabase HTTP call returns, the corresponding bucket entry has
// already been applied, so the next handler can read it without polling.
func TestEndToEnd(t *testing.T) {
	dir, err := os.MkdirTemp("", "CommitteddbE2ETest-*")
	require.Nil(t, err)
	defer os.RemoveAll(dir)

	dsn := startMySQL(t)

	parser := parser.New()
	sync := make(chan *db.SyncableWithID)
	ingest := make(chan *db.IngestableWithID)
	storage, db := createDB(t, parser, dir, sync, ingest)
	h := http.New(db)

	typeID := addType(t, h, "foo")
	addParsers(t, db, typeID)

	p1 := createProposal(typeID, "key", "one")
	propose(t, h, p1.p)
	_ = addIngestable(t, h, "")
	databaseID := addDatabase(t, h, "mysql", dsn)

	syncableID := addSyncable(t, h, typeID, databaseID)

	// Wait for the sync worker to finish syncing every normal proposal in the
	// wal before reading the destination back. We poll the persisted syncable
	// index (bbolt-backed, goroutine-safe) rather than the sink because it is a
	// precise "caught up" signal — it advances only when a proposal has actually
	// been applied downstream.
	waitForSyncCaughtUp(t, storage, syncableID)

	// Quiesce the syncable before reading: closing the db stops the sync
	// goroutine so nothing writes the sink concurrently with view's SELECT. wal
	// storage stays open (closed at test end) for view's read-back.
	require.Nil(t, db.Close())

	view(t, storage, databaseID, p1)

	// TODO Restart/persistence coverage:
	// - Do a second proposal
	// - Shutdown and restart the cluster
	// - Add a third proposal
	// - Check all of the proposals are still saved
	// - Check the database to make sure the other proposals synced properly
	// - Ingest something else to make sure the ingestable restarts and keeps position
}

// TestEndToEnd_HonorsDelete verifies the downstream half of right-to-be-
// forgotten end-to-end: an upsert proposed over HTTP syncs a row into the
// destination database, then a `delete: true` proposal for the same key syncs a
// DELETE that removes it. This is the regression guard for the zombie bug — a
// delete Actual used to unmarshal the sentinel as invalid JSON, get
// dead-lettered by the SQL syncable, and leave the row live forever.
func TestEndToEnd_HonorsDelete(t *testing.T) {
	dir, err := os.MkdirTemp("", "CommitteddbE2EDeleteTest-*")
	require.Nil(t, err)
	defer os.RemoveAll(dir)

	dsn := startMySQL(t)

	parser := parser.New()
	sync := make(chan *db.SyncableWithID)
	ingest := make(chan *db.IngestableWithID)
	storage, db := createDB(t, parser, dir, sync, ingest)
	h := http.New(db)

	typeID := addType(t, h, "foo")
	addParsers(t, db, typeID)

	upsert := createProposal(typeID, "key", "one")
	propose(t, h, upsert.p)
	_ = addIngestable(t, h, "")
	databaseID := addDatabase(t, h, "mysql", dsn)
	syncableID := addSyncable(t, h, typeID, databaseID)

	// The upsert must land downstream first.
	waitForSyncCaughtUp(t, storage, syncableID)
	view(t, storage, databaseID, upsert)

	// Now erase it: a delete proposal for the same key must remove the row.
	floor, err := storage.GetSyncableIndex(syncableID)
	require.Nil(t, err)
	propose(t, h, createDeleteProposal(typeID, "key"))
	waitForSyncIndexAbove(t, storage, syncableID, floor)

	require.Nil(t, db.Close())
	viewDeleted(t, storage, databaseID, "key")
}

// startMySQL brings up a throwaway MySQL container for one test and returns the
// go-sql-driver DSN the syncable dialect opens it with. Ryuk (the testcontainers
// reaper) and the registered cleanup tear it down at test end. Each test gets
// its own container so the two never share (or collide on) the sink table.
func startMySQL(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	c, err := tcmysql.Run(ctx, mysqlImage,
		tcmysql.WithDatabase(mysqlDB),
		tcmysql.WithUsername(mysqlUser),
		tcmysql.WithPassword(mysqlPass),
		// Override the module's default log-line readiness check. It watches for
		// a "port: 3306  MySQL Community Server" line that a slow CI host can
		// leave unmatched inside the timeout, and a log line can't tell "logged
		// ready" from "accepts a query" — the entrypoint's init server drops
		// early connections with EOF. ForSQL opens a real connection and runs a
		// query, retrying until it succeeds, so it returns only on true
		// readiness.
		testcontainers.WithWaitStrategy(
			wait.ForSQL("3306/tcp", "mysql", func(host string, port networktypes.Port) string {
				return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", mysqlUser, mysqlPass, host, port.Port(), mysqlDB)
			}).WithStartupTimeout(3*time.Minute),
		),
	)
	require.NoError(t, err, "start mysql container")
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })

	dsn, err := c.ConnectionString(ctx)
	require.NoError(t, err, "mysql dsn")

	return dsn
}

type Proposal struct {
	key string
	one string
	p   *http.AddProposalRequest
}

func addParsers(t *testing.T, db *test.DB, typeID string) {
	ds := map[string]sql.Dialect{"mysql": &dialects.MySQLDialect{}}
	sqlParser := &sql.DBParser{Dialects: ds}
	db.AddDatabaseParser("sql", sqlParser)
	db.AddSyncableParser("sql", &sql.SyncableParser{})

	// Build the seed proposal the ProposalIngestable will replay. We
	// construct cluster.Type directly from typeID rather than calling
	// db.Type(), which would race against wal storage applying the AddType
	// entry to its bucket. The syncable only inspects Type.ID for topic
	// matching, so a minimal Type is sufficient here.
	seed := &cluster.Proposal{Entities: []*cluster.Entity{{
		Type: &cluster.Type{ID: typeID},
		Key:  []byte("key"),
		Data: []byte(`{"key":"key","one":"one"}`),
	}}}
	db.AddIngestableParser("proposal", ingestable.NewProposalIngestableParser(
		[]*cluster.Proposal{seed},
	))
}

// waitForSyncCaughtUp blocks until the sync worker is no longer making
// forward progress on the named syncable. We can't compare against
// wal.LastIndex because every successful sync writes a SyncableIndex entry,
// which itself bumps lastIndex — the syncable index would chase a moving
// target it can never reach. Instead we sample the persisted syncable
// index, sleep one poll interval, sample again, and accept the snapshot
// once two consecutive samples agree (and at least one normal proposal
// has been processed). This is a "stable for one tick" check, which is
// good enough because the test only proposes one normal entry.
func waitForSyncCaughtUp(t *testing.T, s *wal.Storage, syncableID string) {
	t.Helper()
	const interval = 10 * time.Millisecond
	deadline := time.Now().Add(30 * time.Second)
	var prev uint64
	have := false
	for time.Now().Before(deadline) {
		idx, err := s.GetSyncableIndex(syncableID)
		if err == nil && idx > 0 {
			if have && idx == prev {
				return
			}
			prev = idx
			have = true
		}
		time.Sleep(interval)
	}
	t.Fatalf("timed out waiting for sync to catch up on %q", syncableID)
}

func view(t *testing.T, s *wal.Storage, databaseID string, p *Proposal) {
	database, err := s.Database(databaseID)
	require.Nil(t, err)
	db := database.(*sql.DB).DB

	rows, err := db.Query("SELECT pk, one FROM foo WHERE pk = ?", p.key)
	require.Nil(t, err)
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
		var key string
		var one string
		err := rows.Scan(&key, &one)
		if err != nil {
			require.Nil(t, err)
		}
		require.Equal(t, p.key, key)
		require.Equal(t, p.one, one)
	}
	require.Equal(t, 1, count)
	err = rows.Err()
	if err != nil {
		require.Nil(t, err)
	}
}

func addDatabase(t *testing.T, h *http.HTTP, dialect string, connectionString string) string {
	name := "bar"
	id := "test-db-id"

	body := fmt.Sprintf(`[database]
type = "sql"
name = "%s"
[sql]
dialect=%q
connectionString=%q`, name, dialect, connectionString)

	req := httptest.NewRequest("POST", fmt.Sprintf("http://localhost/v1/database/%s", id), strings.NewReader(body))
	req.Header["Content-Type"] = []string{"text/toml"}

	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 200, resp.StatusCode)
	respID, err := io.ReadAll(resp.Body)
	require.Nil(t, err)

	return string(respID)
}

func addIngestable(t *testing.T, h *http.HTTP, dialect string) string {
	name := "bar"
	id := "test-ingestable-id"
	body := fmt.Sprintf(`[ingestable]
type = "proposal"
name = "%s"
[sql]
dialect="%s"
topic="simple"
connectionString="%s"
primaryKey="pk"

[[sql.mappings]]
jsonName = "pk"
column = "pk"

[[sql.mappings]]
jsonName = "one"
column = "one"`, name, dialect, name)

	req := httptest.NewRequest("POST", fmt.Sprintf("http://localhost/v1/ingestable/%s", id), strings.NewReader(body))
	req.Header["Content-Type"] = []string{"text/toml"}

	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 200, resp.StatusCode)
	respID, err := io.ReadAll(resp.Body)
	require.Nil(t, err)

	return string(respID)
}

func addSyncable(t *testing.T, h *http.HTTP, topicId string, databaseID string) string {
	name := "bar"
	tableName := "foo"
	id := "test-syncable-id"

	body := fmt.Sprintf(`[syncable]
type = "sql"
name = "%s"
[sql]
topic = "%s"
db = "%s"
table = "%s"
primaryKey = "pk"

[[sql.indexes]]
name = "firstIndex"
index = "one"

[[sql.mappings]]
jsonPath = "$.key"
column = "pk"
type = "VARCHAR(128)"

[[sql.mappings]]
jsonPath = "$.one"
column = "one"
type = "VARCHAR(128)"`, name, topicId, databaseID, tableName)

	req := httptest.NewRequest("POST", fmt.Sprintf("http://localhost/v1/syncable/%s", id), strings.NewReader(body))
	req.Header["Content-Type"] = []string{"text/toml"}

	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 200, resp.StatusCode)
	respID, err := io.ReadAll(resp.Body)
	require.Nil(t, err)

	return string(respID)
}

func addType(t *testing.T, h *http.HTTP, name string) string {
	id := "test-type-id"
	body := fmt.Sprintf("[type]\nname = \"%s\"", name)

	req := httptest.NewRequest("POST", fmt.Sprintf("http://localhost/v1/type/%s", id), strings.NewReader(body))
	req.Header["Content-Type"] = []string{"text/toml"}

	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 200, resp.StatusCode)
	respID, err := io.ReadAll(resp.Body)
	require.Nil(t, err)

	return string(respID)
}

func propose(t *testing.T, h *http.HTTP, proposal *http.AddProposalRequest) {
	bs, err := json.Marshal(proposal)
	require.Nil(t, err)
	req := httptest.NewRequest("POST", "http://localhost/v1/proposal", bytes.NewReader(bs))
	req.Header["Content-Type"] = []string{"application/json"}

	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 200, resp.StatusCode)
}

// createDeleteProposal builds an HTTP delete (tombstone) request for
// (typeID, key) — the intake half of right-to-be-forgotten. It carries no data.
func createDeleteProposal(typeID string, key string) *http.AddProposalRequest {
	return &http.AddProposalRequest{
		Entities: []*http.AddEntityRequest{{
			TypeID: typeID,
			Key:    key,
			Delete: true,
		}},
	}
}

// waitForSyncIndexAbove blocks until the syncable's persisted index advances
// strictly past floor — i.e. the worker has processed (and committed) a
// proposal newer than the one at floor. Used after proposing a delete to know
// the DELETE has reached the destination before reading it back.
func waitForSyncIndexAbove(t *testing.T, s *wal.Storage, syncableID string, floor uint64) {
	t.Helper()
	const interval = 10 * time.Millisecond
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		idx, err := s.GetSyncableIndex(syncableID)
		if err == nil && idx > floor {
			return
		}
		time.Sleep(interval)
	}
	t.Fatalf("timed out waiting for sync index above %d on %q", floor, syncableID)
}

// viewDeleted asserts the row keyed by key is gone from the destination table.
func viewDeleted(t *testing.T, s *wal.Storage, databaseID string, key string) {
	database, err := s.Database(databaseID)
	require.Nil(t, err)
	db := database.(*sql.DB).DB

	rows, err := db.Query("SELECT pk, one FROM foo WHERE pk = ?", key)
	require.Nil(t, err)
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	require.Nil(t, rows.Err())
	require.Equal(t, 0, count, "row should be deleted after the delete proposal syncs")
}

func createProposal(typeID string, key string, one string) *Proposal {
	j := fmt.Sprintf("{\"key\":\"%s\",\"one\":\"%s\"}", key, one)
	r := &http.AddProposalRequest{
		Entities: []*http.AddEntityRequest{{
			TypeID: typeID,
			Key:    key,
			Data:   []byte(j),
		}},
	}

	return &Proposal{
		key: key,
		one: one,
		p:   r,
	}
}

func createDB(t *testing.T, p *parser.Parser, dir string, sync chan *db.SyncableWithID, ingest chan *db.IngestableWithID) (*wal.Storage, *test.DB) {
	storage, err := wal.Open(dir, p, sync, ingest)
	require.Nil(t, err)
	// The Storage is caller-owned and outlives db.Close (tests query the SQL sink
	// through it afterward), so close it at test end to stop the scrubber and
	// release the WAL/bbolt + SQL handles. Idempotent; runs after the test body.
	t.Cleanup(func() { _ = storage.Close() })

	db := test.CreateDBWithStorage(storage, p, sync, ingest)

	return storage, db
}
