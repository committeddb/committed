//go:build integration

package http_test

import (
	"bytes"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dolthub/go-mysql-server/driver"
	"github.com/dolthub/go-mysql-server/memory"
	sqle "github.com/dolthub/go-mysql-server/sql"
	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/db"
	"github.com/philborlin/committed/internal/cluster/db/parser"
	test "github.com/philborlin/committed/internal/cluster/db/testing"
	"github.com/philborlin/committed/internal/cluster/db/wal"
	"github.com/philborlin/committed/internal/cluster/http"
	"github.com/philborlin/committed/internal/cluster/ingestable"
	"github.com/philborlin/committed/internal/cluster/syncable/sql"
	"github.com/philborlin/committed/internal/cluster/syncable/sql/dialects"
	"github.com/stretchr/testify/require"
)

// TestEndToEnd verifies the full HTTP → raft → syncable → destination database
// pipeline: data submitted via the HTTP /proposal endpoint must eventually
// appear in a configured destination database after a syncable is wired up.
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
// The two waitFor() calls are workarounds for the Save-does-double-duty issue
// (see .claude-scratch/tickets/save-does-double-duty.md): the HTTP handlers
// for AddProposal and AddSyncable read from storage synchronously, which
// races against the prior raft entry being applied to the storage buckets.
// Once that ticket is resolved these waits become unnecessary.
func TestEndToEnd(t *testing.T) {
	dir, err := os.MkdirTemp("", "CommitteddbE2ETest-*")
	require.Nil(t, err)
	defer os.RemoveAll(dir)

	connectionString := "bar"
	dialect := createDialect(t, connectionString)

	parser := parser.New()
	sync := make(chan *db.SyncableWithID)
	ingest := make(chan *db.IngestableWithID)
	storage, db := createDB(t, parser, dir, sync, ingest)
	h := http.New(db)

	typeID := addType(t, h, "foo")
	addParsers(t, db, dialect, typeID)

	// AddProposal's handler calls c.Type(typeID); wait until the AddType
	// entry has been applied to the type bucket.
	waitFor(t, "type to be queryable", func() bool {
		_, err := db.Type(typeID)
		return err == nil
	})

	p1 := createProposal(typeID, "key", "one")
	propose(t, h, p1.p)
	_ = addIngestable(t, h, "")
	databaseID := addDatabase(t, h, "go-mysql-server")

	// AddSyncable's parser calls storage.Database(databaseID); wait until
	// the database Configuration has been applied to the database bucket.
	waitFor(t, "database to be queryable", func() bool {
		_, err := storage.Database(databaseID)
		return err == nil
	})

	addSyncable(t, h, typeID, databaseID)

	view(t, storage, databaseID, p1, connectionString)

	// TODO Restart/persistence coverage:
	// - Do a second proposal
	// - Shutdown and restart the cluster
	// - Add a third proposal
	// - Check all of the proposals are still saved
	// - Check the database to make sure the other proposals synced properly
	// - Ingest something else to make sure the ingestable restarts and keeps position
}

type dbs []sqle.Database

var _ driver.Provider = dbs{}

func (d dbs) Resolve(name string, options *driver.Options) (string, sqle.DatabaseProvider, error) {
	return name, memory.NewDBProvider(d...), nil
}

func createDialect(t *testing.T, connectionString string) sql.Dialect {
	var memdbs dbs
	memdb := memory.NewDatabase(connectionString)
	memdb.EnablePrimaryKeyIndexes()
	memdbs = append(memdbs, memdb)

	drv := driver.New(memdbs, nil)
	dialect := &dialects.GoMySQLServerDialect{Driver: drv}

	conn, err := drv.OpenConnector(connectionString)
	require.Nil(t, err)

	db := gosql.OpenDB(conn)
	_, err = db.Exec("USE " + connectionString)
	require.Nil(t, err)

	err = db.Close()
	require.Nil(t, err)

	return dialect
}

type Proposal struct {
	key string
	one string
	p   *http.AddProposalRequest
}

func addParsers(t *testing.T, db *test.DB, dialect sql.Dialect, typeID string) {
	ds := make(map[string]sql.Dialect)
	ds["go-mysql-server"] = dialect
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

func view(t *testing.T, s db.Storage, databaseID string, p *Proposal, connectionString string) {
	database, err := s.Database(databaseID)
	require.Nil(t, err)
	db := database.(*sql.DB).DB
	_, err = db.Exec("USE " + connectionString)
	require.Nil(t, err)

	// The syncable applies asynchronously after addSyncable returns, so poll
	// until the row appears (or the deadline expires).
	waitFor(t, "syncable to write row", func() bool {
		var pk string
		err := db.QueryRow(
			"SELECT pk FROM "+connectionString+".foo WHERE pk = ?", p.key,
		).Scan(&pk)
		return err == nil
	})

	rows, err := db.Query("SELECT pk, one FROM "+connectionString+".foo WHERE pk = ?", p.key)
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

// waitFor polls check at 5ms intervals for up to 500ms. With Raft's tick
// interval already lowered to 1ms in tests (see testTickInterval in
// db/testing/db.go), each propose→commit→apply cycle takes a few milliseconds,
// so 500ms is roughly 15× the realistic worst case while still tight enough
// to flag a real regression.
func waitFor(t *testing.T, what string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func addDatabase(t *testing.T, h *http.HTTP, dialect string) string {
	name := "bar"
	id := "test-db-id"

	body := fmt.Sprintf(`[database]
type = "sql"
name = "%s"
[sql]
dialect="%s"
connectionString="%s"`, name, dialect, name)

	req := httptest.NewRequest("POST", fmt.Sprintf("http://localhost/database/%s", id), strings.NewReader(body))
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

	req := httptest.NewRequest("POST", fmt.Sprintf("http://localhost/ingestable/%s", id), strings.NewReader(body))
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
	tableName := name + ".foo"
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

	req := httptest.NewRequest("POST", fmt.Sprintf("http://localhost/syncable/%s", id), strings.NewReader(body))
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

	req := httptest.NewRequest("POST", fmt.Sprintf("http://localhost/type/%s", id), strings.NewReader(body))
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
	req := httptest.NewRequest("POST", "http://localhost/proposal", bytes.NewReader(bs))
	req.Header["Content-Type"] = []string{"application/json"}

	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 200, resp.StatusCode)
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

func createDB(t *testing.T, p *parser.Parser, dir string, sync chan *db.SyncableWithID, ingest chan *db.IngestableWithID) (db.Storage, *test.DB) {
	storage, err := wal.Open(dir, p, sync, ingest)
	require.Nil(t, err)

	db := test.CreateDBWithStorage(storage, p, sync, ingest)
	db.EatCommitC()

	return storage, db
}
