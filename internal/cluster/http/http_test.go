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

func TestEndToEnd(t *testing.T) {
	sleepTime := 2 * time.Second

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
	time.Sleep(sleepTime)

	cp1 := addParsers(t, db, dialect, typeID)

	p1 := createProposal(typeID, "key", "one")
	propose(t, h, p1.p)
	time.Sleep(sleepTime)
	ps, err := db.Ents()
	require.Nil(t, err)
	require.Equal(t, 2, len(ps)) // AddType + ProposedProposal
	require.Equal(t, 1, len(ps[1].Entities))
	require.Equal(t, p1.p.Entities[0].TypeID, ps[1].Entities[0].Type.ID)
	require.Equal(t, p1.p.Entities[0].Key, string(ps[1].Entities[0].Key))
	require.Equal(t, []byte(p1.p.Entities[0].Data), ps[1].Entities[0].Data)

	_ = addIngestable(t, h, "")
	time.Sleep(sleepTime)

	ps, err = db.Ents()
	require.Nil(t, err)
	require.Equal(t, 4, len(ps)) // AddType + ProposedProposal + Configuration + IngestedProposal
	require.Equal(t, 1, len(ps[3].Entities))
	require.Equal(t, cp1.Entities[0].Type.ID, ps[3].Entities[0].Type.ID)
	require.Equal(t, cp1.Entities[0].Key, ps[3].Entities[0].Key)
	require.Equal(t, cp1.Entities[0].Data, ps[3].Entities[0].Data)

	databaseID := addDatabase(t, h, "go-mysql-server")
	time.Sleep(sleepTime)

	addSyncable(t, h, typeID, databaseID)
	time.Sleep(sleepTime)

	view(t, storage, databaseID, p1, connectionString)

	// Do a second proposal
	// Shutdown and restart the cluster
	// Add a third propoal
	// Check all of the proposals are still saved
	// Check the database to make sure the other proposals synced propoerly
	// Ingest something else to make sure the ingestable restarts and keeps position
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

func (p *Proposal) toClusterProposal(t *testing.T, db *test.DB) *cluster.Proposal {
	proposal := &cluster.Proposal{}
	for _, e := range p.p.Entities {
		tipe, err := db.Type(e.TypeID)
		require.Nil(t, err)
		proposal.Entities = append(proposal.Entities, &cluster.Entity{
			Type: tipe,
			Key:  []byte(e.Key),
			Data: e.Data,
		})
	}
	return proposal
}

func addParsers(t *testing.T, db *test.DB, dialect sql.Dialect, typeID string) *cluster.Proposal {
	ds := make(map[string]sql.Dialect)
	ds["go-mysql-server"] = dialect
	sqlParser := &sql.DBParser{Dialects: ds}
	db.AddDatabaseParser("sql", sqlParser)
	db.AddSyncableParser("sql", &sql.SyncableParser{})

	cp1 := createProposal(typeID, "key", "one").toClusterProposal(t, db)
	ingestableProposals := []*cluster.Proposal{cp1}
	db.AddIngestableParser("proposal", ingestable.NewProposalIngestableParser(ingestableProposals))

	return cp1
}

func view(t *testing.T, s db.Storage, databaseID string, p *Proposal, connectionString string) {
	database, err := s.Database(databaseID)
	require.Nil(t, err)
	db := database.(*sql.DB).DB
	_, err = db.Exec("USE " + connectionString)
	require.Nil(t, err)
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

func addDatabase(t *testing.T, h *http.HTTP, dialect string) string {
	name := "bar"

	body := fmt.Sprintf(`[database]
type = "sql"
name = "%s"
[sql]
dialect="%s"
connectionString="%s"`, name, dialect, name)

	req := httptest.NewRequest("POST", "http://localhost/database", strings.NewReader(body))
	req.Header["Content-Type"] = []string{"text/toml"}

	w := httptest.NewRecorder()

	h.AddDatabase(w, req)

	resp := w.Result()
	require.Equal(t, 200, resp.StatusCode)
	id, err := io.ReadAll(resp.Body)
	require.Nil(t, err)

	return string(id)
}

func addIngestable(t *testing.T, h *http.HTTP, dialect string) string {
	name := "bar"
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

	req := httptest.NewRequest("POST", "http://localhost/ingestable", strings.NewReader(body))
	req.Header["Content-Type"] = []string{"text/toml"}

	w := httptest.NewRecorder()

	h.AddIngestable(w, req)

	resp := w.Result()
	require.Equal(t, 200, resp.StatusCode)
	id, err := io.ReadAll(resp.Body)
	require.Nil(t, err)

	return string(id)
}

func addSyncable(t *testing.T, h *http.HTTP, topicId string, databaseID string) string {
	name := "bar"
	tableName := name + ".foo"

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

	req := httptest.NewRequest("POST", "http://localhost/syncable", strings.NewReader(body))
	req.Header["Content-Type"] = []string{"text/toml"}

	w := httptest.NewRecorder()

	h.AddSyncable(w, req)

	resp := w.Result()
	require.Equal(t, 200, resp.StatusCode)
	id, err := io.ReadAll(resp.Body)
	require.Nil(t, err)

	return string(id)
}

func addType(t *testing.T, h *http.HTTP, name string) string {
	body := fmt.Sprintf("[type]\nname = \"%s\"", name)

	req := httptest.NewRequest("POST", "http://localhost/type", strings.NewReader(body))
	req.Header["Content-Type"] = []string{"text/toml"}

	w := httptest.NewRecorder()

	h.AddType(w, req)

	resp := w.Result()
	require.Equal(t, 200, resp.StatusCode)
	id, err := io.ReadAll(resp.Body)
	require.Nil(t, err)

	return string(id)
}

func propose(t *testing.T, h *http.HTTP, proposal *http.AddProposalRequest) {
	bs, err := json.Marshal(proposal)
	require.Nil(t, err)
	req := httptest.NewRequest("POST", "http://localhost/proposal", bytes.NewReader(bs))
	req.Header["Content-Type"] = []string{"text/toml"}

	w := httptest.NewRecorder()

	h.AddProposal(w, req)

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
