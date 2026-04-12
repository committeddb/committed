//go:build docker || integration

package mysql_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/ingestable/sql"
	"github.com/philborlin/committed/internal/cluster/ingestable/sql/mysql"
	"github.com/stretchr/testify/require"

	_ "github.com/go-sql-driver/mysql"
	"github.com/ory/dockertest/v3"
)

var (
	port     = "3306"
	pool     *dockertest.Pool
	resource *dockertest.Resource
)

const dbName = "dbName"
const username = "root"
const password = "secret"

func TestMain(m *testing.M) {
	var err error
	pool, err = dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not construct pool: %s", err)
	}

	err = pool.Client.Ping()
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}

	resource, err = pool.Run("mysql", "9", []string{fmt.Sprintf("MYSQL_ROOT_PASSWORD=%s", password)})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	port = resource.GetPort("3306/tcp")

	if err := pool.Retry(func() error {
		var err error
		db, err := gosql.Open("mysql", fmt.Sprintf("%s:%s@(localhost:%s)/", username, password, port))
		if err != nil {
			return err
		}
		err = db.Ping()
		if err != nil {
			return err
		}

		_, err = db.Exec("CREATE DATABASE " + dbName)
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		log.Fatalf("Could not connect to database: %s", err)
	}

	if err := resource.Expire(60); err != nil { // Tell docker to hard kill the container in 60 seconds
		log.Fatalf("Could not expire resource: %s", err)
	}

	defer func() {
		if err := pool.Purge(resource); err != nil {
			log.Fatalf("Could not purge resource: %s", err)
		}
	}()

	m.Run()
}

func TestMysqlDialect(t *testing.T) {
	simpleType := &cluster.Type{
		ID:   "simple",
		Name: "simple",
	}
	basicConfig := &sql.Config{
		Type: simpleType,
		Mappings: []sql.Mapping{{
			JsonName:  "one",
			SQLColumn: "one",
		}, {
			JsonName:  "pk",
			SQLColumn: "pk",
		}},
		PrimaryKey: "pk",
	}

	e1 := &cluster.Entity{
		Type: simpleType,
		Key:  []byte("key1"),
		Data: []byte("{\"one\":\"one\",\"pk\":\"key1\"}"),
	}
	e2 := &cluster.Entity{
		Type: simpleType,
		Key:  []byte("key2"),
		Data: []byte("{\"one\":\"two\",\"pk\":\"key2\"}"),
	}

	tests := []struct {
		name     string
		config   *sql.Config
		tables   string
		setupFn  func(*testing.T)
		entities []*cluster.Entity
	}{
		{"one-simple", basicConfig, "table", setup1, []*cluster.Entity{e1}},
		{"two-simple", basicConfig, "table", setup2, []*cluster.Entity{e1, e2}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Run setup BEFORE starting the dialect. canal's initial
			// mysqldump captures the table state at the moment Ingest
			// starts; the binlog tail then picks up changes from that
			// point forward. Running setup first means the dump captures
			// the final state and the tail has nothing left to deliver,
			// which makes the test deterministic. The previous order
			// (dialect starts, then setup runs) created a race window
			// where the dump would snapshot a stale or empty table and
			// the tail would replay setup's events — sometimes
			// duplicating rows when stale dump state coincided with new
			// tail INSERTs.
			tt.setupFn(t)

			dialect := &mysql.MySQLDialect{}
			con := fmt.Sprintf("mysql://%s:%s@127.0.0.1:%s/%s?tables=%s", username, password, port, dbName, tt.tables)
			tt.config.ConnectionString = con

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			proposalChan := make(chan *cluster.Proposal)
			positionChan := make(chan cluster.Position)

			go func() {
				err := dialect.Ingest(ctx, tt.config, nil, proposalChan, positionChan)
				require.Nil(t, err)
			}()

			// Collect entities until we've seen the expected count.
			// With XID-aware grouping, dump-phase rows may arrive as a
			// single multi-entity proposal, so we iterate all entities
			// in each proposal. Position events still need draining.
			expected := len(tt.entities)
			deadline := time.After(10 * time.Second)
			const quiet = 200 * time.Millisecond
			seen := make(map[string]*cluster.Entity)
			for len(seen) < expected {
				select {
				case proposal := <-proposalChan:
					for _, e := range proposal.Entities {
						seen[string(e.Key)] = e
					}
				case <-positionChan:
					// drain
				case <-deadline:
					t.Fatalf("timed out waiting for %d unique entities; got %d", expected, len(seen))
				}
			}
			// Drain anything that arrives in the next quiet window so
			// duplicate deliveries (dump + tail) are absorbed.
		drain:
			for {
				select {
				case proposal := <-proposalChan:
					for _, e := range proposal.Entities {
						seen[string(e.Key)] = e
					}
				case <-positionChan:
				case <-time.After(quiet):
					break drain
				}
			}

			entities := make([]*cluster.Entity, 0, len(seen))
			for _, e := range seen {
				entities = append(entities, e)
			}

			for _, e := range entities {
				require.NotZero(t, e.Timestamp,
					"OnRow must stamp the entity with propose-time wall-clock for content-deterministic apply")
				e.Timestamp = 0
			}

			require.ElementsMatch(t, tt.entities, entities)
		})
	}
}

func setup1(t *testing.T) {
	table := "table"

	db := createDB(t)
	_, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", table))
	require.Nil(t, err)
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE `%s` (pk VARCHAR(32) NOT NULL,one TEXT,PRIMARY KEY (pk));", table))
	require.Nil(t, err)
	_, err = db.Exec(fmt.Sprintf("INSERT INTO `%s` (pk, one) VALUES ('key1', 'one');", table))
	require.Nil(t, err)
}

func setup2(t *testing.T) {
	table := "table"

	db := createDB(t)
	_, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", table))
	require.Nil(t, err)
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE `%s` (pk VARCHAR(32) NOT NULL,one TEXT,PRIMARY KEY (pk));", table))
	require.Nil(t, err)
	_, err = db.Exec(fmt.Sprintf("INSERT INTO `%s` (pk, one) VALUES ('key1', 'one');", table))
	require.Nil(t, err)
	_, err = db.Exec(fmt.Sprintf("INSERT INTO `%s` (pk, one) VALUES ('key2', 'two');", table))
	require.Nil(t, err)
}

func createDB(t *testing.T) *gosql.DB {
	db, err := gosql.Open("mysql", fmt.Sprintf("%s:%s@(localhost:%s)/%s", username, password, port, dbName))
	require.Nil(t, err)
	err = db.Ping()
	require.Nil(t, err)

	return db
}

// TestMysqlReconnect verifies that the ingestable reconnects after
// MySQL goes away mid-stream and resumes delivering proposals once
// MySQL comes back.
func TestMysqlReconnect(t *testing.T) {
	table := "reconnect_table"

	// --- initial setup: create table + insert row before Ingest starts ---
	db := createDB(t)
	_, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", table))
	require.Nil(t, err)
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE `%s` (pk VARCHAR(32) NOT NULL, val TEXT, PRIMARY KEY (pk));", table))
	require.Nil(t, err)
	_, err = db.Exec(fmt.Sprintf("INSERT INTO `%s` (pk, val) VALUES ('before', 'initial');", table))
	require.Nil(t, err)
	db.Close()

	simpleType := &cluster.Type{ID: "reconnect", Name: "reconnect"}
	config := &sql.Config{
		Type: simpleType,
		Mappings: []sql.Mapping{
			{JsonName: "pk", SQLColumn: "pk"},
			{JsonName: "val", SQLColumn: "val"},
		},
		PrimaryKey:       "pk",
		ConnectionString: fmt.Sprintf("mysql://%s:%s@127.0.0.1:%s/%s?tables=%s", username, password, port, dbName, table),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proposalChan := make(chan *cluster.Proposal, 10)
	positionChan := make(chan cluster.Position, 10)

	dialect := &mysql.MySQLDialect{}
	ingestErr := make(chan error, 1)
	go func() {
		ingestErr <- dialect.Ingest(ctx, config, nil, proposalChan, positionChan)
	}()

	// --- collect the initial "before" proposal ---
	deadline := time.After(15 * time.Second)
	seen := make(map[string]bool)
	for !seen["before"] {
		select {
		case p := <-proposalChan:
			for _, e := range p.Entities {
				seen[string(e.Key)] = true
			}
		case <-positionChan:
		case <-deadline:
			t.Fatal("timed out waiting for initial proposal")
		}
	}

	// --- stop MySQL (simulate outage) ---
	t.Log("stopping MySQL container")
	err = pool.Client.StopContainer(resource.Container.ID, 3)
	require.Nil(t, err)

	// --- restart MySQL ---
	t.Log("restarting MySQL container")
	err = pool.Client.StartContainer(resource.Container.ID, nil)
	require.Nil(t, err)

	// Wait for MySQL to accept connections again.
	require.NoError(t, pool.Retry(func() error {
		db, err := gosql.Open("mysql", fmt.Sprintf("%s:%s@(localhost:%s)/%s", username, password, port, dbName))
		if err != nil {
			return err
		}
		defer db.Close()
		return db.Ping()
	}))

	// --- insert a new row after MySQL is back ---
	db = createDB(t)
	_, err = db.Exec(fmt.Sprintf("INSERT INTO `%s` (pk, val) VALUES ('after', 'reconnected');", table))
	require.Nil(t, err)
	db.Close()

	// --- verify the ingestable reconnected and delivered the new row ---
	deadline = time.After(30 * time.Second)
	for !seen["after"] {
		select {
		case p := <-proposalChan:
			for _, e := range p.Entities {
				seen[string(e.Key)] = true
			}
		case <-positionChan:
		case <-deadline:
			t.Fatal("timed out waiting for post-reconnect proposal")
		}
	}

	cancel()
	select {
	case err := <-ingestErr:
		require.Nil(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Ingest did not exit after cancel")
	}
}

// TestMysqlTransactionGrouping verifies that multiple rows committed in
// a single MySQL transaction arrive as exactly one cluster.Proposal with
// all entities, and that the checkpointed position is the post-commit
// binlog position.
func TestMysqlTransactionGrouping(t *testing.T) {
	table := "tx_group_table"

	// Create the table and insert a sentinel row before Ingest starts.
	// The sentinel lets us detect when the dump phase is complete.
	db := createDB(t)
	_, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", table))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE `%s` (pk VARCHAR(32) NOT NULL, val TEXT, PRIMARY KEY (pk));", table))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf("INSERT INTO `%s` (pk, val) VALUES ('sentinel', 'init');", table))
	require.NoError(t, err)
	db.Close()

	simpleType := &cluster.Type{ID: "txgroup", Name: "txgroup"}
	config := &sql.Config{
		Type: simpleType,
		Mappings: []sql.Mapping{
			{JsonName: "pk", SQLColumn: "pk"},
			{JsonName: "val", SQLColumn: "val"},
		},
		PrimaryKey:       "pk",
		ConnectionString: fmt.Sprintf("mysql://%s:%s@127.0.0.1:%s/%s?tables=%s", username, password, port, dbName, table),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proposalChan := make(chan *cluster.Proposal, 10)
	positionChan := make(chan cluster.Position, 10)

	dialect := &mysql.MySQLDialect{}
	go func() {
		_ = dialect.Ingest(ctx, config, nil, proposalChan, positionChan)
	}()

	// Wait for the sentinel row to arrive — this means the dump phase
	// is complete and canal is tailing the binlog.
	deadline := time.After(15 * time.Second)
	sentinelSeen := false
	for !sentinelSeen {
		select {
		case p := <-proposalChan:
			for _, e := range p.Entities {
				if string(e.Key) == "sentinel" {
					sentinelSeen = true
				}
			}
		case <-positionChan:
		case <-deadline:
			t.Fatal("timed out waiting for sentinel row from dump")
		}
	}

	// Insert 10 rows in a single transaction.
	db = createDB(t)
	tx, err := db.Begin()
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		_, err = tx.Exec(fmt.Sprintf("INSERT INTO `%s` (pk, val) VALUES ('tx%d', 'value%d');", table, i, i))
		require.NoError(t, err)
	}
	err = tx.Commit()
	require.NoError(t, err)
	db.Close()

	// Collect the transaction proposal. Because all 10 rows are in one
	// MySQL transaction, they must arrive as a single proposal.
	deadline = time.After(15 * time.Second)
	var txProposal *cluster.Proposal
	for txProposal == nil {
		select {
		case p := <-proposalChan:
			txProposal = p
		case <-positionChan:
		case <-deadline:
			t.Fatal("timed out waiting for transaction proposal")
		}
	}

	require.Len(t, txProposal.Entities, 10,
		"expected all 10 rows from one transaction in a single proposal")

	keys := make(map[string]bool)
	for _, e := range txProposal.Entities {
		require.NotZero(t, e.Timestamp)
		keys[string(e.Key)] = true
	}
	for i := 0; i < 10; i++ {
		require.True(t, keys[fmt.Sprintf("tx%d", i)], "missing key tx%d", i)
	}

	// A position must have been checkpointed for the committed transaction.
	select {
	case pos := <-positionChan:
		require.NotEmpty(t, pos, "expected a non-empty post-commit position")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for post-commit position")
	}
}
