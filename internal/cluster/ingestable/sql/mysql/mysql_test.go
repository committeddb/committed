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

	p1 := &cluster.Proposal{
		Entities: []*cluster.Entity{{
			Type: simpleType,
			Key:  []byte("key1"),
			Data: []byte("{\"one\":\"one\",\"pk\":\"key1\"}"),
		}},
	}
	p2 := &cluster.Proposal{
		Entities: []*cluster.Entity{{
			Type: simpleType,
			Key:  []byte("key2"),
			Data: []byte("{\"one\":\"two\",\"pk\":\"key2\"}"),
		}},
	}

	tests := []struct {
		name    string
		config  *sql.Config
		tables  string
		setupFn func(*testing.T)
		ps      []*cluster.Proposal
	}{
		{"one-simple", basicConfig, "table", setup1, []*cluster.Proposal{p1}},
		{"two-simple", basicConfig, "table", setup2, []*cluster.Proposal{p1, p2}},
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

			// Collect proposals until we've seen the expected number AND
			// no new ones arrive for a quiet period. The quiet-period
			// check guards against canal occasionally re-delivering a
			// row through the binlog tail in addition to the dump
			// (cross-subtest binlog state can leak through). Position
			// events still need draining — the dialect blocks on the
			// unbuffered positionChan, and a backed-up channel would
			// stall the binlog goroutine before it could deliver
			// downstream rows.
			expected := len(tt.ps)
			deadline := time.After(10 * time.Second)
			const quiet = 200 * time.Millisecond
			seen := make(map[string]*cluster.Proposal)
			for len(seen) < expected {
				select {
				case proposal := <-proposalChan:
					seen[string(proposal.Entities[0].Key)] = proposal
				case <-positionChan:
					// drain — see comment above
				case <-deadline:
					t.Fatalf("timed out waiting for %d unique proposals; got %d", expected, len(seen))
				}
			}
			// Now that we have the expected count, drain anything that
			// arrives in the next quiet window so duplicate deliveries
			// (dump + tail) are absorbed before the assertion.
		drain:
			for {
				select {
				case proposal := <-proposalChan:
					seen[string(proposal.Entities[0].Key)] = proposal
				case <-positionChan:
				case <-time.After(quiet):
					break drain
				}
			}

			proposals := make([]*cluster.Proposal, 0, len(seen))
			for _, p := range seen {
				proposals = append(proposals, p)
			}

			// Each entity must have a non-zero propose-time Timestamp
			// (set in mysql.go's OnRow handler since PR4). The exact
			// value depends on wall-clock at run time, so we assert
			// non-zero and then clear it before the ElementsMatch
			// check so the comparison doesn't depend on timing.
			for _, p := range proposals {
				for _, e := range p.Entities {
					require.NotZero(t, e.Timestamp,
						"OnRow must stamp the entity with propose-time wall-clock for content-deterministic apply")
					e.Timestamp = 0
				}
			}

			require.ElementsMatch(t, tt.ps, proposals)
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
			seen[string(p.Entities[0].Key)] = true
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
			seen[string(p.Entities[0].Key)] = true
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
