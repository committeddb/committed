//go:build docker || integration

package mysql_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/docker/docker/client"
	_ "github.com/go-sql-driver/mysql"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/ingestable/sql"
	"github.com/philborlin/committed/internal/cluster/ingestable/sql/mysql"
	"github.com/stretchr/testify/require"
)

var (
	mysqlContainer *tcmysql.MySQLContainer
	dsn            string // go-sql-driver DSN (user:pass@tcp(host:port)/db)
	ingestURL      string // mysql:// URL for ingestable config
)

const (
	dbName   = "dbName"
	username = "root"
	password = "secret"
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	var err error
	mysqlContainer, err = tcmysql.Run(ctx,
		"mysql:9",
		tcmysql.WithDatabase(dbName),
		tcmysql.WithUsername(username),
		tcmysql.WithPassword(password),
	)
	if err != nil {
		log.Fatalf("Could not start MySQL container: %v", err)
	}

	dsn, err = mysqlContainer.ConnectionString(ctx)
	if err != nil {
		log.Fatalf("Could not get DSN: %v", err)
	}

	host, err := mysqlContainer.Host(ctx)
	if err != nil {
		log.Fatalf("Could not get host: %v", err)
	}
	port, err := mysqlContainer.MappedPort(ctx, "3306/tcp")
	if err != nil {
		log.Fatalf("Could not get port: %v", err)
	}
	ingestURL = fmt.Sprintf("mysql://%s:%s@%s:%s/%s", username, password, host, port.Port(), dbName)

	code := m.Run()

	// Ryuk (the testcontainers reaper) handles cleanup automatically.
	os.Exit(code)
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
			// Run setup BEFORE starting the dialect. The pure-SQL
			// snapshot captures the table state at the moment Ingest
			// starts; the binlog tail then picks up changes from that
			// point forward. Running setup first means the snapshot
			// captures the final state and the tail has nothing left to
			// deliver, which makes the test deterministic.
			tt.setupFn(t)

			dialect := &mysql.MySQLDialect{}
			tt.config.ConnectionString = ingestURL
			tt.config.Tables = []string{tt.tables}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			proposalChan := make(chan *cluster.Proposal)
			positionChan := make(chan cluster.Position)

			go func() {
				err := dialect.Ingest(ctx, tt.config, nil, proposalChan, positionChan)
				require.Nil(t, err)
			}()

			// Collect entities until we've seen the expected count.
			// With XID-aware grouping, snapshot-phase rows may arrive
			// as a single multi-entity proposal, so we iterate all entities
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
			// any duplicate deliveries are absorbed.
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
	db, err := gosql.Open("mysql", dsn)
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
		ConnectionString: ingestURL,
		Tables:           []string{table},
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

	// --- pause MySQL (simulate network outage) ---
	// Use Docker pause/unpause instead of stop/start because
	// OrbStack reassigns host port mappings on container restart.
	// Pause freezes all processes and network without destroying
	// the port mapping, so the canal can reconnect on the same port.
	t.Log("pausing MySQL container")
	cli, err := client.NewClientWithOpts(client.FromEnv)
	require.Nil(t, err)
	defer cli.Close()

	containerID := mysqlContainer.GetContainerID()
	err = cli.ContainerPause(ctx, containerID)
	require.Nil(t, err)

	// Give the canal time to detect the frozen connection.
	time.Sleep(3 * time.Second)

	// --- unpause MySQL ---
	t.Log("unpausing MySQL container")
	err = cli.ContainerUnpause(ctx, containerID)
	require.Nil(t, err)

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
	// The sentinel lets us detect when the snapshot phase is complete.
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
		ConnectionString: ingestURL,
		Tables:           []string{table},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proposalChan := make(chan *cluster.Proposal, 10)
	positionChan := make(chan cluster.Position, 10)

	dialect := &mysql.MySQLDialect{}
	go func() {
		_ = dialect.Ingest(ctx, config, nil, proposalChan, positionChan)
	}()

	// Wait for the sentinel row to arrive — this means the snapshot
	// phase is complete and canal is tailing the binlog.
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
			t.Fatal("timed out waiting for sentinel row from snapshot")
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

// TestMysqlPositionResume verifies that checkpointed binlog positions are
// correctly restored on restart: a new Ingest call with a previously
// checkpointed position only receives changes committed after that position.
func TestMysqlPositionResume(t *testing.T) {
	table := "resume_table"

	db := createDB(t)
	_, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", table))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE `%s` (pk VARCHAR(32) NOT NULL, val TEXT, PRIMARY KEY (pk));", table))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf("INSERT INTO `%s` (pk, val) VALUES ('before', 'initial');", table))
	require.NoError(t, err)
	db.Close()

	simpleType := &cluster.Type{ID: "resume", Name: "resume"}
	config := &sql.Config{
		Type: simpleType,
		Mappings: []sql.Mapping{
			{JsonName: "pk", SQLColumn: "pk"},
			{JsonName: "val", SQLColumn: "val"},
		},
		PrimaryKey:       "pk",
		ConnectionString: ingestURL,
		Tables:           []string{table},
	}

	// --- Phase 1: start dialect, collect "before" proposal and position ---
	ctx1, cancel1 := context.WithCancel(context.Background())
	proposalChan1 := make(chan *cluster.Proposal, 10)
	positionChan1 := make(chan cluster.Position, 10)

	dialect1 := &mysql.MySQLDialect{}
	go func() {
		_ = dialect1.Ingest(ctx1, config, nil, proposalChan1, positionChan1)
	}()

	deadline := time.After(15 * time.Second)
	seen := make(map[string]bool)
	var lastPos cluster.Position
	for !seen["before"] || lastPos == nil {
		select {
		case p := <-proposalChan1:
			for _, e := range p.Entities {
				seen[string(e.Key)] = true
			}
		case pos := <-positionChan1:
			lastPos = pos
		case <-deadline:
			t.Fatal("timed out waiting for initial proposal and position")
		}
	}

	// Insert another row while phase 1 is running so the binlog advances
	// past "before". Then collect the position after "during" commits.
	db = createDB(t)
	_, err = db.Exec(fmt.Sprintf("INSERT INTO `%s` (pk, val) VALUES ('during', 'phase1');", table))
	require.NoError(t, err)
	db.Close()

	// Collect until we've seen "during" AND received a position
	// checkpoint emitted after it. OnXID sends the proposal first and
	// then the position, so requiring a post-"during" position guarantees
	// lastPos is past the "during" commit.
	deadline = time.After(15 * time.Second)
	seenDuring := false
	posAfterDuring := false
	for !seenDuring || !posAfterDuring {
		select {
		case p := <-proposalChan1:
			for _, e := range p.Entities {
				seen[string(e.Key)] = true
				if string(e.Key) == "during" {
					seenDuring = true
				}
			}
		case pos := <-positionChan1:
			lastPos = pos
			if seenDuring {
				posAfterDuring = true
			}
		case <-deadline:
			t.Fatal("timed out waiting for 'during' proposal and position")
		}
	}

	// Stop the first dialect.
	cancel1()
	require.NotEmpty(t, lastPos, "should have a checkpointed position")

	// --- Phase 2: start new dialect from checkpointed position ---
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	proposalChan2 := make(chan *cluster.Proposal, 10)
	positionChan2 := make(chan cluster.Position, 10)

	// Insert a new row before starting phase 2 — this is committed
	// AFTER lastPos, so the dialect should pick it up via binlog.
	db = createDB(t)
	_, err = db.Exec(fmt.Sprintf("INSERT INTO `%s` (pk, val) VALUES ('after', 'resumed');", table))
	require.NoError(t, err)
	db.Close()

	dialect2 := &mysql.MySQLDialect{}
	ingestErr := make(chan error, 1)
	go func() {
		ingestErr <- dialect2.Ingest(ctx2, config, lastPos, proposalChan2, positionChan2)
	}()

	// Collect: "after" should appear. "before" and "during" should NOT
	// re-appear since they were committed before the checkpointed position.
	deadline = time.After(15 * time.Second)
	seen2 := make(map[string]bool)
	for !seen2["after"] {
		select {
		case p := <-proposalChan2:
			for _, e := range p.Entities {
				seen2[string(e.Key)] = true
			}
		case <-positionChan2:
		case <-deadline:
			t.Fatal("timed out waiting for post-resume proposal")
		}
	}

	require.False(t, seen2["before"],
		"'before' should not re-appear when resuming from checkpointed position")
	require.False(t, seen2["during"],
		"'during' should not re-appear when resuming from checkpointed position")

	cancel2()
	select {
	case err := <-ingestErr:
		require.Nil(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Ingest did not exit after cancel")
	}
}

// TestMysqlSnapshotOnFreshStart verifies that pre-existing rows are
// delivered as proposals via the pure-SQL snapshot on a fresh start
// (no saved position), and that binlog streaming picks up new changes
// seamlessly afterward.
func TestMysqlSnapshotOnFreshStart(t *testing.T) {
	table := "snap_table"

	db := createDB(t)
	_, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", table))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE `%s` (pk VARCHAR(32) NOT NULL, val TEXT, PRIMARY KEY (pk));", table))
	require.NoError(t, err)

	// Insert rows BEFORE starting the dialect — these must arrive via snapshot.
	_, err = db.Exec(fmt.Sprintf("INSERT INTO `%s` (pk, val) VALUES ('pre1', 'existing1');", table))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf("INSERT INTO `%s` (pk, val) VALUES ('pre2', 'existing2');", table))
	require.NoError(t, err)
	db.Close()

	simpleType := &cluster.Type{ID: "snap", Name: "snap"}
	config := &sql.Config{
		Type: simpleType,
		Mappings: []sql.Mapping{
			{JsonName: "pk", SQLColumn: "pk"},
			{JsonName: "val", SQLColumn: "val"},
		},
		PrimaryKey:       "pk",
		ConnectionString: ingestURL,
		Tables:           []string{table},
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

	// Collect snapshot entities — the 2 pre-existing rows.
	deadline := time.After(15 * time.Second)
	seen := make(map[string]*cluster.Entity)
	for len(seen) < 2 {
		select {
		case p := <-proposalChan:
			for _, e := range p.Entities {
				seen[string(e.Key)] = e
			}
		case <-positionChan:
		case <-deadline:
			t.Fatalf("timed out waiting for snapshot entities; got %d", len(seen))
		}
	}

	require.Contains(t, seen, "pre1")
	require.Contains(t, seen, "pre2")

	// Insert a new row to verify binlog streaming works after the snapshot.
	db = createDB(t)
	_, err = db.Exec(fmt.Sprintf("INSERT INTO `%s` (pk, val) VALUES ('post1', 'streamed');", table))
	require.NoError(t, err)
	db.Close()

	deadline = time.After(15 * time.Second)
	for seen["post1"] == nil {
		select {
		case p := <-proposalChan:
			for _, e := range p.Entities {
				seen[string(e.Key)] = e
			}
		case <-positionChan:
		case <-deadline:
			t.Fatal("timed out waiting for streamed entity after snapshot")
		}
	}

	require.NotNil(t, seen["post1"])

	cancel()
	select {
	case err := <-ingestErr:
		require.Nil(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Ingest did not exit after cancel")
	}
}
