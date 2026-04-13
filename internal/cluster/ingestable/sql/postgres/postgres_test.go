//go:build docker || integration

package postgres_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/ingestable/sql"
	"github.com/philborlin/committed/internal/cluster/ingestable/sql/postgres"
	"github.com/stretchr/testify/require"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/ory/dockertest/v3"
)

var (
	port     string
	pool     *dockertest.Pool
	resource *dockertest.Resource
)

const (
	dbName   = "testdb"
	username = "postgres"
	password = "secret"
)

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

	resource, err = pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "postgres",
		Tag:        "16",
		Env: []string{
			fmt.Sprintf("POSTGRES_PASSWORD=%s", password),
			fmt.Sprintf("POSTGRES_DB=%s", dbName),
		},
		Cmd: []string{
			"postgres",
			"-c", "wal_level=logical",
			"-c", "max_replication_slots=10",
			"-c", "max_wal_senders=10",
		},
	})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	port = resource.GetPort("5432/tcp")

	if err := pool.Retry(func() error {
		db, err := gosql.Open("pgx", fmt.Sprintf("postgres://%s:%s@localhost:%s/%s?sslmode=disable", username, password, port, dbName))
		if err != nil {
			return err
		}
		defer db.Close()
		return db.Ping()
	}); err != nil {
		log.Fatalf("Could not connect to database: %s", err)
	}

	if err := resource.Expire(300); err != nil {
		log.Fatalf("Could not expire resource: %s", err)
	}

	defer func() {
		if err := pool.Purge(resource); err != nil {
			log.Fatalf("Could not purge resource: %s", err)
		}
	}()

	m.Run()
}

func baseConnString() string {
	return fmt.Sprintf(
		"postgres://%s:%s@localhost:%s/%s?sslmode=disable",
		username, password, port, dbName,
	)
}

func TestPostgresDialect(t *testing.T) {
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
		Data: []byte(`{"one":"one","pk":"key1"}`),
	}
	e2 := &cluster.Entity{
		Type: simpleType,
		Key:  []byte("key2"),
		Data: []byte(`{"one":"two","pk":"key2"}`),
	}

	tests := []struct {
		name     string
		config   *sql.Config
		table    string
		insertFn func(*testing.T, string)
		entities []*cluster.Entity
	}{
		{"one_simple", basicConfig, "pgtest_one", insertOne, []*cluster.Entity{e1}},
		{"two_simple", basicConfig, "pgtest_two", insertTwo, []*cluster.Entity{e1, e2}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create the table before starting the dialect. The
			// publication references the table so it must exist.
			db := createDB(t)
			_, err := db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, tt.table))
			require.NoError(t, err)
			_, err = db.Exec(fmt.Sprintf(`CREATE TABLE %s (pk VARCHAR(32) NOT NULL PRIMARY KEY, one TEXT)`, tt.table))
			require.NoError(t, err)
			db.Close()

			slotName := fmt.Sprintf("slot_%s", tt.name)
			pubName := fmt.Sprintf("pub_%s", tt.name)

			dialect := &postgres.PostgreSQLDialect{}
			tt.config.ConnectionString = baseConnString()
			tt.config.Tables = []string{tt.table}
			tt.config.Options = map[string]string{
				"slot_name":   slotName,
				"publication": pubName,
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			proposalChan := make(chan *cluster.Proposal, 10)
			positionChan := make(chan cluster.Position, 10)

			ingestErr := make(chan error, 1)
			go func() {
				ingestErr <- dialect.Ingest(ctx, tt.config, nil, proposalChan, positionChan)
			}()

			waitForSlot(t, slotName)

			// Insert rows AFTER the dialect is running — pgoutput
			// only captures changes from the slot creation point.
			tt.insertFn(t, tt.table)

			// Collect entities until we've seen the expected count.
			expected := len(tt.entities)
			deadline := time.After(15 * time.Second)
			seen := make(map[string]*cluster.Entity)
			for len(seen) < expected {
				select {
				case proposal := <-proposalChan:
					for _, e := range proposal.Entities {
						seen[string(e.Key)] = e
					}
				case <-positionChan:
				case <-deadline:
					t.Fatalf("timed out waiting for %d unique entities; got %d", expected, len(seen))
				}
			}

			entities := make([]*cluster.Entity, 0, len(seen))
			for _, e := range seen {
				entities = append(entities, e)
			}

			for _, e := range entities {
				require.NotZero(t, e.Timestamp,
					"entity must have propose-time wall-clock for content-deterministic apply")
				e.Timestamp = 0
			}

			require.ElementsMatch(t, tt.entities, entities)

			cancel()
			select {
			case err := <-ingestErr:
				require.Nil(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("Ingest did not exit after cancel")
			}
		})
	}
}

func insertOne(t *testing.T, table string) {
	db := createDB(t)
	defer db.Close()
	_, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (pk, one) VALUES ('key1', 'one')`, table))
	require.NoError(t, err)
}

func insertTwo(t *testing.T, table string) {
	db := createDB(t)
	defer db.Close()
	_, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (pk, one) VALUES ('key1', 'one')`, table))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf(`INSERT INTO %s (pk, one) VALUES ('key2', 'two')`, table))
	require.NoError(t, err)
}

func createDB(t *testing.T) *gosql.DB {
	db, err := gosql.Open("pgx", fmt.Sprintf("postgres://%s:%s@localhost:%s/%s?sslmode=disable", username, password, port, dbName))
	require.NoError(t, err)
	require.NoError(t, db.Ping())
	return db
}

// waitForSlot polls pg_replication_slots until the named slot is active,
// replacing fragile time.Sleep calls.
func waitForSlot(t *testing.T, slotName string) {
	t.Helper()
	require.Eventually(t, func() bool {
		db := createDB(t)
		defer db.Close()
		var active bool
		err := db.QueryRow(
			"SELECT active FROM pg_replication_slots WHERE slot_name = $1", slotName,
		).Scan(&active)
		return err == nil && active
	}, 10*time.Second, 100*time.Millisecond)
}

// TestPostgresPositionResume verifies that checkpointed LSN positions are
// correctly restored on restart: a new Ingest call with a previously
// checkpointed position only receives changes committed after that LSN.
func TestPostgresPositionResume(t *testing.T) {
	table := "resume_table"

	db := createDB(t)
	_, err := db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, table))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf(`CREATE TABLE %s (pk VARCHAR(32) NOT NULL PRIMARY KEY, val TEXT)`, table))
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
		ConnectionString: baseConnString(),
		Tables:           []string{table},
		Options: map[string]string{
			"slot_name":   "slot_resume",
			"publication": "pub_resume",
		},
	}

	// --- Phase 1: start dialect, insert "before", collect position ---
	ctx1, cancel1 := context.WithCancel(context.Background())
	proposalChan1 := make(chan *cluster.Proposal, 10)
	positionChan1 := make(chan cluster.Position, 10)

	dialect1 := &postgres.PostgreSQLDialect{}
	go func() {
		_ = dialect1.Ingest(ctx1, config, nil, proposalChan1, positionChan1)
	}()

	waitForSlot(t, "slot_resume")

	db = createDB(t)
	_, err = db.Exec(fmt.Sprintf(`INSERT INTO %s (pk, val) VALUES ('before', 'initial')`, table))
	require.NoError(t, err)
	db.Close()

	// Collect the "before" proposal and its position.
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

	// Stop the first dialect.
	cancel1()
	require.NotEmpty(t, lastPos, "should have a checkpointed position")

	// --- Phase 2: start new dialect from checkpointed position, insert "after" ---
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	proposalChan2 := make(chan *cluster.Proposal, 10)
	positionChan2 := make(chan cluster.Position, 10)

	dialect2 := &postgres.PostgreSQLDialect{}
	ingestErr := make(chan error, 1)
	go func() {
		ingestErr <- dialect2.Ingest(ctx2, config, lastPos, proposalChan2, positionChan2)
	}()

	waitForSlot(t, "slot_resume")

	db = createDB(t)
	_, err = db.Exec(fmt.Sprintf(`INSERT INTO %s (pk, val) VALUES ('after', 'resumed')`, table))
	require.NoError(t, err)
	db.Close()

	// Collect: "after" should appear, "before" should NOT re-appear.
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

	cancel2()
	select {
	case err := <-ingestErr:
		require.Nil(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Ingest did not exit after cancel")
	}
}

// TestPostgresTransactionGrouping verifies that multiple rows committed in
// a single Postgres transaction arrive as exactly one cluster.Proposal with
// all entities.
func TestPostgresTransactionGrouping(t *testing.T) {
	table := "tx_group_table"

	db := createDB(t)
	_, err := db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, table))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf(`CREATE TABLE %s (pk VARCHAR(32) NOT NULL PRIMARY KEY, val TEXT)`, table))
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
		ConnectionString: baseConnString(),
		Tables:           []string{table},
		Options: map[string]string{
			"slot_name":   "slot_txgroup",
			"publication": "pub_txgroup",
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proposalChan := make(chan *cluster.Proposal, 10)
	positionChan := make(chan cluster.Position, 10)

	dialect := &postgres.PostgreSQLDialect{}
	go func() {
		_ = dialect.Ingest(ctx, config, nil, proposalChan, positionChan)
	}()

	waitForSlot(t, "slot_txgroup")

	// Insert a sentinel row so we know streaming is active.
	db = createDB(t)
	_, err = db.Exec(fmt.Sprintf(`INSERT INTO %s (pk, val) VALUES ('sentinel', 'init')`, table))
	require.NoError(t, err)
	db.Close()

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
			t.Fatal("timed out waiting for sentinel row")
		}
	}

	// Insert 10 rows in a single transaction.
	db = createDB(t)
	tx, err := db.Begin()
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		_, err = tx.Exec(fmt.Sprintf(`INSERT INTO %s (pk, val) VALUES ('tx%d', 'value%d')`, table, i, i))
		require.NoError(t, err)
	}
	err = tx.Commit()
	require.NoError(t, err)
	db.Close()

	// All 10 rows must arrive as a single proposal.
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

	// A position must have been checkpointed.
	select {
	case pos := <-positionChan:
		require.NotEmpty(t, pos, "expected a non-empty post-commit position")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for post-commit position")
	}
}

// TestPostgresSnapshotOnNewSlot verifies that pre-existing rows are
// delivered as proposals when a new replication slot is created
// (initial snapshot), and that streaming picks up new changes
// seamlessly afterward.
func TestPostgresSnapshotOnNewSlot(t *testing.T) {
	table := "snap_table"

	db := createDB(t)
	_, err := db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, table))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf(`CREATE TABLE %s (pk VARCHAR(32) NOT NULL PRIMARY KEY, val TEXT)`, table))
	require.NoError(t, err)

	// Insert rows BEFORE starting the dialect — these must arrive via snapshot.
	_, err = db.Exec(fmt.Sprintf(`INSERT INTO %s (pk, val) VALUES ('pre1', 'existing1')`, table))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf(`INSERT INTO %s (pk, val) VALUES ('pre2', 'existing2')`, table))
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
		ConnectionString: baseConnString(),
		Tables:           []string{table},
		Options: map[string]string{
			"slot_name":   "slot_snap",
			"publication": "pub_snap",
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proposalChan := make(chan *cluster.Proposal, 10)
	positionChan := make(chan cluster.Position, 10)

	dialect := &postgres.PostgreSQLDialect{}
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

	// Now wait for the slot to be active and insert a new row to verify
	// streaming works after the snapshot.
	waitForSlot(t, "slot_snap")

	db = createDB(t)
	_, err = db.Exec(fmt.Sprintf(`INSERT INTO %s (pk, val) VALUES ('post1', 'streamed')`, table))
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
