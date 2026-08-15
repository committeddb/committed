//go:build docker || integration

package postgres_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/postgres"
)

var (
	provOrdersType    = &cluster.Type{ID: "prov_orders_topic", Name: "orders"}
	provCustomersType = &cluster.Type{ID: "prov_customers_topic", Name: "customers"}
)

// TestPostgresCaptureProvenance proves CDC proposals carry the source commit
// timestamp and xid from the pgoutput Begin message: a source transaction
// touching two tables emits two per-topic proposals SHARING one SourceTxnID,
// a second transaction gets a different one, and snapshot-phase proposals
// carry the zero values (a snapshot row has no source transaction).
func TestPostgresCaptureProvenance(t *testing.T) {
	db := createDB(t)
	for _, s := range []string{
		`DROP TABLE IF EXISTS prov_orders`,
		`DROP TABLE IF EXISTS prov_customers`,
		`CREATE TABLE prov_orders (id VARCHAR(32) PRIMARY KEY, amount TEXT)`,
		`CREATE TABLE prov_customers (cust_id VARCHAR(32) PRIMARY KEY, name TEXT)`,
		`INSERT INTO prov_orders (id, amount) VALUES ('o_sentinel','0')`,
		`INSERT INTO prov_customers (cust_id, name) VALUES ('c_sentinel','init')`,
	} {
		_, err := db.Exec(s)
		require.NoError(t, err)
	}
	db.Close()

	slot, pub := "slot_provenance", "pub_provenance"
	cleanReplication(t, slot, pub)
	t.Cleanup(func() { cleanReplication(t, slot, pub) })

	config := &sql.Config{
		ConnectionString: connString,
		Tables:           []string{"prov_orders", "prov_customers"},
		Options:          map[string]string{"slot_name": slot, "publication": pub},
		Topics: []sql.TopicSpec{
			{
				Type:       provOrdersType,
				Tables:     []string{"prov_orders"},
				PrimaryKey: []string{"id"},
				Mappings:   []sql.Mapping{{JsonName: "id", SQLColumn: "id"}, {JsonName: "amount", SQLColumn: "amount"}},
			},
			{
				Type:       provCustomersType,
				Tables:     []string{"prov_customers"},
				PrimaryKey: []string{"cust_id"},
				Mappings:   []sql.Mapping{{JsonName: "cust_id", SQLColumn: "cust_id"}, {JsonName: "name", SQLColumn: "name"}},
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pr := make(chan *cluster.Proposal, 32)
	po := make(chan cluster.Position, 32)
	dialect := &postgres.PostgreSQLDialect{}
	ingestErr := make(chan error, 1)
	go func() { ingestErr <- dialect.Ingest(ctx, config, nil, 0, pr, po) }()
	// Join the worker before the slot/publication cleanup runs: a test that
	// returns while Ingest is still alive races the cleanup (the worker can
	// recreate a just-dropped slot), poisoning the rest of the suite.
	defer func() {
		cancel()
		select {
		case err := <-ingestErr:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Error("Ingest did not exit after cancel")
		}
	}()

	// Snapshot phase: both sentinels arrive with ZERO provenance.
	seenSentinels := 0
	deadline := time.After(30 * time.Second)
	for seenSentinels < 2 {
		select {
		case p := <-pr:
			if isRefreshMarkerProposal(p) {
				continue
			}
			for _, e := range p.Entities {
				if k := string(e.Key); k == "o_sentinel" || k == "c_sentinel" {
					require.Zero(t, p.SourceCommitUnixNano, "snapshot proposals carry no source commit time")
					require.Empty(t, p.SourceTxnID, "snapshot proposals carry no source transaction id")
					seenSentinels++
				}
			}
		case <-po:
		case <-deadline:
			t.Fatal("timed out waiting for snapshot sentinels")
		}
	}

	// Transaction 1 touches BOTH tables; transaction 2 touches one.
	db = createDB(t)
	before := time.Now()
	tx, err := db.Begin()
	require.NoError(t, err)
	_, err = tx.Exec(`INSERT INTO prov_orders (id, amount) VALUES ('o1','999')`)
	require.NoError(t, err)
	_, err = tx.Exec(`INSERT INTO prov_customers (cust_id, name) VALUES ('c1','Zoe')`)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	_, err = db.Exec(`INSERT INTO prov_orders (id, amount) VALUES ('o2','1')`)
	require.NoError(t, err)
	after := time.Now()
	db.Close()

	byKey := map[string]*cluster.Proposal{}
	deadline = time.After(30 * time.Second)
	for len(byKey) < 3 {
		select {
		case p := <-pr:
			if isRefreshMarkerProposal(p) {
				continue
			}
			for _, e := range p.Entities {
				switch k := string(e.Key); k {
				case "o1", "c1", "o2":
					byKey[k] = p
				}
			}
		case <-po:
		case <-deadline:
			t.Fatalf("timed out waiting for CDC rows; got %d/3", len(byKey))
		}
	}

	// Transaction 1's per-topic proposals share one xid; transaction 2 has
	// its own, and an xid is a decimal number.
	require.NotEmpty(t, byKey["o1"].SourceTxnID)
	_, err = strconv.ParseUint(byKey["o1"].SourceTxnID, 10, 64)
	require.NoError(t, err, "a Postgres transaction identity is the decimal xid")
	require.Equal(t, byKey["o1"].SourceTxnID, byKey["c1"].SourceTxnID,
		"rows changed in one source transaction share a SourceTxnID across per-topic proposals")
	require.NotEmpty(t, byKey["o2"].SourceTxnID)
	require.NotEqual(t, byKey["o1"].SourceTxnID, byKey["o2"].SourceTxnID,
		"distinct source transactions carry distinct identities")

	// Commit timestamps bracket the source commits.
	for k, p := range byKey {
		require.GreaterOrEqual(t, p.SourceCommitUnixNano, before.Add(-2*time.Second).UnixNano(),
			"row %s commit time too early", k)
		require.LessOrEqual(t, p.SourceCommitUnixNano, after.Add(2*time.Second).UnixNano(),
			"row %s commit time too late", k)
	}
}
