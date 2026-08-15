//go:build docker || integration

package mysql_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/mysql"
)

var (
	provOrdersType    = &cluster.Type{ID: "prov_orders_topic", Name: "orders"}
	provCustomersType = &cluster.Type{ID: "prov_customers_topic", Name: "customers"}
)

// TestMysqlCaptureProvenance proves CDC proposals carry the source commit
// timestamp and transaction identity (the container runs gtid_mode=ON, so the
// identity is the transaction's GTID): a source transaction touching two
// tables emits two per-topic proposals SHARING one SourceTxnID, a second
// transaction gets a different one, and snapshot-phase proposals carry the
// zero values (a snapshot row has no source transaction).
func TestMysqlCaptureProvenance(t *testing.T) {
	db := createDB(t)
	for _, s := range []string{
		"DROP TABLE IF EXISTS prov_orders",
		"DROP TABLE IF EXISTS prov_customers",
		"CREATE TABLE prov_orders (id VARCHAR(32) NOT NULL PRIMARY KEY, amount TEXT)",
		"CREATE TABLE prov_customers (cust_id VARCHAR(32) NOT NULL PRIMARY KEY, name TEXT)",
		"INSERT INTO prov_orders (id, amount) VALUES ('o_sentinel','0')",
		"INSERT INTO prov_customers (cust_id, name) VALUES ('c_sentinel','init')",
	} {
		_, err := db.Exec(s)
		require.NoError(t, err)
	}
	db.Close()

	config := &sql.Config{
		ConnectionString: ingestURL,
		Tables:           []string{"prov_orders", "prov_customers"},
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
	dialect := &mysql.MySQLDialect{}
	ingestDone := make(chan struct{})
	go func() { defer close(ingestDone); _ = dialect.Ingest(ctx, config, nil, 0, pr, po) }()
	// Join the worker on exit so the test never returns while Ingest is still
	// alive (a straggler would race later tests' use of the shared container).
	defer func() {
		cancel()
		select {
		case <-ingestDone:
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
	_, err = tx.Exec("INSERT INTO prov_orders (id, amount) VALUES ('o1','999')")
	require.NoError(t, err)
	_, err = tx.Exec("INSERT INTO prov_customers (cust_id, name) VALUES ('c1','Zoe')")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	_, err = db.Exec("INSERT INTO prov_orders (id, amount) VALUES ('o2','1')")
	require.NoError(t, err)
	after := time.Now()
	db.Close()

	// Collect the three CDC entities' proposals.
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
			t.Fatalf("timed out waiting for CDC rows; got %v", keys(byKey))
		}
	}

	// Transaction 1's two per-topic proposals share one identity; transaction
	// 2 has its own.
	require.NotEmpty(t, byKey["o1"].SourceTxnID)
	require.Equal(t, byKey["o1"].SourceTxnID, byKey["c1"].SourceTxnID,
		"rows changed in one source transaction share a SourceTxnID across per-topic proposals")
	require.NotEmpty(t, byKey["o2"].SourceTxnID)
	require.NotEqual(t, byKey["o1"].SourceTxnID, byKey["o2"].SourceTxnID,
		"distinct source transactions carry distinct identities")

	// Commit timestamps bracket the source commit (header resolution is one
	// second, so widen the bracket accordingly).
	for k, p := range byKey {
		require.GreaterOrEqual(t, p.SourceCommitUnixNano, before.Add(-2*time.Second).UnixNano(),
			"row %s commit time too early", k)
		require.LessOrEqual(t, p.SourceCommitUnixNano, after.Add(2*time.Second).UnixNano(),
			"row %s commit time too late", k)
	}
}

func keys(m map[string]*cluster.Proposal) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
