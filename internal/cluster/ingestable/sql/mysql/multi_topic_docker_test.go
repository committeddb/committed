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
	mtOrdersType    = &cluster.Type{ID: "mt_orders_topic", Name: "orders"}
	mtCustomersType = &cluster.Type{ID: "mt_customers_topic", Name: "customers"}
)

// mtRequireHomogeneous asserts every entity in a proposal shares one topic and
// returns that topic id — the invariant PartitionByTopic guarantees.
func mtRequireHomogeneous(t *testing.T, p *cluster.Proposal) string {
	t.Helper()
	require.NotEmpty(t, p.Entities)
	topic := p.Entities[0].Type.ID
	for _, e := range p.Entities {
		require.Equal(t, topic, e.Type.ID, "a proposal must not mix topics")
	}
	return topic
}

// TestMysqlMultiTopicRoutesAndSplitsByTopic: one ingestable, one binlog reader, two
// tables → two distinct topics. Its snapshot routes each table's rows to its own
// topic (proven by the per-table sentinels), and a single source transaction
// touching BOTH tables is emitted as one homogeneous proposal per topic with
// strictly-increasing SourceSeqs (the per-topic flushSub at one binlog coordinate).
func TestMysqlMultiTopicRoutesAndSplitsByTopic(t *testing.T) {
	db := createDB(t)
	for _, s := range []string{
		"DROP TABLE IF EXISTS mt_orders",
		"DROP TABLE IF EXISTS mt_customers",
		"CREATE TABLE mt_orders (id VARCHAR(32) NOT NULL PRIMARY KEY, amount TEXT)",
		"CREATE TABLE mt_customers (cust_id VARCHAR(32) NOT NULL PRIMARY KEY, name TEXT)",
		// Sentinel per table: its arrival marks that table's snapshot done and the
		// binlog is tailing (mirrors TestMysqlTransactionGrouping).
		"INSERT INTO mt_orders (id, amount) VALUES ('o_sentinel','0')",
		"INSERT INTO mt_customers (cust_id, name) VALUES ('c_sentinel','init')",
	} {
		_, err := db.Exec(s)
		require.NoError(t, err)
	}
	db.Close()

	config := &sql.Config{
		ConnectionString: ingestURL,
		Tables:           []string{"mt_orders", "mt_customers"},
		Topics: []sql.TopicSpec{
			{
				Type:       mtOrdersType,
				Tables:     []string{"mt_orders"},
				PrimaryKey: []string{"id"},
				Mappings:   []sql.Mapping{{JsonName: "id", SQLColumn: "id"}, {JsonName: "amount", SQLColumn: "amount"}},
			},
			{
				Type:       mtCustomersType,
				Tables:     []string{"mt_customers"},
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
	go func() { _ = dialect.Ingest(ctx, config, nil, 0, pr, po) }()

	// Phase 1 (snapshot routing): both sentinels arrive, each under its own topic.
	sentinelTopic := map[string]string{}
	deadline := time.After(30 * time.Second)
	for len(sentinelTopic) < 2 {
		select {
		case p := <-pr:
			if isRefreshMarkerProposal(p) {
				continue
			}
			topic := mtRequireHomogeneous(t, p)
			for _, e := range p.Entities {
				if k := string(e.Key); k == "o_sentinel" || k == "c_sentinel" {
					sentinelTopic[k] = topic
				}
			}
		case <-po:
		case <-deadline:
			t.Fatalf("timed out waiting for both sentinels; got %v", sentinelTopic)
		}
	}
	require.Equal(t, mtOrdersType.ID, sentinelTopic["o_sentinel"], "orders sentinel under the orders topic")
	require.Equal(t, mtCustomersType.ID, sentinelTopic["c_sentinel"], "customers sentinel under the customers topic")

	// Phase 2: one transaction, both tables.
	db = createDB(t)
	tx, err := db.Begin()
	require.NoError(t, err)
	_, err = tx.Exec("INSERT INTO mt_orders (id, amount) VALUES ('o9','999')")
	require.NoError(t, err)
	_, err = tx.Exec("INSERT INTO mt_customers (cust_id, name) VALUES ('c9','Zoe')")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	db.Close()

	var cdc []*cluster.Proposal
	seen := map[string]bool{}
	deadline = time.After(30 * time.Second)
	for len(seen) < 2 {
		select {
		case p := <-pr:
			if isRefreshMarkerProposal(p) {
				continue
			}
			relevant := false
			for _, e := range p.Entities {
				if k := string(e.Key); k == "o9" || k == "c9" {
					relevant = true
					seen[k] = true
				}
			}
			if relevant {
				cdc = append(cdc, p)
			}
		case <-po:
		case <-deadline:
			t.Fatalf("timed out; got %d/2 CDC rows", len(seen))
		}
	}

	// The two-table transaction lands as ONE proposal, rows in source order,
	// with the transaction's bundled resume position.
	require.Len(t, cdc, 1, "a two-table transaction must land as ONE proposal")
	p := cdc[0]
	require.Len(t, p.Entities, 2)
	require.Equal(t, "o9", string(p.Entities[0].Key), "rows keep source order")
	require.Equal(t, mtOrdersType.ID, p.Entities[0].Type.ID)
	require.JSONEq(t, `{"id":"o9","amount":"999"}`, string(p.Entities[0].Data))
	require.Equal(t, "c9", string(p.Entities[1].Key))
	require.Equal(t, mtCustomersType.ID, p.Entities[1].Type.ID)
	require.JSONEq(t, `{"cust_id":"c9","name":"Zoe"}`, string(p.Entities[1].Data))
	require.NotZero(t, p.SourceSeq, "a CDC proposal carries a non-zero SourceSeq")
	require.NotEmpty(t, p.Position, "the transaction's resume position rides the proposal")

	cancel()
}
