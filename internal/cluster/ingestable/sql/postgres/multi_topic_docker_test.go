//go:build docker || integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/postgres"
)

// mtOrdersType / mtCustomersType are the two distinct topics a single multi-topic
// ingestable produces from two source tables.
var (
	mtOrdersType    = &cluster.Type{ID: "mt_orders_topic", Name: "orders"}
	mtCustomersType = &cluster.Type{ID: "mt_customers_topic", Name: "customers"}
)

// multiTopicConfig builds a two-topic ingestable config over one connection/slot:
// mt_orders → orders topic (keyed by id), mt_customers → customers topic (keyed by
// cust_id).
func multiTopicConfig(slot, pub string) *sql.Config {
	return &sql.Config{
		ConnectionString: connString,
		Tables:           []string{"mt_orders", "mt_customers"},
		Options:          sql.Options{SlotName: slot, Publication: pub},
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
}

// createMultiTopicTables (re)creates the two source tables, optionally seeding the
// given rows for the snapshot phase.
func createMultiTopicTables(t *testing.T, seed bool) {
	t.Helper()
	db := createDB(t)
	defer db.Close()
	stmts := []string{
		`DROP TABLE IF EXISTS mt_orders`,
		`DROP TABLE IF EXISTS mt_customers`,
		`CREATE TABLE mt_orders (id VARCHAR(32) PRIMARY KEY, amount TEXT)`,
		`CREATE TABLE mt_customers (cust_id VARCHAR(32) PRIMARY KEY, name TEXT)`,
	}
	if seed {
		stmts = append(stmts,
			`INSERT INTO mt_orders (id, amount) VALUES ('o1','100'),('o2','200')`,
			`INSERT INTO mt_customers (cust_id, name) VALUES ('c1','Alice'),('c2','Bob')`,
		)
	}
	for _, s := range stmts {
		_, err := db.Exec(s)
		require.NoError(t, err)
	}
}

func keyMap(es []*cluster.Entity) map[string]*cluster.Entity {
	m := make(map[string]*cluster.Entity, len(es))
	for _, e := range es {
		m[string(e.Key)] = e
	}
	return m
}

// requireHomogeneous asserts every entity in a proposal shares one topic and returns
// that topic id — the invariant PartitionByTopic guarantees (one proposal per topic).
func requireHomogeneous(t *testing.T, p *cluster.Proposal) string {
	t.Helper()
	require.NotEmpty(t, p.Entities)
	topic := p.Entities[0].Type.ID
	for _, e := range p.Entities {
		require.Equal(t, topic, e.Type.ID, "a proposal must not mix topics")
	}
	return topic
}

// TestPostgresMultiTopicSnapshotRoutesToOwnTopics: one ingestable, one slot, two
// tables → two distinct topics. Each table's snapshot rows land under its own topic
// (its spec's Type), correctly keyed and mapped, and the ingestable holds exactly
// one replication slot.
func TestPostgresMultiTopicSnapshotRoutesToOwnTopics(t *testing.T) {
	createMultiTopicTables(t, true)

	slot, pub := "slot_mt_snap", "pub_mt_snap"
	cleanReplication(t, slot, pub)
	// Free the slot at test END too: the shared container caps max_replication_slots
	// (16), so a leaked slot starves later tests in the package.
	t.Cleanup(func() { cleanReplication(t, slot, pub) })

	dialect := &postgres.PostgreSQLDialect{}
	config := multiTopicConfig(slot, pub)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pr := make(chan *cluster.Proposal, 32)
	po := make(chan cluster.Position, 32)
	ingestErr := make(chan error, 1)
	go func() { ingestErr <- dialect.Ingest(ctx, config, nil, 0, pr, po) }()

	// Collect the four snapshot rows (two per topic).
	byTopic := map[string][]*cluster.Entity{}
	total := 0
	deadline := time.After(30 * time.Second)
	for total < 4 {
		select {
		case p := <-pr:
			if isRefreshMarkerProposal(p) {
				continue
			}
			topic := requireHomogeneous(t, p)
			for _, e := range p.Entities {
				byTopic[topic] = append(byTopic[topic], e)
				total++
			}
		case <-po:
		case <-deadline:
			t.Fatalf("timed out; got %d/4 snapshot rows", total)
		}
	}

	orders := keyMap(byTopic[mtOrdersType.ID])
	require.Len(t, orders, 2, "both orders rows land under the orders topic")
	require.JSONEq(t, `{"id":"o1","amount":"100"}`, string(orders["o1"].Data))
	require.Equal(t, mtOrdersType.ID, orders["o1"].Type.ID)

	custs := keyMap(byTopic[mtCustomersType.ID])
	require.Len(t, custs, 2, "both customers rows land under the customers topic")
	require.JSONEq(t, `{"cust_id":"c1","name":"Alice"}`, string(custs["c1"].Data))
	require.Equal(t, mtCustomersType.ID, custs["c1"].Type.ID)

	// The whole multi-topic ingestable holds exactly ONE replication slot.
	db := createDB(t)
	var slotCount int
	require.NoError(t, db.QueryRow(
		`SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1`, slot).Scan(&slotCount))
	db.Close()
	require.Equal(t, 1, slotCount, "a multi-topic ingestable must hold exactly one slot")

	cancel()
	select {
	case err := <-ingestErr:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Ingest did not exit after cancel")
	}
}

// TestPostgresMultiTopicTransactionIsOneProposal: a source transaction touching
// BOTH tables lands as ONE proposal holding both topics' rows in source order,
// with the transaction's bundled resume position — so it commits as a unit and
// no reader ever sees half of it.
func TestPostgresMultiTopicTransactionIsOneProposal(t *testing.T) {
	createMultiTopicTables(t, false) // no seed — this test exercises the CDC path

	slot, pub := "slot_mt_cdc", "pub_mt_cdc"
	cleanReplication(t, slot, pub)
	// Free the slot at test END too (see the snapshot test) so it doesn't starve
	// later tests of the capped max_replication_slots budget.
	t.Cleanup(func() { cleanReplication(t, slot, pub) })

	dialect := &postgres.PostgreSQLDialect{}
	config := multiTopicConfig(slot, pub)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pr := make(chan *cluster.Proposal, 32)
	po := make(chan cluster.Position, 32)
	ingestErr := make(chan error, 1)
	go func() { ingestErr <- dialect.Ingest(ctx, config, nil, 0, pr, po) }()

	// Wait until the dialect is streaming so the transaction below is captured by
	// pgoutput (not the snapshot) — a change captured by both would double-emit.
	waitForSlot(t, slot)

	// One transaction, both tables.
	db := createDB(t)
	tx, err := db.Begin()
	require.NoError(t, err)
	_, err = tx.Exec(`INSERT INTO mt_orders (id, amount) VALUES ('o9','999')`)
	require.NoError(t, err)
	_, err = tx.Exec(`INSERT INTO mt_customers (cust_id, name) VALUES ('c9','Zoe')`)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	db.Close()

	// Collect the CDC proposals carrying the transaction's rows.
	var cdc []*cluster.Proposal
	seen := map[string]bool{}
	deadline := time.After(30 * time.Second)
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
	select {
	case err := <-ingestErr:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Ingest did not exit after cancel")
	}
}
