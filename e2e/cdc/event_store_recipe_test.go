//go:build docker

package cdc_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/pelletier/go-toml/v2"
	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/e2e/cdc/harness"
)

// The event-store recipe (docs/recipes/event-store-read-models.md), run as
// published on every SQL engine it names. Every config comes from the
// recipe's own fenced blocks; the test changes only what the recipe says
// changes per engine: the dialect, the connection strings, jsonColumns for a
// text payload, and the Postgres-only [sql.options].

func TestEventStoreRecipe_Postgres(t *testing.T) {
	runEventStoreRecipe(t, harness.EventStoreVariant{Engine: "postgres", PayloadType: "jsonb"})
}

func TestEventStoreRecipe_MySQLJSON(t *testing.T) {
	runEventStoreRecipe(t, harness.EventStoreVariant{Engine: "mysql", PayloadType: "JSON"})
}

func TestEventStoreRecipe_MySQLText(t *testing.T) {
	runEventStoreRecipe(t, harness.EventStoreVariant{Engine: "mysql", PayloadType: "TEXT"})
}

func TestEventStoreRecipe_SQLServer(t *testing.T) {
	runEventStoreRecipe(t, harness.EventStoreVariant{Engine: "sqlserver", PayloadType: "NVARCHAR(MAX)"})
}

const (
	streamA = "11111111-1111-1111-1111-111111111111"
	streamG = "22222222-2222-2222-2222-222222222222"
	wait    = 90 * time.Second
)

func row(id, customer, status, items, total, version string) string {
	return strings.Join([]string{id, customer, status, items, total, version}, " | ")
}

func runEventStoreRecipe(t *testing.T, v harness.EventStoreVariant) {
	e := harness.NewEventStore(t, v)
	cfg := recipeConfigs(t)

	// The recipe's example events: half are in the table before the
	// ingestable exists (the snapshot), half arrive after (the change stream).
	placedA := harness.Event{Stream: streamA, Version: 1, Type: "order_placed", Data: `{"customer": "ada"}`}
	e.Append(t,
		placedA,
		harness.Event{Stream: streamA, Version: 2, Type: "item_added", Data: `{"sku": "pen", "qty": 2, "price": 1.50}`},
		harness.Event{Stream: streamG, Version: 1, Type: "order_placed", Data: `{"customer": "grace"}`},
	)

	e.Post(t, "/v1/type/order-events", cfg.typ)
	ingestable := cfg.ingestable(t, e, v)
	e.Post(t, "/v1/ingestable/order-events", ingestable)
	e.WaitStreaming(t, "order-events", wait)
	e.Post(t, "/v1/database/read", cfg.database(t, e))
	e.Post(t, "/v1/syncable/orders", cfg.orders)
	compact := cfg.compactOrders(t)
	e.Post(t, "/v1/syncable/orders_compact", compact)

	e.Append(t,
		harness.Event{Stream: streamA, Version: 3, Type: "item_added", Data: `{"sku": "ink", "qty": 1, "price": 4.25}`},
		harness.Event{Stream: streamG, Version: 2, Type: "item_added", Data: `{"sku": "pad", "qty": 3, "price": 2.00}`},
		harness.Event{Stream: streamA, Version: 4, Type: "order_shipped", Data: `{}`},
	)

	// The recipe's output table, exactly — from both projections.
	e.WaitRows(t, "orders", cfg.exampleOutput, wait)
	e.WaitRows(t, "orders_compact", cfg.exampleOutput, wait)

	// Step 4's status call.
	waitSyncableStatus(t, e, "orders")

	// Step 3's dry-run.
	code, body := e.Do(t, http.MethodPost, "/v1/syncable/dryrun", "text/toml", cfg.orders)
	require.Equal(t, http.StatusOK, code, body)
	require.Contains(t, body, `"order"`, "the report covers the recipe's stage")

	// A masking correction and a deleted event recompute their streams.
	e.SetData(t, streamA, 1, `{"customer": "***"}`)
	e.DeleteEvent(t, streamG, 2)
	afterEdits := []string{
		row(streamA, "***", "order_shipped", "3", "7.25", "4"),
		row(streamG, "grace", "order_placed", "0", "0.00", "1"),
	}
	e.WaitRows(t, "orders", afterEdits, wait)
	e.WaitRows(t, "orders_compact", afterEdits, wait)

	// A fresh snapshot gives what the change stream built: recreate the
	// ingestable (a full re-snapshot), then append one event so its arrival
	// proves the snapshot's rows were applied before it.
	e.Delete(t, "/v1/ingestable/order-events")
	e.Post(t, "/v1/ingestable/order-events", ingestable)
	e.WaitStreaming(t, "order-events", wait)
	e.Append(t, harness.Event{Stream: streamG, Version: 3, Type: "item_added", Data: `{"sku": "pad", "qty": 1, "price": 2.00}`})
	afterResnapshot := []string{
		row(streamA, "***", "order_shipped", "3", "7.25", "4"),
		row(streamG, "grace", "order_placed", "1", "2.00", "3"),
	}
	e.WaitRows(t, "orders", afterResnapshot, wait)
	e.WaitRows(t, "orders_compact", afterResnapshot, wait)

	// Stream compaction: the earlier events go and one snapshot event takes
	// their place, in one transaction. With the recipe's compaction emits the
	// row is unchanged; the ordinary emits see no known event and blank it.
	e.Compact(t, streamA, 4, harness.Event{
		Stream: streamA, Version: 4, Type: "order_snapshot",
		Data: `{"customer": "***", "status": "order_shipped", "items": 3, "total": 7.25}`,
	})
	e.WaitRows(t, "orders_compact", afterResnapshot, wait)
	e.WaitRows(t, "orders", []string{
		row(streamA, "", "", "0", "0.00", "4"),
		row(streamG, "grace", "order_placed", "1", "2.00", "3"),
	}, wait)

	// A later event adds to the snapshot's totals.
	e.Append(t, harness.Event{Stream: streamA, Version: 5, Type: "item_added", Data: `{"sku": "pad", "qty": 1, "price": 2.00}`})
	e.WaitRows(t, "orders_compact", []string{
		row(streamA, "***", "order_shipped", "4", "9.25", "5"),
		row(streamG, "grace", "order_placed", "1", "2.00", "3"),
	}, wait)

	// Deleting every event of a stream removes its row.
	e.DeleteStream(t, streamG)
	e.WaitRows(t, "orders_compact", []string{row(streamA, "***", "order_shipped", "4", "9.25", "5")}, wait)
	e.WaitRows(t, "orders", []string{row(streamA, "", "", "1", "2.00", "5")}, wait)

	// The erasure section's type declaration is accepted.
	e.Post(t, "/v1/type/order-events", cfg.typeOnSupersede)
}

// waitSyncableStatus waits for the status step 4 of the recipe shows.
func waitSyncableStatus(t *testing.T, e *harness.EventStore, id string) {
	t.Helper()
	var st struct {
		Stuck       bool    `json:"stuck"`
		WorkerState string  `json:"workerState"`
		Lag         *uint64 `json:"lag"`
		CaughtUp    bool    `json:"caughtUp"`
		DeadLetters *int    `json:"deadLetters"`
	}
	require.Eventually(t, func() bool {
		code, body := e.Do(t, http.MethodGet, "/v1/syncable/"+id+"/status", "", "")
		if code != http.StatusOK || json.Unmarshal([]byte(body), &st) != nil {
			return false
		}
		return st.CaughtUp
	}, wait, 100*time.Millisecond)
	require.False(t, st.Stuck)
	require.Equal(t, "running", st.WorkerState)
	require.NotNil(t, st.Lag)
	require.Zero(t, *st.Lag)
	require.NotNil(t, st.DeadLetters, "the recipe shows deadLetters")
	require.Zero(t, *st.DeadLetters)
}

type eventStoreRecipe struct {
	typ, typeOnSupersede, ingestableTOML, databaseTOML, orders string
	compactEmit                                                string
	exampleOutput                                              []string
}

func recipeConfigs(t *testing.T) eventStoreRecipe {
	t.Helper()
	var r eventStoreRecipe
	for _, b := range harness.RecipeBlocks(t, "toml") {
		switch {
		case strings.HasPrefix(b, "# POST /v1/type/order-events") && strings.Contains(b, "onSupersede"):
			r.typeOnSupersede = b
		case strings.HasPrefix(b, "# POST /v1/type/order-events"):
			r.typ = b
		case strings.HasPrefix(b, "# POST /v1/ingestable/order-events"):
			r.ingestableTOML = b
		case strings.HasPrefix(b, "# POST /v1/database/read"):
			r.databaseTOML = b
		case strings.HasPrefix(b, "# POST /v1/syncable/orders"):
			r.orders = b
		case strings.HasPrefix(b, "emit = [") && strings.Contains(b, "order_snapshot"):
			r.compactEmit = b
		}
	}
	for _, b := range harness.RecipeBlocks(t, "") {
		if !strings.Contains(b, "order_id") || !strings.Contains(b, "customer") {
			continue
		}
		for _, line := range strings.Split(b, "\n") {
			cols := strings.Split(line, "|")
			if len(cols) != 6 || strings.Contains(line, "order_id") {
				continue
			}
			for i := range cols {
				cols[i] = strings.TrimSpace(cols[i])
			}
			r.exampleOutput = append(r.exampleOutput, strings.Join(cols, " | "))
		}
	}
	require.NotEmpty(t, r.typ, "type block")
	require.NotEmpty(t, r.typeOnSupersede, "erasure type block")
	require.NotEmpty(t, r.ingestableTOML, "ingestable block")
	require.NotEmpty(t, r.databaseTOML, "database block")
	require.NotEmpty(t, r.orders, "projection block")
	require.NotEmpty(t, r.compactEmit, "compaction emits")
	require.Len(t, r.exampleOutput, 2, "the example output table")
	return r
}

func decodeTOML(t *testing.T, s string) map[string]any {
	t.Helper()
	m := map[string]any{}
	require.NoError(t, toml.Unmarshal([]byte(s), &m))
	return m
}

func encodeTOML(t *testing.T, m map[string]any) string {
	t.Helper()
	b, err := toml.Marshal(m)
	require.NoError(t, err)
	return string(b)
}

// ingestable is the recipe's ingestable for the variant: the dialect and
// connection string change; [sql.options] (slot and publication) is
// Postgres's; a text payload column is listed under jsonColumns.
func (r eventStoreRecipe) ingestable(t *testing.T, e *harness.EventStore, v harness.EventStoreVariant) string {
	m := decodeTOML(t, r.ingestableTOML)
	sql := m["sql"].(map[string]any)
	sql["dialect"] = v.Engine
	sql["connectionString"] = e.SrcURL
	if v.Engine != "postgres" {
		delete(sql, "options")
	}
	if v.PayloadType == "TEXT" || strings.HasPrefix(v.PayloadType, "NVARCHAR") {
		sql["jsonColumns"] = []string{"data"}
	}
	return encodeTOML(t, m)
}

func (r eventStoreRecipe) database(t *testing.T, e *harness.EventStore) string {
	m := decodeTOML(t, r.databaseTOML)
	m["sql"].(map[string]any)["connectionString"] = e.ReadURL
	return encodeTOML(t, m)
}

// compactOrders is the recipe's projection with its compaction emits, as a
// second projection into its own table.
func (r eventStoreRecipe) compactOrders(t *testing.T) string {
	m := decodeTOML(t, r.orders)
	m["syncable"].(map[string]any)["name"] = "orders_compact"
	m["projection"].(map[string]any)["table"] = "orders_compact"
	stage := m["projection"].(map[string]any)["stages"].([]any)[0].(map[string]any)
	stage["emit"] = decodeTOML(t, r.compactEmit)["emit"]
	return encodeTOML(t, m)
}
