package sql_test

import (
	"context"
	"database/sql/driver"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/sql"
	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects/testdialects"
)

// TestMappingPathRunFlipsToConfigShaped pins the site-level ambiguity wiring
// for plain-syncable mapping paths: a syntactically-valid path that misses a
// run of consecutive distinct rows with no success (an operator typo wrong
// for the whole topic) flips from Permanent (dead-letter) to config-shaped
// (transient — the worker wedges) at the evidence threshold. The extraction
// failure happens before any SQL executes, so each failing Sync costs only a
// Begin/Rollback pair.
func TestMappingPathRunFlipsToConfigShaped(t *testing.T) {
	dialect, mock, err := testdialects.NewSQLMockDialect()
	require.Nil(t, err)
	db, err := sql.NewDB(dialect, "")
	require.Nil(t, err)
	defer db.Close()

	config := &sql.Config{
		Topic: "simple",
		Table: "events",
		Mappings: []sql.Mapping{
			{JsonPath: "$.key", Column: "pk", SQLType: "VARCHAR(64)"},
			{JsonPath: "$.customerId", Column: "customer", SQLType: "VARCHAR(64)"}, // typo: rows carry customer_id
		},
		PrimaryKey: []string{"pk"},
	}
	mock.ExpectExec(dialect.CreateDDL(config)).WillReturnResult(driver.ResultNoRows)
	mock.ExpectPrepare(dialect.CreateGenerationUpsertSQL(config))
	mock.ExpectPrepare(dialect.CreateDeleteSQL(config))
	mock.ExpectPrepare(dialect.CreateGenerationSweepSQL(config))

	syncable := sql.New(db, config)
	require.Nil(t, syncable.Init())

	for i := 1; i <= cluster.AmbiguityEvidenceThreshold; i++ {
		mock.ExpectBegin()
		mock.ExpectRollback()
		key := fmt.Sprintf("k%02d", i)
		_, err := syncable.Sync(context.Background(), &cluster.Actual{Index: uint64(i), Entities: []*cluster.Entity{
			cluster.NewUpsertEntity(simpleType, []byte(key), fmt.Appendf(nil, `{"key":%q,"customer_id":"c%d"}`, key, i)),
		}})
		if i < cluster.AmbiguityEvidenceThreshold {
			require.ErrorIs(t, err, cluster.ErrPermanent, "miss %d may still be entry-specific → dead-letter", i)
			require.NotErrorIs(t, err, cluster.ErrConfigShaped)
		} else {
			require.ErrorIs(t, err, cluster.ErrConfigShaped, "the threshold-th distinct row establishes the path config-shaped")
			require.NotErrorIs(t, err, cluster.ErrPermanent, "config-shaped must wedge, not dead-letter")
		}
	}
	require.Nil(t, mock.ExpectationsWereMet())
}
