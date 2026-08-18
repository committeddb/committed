package sql_test

import (
	"context"
	"fmt"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/committeddb/committed/internal/cluster"
)

// A long consecutive run of rule-unmatched events warns ONCE with the
// probable cause — the field case was a type-mismatched equals
// (`equals = "true"` vs JSON boolean) silently matching 0 of 248,854
// rows. A match mid-run resets the counter, so healthy topics with
// occasional foreign variants never trip it.
func TestProjectionWarnsOnLongUnmatchedRun(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	prev := zap.L()
	zap.ReplaceGlobals(zap.New(core))
	defer zap.ReplaceGlobals(prev)

	config := tenantProjectionConfig()
	projection, mock, _, _ := newMockProjection(t, config, nil)

	// 1000 consecutive events matching no rule (event_type nothing binds to).
	for i := 0; i < 1000; i++ {
		mock.ExpectBegin()
		mock.ExpectCommit()
		_, err := projection.Sync(context.Background(), &cluster.Actual{Entities: []*cluster.Entity{
			cluster.NewUpsertEntity(tenantEventType, []byte(fmt.Sprintf("k%d", i)),
				eventJSON(t, map[string]any{"tenant_id": "t", "event_type": "tenant.UNMATCHED"})),
		}})
		require.NoError(t, err)
	}

	warns := logs.FilterMessageSnippet("matched no rules for a long run").All()
	require.Len(t, warns, 1, "the long-run warning fires exactly once at the threshold")
	require.Equal(t, "controlplane-event", warns[0].ContextMap()["topic"])
}

// The reset: a single match inside the run keeps the warning silent.
func TestProjectionUnmatchedRunResetsOnMatch(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	prev := zap.L()
	zap.ReplaceGlobals(zap.New(core))
	defer zap.ReplaceGlobals(prev)

	config := tenantProjectionConfig()
	projection, mock, rules, _ := newMockProjection(t, config, nil)

	sync := func(eventType string, expectExec bool) {
		mock.ExpectBegin()
		if expectExec {
			rules[0].ExpectExec().WillReturnResult(sqlmock.NewResult(1, 1))
		}
		mock.ExpectCommit()
		_, err := projection.Sync(context.Background(), &cluster.Actual{Entities: []*cluster.Entity{
			cluster.NewUpsertEntity(tenantEventType, []byte("k"),
				eventJSON(t, map[string]any{"tenant_id": "t", "event_type": eventType, "tier": "dev"})),
		}})
		require.NoError(t, err)
	}

	for i := 0; i < 999; i++ {
		sync("tenant.UNMATCHED", false)
	}
	sync("tenant.created", true) // resets the run at 999
	for i := 0; i < 999; i++ {
		sync("tenant.UNMATCHED", false)
	}

	require.Empty(t, logs.FilterMessageSnippet("matched no rules for a long run").All(),
		"a match mid-run must reset the counter — no warning")
}
