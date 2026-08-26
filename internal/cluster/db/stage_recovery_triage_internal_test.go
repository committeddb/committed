package db

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// existsStub overrides only the one Storage method the triage touches;
// everything else panics via the nil embedded interface (dbfakes would
// be an import cycle from inside package db).
type existsStub struct {
	Storage
	exists bool
	err    error
}

func (s *existsStub) SyncableExists(string) (bool, error) { return s.exists, s.err }

// stageRecoveryFailed decides whether a worker whose stage-state recovery
// failed should exit or back off and retry. The field zombie: a DELETED
// syncable's worker hot-looped "database not open" recovery attempts
// forever — deletion must end the loop no matter how the worker reached
// the failure (abandoned drain, racing rebuild, any path).
func TestStageRecoveryFailedTriage(t *testing.T) {
	st := &existsStub{}
	d := &DB{storage: st, logger: zap.NewNop()}

	// A deleted syncable exits the worker.
	st.exists, st.err = false, nil
	require.True(t, d.stageRecoveryFailed(context.Background(), "gone", errors.New("database not open")),
		"a deleted syncable's worker must exit, not retry")

	// A live syncable retries (with the loop's idle backoff).
	st.exists, st.err = true, nil
	require.False(t, d.stageRecoveryFailed(context.Background(), "alive", errors.New("transient")))

	// An existence-check error is indistinguishable from live: retry (the
	// backoff paces it; a genuinely deleted id resolves on a later pass).
	st.exists, st.err = false, errors.New("storage briefly unavailable")
	require.False(t, d.stageRecoveryFailed(context.Background(), "unknown", errors.New("x")))

	// A canceled context (delete/replace/Close interrupted the scan) exits.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.True(t, d.stageRecoveryFailed(ctx, "any", errors.New("interrupted")))
}
