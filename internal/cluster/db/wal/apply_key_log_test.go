package wal_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// TestApplyCommitted_DoesNotLogRawEntityKey is the PII-in-logs regression: the
// apply path logged the raw entity Key at Debug on every applied entity —
// including RTBF delete keys, which are the exact erasure subject — so anywhere
// Debug is enabled leaked the erasure target into the logs. The apply log must
// carry no raw Key at any level, only non-PII correlation (type + index).
func TestApplyCommitted_DoesNotLogRawEntityKey(t *testing.T) {
	core, logs := observer.New(zap.DebugLevel)
	s, err := wal.Open(t.TempDir(), nil, nil, nil, wal.WithoutFsync(), wal.WithLogger(zap.New(core)))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	// Register a user type, then apply an entity whose Key stands in for an RTBF
	// subject — exactly the shape a user upsert/delete key has.
	typeEntity, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: "person", Name: "person", Version: 1})
	require.NoError(t, err)
	saveEntity(t, typeEntity, s, 1, 1)

	const subjectKey = "erasure-subject@example.com"
	userEntity := cluster.NewUpsertEntity(&cluster.Type{ID: "person", Version: 1}, []byte(subjectKey), []byte(`{"x":1}`))
	saveEntity(t, userEntity, s, 1, 2)

	applyLogs := logs.FilterMessage("applying entity").All()
	require.NotEmpty(t, applyLogs, "the apply path logs 'applying entity' at Debug")

	// No field, at any level, may carry the raw entity Key.
	for _, e := range applyLogs {
		for _, f := range e.Context {
			require.NotContains(t, f.String, subjectKey, "field %q must not carry the raw entity Key", f.Key)
		}
	}

	// Non-PII correlation survives: the user entity's apply is logged with its
	// typeID and raft index.
	var sawPerson bool
	for _, e := range applyLogs {
		m := e.ContextMap()
		if m["typeID"] == "person" {
			sawPerson = true
			require.Equal(t, uint64(2), m["index"], "index correlation is preserved")
		}
	}
	require.True(t, sawPerson, "the user entity's apply is logged with its typeID")
}
