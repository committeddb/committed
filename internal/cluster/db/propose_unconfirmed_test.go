package db_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
)

// TestProposalUnconfirmedErrorsWrapClusterSentinel: db.ErrProposalUnknown and
// ErrProposalLost both satisfy errors.Is(cluster.ErrProposalUnconfirmed), so a
// caller across the cluster.Cluster boundary (the HTTP layer) can classify a
// retryable propose outcome as 503 request_unconfirmed without importing db — and
// an unrelated propose error still is not classified as unconfirmed.
func TestProposalUnconfirmedErrorsWrapClusterSentinel(t *testing.T) {
	require.ErrorIs(t, db.ErrProposalUnknown, cluster.ErrProposalUnconfirmed)
	require.ErrorIs(t, db.ErrProposalLost, cluster.ErrProposalUnconfirmed)
	require.NotErrorIs(t, cluster.ErrProposalTooLarge, cluster.ErrProposalUnconfirmed)
}
