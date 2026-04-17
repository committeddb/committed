package db_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/philborlin/committed/internal/cluster/db"
)

func TestLeaderState(t *testing.T) {
	l := &db.LeaderState{}

	l.SetLeader(true)
	require.Equal(t, true, l.IsLeader())

	// Tests that calling SetLeader twice with the same value doesn't cause mutex deadlock
	l.SetLeader(true)
	require.Equal(t, true, l.IsLeader())

	l.SetLeader(false)
	require.Equal(t, false, l.IsLeader())

	// Tests that calling SetLeader twice with the same value doesn't cause mutex deadlock
	l.SetLeader(false)
	require.Equal(t, false, l.IsLeader())
}
