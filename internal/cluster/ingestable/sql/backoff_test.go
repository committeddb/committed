package sql

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBackoff_DoublesToTheLimitAndResets(t *testing.T) {
	b := NewBackoff(time.Millisecond, 5*time.Millisecond)
	ctx := context.Background()

	require.Equal(t, time.Millisecond, b.Delay())
	require.NoError(t, b.Wait(ctx))
	require.Equal(t, 2*time.Millisecond, b.Delay(), "doubles after a wait")
	require.NoError(t, b.Wait(ctx))
	require.Equal(t, 4*time.Millisecond, b.Delay())
	require.NoError(t, b.Wait(ctx))
	require.Equal(t, 5*time.Millisecond, b.Delay(), "capped at the limit, not 8ms")
	require.NoError(t, b.Wait(ctx))
	require.Equal(t, 5*time.Millisecond, b.Delay(), "stays at the limit")

	b.Reset()
	require.Equal(t, time.Millisecond, b.Delay(), "a healthy session starts the climb over")
}

func TestBackoff_CancelledWaitReturnsCtxErrAndKeepsTheDelay(t *testing.T) {
	b := NewBackoff(time.Hour, 2*time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, b.Wait(ctx), context.Canceled)
	require.Equal(t, time.Hour, b.Delay(), "a cancelled wait is not a failure; the delay is unchanged")
}

func TestNewReconnectBackoff_SharedBounds(t *testing.T) {
	b := NewReconnectBackoff()
	require.Equal(t, ReconnectBackoffMin, b.Delay())
	require.Equal(t, time.Second, ReconnectBackoffMin)
	require.Equal(t, 30*time.Second, ReconnectBackoffMax)
}
