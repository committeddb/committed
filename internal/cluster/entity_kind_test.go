package cluster_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

func TestEntityKindRoundTripsThroughMarshal(t *testing.T) {
	in := &cluster.Type{
		ID:            "tenant-events",
		Name:          "TenantEvents",
		Version:       3,
		EntityKind:    cluster.EntityKindEvent,
		Discriminator: "$.event_type",
	}

	bs, err := in.Marshal()
	require.NoError(t, err)

	out := &cluster.Type{}
	require.NoError(t, out.Unmarshal(bs))
	require.Equal(t, cluster.EntityKindEvent, out.EntityKind)
	require.Equal(t, "$.event_type", out.Discriminator)
}

// Bytes written before the EntityKind field existed (proto3 omits zero
// fields, so a kindless marshal is byte-identical to a pre-kind one)
// unmarshal as EntityKindUnspecified — the grandfathered default.
func TestEntityKindPreKindBytesUnmarshalAsUnspecified(t *testing.T) {
	in := &cluster.Type{ID: "person", Name: "Person", Version: 1}
	bs, err := in.Marshal()
	require.NoError(t, err)

	out := &cluster.Type{}
	require.NoError(t, out.Unmarshal(bs))
	require.Equal(t, cluster.EntityKindUnspecified, out.EntityKind)
	require.Empty(t, out.Discriminator)
}

func TestParseEntityKind(t *testing.T) {
	for s, want := range map[string]cluster.EntityKind{
		"":           cluster.EntityKindUnspecified,
		"snapshot":   cluster.EntityKindSnapshot,
		"delta":      cluster.EntityKindDelta,
		"event":      cluster.EntityKindEvent,
		"command":    cluster.EntityKindCommand,
		"standalone": cluster.EntityKindStandalone,
		"revision":   cluster.EntityKindRevision,
	} {
		got, err := cluster.ParseEntityKind(s)
		require.NoError(t, err)
		require.Equal(t, want, got)
		if s != "" {
			require.Equal(t, s, got.String(), "String must invert ParseEntityKind")
		}
	}

	_, err := cluster.ParseEntityKind("Event")
	require.Error(t, err, "entity kinds are case-sensitive, like syncable modes")
	require.Contains(t, err.Error(), `unknown entity kind "Event"`)
}
