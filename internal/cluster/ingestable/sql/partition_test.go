package sql_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
)

// TestPartitionByTopic covers the flush-partition helper: a mixed-table
// transaction is split into one homogeneous group per topic, in first-appearance
// order, so each topic can be emitted as its own proposal with a strictly-increasing
// SourceSeq. Determinism (stable order for both groups and members) is what keeps a
// resume's per-topic SourceSeqs identical to the original session's.
func TestPartitionByTopic(t *testing.T) {
	ent := func(topic, key string) *cluster.Entity {
		return &cluster.Entity{Type: &cluster.Type{ID: topic}, Key: []byte(key)}
	}
	topics := func(groups [][]*cluster.Entity) []string {
		out := make([]string, len(groups))
		for i, g := range groups {
			out[i] = g[0].Type.ID
		}
		return out
	}
	keys := func(group []*cluster.Entity) []string {
		out := make([]string, len(group))
		for i, e := range group {
			out[i] = string(e.Key)
		}
		return out
	}

	t.Run("empty input yields no groups", func(t *testing.T) {
		require.Empty(t, sql.PartitionByTopic(nil))
	})

	t.Run("single topic yields one group in input order (byte-compat flush)", func(t *testing.T) {
		in := []*cluster.Entity{ent("orders", "1"), ent("orders", "2"), ent("orders", "3")}
		groups := sql.PartitionByTopic(in)
		require.Len(t, groups, 1)
		require.Equal(t, []string{"1", "2", "3"}, keys(groups[0]))
	})

	t.Run("interleaved topics group by type, preserving first-appearance order", func(t *testing.T) {
		in := []*cluster.Entity{
			ent("orders", "o1"),
			ent("customers", "c1"),
			ent("orders", "o2"),
			ent("customers", "c2"),
			ent("orders", "o3"),
		}
		groups := sql.PartitionByTopic(in)
		require.Len(t, groups, 2)
		// orders appeared first, then customers.
		require.Equal(t, []string{"orders", "customers"}, topics(groups))
		require.Equal(t, []string{"o1", "o2", "o3"}, keys(groups[0]))
		require.Equal(t, []string{"c1", "c2"}, keys(groups[1]))
	})

	t.Run("group order follows first appearance, not lexical order", func(t *testing.T) {
		in := []*cluster.Entity{ent("zeta", "z"), ent("alpha", "a")}
		require.Equal(t, []string{"zeta", "alpha"}, topics(sql.PartitionByTopic(in)))
	})

	t.Run("a nil Type groups under the empty id rather than panicking", func(t *testing.T) {
		in := []*cluster.Entity{{Key: []byte("k")}, ent("orders", "o")}
		groups := sql.PartitionByTopic(in)
		require.Len(t, groups, 2)
		require.Empty(t, groups[0][0].Type)
		require.Equal(t, "orders", groups[1][0].Type.ID)
	})
}
