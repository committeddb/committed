package iceberg_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/iceberg"
)

// An iceberg syncable batches by its own flushRows / flushInterval, so the
// envelope's checkpoint cadence has nothing to apply to. Accepting and
// ignoring it is the class the closed vocabulary exists to kill (the README
// told operators to raise checkpointEvery on kinds that never read it), so it
// is refused with the kind's own knob named.
func TestIceberg_RefusesCheckpointCadence(t *testing.T) {
	for _, key := range []string{"checkpointEvery = 100", "checkpointMaxAge = \"1s\""} {
		doc := "[syncable]\nname = \"i\"\ntype = \"iceberg\"\n" + key + "\n\n[iceberg]\ntopic = \"a\"\ncatalog = \"http://c\"\nwarehouse = \"w\"\nnamespace = \"n\"\ntable = \"t\"\n"
		v, err := cluster.ParseConfigBytes("text/toml", []byte(doc))
		require.NoError(t, err)

		_, err = (&iceberg.SyncableParser{}).ParseConfig(v)
		require.Error(t, err, key)
		var fe *cluster.FieldError
		require.ErrorAs(t, err, &fe)
		require.Contains(t, fe.Issue, "flushRows", "the refusal must name the knob that does apply")
	}
}
