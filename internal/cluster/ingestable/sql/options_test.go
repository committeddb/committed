package sql_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	sql "github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/sqlfakes"
)

// allOptionsDialect reads every option, so the value rules can be pinned
// apart from the per-dialect applicability rule (tolerance_test.go pins
// that one against the real Postgres dialect).
type allOptionsDialect struct{ stubDialect }

func (allOptionsDialect) OptionKeys() []string {
	return []string{sql.OptionSlotName, sql.OptionPublication, sql.OptionPollInterval, sql.OptionBatchSize, sql.OptionSnapshotReaders}
}

func parseWithOptions(t *testing.T, options string) (*sql.Config, error) {
	t.Helper()
	doc := `
[sql]
dialect          = "stub"
topic            = "simple"
connectionString = "postgres://user:pass@localhost:5432/db"
primaryKey       = "pk"
tables           = ["t"]
[[sql.mappings]]
jsonName = "pk"
column   = "pk"
[sql.options]
` + options
	v := readConfig(t, "toml", bytes.NewReader([]byte(doc)))
	tiper := &sqlfakes.FakeTyper{}
	tiper.ResolveTypeReturns(simpleType, nil)
	p := sql.NewIngestableParser(tiper)
	p.Dialects["stub"] = allOptionsDialect{}
	config, _, err := p.ParseConfig(v)
	return config, err
}

// Every option decodes typed; the values are validated at admission rather
// than defaulted silently at first use.
func TestOptions_TypedAndValidated(t *testing.T) {
	config, err := parseWithOptions(t, "slotName = \"s\"\npublication = \"p\"\npollInterval = \"10s\"\nbatchSize = 250\nsnapshotReaders = 4\n")
	require.NoError(t, err)
	require.Equal(t, sql.Options{SlotName: "s", Publication: "p", PollInterval: 10 * time.Second, BatchSize: 250, SnapshotReaders: 4}, config.Options)

	config, err = parseWithOptions(t, "")
	require.NoError(t, err)
	require.Equal(t, sql.Options{}, config.Options, "unset options are zero; the accessors default them")

	for _, tc := range []struct{ options, field, issue string }{
		{"pollInterval = \"-1s\"\n", "sql.options.pollInterval", "positive Go duration"},
		{"pollInterval = \"soon\"\n", "sql.options.pollInterval", "positive Go duration"},
		{"pollInterval = 3\n", "sql.options", ""},
		{"batchSize = -42\n", "sql.options.batchSize", "positive integer"},
		{"snapshotReaders = 0\n", "sql.options.snapshotReaders", "between 1 and"},
		{"snapshotReaders = 17\n", "sql.options.snapshotReaders", "between 1 and"},
		{"readers = 2\n", "sql.options.readers", "unknown key"},
		{"snapshot_readers = 2\n", "sql.options.snapshot_readers", `spell it "snapshotReaders"`},
	} {
		t.Run(tc.options, func(t *testing.T) {
			_, err := parseWithOptions(t, tc.options)
			require.Error(t, err)
			require.Equal(t, tc.field, cluster.NewConfigError(err).Field)
			if tc.issue != "" {
				require.Contains(t, err.Error(), tc.issue)
			}
		})
	}
}

// A dialect declaring no options refuses every one it is handed: the
// accepted-and-ignored knob is the class this closes.
func TestOptions_RefusedWhenTheDialectDoesNotReadThem(t *testing.T) {
	doc := `
[sql]
dialect          = "stub"
topic            = "simple"
connectionString = "postgres://user:pass@localhost:5432/db"
primaryKey       = "pk"
tables           = ["t"]
[[sql.mappings]]
jsonName = "pk"
column   = "pk"
[sql.options]
batchSize = 10
`
	v := readConfig(t, "toml", bytes.NewReader([]byte(doc)))
	tiper := &sqlfakes.FakeTyper{}
	tiper.ResolveTypeReturns(simpleType, nil)
	p := sql.NewIngestableParser(tiper)
	p.Dialects["stub"] = stubDialect{}
	_, _, err := p.ParseConfig(v)
	require.Error(t, err)
	require.Equal(t, "sql.options.batchSize", cluster.NewConfigError(err).Field)
	require.Contains(t, err.Error(), "not read by the stub dialect")
}
