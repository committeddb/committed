package iceberg

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

func parse(t *testing.T, toml string) *cluster.ParsedConfig {
	t.Helper()
	v, err := cluster.ParseConfigBytes("text/toml", []byte(toml))
	require.NoError(t, err)
	return v
}

func TestParseConfig_Validation(t *testing.T) {
	p := &SyncableParser{}

	cases := []struct {
		name string
		toml string
		want string
	}{
		{"missing topic", "[iceberg]\ncatalog = \"http://c:8181\"\nnamespace = \"n\"\ntable = \"t\"\n", "iceberg.topic"},
		{"missing catalog", "[iceberg]\ntopic = \"a\"\nnamespace = \"n\"\ntable = \"t\"\n", "iceberg.catalog"},
		{"non-http catalog", "[iceberg]\ntopic = \"a\"\ncatalog = \"s3://bucket\"\nnamespace = \"n\"\ntable = \"t\"\n", "http(s) REST catalog"},
		{
			"credentials in catalog URI",
			"[iceberg]\ntopic = \"a\"\ncatalog = \"http://user:secret@c:8181\"\nnamespace = \"n\"\ntable = \"t\"\n",
			"must not carry credentials",
		},
		{"missing namespace", "[iceberg]\ntopic = \"a\"\ncatalog = \"http://c:8181\"\ntable = \"t\"\n", "iceberg.namespace"},
		{"missing table", "[iceberg]\ntopic = \"a\"\ncatalog = \"http://c:8181\"\nnamespace = \"n\"\n", "iceberg.table"},
		{
			"bad flushRows",
			"[iceberg]\ntopic = \"a\"\ncatalog = \"http://c:8181\"\nnamespace = \"n\"\ntable = \"t\"\nflushRows = -1\n",
			"positive integer",
		},
		{
			"bad flushInterval",
			"[iceberg]\ntopic = \"a\"\ncatalog = \"http://c:8181\"\nnamespace = \"n\"\ntable = \"t\"\nflushInterval = \"soon\"\n",
			"positive Go duration",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := p.ParseConfig(parse(t, tc.toml))
			require.ErrorContains(t, err, tc.want)
		})
	}

	cfg, err := p.ParseConfig(parse(t, `[iceberg]
topic = "photos"
catalog = "http://catalog:8181"
warehouse = "s3://lake/warehouse"
namespace = "committed"
table = "photos"
flushRows = 500
flushInterval = "5s"
[iceberg.props]
"s3.endpoint" = "http://minio:9000"
`))
	require.NoError(t, err)
	require.Equal(t, "photos", cfg.Topic)
	require.Equal(t, "http://catalog:8181", cfg.CatalogURI)
	require.Equal(t, "s3://lake/warehouse", cfg.Warehouse)
	require.Equal(t, 500, cfg.FlushRows)
	require.Equal(t, 5*time.Second, cfg.FlushInterval)
	require.Equal(t, "http://minio:9000", cfg.Props["s3.endpoint"])

	// Defaults.
	cfg, err = p.ParseConfig(parse(t, "[iceberg]\ntopic = \"a\"\ncatalog = \"http://c:8181\"\nnamespace = \"n\"\ntable = \"t\"\n"))
	require.NoError(t, err)
	require.Equal(t, defaultFlushRows, cfg.FlushRows)
	require.Equal(t, defaultFlushInterval, cfg.FlushInterval)
}

func TestExtractors(t *testing.T) {
	p := &SyncableParser{}
	v := parse(t, "[syncable]\nname = \"x\"\ntype = \"iceberg\"\n[iceberg]\ntopic = \"a\"\ncatalog = \"http://c:8181\"\nnamespace = \"n\"\ntable = \"t\"\n")
	require.Equal(t, []string{"a"}, p.TopicsFromConfig(v))
}

// TestBufferCollapse pins the in-buffer merge semantics: later entries for a
// key overwrite earlier ones (log order collapses exactly as the table merge
// would), tombstones supersede upserts and vice versa, and liveRecord emits
// only live rows, in key order.
func TestBufferCollapse(t *testing.T) {
	s := &Syncable{
		config: &Config{Topic: "t", FlushRows: 1000, FlushInterval: time.Hour},
		buffer: map[string]*bufferedRow{},
	}
	tp := &cluster.Type{ID: "t"}

	sync := func(index uint64, entities ...*cluster.Entity) {
		t.Helper()
		should, err := s.Sync(t.Context(), &cluster.Actual{Index: index, Entities: entities})
		require.NoError(t, err)
		require.False(t, bool(should), "buffering only — no flush at these sizes")
	}

	e1 := cluster.NewUpsertEntity(tp, []byte("k1"), []byte(`{"v":1}`))
	e1.Generation = 3
	sync(10, e1)
	sync(11, cluster.NewUpsertEntity(tp, []byte("k2"), []byte(`{"v":2}`)))
	sync(12, cluster.NewUpsertEntity(tp, []byte("k1"), []byte(`{"v":9}`))) // supersedes e1
	sync(13, cluster.NewDeleteEntity(tp, []byte("k2")))                    // tombstone supersedes k2
	sync(14, cluster.NewUpsertEntity(tp, []byte("k0"), []byte(`{"v":0}`)))

	require.Equal(t, uint64(14), s.pendingIndex)
	require.Len(t, s.buffer, 3)
	require.True(t, s.buffer["k2"].delete)

	rec, live, err := s.liveRecord()
	require.NoError(t, err)
	defer rec.Release()
	require.Equal(t, 2, live)
	require.Equal(t, int64(2), rec.NumRows())
	// Key order, latest values, index/generation stamped.
	require.Equal(t, `["k0" "k1"]`, rec.Column(0).String())
	require.Equal(t, `["{\"v\":0}" "{\"v\":9}"]`, rec.Column(1).String())
	require.Equal(t, "[14 12]", rec.Column(2).String())

	// A foreign-topic actual is ignored entirely.
	should, err := s.Sync(t.Context(), &cluster.Actual{Index: 15, Entities: []*cluster.Entity{
		cluster.NewUpsertEntity(&cluster.Type{ID: "other"}, []byte("x"), []byte(`{}`)),
	}})
	require.NoError(t, err)
	require.False(t, bool(should))
	require.Equal(t, uint64(14), s.pendingIndex, "foreign actuals don't advance the pending index")
}

// TestFlushTriggers pins when a flush becomes due: row-count threshold,
// buffer age, and never on an empty buffer.
func TestFlushTriggers(t *testing.T) {
	s := &Syncable{
		config: &Config{Topic: "t", FlushRows: 2, FlushInterval: time.Hour},
		buffer: map[string]*bufferedRow{},
	}
	require.False(t, s.flushDue(), "empty buffer never flushes")

	s.bufferPut("k1", &bufferedRow{payload: "{}"})
	require.False(t, s.flushDue())
	s.bufferPut("k1", &bufferedRow{payload: "{}"})
	require.False(t, s.flushDue(), "same key collapses — still one row")
	s.bufferPut("k2", &bufferedRow{payload: "{}"})
	require.True(t, s.flushDue(), "row threshold reached")

	s.clearBuffer()
	s.config.FlushRows = 1000
	s.config.FlushInterval = time.Nanosecond
	s.bufferPut("k1", &bufferedRow{payload: "{}"})
	time.Sleep(time.Millisecond)
	require.True(t, s.flushDue(), "age threshold reached")
}

func TestParseConfigRejectsUnknownKeys(t *testing.T) {
	p := &SyncableParser{}
	_, err := p.ParseConfig(parse(t, "[iceberg]\ntopic = \"a\"\ncatalog = \"http://c:8181\"\nnamespce = \"n\"\ntable = \"t\"\n"))
	require.Error(t, err)
	require.Equal(t, "iceberg.namespce", cluster.NewConfigError(err).Field)
	require.Contains(t, err.Error(), `did you mean "namespace"?`)
}

func TestIcebergVocabulary_EqualsTheReads(t *testing.T) {
	read := cluster.ObserveConfigReads(func() {
		v := parse(t, `[iceberg]
topic = "photos"
catalog = "http://catalog:8181"
warehouse = "s3://lake/warehouse"
namespace = "committed"
table = "photos"
flushRows = 500
flushInterval = "5s"
[iceberg.props]
"s3.endpoint" = "http://minio:9000"
`)
		p := &SyncableParser{}
		_, err := p.ParseConfig(v)
		require.NoError(t, err)
		_ = p.TopicsFromConfig(v)
	})
	undeclared, unread := cluster.VocabularyDiff(icebergKeys, read["iceberg"])
	require.Empty(t, undeclared, "[iceberg]: keys read but not declared")
	require.Empty(t, unread, "[iceberg]: keys declared but never read")
}
