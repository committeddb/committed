package loopback_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/loopback"
)

// typeStorage is the minimal DatabaseStorage + TypeResolver double the
// parser needs: it resolves the declared types and nothing else.
type typeStorage struct {
	types map[string]*cluster.Type
}

func (s *typeStorage) Database(id string) (cluster.Database, error) {
	return nil, fmt.Errorf("no databases in this test double")
}

func (s *typeStorage) ResolveType(ref cluster.TypeRef) (*cluster.Type, error) {
	return s.types[ref.ID], nil
}

func parse(t *testing.T, toml string) *cluster.ParsedConfig {
	t.Helper()
	v, err := cluster.ParseConfigBytes("text/toml", []byte(toml))
	require.NoError(t, err)
	return v
}

func TestTransform_PassthroughAndProjection(t *testing.T) {
	// No mappings: verbatim passthrough, never decoded (binary-safe).
	raw := []byte("not json \x00\x01")
	out, err := loopback.Transform(raw, nil)
	require.NoError(t, err)
	require.Equal(t, raw, out)

	// Mappings: number-exact, sorted-key canonical output.
	src := []byte(`{"id":"a1","meta":{"title":"T"},"n":90071992547409919,"price":1.10,"zz":"drop me"}`)
	out, err = loopback.Transform(src, []loopback.Mapping{
		{JsonPath: "$.n", Field: "n"},
		{JsonPath: "$.id", Field: "id"},
		{JsonPath: "$.meta.title", Field: "title"},
		{JsonPath: "$.price", Field: "price"},
	})
	require.NoError(t, err)
	require.Equal(t, `{"id":"a1","n":90071992547409919,"price":1.10,"title":"T"}`, string(out),
		"sorted keys, json.Number-exact values")

	// Deterministic: the same input yields the same bytes.
	again, err := loopback.Transform(src, []loopback.Mapping{
		{JsonPath: "$.n", Field: "n"},
		{JsonPath: "$.id", Field: "id"},
		{JsonPath: "$.meta.title", Field: "title"},
		{JsonPath: "$.price", Field: "price"},
	})
	require.NoError(t, err)
	require.Equal(t, string(out), string(again))
}

func TestTransform_Errors(t *testing.T) {
	_, err := loopback.Transform([]byte(`not json`), []loopback.Mapping{{JsonPath: "$.a", Field: "a"}})
	require.Error(t, err, "mapped transforms require a JSON payload")

	_, err = loopback.Transform([]byte(`{"a":1}`), []loopback.Mapping{{JsonPath: "$.missing", Field: "m"}})
	require.Error(t, err, "a missing path is an error (dead-letter), not a silent null")
}

func TestParseConfig_Validation(t *testing.T) {
	p := &loopback.SyncableParser{}

	cases := []struct {
		name string
		toml string
		want string
	}{
		{"missing topic", "[loopback]\ntarget = \"b\"\n", "loopback.topic"},
		{"missing target", "[loopback]\ntopic = \"a\"\n", "loopback.target"},
		{"self loop", "[loopback]\ntopic = \"a\"\ntarget = \"a\"\n", "infinite consensus loop"},
		{"keyPath refused", "[loopback]\ntopic = \"a\"\ntarget = \"b\"\nkeyPath = \"$.id\"\n", "re-keying is not supported"},
		{
			"missing field",
			"[loopback]\ntopic = \"a\"\ntarget = \"b\"\n[[loopback.mappings]]\njsonPath = \"$.x\"\n",
			"field is required",
		},
		{
			"duplicate field",
			"[loopback]\ntopic = \"a\"\ntarget = \"b\"\n[[loopback.mappings]]\njsonPath = \"$.x\"\nfield = \"x\"\n[[loopback.mappings]]\njsonPath = \"$.y\"\nfield = \"x\"\n",
			"mapped twice",
		},
		{
			"missing jsonPath",
			"[loopback]\ntopic = \"a\"\ntarget = \"b\"\n[[loopback.mappings]]\nfield = \"x\"\n",
			"jsonPath is required",
		},
		{
			"invalid jsonPath",
			"[loopback]\ntopic = \"a\"\ntarget = \"b\"\n[[loopback.mappings]]\njsonPath = \"$[\"\nfield = \"x\"\n",
			"invalid jsonpath",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := p.ParseConfig(parse(t, tc.toml))
			require.ErrorContains(t, err, tc.want)
		})
	}

	cfg, err := p.ParseConfig(parse(t,
		"[loopback]\ntopic = \"a\"\ntarget = \"b\"\nacknowledgeAppendSemantics = true\n[[loopback.mappings]]\njsonPath = \"$.x\"\nfield = \"x\"\n"))
	require.NoError(t, err)
	require.Equal(t, "a", cfg.SourceTopic)
	require.Equal(t, "b", cfg.TargetTopic)
	require.True(t, cfg.AcknowledgeAppendSemantics)
	require.Len(t, cfg.Mappings, 1)
}

func TestParse_TargetKindGate(t *testing.T) {
	storage := &typeStorage{types: map[string]*cluster.Type{
		"snap": {ID: "snap", Name: "snap", Version: 1, EntityKind: cluster.EntityKindSnapshot},
		"ev":   {ID: "ev", Name: "ev", Version: 1, EntityKind: cluster.EntityKindEvent},
	}}
	p := &loopback.SyncableParser{Proposer: proposerFunc(nil)}

	// Snapshot-kind target: admissible as-is.
	_, err := p.Parse(parse(t, "[loopback]\ntopic = \"a\"\ntarget = \"snap\"\n"), storage)
	require.NoError(t, err)

	// Undeclared target type: loud at POST, names the fix.
	_, err = p.Parse(parse(t, "[loopback]\ntopic = \"a\"\ntarget = \"nope\"\n"), storage)
	require.ErrorContains(t, err, "not declared")

	// Event-kind target needs the explicit append acknowledgment.
	_, err = p.Parse(parse(t, "[loopback]\ntopic = \"a\"\ntarget = \"ev\"\n"), storage)
	require.ErrorContains(t, err, "acknowledgeAppendSemantics")
	_, err = p.Parse(parse(t, "[loopback]\ntopic = \"a\"\ntarget = \"ev\"\nacknowledgeAppendSemantics = true\n"), storage)
	require.NoError(t, err)
}

func TestExtractors(t *testing.T) {
	p := &loopback.SyncableParser{}
	v := parse(t, "[syncable]\nname = \"x\"\ntype = \"loopback\"\n[loopback]\ntopic = \"a\"\ntarget = \"b\"\n")
	require.Equal(t, []string{"a"}, p.TopicsFromConfig(v))
	require.Equal(t, []string{"b"}, p.DerivedTopicsFromConfig(v))
}

// proposerFunc adapts a func to loopback.Proposer; nil means "never called".
type proposerFunc func(*cluster.Proposal) error

func (f proposerFunc) Propose(_ context.Context, p *cluster.Proposal) error {
	if f == nil {
		return nil
	}
	return f(p)
}
