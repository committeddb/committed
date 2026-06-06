package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/db"
)

func TestParseNodeID(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    uint64
		wantErr bool
	}{
		{name: "empty defaults to 1", raw: "", want: 1},
		{name: "valid id", raw: "7", want: 7},
		{name: "zero is rejected", raw: "0", wantErr: true},
		{name: "negative is rejected", raw: "-1", wantErr: true},
		{name: "non-numeric is rejected", raw: "abc", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseNodeID(tt.raw)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestParsePeers_Unset_SingleNode(t *testing.T) {
	peers, err := parsePeers(3, "", "http://self:9022")
	require.NoError(t, err)
	require.Equal(t, db.Peers{3: "http://self:9022"}, peers)
}

func TestParsePeers_FullSet(t *testing.T) {
	raw := "1=http://n1:9022,2=http://n2:9022,3=http://n3:9022"
	peers, err := parsePeers(2, raw, "http://self:9022")
	require.NoError(t, err)
	require.Equal(t, db.Peers{
		1: "http://n1:9022",
		2: "http://n2:9022",
		3: "http://n3:9022",
	}, peers)
}

func TestParsePeers_TolerateWhitespaceAndTrailingComma(t *testing.T) {
	raw := " 1 = http://n1:9022 , 2=http://n2:9022 , "
	peers, err := parsePeers(1, raw, "http://self:9022")
	require.NoError(t, err)
	require.Equal(t, db.Peers{
		1: "http://n1:9022",
		2: "http://n2:9022",
	}, peers)
}

func TestParsePeers_Errors(t *testing.T) {
	tests := []struct {
		name string
		id   uint64
		raw  string
	}{
		{name: "self id missing from set", id: 9, raw: "1=http://n1:9022,2=http://n2:9022"},
		{name: "entry not id=url", id: 1, raw: "1=http://n1:9022,garbage"},
		{name: "empty url", id: 1, raw: "1="},
		{name: "empty id", id: 1, raw: "=http://n1:9022"},
		{name: "zero peer id", id: 1, raw: "0=http://n1:9022"},
		{name: "non-numeric peer id", id: 1, raw: "x=http://n1:9022"},
		{name: "duplicate peer id", id: 1, raw: "1=http://n1:9022,1=http://other:9022"},
		{name: "only whitespace/commas", id: 1, raw: " , , "},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parsePeers(tt.id, tt.raw, "http://self:9022")
			require.Error(t, err)
		})
	}
}

func TestGetenvDefault(t *testing.T) {
	t.Setenv("COMMITTED_TEST_GETENV", "set-value")
	require.Equal(t, "set-value", getenvDefault("COMMITTED_TEST_GETENV", "fallback"))
	require.Equal(t, "fallback", getenvDefault("COMMITTED_TEST_GETENV_UNSET", "fallback"))
}
