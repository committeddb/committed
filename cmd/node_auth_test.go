package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadAuthTokens(t *testing.T) {
	for _, tc := range []struct {
		name, api, member, peer string
		split, invalid          bool
	}{
		{name: "development"},
		{name: "legacy", api: " legacy "},
		{name: "split", api: " api ", member: " member ", peer: " peer ", split: true},
		{name: "partial", api: "api", peer: "peer", invalid: true},
		{name: "missing API", member: "member", peer: "peer", invalid: true},
		{name: "duplicate", api: "secret", member: " secret ", peer: "peer", invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("COMMITTED_API_TOKEN", tc.api)
			t.Setenv("COMMITTED_MEMBERSHIP_TOKEN", tc.member)
			t.Setenv("COMMITTED_PEER_TOKEN", tc.peer)
			tokens, err := loadAuthTokens()
			if tc.invalid {
				require.Error(t, err)
				require.NotContains(t, err.Error(), "secret")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.split, tokens.Split())
			if tc.split {
				require.Equal(t, "peer", tokens.Peer())
				require.Equal(t, "member", tokens.Membership())
			} else {
				require.Equal(t, tokens.API(), tokens.Peer())
			}
		})
	}
}
