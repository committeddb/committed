package auth

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTokenModes(t *testing.T) {
	for mask := range 8 {
		values := [3]string{}
		for i, token := range []string{"api-secret", "membership-secret", "peer-secret"} {
			if mask&(1<<i) != 0 {
				values[i] = " " + token + "\n"
			}
		}
		tokens, err := NewTokens(values[0], values[1], values[2])
		if mask != 0 && mask != 1 && mask != 7 {
			require.Error(t, err)
			for _, value := range []string{"api-secret", "membership-secret", "peer-secret"} {
				require.NotContains(t, err.Error(), value)
			}
			continue
		}
		require.NoError(t, err)
		require.Equal(t, mask == 7, tokens.Split())
		switch mask {
		case 0:
			require.Equal(t, Tokens{}, tokens)
		case 1:
			require.Equal(t, "api-secret", tokens.API())
			require.Equal(t, tokens.API(), tokens.Membership())
			require.Equal(t, tokens.API(), tokens.Peer())
		case 7:
			require.Equal(t, "api-secret", tokens.API())
			require.Equal(t, "membership-secret", tokens.Membership())
			require.Equal(t, "peer-secret", tokens.Peer())
		}
	}
	for _, values := range [][3]string{{"same", "same", "peer"}, {"same", "member", "same"}, {"api", "same", "same"}, {" same ", "same\n", "same"}, {"api", " \n", "peer"}} {
		_, err := NewTokens(values[0], values[1], values[2])
		require.Error(t, err)
		require.NotContains(t, err.Error(), "same")
	}
}
