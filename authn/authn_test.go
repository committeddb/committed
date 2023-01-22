package authn_test

import (
	"crypto/rand"
	"math/big"
	"testing"

	"github.com/philborlin/committed/authn"
	"github.com/stretchr/testify/assert"
)

func generateRandomString(n int) ([]byte, error) {
	const letters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-"
	ret := make([]byte, n)
	for i := 0; i < n; i++ {
		num, err := rand.Int(rand.Reader, big.NewInt(int64(len(letters))))
		if err != nil {
			return nil, err
		}
		ret[i] = letters[num.Int64()]
	}

	return ret, nil
}

func TestGenerate(t *testing.T) {
	signingKey, err := generateRandomString(32)
	assert.NoError(t, err)

	key := "foo"
	value := "bar"

	tests := map[string]struct {
		key        string
		value      string
		signingKey any
		err        string
	}{
		"simple": {key: key, value: value, signingKey: signingKey, err: ""},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			claims := make(map[string]any)
			claims[tc.key] = tc.value

			a := authn.New(tc.signingKey)
			ts, err := a.Generate(claims)
			assert.NoError(t, err)

			c, err := a.GetClaims(ts)
			if tc.err == "" {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				if err != nil {
					assert.Contains(t, err.Error(), tc.err)
				}
			}

			val, ok := c[tc.key]
			assert.True(t, ok)
			assert.Equal(t, tc.value, val)
		})
	}
}
