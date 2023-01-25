package authn_test

import (
	"testing"

	"github.com/philborlin/committed/internal/controlplane/authn"
	"github.com/stretchr/testify/assert"
)

func TestGenerate(t *testing.T) {
	signingKey := []byte("foo")
	badSigningKey := ""

	key := "foo"
	value := "bar"

	tests := map[string]struct {
		key            string
		value          string
		signingKey     any
		generateError  string
		getClaimsError string
	}{
		"simple":          {key: key, value: value, signingKey: signingKey, generateError: "", getClaimsError: ""},
		"bad signing key": {key: key, value: value, signingKey: badSigningKey, generateError: "key is of invalid type", getClaimsError: "token contains an invalid number of segments"},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			claims := make(map[string]any)
			claims[tc.key] = tc.value

			a := authn.New(tc.signingKey)
			ts, generateError := a.Generate(claims)
			assertError(t, generateError, tc.generateError)

			c, getClaimsError := a.GetClaims(ts)
			assertError(t, getClaimsError, tc.getClaimsError)

			if getClaimsError == nil {
				val, ok := c[tc.key]
				assert.True(t, ok)
				assert.Equal(t, tc.value, val)
			}
		})
	}
}

func assertError(t *testing.T, err error, errorString string) {
	if errorString == "" {
		assert.NoError(t, err)
	} else if err != nil {
		assert.Contains(t, err.Error(), errorString)
	} else {
		assert.Error(t, err)
	}
}
