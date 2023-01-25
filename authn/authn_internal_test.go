package authn

import (
	"testing"

	"github.com/golang-jwt/jwt/v4"
	"github.com/stretchr/testify/assert"
)

func TestUnexpectedSigningError(t *testing.T) {
	signingKey := []byte("foo")
	claims := make(map[string]any)

	a1 := New(signingKey)
	a2 := New(signingKey)

	a2.signingMethod = jwt.SigningMethodHS256

	ts, err := a1.Generate(claims)
	assert.NoError(t, err)

	_, err = a2.GetClaims(ts)
	assert.Contains(t, err.Error(), "unexpected signing method")
}
