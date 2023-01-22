package authn

import (
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v4"
)

type Authn struct {
	key           any
	signingMethod jwt.SigningMethod
}

func New(key any) *Authn {
	return &Authn{
		key:           key,
		signingMethod: jwt.SigningMethodHS512,
	}
}

func (a *Authn) Generate(claims map[string]any) (string, error) {
	now := time.Now().Unix()

	c := jwt.MapClaims{
		"iat": now,
		// "nbf": now,
		// "exp": time.Now().Add(15 * time.Minute).Unix(),
	}

	for k, v := range claims {
		c[k] = v
	}

	token := jwt.NewWithClaims(a.signingMethod, c)

	tokenString, err := token.SignedString(a.key)
	if err != nil {
		return "", err
	}

	return tokenString, nil
}

func (a *Authn) GetClaims(token string) (map[string]any, error) {
	t, err := jwt.Parse(token, func(token *jwt.Token) (interface{}, error) {
		if token.Method.Alg() != a.signingMethod.Alg() {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}

		return a.key, nil
	})
	if err != nil {
		return nil, err
	}

	if claims, ok := t.Claims.(jwt.MapClaims); ok && t.Valid {
		return claims, nil
	} else {
		return nil, err
	}
}
