package workflow

import (
	"github.com/alexedwards/argon2id"
)

type UsernamePassword struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type UnauthorizedError struct{}

func (e *UnauthorizedError) Error() string {
	return "unauthorized"
}

func (w *Workflow) UsernamePassword(up UsernamePassword) (string, error) {
	hash, err := w.KeyValueStore.Get(usernamePasswordStoreID, up.Username)
	if err != nil {
		return "", err
	}

	match, err := argon2id.ComparePasswordAndHash(up.Password, hash)
	if err != nil {
		return "", err
	}

	if !match {
		return "", &UnauthorizedError{}
	}

	// TODO Generate jwt token and store it. Look at authn package

	return "", nil
}
