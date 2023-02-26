package api

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/philborlin/committed/internal/controlplane/workflow"
)

type AuthenticationWorkflow interface {
	UsernamePassword(up *workflow.UsernamePassword) (string, error)
}

type JWTResponse struct {
	Token string `json:"token"`
}

func (a *API) UsernamePasswordAuthenticationHandler(w http.ResponseWriter, r *http.Request) {
	up := &workflow.UsernamePassword{}
	err := unmarshalBody(r, *up)
	if err != nil {
		badRequest(w, err)
		return
	}

	jwt, err := a.Authentication.UsernamePassword(up)
	if err != nil {
		if errors.Is(err, &workflow.UnauthorizedError{}) {
			unauthorized(w, fmt.Errorf("Unauthorized password for user: %s", up.Username))
		} else {
			internalServerError(w, err)
		}
		return
	}

	err = marshalToResponse(w, &JWTResponse{Token: jwt})
	if err != nil {
		internalServerError(w, err)
	}
}
