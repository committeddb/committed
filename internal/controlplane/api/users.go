package api

import (
	"net/http"
)

type newClusterTopicRequest struct {
	Name      string
	NodeCount int
}

func (a *API) UsersHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
}
