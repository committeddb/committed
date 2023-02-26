package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
)

func (a *API) createMux() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/controlplane/users", newLoggingHandler(http.HandlerFunc(a.UsersHandler)))
	return mux
}

type loggingHandler struct {
	handler http.Handler
}

func newLoggingHandler(handler http.Handler) http.Handler {
	return &loggingHandler{handler: handler}
}

// ServeHTTP implements Handler
func (h *loggingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("Received: %s %s\n", r.Method, r.RequestURI)
	fmt.Printf("%v -> %v\n", r.RemoteAddr, r.RequestURI)
	h.handler.ServeHTTP(w, r)
}

type API struct {
	server         *http.Server
	Authentication AuthenticationWorkflow
}

// ServeAPI starts the committed API.
func ServeAPI(port int) *API {
	a := &API{}

	srv := http.Server{
		Addr:    ":" + strconv.Itoa(port),
		Handler: a.createMux(),
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Printf("HTTP API error: %v", err)
		}
	}()

	a.server = &srv

	return a
}

// Shutdown shuts the server down
func (a *API) Shutdown() error {
	return a.server.Shutdown(context.Background())
}

func badRequest(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusBadRequest)
	fmt.Printf("%v", err)
}

func internalServerError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusInternalServerError)
	fmt.Printf("%v", err)
}

func unauthorized(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusUnauthorized)
	fmt.Printf("%v", err)
}

func unmarshalBody(r *http.Request, v any) error {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}

	err = json.Unmarshal(body, &v)
	if err != nil {
		return err
	}

	return nil
}

func marshalToResponse(w http.ResponseWriter, v any) error {
	bs, err := json.Marshal(v)
	if err != nil {
		return err
	}

	_, err = w.Write(bs)
	if err != nil {
		return err
	}

	return nil
}
