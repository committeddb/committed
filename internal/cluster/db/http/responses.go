package http

import (
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"

	"github.com/committeddb/committed/internal/cluster"
)

// Request-body decoding and response-body writing helpers shared across
// handlers. Error responses live in errors.go; these cover the success
// paths — reading a configuration in, and writing configurations / JSON
// arrays back out.

// unmarshalBody reads r.Body and JSON-decodes into v. Oversize
// bodies are not rejected here — the proposal-size limit lives at
// the raft choke point (db.proposeAsync) so every proposal source
// is checked uniformly. See cluster.ErrProposalTooLarge.
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

func createConfiguration(r *http.Request) (*cluster.Configuration, error) {
	id := r.PathValue("id")
	if id == "" {
		return nil, errors.New("id is empty")
	}

	// Derive the config's MIME type from the Content-Type header, defaulting to
	// text/toml. ParseMediaType strips any parameters (e.g. "; charset=utf-8") and
	// lowercases the base type, so downstream exact-match parsing isn't tripped by
	// a charset or a header-case quirk; an absent or malformed header keeps the
	// default.
	mimeType := "text/toml"
	if mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type")); err == nil && mt != "" {
		mimeType = mt
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	configuration := &cluster.Configuration{
		ID:       string(id),
		MimeType: mimeType,
		Data:     body,
	}

	return configuration, nil
}

type ConfigurationResponse struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	MimeType string `json:"mimeType"`
	Data     string `json:"data"`
}

func writeConfigurations(w http.ResponseWriter, cfgs []*cluster.Configuration) {
	rs := make([]*ConfigurationResponse, 0, len(cfgs))

	for _, cfg := range cfgs {
		rs = append(rs, &ConfigurationResponse{
			ID:       cfg.ID,
			Name:     cfg.Name,
			MimeType: cfg.MimeType,
			Data:     string(cfg.Data),
		})
	}

	writeArrayBody(w, rs)
}

// writeConfiguration writes ONE configuration as a bare object — the by-key
// GET shape (a single-element array made every client unwrap [0] forever).
func writeConfiguration(w http.ResponseWriter, cfg *cluster.Configuration) {
	bs, err := json.Marshal(&ConfigurationResponse{
		ID:       cfg.ID,
		Name:     cfg.Name,
		MimeType: cfg.MimeType,
		Data:     string(cfg.Data),
	})
	if err != nil {
		writeInternalError(w, "failed to marshal response", err)
		return
	}
	writeJson(w, bs)
}

func writeArrayBody[T any](w http.ResponseWriter, body []T) {
	if body == nil {
		writeJson(w, []byte("[]"))
		return
	}

	bs, err := json.Marshal(body)
	if err != nil {
		writeInternalError(w, "failed to marshal response", err)
		return
	}

	writeJson(w, bs)
}

func writeJson(w http.ResponseWriter, bs []byte) {
	w.Header().Add("Content-Type", "application/json")
	_, _ = w.Write(bs)
}
