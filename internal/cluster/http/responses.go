package http

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/philborlin/committed/internal/cluster"
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

	mimeType := "text/toml"
	header, ok := r.Header["Content-Type"]
	if ok && len(header) == 1 {
		mimeType = header[0]
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
			Name:     "", // TODO - get name configuration
			MimeType: cfg.MimeType,
			Data:     string(cfg.Data),
		})
	}

	writeArrayBody(w, rs)
}

func writeArrayBody[T any](w http.ResponseWriter, body []T) {
	if body == nil {
		writeJson(w, []byte("[]"))
		return
	}

	bs, err := json.Marshal(body)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to marshal response")
		return
	}

	writeJson(w, bs)
}

func writeJson(w http.ResponseWriter, bs []byte) {
	w.Header().Add("Content-Type", "application/json")
	_, _ = w.Write(bs)
}
