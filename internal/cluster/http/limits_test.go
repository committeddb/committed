package http_test

import (
	"encoding/json"
	"fmt"
	"io"
	httpgo "net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/clusterfakes"
	"github.com/philborlin/committed/internal/cluster/http"
)

// Tests here cover the 413 translation path for
// cluster.ErrProposalTooLarge. The actual size check lives in
// db/db.go (proposeAsync) so that every proposal source — HTTP
// API, ingest worker, config propose, sync index bumps — funnels
// through one limit. These tests use a fake Cluster whose Propose*
// methods return the sentinel so we can assert the handler
// translation without standing up a real raft.

func TestPropose_ReturnsTooLarge_MapsTo413(t *testing.T) {
	// Propose* paths cover /proposal plus every /<resource>/{id}
	// POST. Each returns cluster.ErrProposalTooLarge from the
	// fake; the handler should translate uniformly to 413 with
	// the proposal_too_large code.
	tests := []struct {
		name    string
		method  string
		path    string
		ct      string
		body    string
		setupFn func(*clusterfakes.FakeCluster)
	}{
		{
			name:   "proposal",
			method: "POST",
			path:   "/proposal",
			ct:     "application/json",
			body:   `{"entities":[{"typeId":"t","key":"k","data":{}}]}`,
			setupFn: func(fake *clusterfakes.FakeCluster) {
				// AddProposal resolves the type before proposing, so
				// the fake has to satisfy ResolveType first. A minimal
				// Type with no schema validator lets us reach the
				// Propose call.
				fake.ResolveTypeReturns(&cluster.Type{ID: "t", Version: 1}, nil)
				fake.ProposeReturns(fmt.Errorf("wrap: %w", cluster.ErrProposalTooLarge))
			},
		},
		{
			name:   "database",
			method: "POST",
			path:   "/database/db-1",
			ct:     "text/toml",
			body:   "[config]\nname = \"x\"",
			setupFn: func(fake *clusterfakes.FakeCluster) {
				fake.ProposeDatabaseReturns(fmt.Errorf("wrap: %w", cluster.ErrProposalTooLarge))
			},
		},
		{
			name:   "syncable",
			method: "POST",
			path:   "/syncable/s-1",
			ct:     "text/toml",
			body:   "[config]\nname = \"x\"",
			setupFn: func(fake *clusterfakes.FakeCluster) {
				fake.ProposeSyncableReturns(fmt.Errorf("wrap: %w", cluster.ErrProposalTooLarge))
			},
		},
		{
			name:   "ingestable",
			method: "POST",
			path:   "/ingestable/i-1",
			ct:     "text/toml",
			body:   "[config]\nname = \"x\"",
			setupFn: func(fake *clusterfakes.FakeCluster) {
				fake.ProposeIngestableReturns(fmt.Errorf("wrap: %w", cluster.ErrProposalTooLarge))
			},
		},
		{
			name:   "type",
			method: "POST",
			path:   "/type/t-1",
			ct:     "text/toml",
			body:   "[config]\nname = \"x\"",
			setupFn: func(fake *clusterfakes.FakeCluster) {
				fake.ProposeTypeReturns(fmt.Errorf("wrap: %w", cluster.ErrProposalTooLarge))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := &clusterfakes.FakeCluster{}
			tc.setupFn(fake)
			h := http.New(fake)

			req := httptest.NewRequest(tc.method, "http://localhost"+tc.path, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", tc.ct)
			w := httptest.NewRecorder()

			h.ServeHTTP(w, req)

			resp := w.Result()
			require.Equal(t, httpgo.StatusRequestEntityTooLarge, resp.StatusCode,
				"expected 413 for %s", tc.name)

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			var errResp http.ErrorResponse
			require.NoError(t, json.Unmarshal(body, &errResp))
			require.Equal(t, "proposal_too_large", errResp.Code)
		})
	}
}
