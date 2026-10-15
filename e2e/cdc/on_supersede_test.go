//go:build docker

package cdc_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/e2e/cdc/harness"
	"github.com/committeddb/committed/e2e/cdc/mutation"
)

// TestOnSupersedeScrubRemovesMaskedOriginal drives masking at the source end to
// end: a Postgres table ingested into a type declared onSupersede = "scrub", a
// row updated in place to mask a value, then a scrub. Afterwards the original
// is gone from the node's permanent log, which a syncable created after the
// scrub proves: replaying the topic from the start, it sees only the masked
// version.
func TestOnSupersedeScrubRemovesMaskedOriginal(t *testing.T) {
	h := harness.New(t, harness.Options{Tables: []string{"region"}})
	h.PostConfig(t, "/v1/type/region", "[type]\nname = \"region\"\nentityKind = \"event\"\nonSupersede = \"scrub\"\n")

	s := mutation.NewScript()
	s.Insert("region", regionRow(1, "AMERICA", "ada@example.com"))
	require.NoError(t, h.RunScript(context.Background(), s))
	m := mutation.NewScript()
	m.Update("region", regionRow(1, "AMERICA", "***"))
	require.NoError(t, h.RunScript(context.Background(), m))
	h.Capture(t, map[string]int{"region": 2})

	var status struct {
		AppliedIndex uint64 `json:"appliedIndex"`
		Scrub        struct {
			CompletedBound uint64 `json:"completedBound"`
		} `json:"scrub"`
	}
	require.NoError(t, json.Unmarshal(h.GetJSON(t, "/v1/node/status"), &status))
	masked := status.AppliedIndex

	h.PostConfig(t, "/v1/scrub", "")
	require.Eventually(t, func() bool {
		if err := json.Unmarshal(h.GetJSON(t, "/v1/node/status"), &status); err != nil {
			return false
		}
		return status.Scrub.CompletedBound >= masked
	}, 60*time.Second, 200*time.Millisecond, "the scrub completes past the mask")

	var mu sync.Mutex
	var comments []string
	replay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Entities []struct {
				Op   string          `json:"op"`
				Data json.RawMessage `json:"data"`
			} `json:"entities"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err == nil {
			mu.Lock()
			for _, e := range body.Entities {
				if e.Op == "upsert" {
					var row map[string]any
					if json.Unmarshal(e.Data, &row) == nil {
						comments = append(comments, fmt.Sprint(row["r_comment"]))
					}
				}
			}
			mu.Unlock()
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer replay.Close()
	h.PostConfig(t, "/v1/syncable/replay", fmt.Sprintf(
		"[syncable]\nname = \"replay\"\ntype = \"http\"\n\n[http]\ntopic = \"region\"\nurl = %q\nmethod = \"POST\"\n", replay.URL))

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(comments) > 0
	}, 60*time.Second, 200*time.Millisecond, "the new syncable replays the topic")
	time.Sleep(2 * time.Second) // let any further delivery arrive
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"***"}, comments, "only the masked version remains in the log: %s", strings.Join(comments, ", "))
}
