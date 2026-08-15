//go:build docker

package cdc_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/e2e/cdc/harness"
)

// TestCensusOnRealSnapshotPass is the census ticket's e2e: a real Postgres
// table with a JSONB column — the SmugMug shape, where the interesting
// structure hides inside the blob — is seeded with two interleaved shapes,
// ingested through a real committed binary, and the snapshot pass's census
// surfaces on GET /v1/ingestable/{id}/status: every nested path with types
// and counts, both shapes with their row ranges, and a draft schema ready
// for POST /type review. Census is DEFAULT-ON: the config declares nothing.
func TestCensusOnRealSnapshotPass(t *testing.T) {
	h := harness.New(t, harness.Options{Tables: []string{"region"}})
	ctx := context.Background()

	// Seed BEFORE the ingestable exists, so the snapshot pass observes the
	// historical rows — the free census pass.
	conn := h.Conn()
	_, err := conn.Exec(ctx, `CREATE TABLE public.photos (id INT PRIMARY KEY, meta JSONB)`)
	require.NoError(t, err)
	for i, meta := range []string{
		`{"caption":"a","size":10}`,
		`{"caption":"b","ai":{"model":"v9"}}`,
		`{"caption":"c","size":30}`,
	} {
		_, err = conn.Exec(ctx, `INSERT INTO public.photos (id, meta) VALUES ($1, $2)`, i+1, meta)
		require.NoError(t, err)
	}

	h.PostConfig(t, "/v1/type/photos", "[type]\nname = \"photos\"\n")
	h.PostConfig(t, "/v1/ingestable/photos", fmt.Sprintf(`[ingestable]
name = "photos"
type = "sql"

[sql]
dialect = "postgres"
topic = "photos"
connectionString = %q
primaryKey = "id"
tables = ["public.photos"]

[sql.postgres]
slot_name = "slot_census"
publication = "pub_census"

[[sql.mappings]]
jsonName = "id"
column = "id"

[[sql.mappings]]
jsonName = "meta"
column = "meta"
`, h.ConnString()))

	type pathCensus struct {
		Path  string   `json:"path"`
		Types []string `json:"types"`
		Count uint64   `json:"count"`
	}
	type topicCensus struct {
		Rows   uint64 `json:"rows"`
		Shapes []struct {
			Count    uint64 `json:"count"`
			FirstRow uint64 `json:"firstRow"`
			LastRow  uint64 `json:"lastRow"`
		} `json:"shapes"`
		Paths       []pathCensus `json:"paths"`
		DraftSchema string       `json:"draftSchema"`
	}
	var census topicCensus
	require.Eventually(t, func() bool {
		var resp struct {
			Census map[string]topicCensus `json:"census"`
		}
		if err := json.Unmarshal(h.GetJSON(t, "/v1/ingestable/photos/status"), &resp); err != nil {
			return false
		}
		c, ok := resp.Census["photos"]
		if !ok || c.Rows != 3 {
			return false
		}
		census = c
		return true
	}, 60*time.Second, 250*time.Millisecond, "census never surfaced on the status endpoint")

	require.Len(t, census.Shapes, 2, "two interleaved shapes")

	paths := map[string]pathCensus{}
	for _, p := range census.Paths {
		paths[p.Path] = p
	}
	require.Equal(t, uint64(3), paths["$.meta.caption"].Count,
		"the JSONB column's nested paths are censused: %v", census.Paths)
	require.Equal(t, []string{"string"}, paths["$.meta.caption"].Types)
	require.Equal(t, uint64(2), paths["$.meta.size"].Count)
	require.Equal(t, uint64(1), paths["$.meta.ai.model"].Count)
	require.Equal(t, uint64(3), paths["$.id"].Count)

	require.Contains(t, census.DraftSchema, `"additionalProperties": false`)
	require.Contains(t, census.DraftSchema, `"ai"`)
	require.NotContains(t, census.DraftSchema, `"enum"`,
		"no value tracking without the censusValues opt-in — the PII posture")
}
