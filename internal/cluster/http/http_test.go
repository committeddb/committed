package http_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/philborlin/committed/internal/cluster/db/parser"
	test "github.com/philborlin/committed/internal/cluster/db/testing"
	"github.com/philborlin/committed/internal/cluster/db/wal"
	"github.com/philborlin/committed/internal/cluster/http"
	"github.com/stretchr/testify/require"
)

func TestEndToEnd(t *testing.T) {
	dir, err := os.MkdirTemp("", "CommitteddbE2ETest-*")
	require.Nil(t, err)
	defer os.RemoveAll(dir)

	db := createDB(t, dir)
	h := http.New(db)

	typeID := addType(t, h, "foo")
	time.Sleep(5 * time.Second)
	p := createProposal(typeID, "key", "one")
	propose(t, h, p)
	time.Sleep(5 * time.Second)
	ps, err := db.Ents()
	require.Nil(t, err)
	require.Equal(t, 2, len(ps)) // AddType + Proposal
	require.Equal(t, 1, len(ps[1].Entities))
	require.Equal(t, p.Entities[0].TypeID, ps[1].Entities[0].Type.ID)
	require.Equal(t, p.Entities[0].Key, string(ps[1].Entities[0].Key))
	require.Equal(t, []byte(p.Entities[0].Data), ps[1].Entities[0].Data)
}

func addType(t *testing.T, h *http.HTTP, name string) string {
	body := fmt.Sprintf("[type]\nname = \"%s\"", name)

	req := httptest.NewRequest("POST", "http://localhost/type", strings.NewReader(body))
	req.Header["Content-Type"] = []string{"text/toml"}

	w := httptest.NewRecorder()

	h.AddType(w, req)

	resp := w.Result()
	require.Equal(t, 200, resp.StatusCode)
	id, err := io.ReadAll(resp.Body)
	require.Nil(t, err)

	return string(id)
}

func propose(t *testing.T, h *http.HTTP, proposal *http.ProposalRequest) {
	bs, err := json.Marshal(proposal)
	require.Nil(t, err)
	req := httptest.NewRequest("POST", "http://localhost/proposal", bytes.NewReader(bs))
	req.Header["Content-Type"] = []string{"text/toml"}

	w := httptest.NewRecorder()

	h.AddProposal(w, req)

	resp := w.Result()
	require.Equal(t, 200, resp.StatusCode)
}

func createProposal(typeID string, key string, one string) *http.ProposalRequest {
	j := fmt.Sprintf("{\"key\":\"%s\",\"one\":\"%s\"}", key, one)

	return &http.ProposalRequest{
		Entities: []*http.EntityRequest{{
			TypeID: typeID,
			Key:    key,
			Data:   []byte(j),
		}},
	}
}

func createDB(t *testing.T, dir string) *test.DB {
	p := parser.New()
	s, err := wal.Open(dir, p)
	require.Nil(t, err)

	db := test.CreateDBWithStorage(s)
	db.EatCommitC()

	return db
}
