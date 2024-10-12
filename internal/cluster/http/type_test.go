package http_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	test "github.com/philborlin/committed/internal/cluster/db/testing"
	"github.com/philborlin/committed/internal/cluster/http"
	"github.com/stretchr/testify/require"
)

func TestAddType(t *testing.T) {
	body := "[type]\nname = \"foo\""

	req := httptest.NewRequest("GET", "http://google.com", strings.NewReader(body))
	req.Header["Content-Type"] = []string{"text/toml"}

	w := httptest.NewRecorder()

	c := createDB()
	h := http.New(c)
	h.AddType(w, req)

	resp := w.Result()
	require.Equal(t, 200, resp.StatusCode)
}

func createDB() *test.DB {
	return test.CreateDB()
}
