package http_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// TestGetTypeMigrationErrors_Success asserts the handler renders migration
// dead-letter records as JSON and threads the since/limit cursor params
// through to the cluster.
func TestGetTypeMigrationErrors_Success(t *testing.T) {
	h, fake := setupTest()
	fake.TypeMigrationDeadLettersReturns([]cluster.TypeMigrationDeadLetter{
		{TypeID: "person", Index: 7, TimestampUnixNano: 1_700_000_000_000_000_000, FromVersion: 1, ToVersion: 2, Message: "jq runtime: boom"},
		{TypeID: "person", Index: 12, TimestampUnixNano: 1_700_000_001_000_000_000, FromVersion: 2, ToVersion: 3, Message: "no output"},
	}, nil)

	req := httptest.NewRequest("GET", "http://localhost/v1/type/person/migration-errors?since=5&limit=10", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 200, resp.StatusCode)
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var got []struct {
		Index       uint64 `json:"index"`
		FromVersion int    `json:"fromVersion"`
		ToVersion   int    `json:"toVersion"`
		Timestamp   string `json:"timestamp"`
		Message     string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(body, &got))
	require.Len(t, got, 2)
	require.Equal(t, uint64(7), got[0].Index)
	require.Equal(t, 1, got[0].FromVersion)
	require.Equal(t, 2, got[0].ToVersion)
	require.Equal(t, "jq runtime: boom", got[0].Message)
	require.Equal(t, "2023-11-14T22:13:20Z", got[0].Timestamp, "nanos must render as RFC3339 UTC")

	// The cursor params reach the cluster verbatim.
	id, since, limit := fake.TypeMigrationDeadLettersArgsForCall(0)
	require.Equal(t, "person", id)
	require.Equal(t, uint64(5), since)
	require.Equal(t, 10, limit)
}

// TestGetTypeMigrationErrors_Defaults asserts that with no query params the
// handler uses since=0 and the default page size, and an empty result
// serializes as [] (not null).
func TestGetTypeMigrationErrors_Defaults(t *testing.T) {
	h, fake := setupTest()
	fake.TypeMigrationDeadLettersReturns(nil, nil)

	req := httptest.NewRequest("GET", "http://localhost/v1/type/person/migration-errors", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 200, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "[]", string(body))

	id, since, limit := fake.TypeMigrationDeadLettersArgsForCall(0)
	require.Equal(t, "person", id)
	require.Equal(t, uint64(0), since)
	require.Equal(t, 100, limit)
}

// TestGetTypeMigrationErrors_BadParams asserts invalid cursor params are
// rejected with 400 before the cluster is consulted.
func TestGetTypeMigrationErrors_BadParams(t *testing.T) {
	for _, path := range []string{
		"/v1/type/person/migration-errors?since=notanumber",
		"/v1/type/person/migration-errors?limit=0",
		"/v1/type/person/migration-errors?limit=-3",
		"/v1/type/person/migration-errors?limit=abc",
	} {
		t.Run(path, func(t *testing.T) {
			h, fake := setupTest()
			req := httptest.NewRequest("GET", "http://localhost"+path, nil)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)

			require.Equal(t, 400, w.Result().StatusCode)
			require.Zero(t, fake.TypeMigrationDeadLettersCallCount(), "must not query the cluster on a bad request")
		})
	}
}

// TestGetTypeMigrationErrors_InternalError maps a storage error to 500.
func TestGetTypeMigrationErrors_InternalError(t *testing.T) {
	h, fake := setupTest()
	fake.TypeMigrationDeadLettersReturns(nil, io.ErrUnexpectedEOF)

	req := httptest.NewRequest("GET", "http://localhost/v1/type/person/migration-errors", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	require.Equal(t, 500, w.Result().StatusCode)
}

// TestReplayTypeMigrationDeadLetterHandler_Success maps a nil error to 200
// and threads id + index through to the cluster.
func TestReplayTypeMigrationDeadLetterHandler_Success(t *testing.T) {
	h, fake := setupTest()
	fake.ReplayTypeMigrationDeadLetterReturns(nil)

	req := httptest.NewRequest("POST", "http://localhost/v1/type/person/migration-retry/7", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	require.Equal(t, 200, w.Result().StatusCode)
	require.Equal(t, 1, fake.ReplayTypeMigrationDeadLetterCallCount())
	_, gotID, gotIndex := fake.ReplayTypeMigrationDeadLetterArgsForCall(0)
	require.Equal(t, "person", gotID)
	require.Equal(t, uint64(7), gotIndex)
}

// TestReplayTypeMigrationDeadLetterHandler_NotFound maps ErrNotDeadLettered
// to 404.
func TestReplayTypeMigrationDeadLetterHandler_NotFound(t *testing.T) {
	h, fake := setupTest()
	fake.ReplayTypeMigrationDeadLetterReturns(cluster.ErrNotDeadLettered)

	req := httptest.NewRequest("POST", "http://localhost/v1/type/person/migration-retry/7", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	require.Equal(t, 404, w.Result().StatusCode)
}

// TestReplayTypeMigrationDeadLetterHandler_StillFailing maps a retry that
// still fails to 502 with the cause in details; the record stays.
func TestReplayTypeMigrationDeadLetterHandler_StillFailing(t *testing.T) {
	h, fake := setupTest()
	fake.ReplayTypeMigrationDeadLetterReturns(
		fmt.Errorf("%w: jq runtime: cannot derive email", cluster.ErrReplayMigrationFailed))

	req := httptest.NewRequest("POST", "http://localhost/v1/type/person/migration-retry/7", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 502, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var got struct {
		Code    string `json:"code"`
		Details string `json:"details"`
	}
	require.NoError(t, json.Unmarshal(body, &got))
	require.Equal(t, "migration_retry_failed", got.Code)
	require.Contains(t, got.Details, "cannot derive email", "the failure cause must be surfaced")
}

// TestReplayTypeMigrationDeadLetterHandler_BadIndex rejects a non-numeric
// index with 400 before the cluster is consulted.
func TestReplayTypeMigrationDeadLetterHandler_BadIndex(t *testing.T) {
	h, fake := setupTest()

	req := httptest.NewRequest("POST", "http://localhost/v1/type/person/migration-retry/notanumber", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	require.Equal(t, 400, w.Result().StatusCode)
	require.Zero(t, fake.ReplayTypeMigrationDeadLetterCallCount())
}

// TestReplayTypeMigrationDeadLetterHandler_InternalError maps any other
// error to 500.
func TestReplayTypeMigrationDeadLetterHandler_InternalError(t *testing.T) {
	h, fake := setupTest()
	fake.ReplayTypeMigrationDeadLetterReturns(io.ErrUnexpectedEOF)

	req := httptest.NewRequest("POST", "http://localhost/v1/type/person/migration-retry/7", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	require.Equal(t, 500, w.Result().StatusCode)
}
