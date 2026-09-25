package httptransport

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

func TestDiskReportAuthorization(t *testing.T) {
	tr := New(1, nil, zap.NewNop(), &fakeRaft{}, nil, nil, "peer-secret")
	t.Cleanup(tr.Stop)
	calls := 0
	tr.reportDisk = func(id uint64, state string) (cluster.DiskVerdict, error) {
		calls++
		require.Equal(t, uint64(2), id)
		require.Equal(t, "critical", state)
		return cluster.DiskVerdict{State: "critical", LeaderID: 1}, nil
	}
	for _, token := range []string{"", "api-secret", "membership-secret", "peer-secret"} {
		t.Run(token, func(t *testing.T) {
			before := calls
			req := httptest.NewRequest(http.MethodPost, diskReportPath, strings.NewReader(`{"node":2,"state":"critical"}`))
			req.Header.Set(clusterIDHeader, clusterID)
			req.Header.Set(protocolHeader, protocolVersion)
			req.Header.Set("Authorization", "Bearer "+token)
			rec := httptest.NewRecorder()
			tr.handler().ServeHTTP(rec, req)
			if token == "peer-secret" {
				require.Equal(t, http.StatusOK, rec.Code)
				require.Equal(t, before+1, calls)
			} else {
				require.Equal(t, http.StatusUnauthorized, rec.Code)
				require.Equal(t, before, calls)
			}
		})
	}
}

func TestDiskReportValidation(t *testing.T) {
	for _, tc := range []struct {
		name, method, protocol, body string
		err                          error
		status, calls                int
	}{
		{name: "method", method: http.MethodGet, status: 405},
		{name: "protocol", method: http.MethodPost, status: 412},
		{name: "json", method: http.MethodPost, protocol: protocolVersion, body: `{`, status: 400},
		{name: "zero node", method: http.MethodPost, protocol: protocolVersion, body: `{"state":"ok"}`, status: 400},
		{name: "oversized", method: http.MethodPost, protocol: protocolVersion, body: strings.Repeat(" ", maxDiskReportBytes) + `{}`, status: 400},
		{name: "trailing json", method: http.MethodPost, protocol: protocolVersion, body: `{"node":2} {}`, status: 400},
		{name: "invalid state", method: http.MethodPost, protocol: protocolVersion, body: `{"node":2,"state":"bad"}`, err: errors.New("invalid state"), status: 400, calls: 1},
		{name: "not leader", method: http.MethodPost, protocol: protocolVersion, body: `{"node":2,"state":"ok"}`, err: cluster.ErrNotLeader, status: 503, calls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr := New(1, nil, zap.NewNop(), &fakeRaft{}, nil, nil, "")
			defer tr.Stop()
			calls := 0
			tr.reportDisk = func(uint64, string) (cluster.DiskVerdict, error) { calls++; return cluster.DiskVerdict{}, tc.err }
			req := httptest.NewRequest(tc.method, diskReportPath, strings.NewReader(tc.body))
			req.Header.Set(clusterIDHeader, clusterID)
			req.Header.Set(protocolHeader, tc.protocol)
			rec := httptest.NewRecorder()
			tr.handler().ServeHTTP(rec, req)
			require.Equal(t, tc.status, rec.Code)
			require.Equal(t, tc.calls, calls)
		})
	}
}

func TestSendDiskReport(t *testing.T) {
	serverTransport := FactoryWithDiskReports(func(id uint64, state string) (cluster.DiskVerdict, error) {
		if id != 2 || state != "full" {
			return cluster.DiskVerdict{}, errors.New("unexpected report")
		}
		return cluster.DiskVerdict{State: "full", Reason: "leader disk full", LeaderID: 1}, nil
	})(1, nil, zap.NewNop(), &fakeRaft{}, nil, nil, "peer-secret").(*HttpTransport)
	defer serverTransport.Stop()
	server := httptest.NewTLSServer(serverTransport.handler())
	defer server.Close()
	client := New(2, nil, zap.NewNop(), &fakeRaft{}, nil, nil, "peer-secret")
	defer client.Stop()
	client.client = server.Client()
	require.NoError(t, client.AddPeer(raft.Peer{ID: 1, Context: []byte(server.URL)}))
	verdict, err := client.SendDiskReport(context.Background(), 1, "full")
	require.NoError(t, err)
	require.Equal(t, cluster.DiskVerdict{State: "full", Reason: "leader disk full", LeaderID: 1}, verdict)
	_, err = client.SendDiskReport(context.Background(), 3, "full")
	require.ErrorContains(t, err, "not registered")
	client.token = "api-secret"
	_, err = client.SendDiskReport(context.Background(), 1, "full")
	require.ErrorContains(t, err, "401")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = client.SendDiskReport(ctx, 1, "full")
	require.ErrorIs(t, err, context.Canceled)
}

func TestSendDiskReportRejectsRedirect(t *testing.T) {
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("disk reporter followed redirect")
	}))
	defer destination.Close()
	server := httptest.NewServer(http.RedirectHandler(destination.URL, http.StatusTemporaryRedirect))
	defer server.Close()
	client := New(2, nil, zap.NewNop(), &fakeRaft{}, nil, nil, "peer-secret")
	defer client.Stop()
	require.NoError(t, client.AddPeer(raft.Peer{ID: 1, Context: []byte(server.URL)}))
	_, err := client.SendDiskReport(context.Background(), 1, "ok")
	require.ErrorContains(t, err, "307")
}

func TestSendDiskReportInvalidResponse(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
	}{
		{name: "unavailable", status: 503},
		{name: "malformed", status: 200, body: `{`},
		{name: "oversized", status: 200, body: strings.Repeat(" ", maxDiskReportBytes) + `{}`},
		{name: "trailing data", status: 200, body: `{"state":"ok"} {}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			client := New(2, nil, zap.NewNop(), &fakeRaft{}, nil, nil, "")
			defer client.Stop()
			require.NoError(t, client.AddPeer(raft.Peer{ID: 1, Context: []byte(server.URL)}))
			_, err := client.SendDiskReport(context.Background(), 1, "ok")
			require.Error(t, err)
		})
	}
}

func TestStopCancelsDiskReport(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(started)
		<-r.Context().Done()
	}))
	defer server.Close()
	client := New(2, nil, zap.NewNop(), &fakeRaft{}, nil, nil, "")
	defer client.Stop()
	require.NoError(t, client.AddPeer(raft.Peer{ID: 1, Context: []byte(server.URL)}))
	result := make(chan error, 1)
	go func() { _, err := client.SendDiskReport(context.Background(), 1, "ok"); result <- err }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("report did not start")
	}
	client.Stop()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("Stop did not cancel report")
	}
}
