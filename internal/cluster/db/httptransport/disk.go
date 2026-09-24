package httptransport

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/committeddb/committed/internal/cluster"
)

const (
	diskReportPath     = "/raft/disk-report"
	maxDiskReportBytes = 4096
)

type diskReport struct {
	Node  uint64 `json:"node"`
	State string `json:"state"`
}

type diskVerdict struct {
	State  string `json:"state"`
	Reason string `json:"reason"`
	Leader uint64 `json:"leader"`
}

func (t *HttpTransport) handleDiskReport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if r.Header.Get(clusterIDHeader) != clusterID || r.Header.Get(protocolHeader) != protocolVersion {
		http.Error(w, "wrong cluster or protocol", http.StatusPreconditionFailed)
		return
	}
	if t.token != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+t.token)) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if t.reportDisk == nil {
		http.Error(w, "disk coordinator unavailable", http.StatusServiceUnavailable)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxDiskReportBytes))
	var report diskReport
	if err != nil || json.Unmarshal(body, &report) != nil || report.Node == 0 {
		http.Error(w, "invalid disk report", http.StatusBadRequest)
		return
	}
	verdict, err := t.reportDisk(report.Node, report.State)
	if err != nil {
		if errors.Is(err, cluster.ErrNotLeader) {
			http.Error(w, "disk reports require the current leader", http.StatusServiceUnavailable)
		} else {
			message, _ := cluster.RedactedMessage(err)
			http.Error(w, message, http.StatusBadRequest)
		}
		return
	}
	w.Header().Set("Content-Type", "application/json")
	// Encoding these scalar fields cannot fail; a write failure ends the exchange.
	_ = json.NewEncoder(w).Encode(diskVerdict{State: verdict.State, Reason: verdict.Reason, Leader: verdict.LeaderID})
}

// SendDiskReport sends this node's disk state to the registered leader's peer
// URL, using the same TLS client and bearer token as Raft messages. The caller
// retries against its current leader on the next reporting cycle after errors.
func (t *HttpTransport) SendDiskReport(ctx context.Context, leader uint64, state string) (cluster.DiskVerdict, error) {
	t.mu.RLock()
	p := t.peers[leader]
	t.mu.RUnlock()
	if p == nil {
		return cluster.DiskVerdict{}, fmt.Errorf("disk report: peer %d is not registered", leader)
	}
	target, err := url.JoinPath(p.url, diskReportPath)
	if err != nil {
		return cluster.DiskVerdict{}, err
	}
	body, err := json.Marshal(diskReport{Node: t.id, State: state})
	if err != nil {
		return cluster.DiskVerdict{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	stop := context.AfterFunc(t.baseCtx, cancel)
	defer stop()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return cluster.DiskVerdict{}, err
	}
	req.Header.Set(clusterIDHeader, clusterID)
	req.Header.Set(protocolHeader, protocolVersion)
	req.Header.Set("Content-Type", "application/json")
	if t.token != "" {
		req.Header.Set("Authorization", "Bearer "+t.token)
	}
	// A peer redirect must not carry the infrastructure credential elsewhere.
	client := *t.client
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := client.Do(req)
	if err != nil {
		return cluster.DiskVerdict{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return cluster.DiskVerdict{}, fmt.Errorf("disk report: peer returned HTTP %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxDiskReportBytes+1))
	if err != nil {
		return cluster.DiskVerdict{}, err
	}
	if len(data) > maxDiskReportBytes {
		return cluster.DiskVerdict{}, errors.New("disk report: verdict exceeds size limit")
	}
	var verdict diskVerdict
	if err := json.Unmarshal(data, &verdict); err != nil {
		return cluster.DiskVerdict{}, err
	}
	return cluster.DiskVerdict{State: verdict.State, Reason: verdict.Reason, LeaderID: verdict.Leader}, nil
}
