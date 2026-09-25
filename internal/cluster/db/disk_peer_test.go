package db_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/db"
	"github.com/committeddb/committed/internal/cluster/db/httptransport"
	"github.com/committeddb/committed/internal/cluster/db/parser"
)

// No API listener or advertised API URL exists in this cluster. Both Raft
// replication and follower disk admission must use the authenticated peer link.
func TestPeerDiskReportsWithoutAPI(t *testing.T) {
	ports := pickFreePorts(3)
	peers := db.Peers{}
	for i, port := range ports {
		peers[uint64(i+1)] = fmt.Sprintf("http://127.0.0.1:%d", port)
	}
	nodes := make([]*db.DB, 0, 3)
	for id := uint64(1); id <= 3; id++ {
		d := db.New(id, peers, NewMemoryStorage(), parser.New(), nil, nil,
			db.WithTransportFactory(httptransport.Factory()), db.WithPeerToken("peer-secret"),
			db.WithPeerDiskReports(), db.WithTickInterval(testTickInterval), db.WithDiskReportInterval(20*time.Millisecond))
		t.Cleanup(func() { require.NoError(t, d.Close()) })
		nodes = append(nodes, d)
	}
	require.Eventually(t, func() bool {
		leader := nodes[0].Leader()
		if leader == 0 {
			return false
		}
		for _, d := range nodes {
			adm := d.DiskAdmission()
			if d.Leader() != leader || adm.Source != "cluster" || adm.LeaderID != leader {
				return false
			}
		}
		return true
	}, 10*time.Second, 10*time.Millisecond)
	// Keep every voter under equal pressure so leadership transfer cannot find
	// a healthier candidate. Followers must receive the leader's full verdict.
	for _, d := range nodes {
		d.SetLocalDiskStateForTest("full")
	}
	require.Eventually(t, func() bool {
		for _, d := range nodes {
			adm := d.DiskAdmission()
			if adm.Source != "cluster" || adm.State != "full" || adm.Admitted {
				return false
			}
		}
		return true
	}, 5*time.Second, 10*time.Millisecond)
	for _, d := range nodes {
		d.SetLocalDiskStateForTest("ok")
	}
	require.Eventually(t, func() bool {
		for _, d := range nodes {
			adm := d.DiskAdmission()
			if adm.Source != "cluster" || adm.State != "ok" || !adm.Admitted {
				return false
			}
		}
		return true
	}, 5*time.Second, 10*time.Millisecond)
}
