package db_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
)

// TestIngestRecovering_StatusReportsRecovering: while an ingest worker is frozen on
// a proposal it cannot commit and the supervisor is restarting it, GET
// /ingestable/{id}/status must report workerState "recovering" (node-local) rather
// than the live "streaming" phase a frozen-but-registered worker otherwise surfaces.
func TestIngestRecovering_StatusReportsRecovering(t *testing.T) {
	d, s := newWalDBOpts(t, db.WithMaxProposalBytes(4096))
	const id = "recovering"
	seedIngestableConfig(t, d, id)
	typ := oversizeTestTopic(t, d, s)

	ing := &oversizeIngestable{
		proposal: &cluster.Proposal{
			Entities: []*cluster.Entity{{
				Type: typ,
				Key:  []byte("k"),
				Data: append([]byte("v-"), make([]byte, 8192)...),
			}},
			Position: cluster.Position("cp"),
		},
		position: cluster.Position("cp-after"),
		handed:   make(chan struct{}),
	}
	require.NoError(t, d.Ingest(context.Background(), id, ing))

	<-ing.handed
	require.Eventually(t, func() bool {
		st, err := d.IngestableStatus(context.Background(), id)
		return err == nil && st.WorkerState == cluster.WorkerStateRecovering
	}, 3*time.Second, 20*time.Millisecond,
		"a frozen worker being restarted must report workerState=recovering")
}
