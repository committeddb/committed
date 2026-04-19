package db_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/db"
)

// TestPropose_OverSize_ReturnsTooLarge verifies that proposeAsync
// rejects oversize marshaled proposals with
// cluster.ErrProposalTooLarge before they reach raft. Checking post-
// Marshal catches every proposal source uniformly (HTTP, ingest,
// config propose). We drive it through Propose here because that's
// the public entry point; the check lives in proposeAsync so every
// wrapper sees the same failure.
func TestPropose_OverSize_ReturnsTooLarge(t *testing.T) {
	d := newFailFastDB(t, db.WithMaxProposalBytes(128))
	waitForStableLeader(t, d)

	// A single Data blob larger than the whole cap guarantees the
	// marshaled proposal exceeds 128 bytes regardless of protobuf
	// framing overhead.
	big := strings.Repeat("x", 512)
	p := &cluster.Proposal{Entities: []*cluster.Entity{{
		Type: &cluster.Type{ID: "t"},
		Key:  []byte("k"),
		Data: []byte(big),
	}}}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	err := d.Propose(ctx, p)
	require.Error(t, err)
	require.True(t, errors.Is(err, cluster.ErrProposalTooLarge),
		"expected ErrProposalTooLarge, got: %v", err)
}

// TestPropose_WithinSize_Succeeds is the happy path — a small
// proposal under the cap proposes successfully. Serves as the
// positive control for the size-check test above so a future
// regression in the wiring (e.g. the limit applied to every call
// regardless of size) is caught immediately.
func TestPropose_WithinSize_Succeeds(t *testing.T) {
	d := newFailFastDB(t, db.WithMaxProposalBytes(4096))
	waitForStableLeader(t, d)

	p := &cluster.Proposal{Entities: []*cluster.Entity{{
		Type: &cluster.Type{ID: "t"},
		Key:  []byte("k"),
		Data: []byte("small"),
	}}}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	require.NoError(t, d.Propose(ctx, p))
}
