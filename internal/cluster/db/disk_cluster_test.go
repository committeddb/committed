package db

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

// voterSet builds the memberStatus-shaped voter map the verdict math takes.
func voterSet(ids ...uint64) map[uint64]struct{} {
	m := make(map[uint64]struct{}, len(ids))
	for _, id := range ids {
		m[id] = struct{}{}
	}
	return m
}

// freshReports builds a report map with every entry stamped now (fresh).
func freshReports(now time.Time, states map[uint64]diskState) map[uint64]diskReport {
	m := make(map[uint64]diskReport, len(states))
	for id, s := range states {
		m[id] = diskReport{state: s, at: now}
	}
	return m
}

const verdictTestTTL = 30 * time.Second

// TestDiskVerdictFrom_TicketScenarios pins the quorum math to the three
// target scenarios from the cluster-aware admission design (3-node cluster
// {L=1, F1=2, F2=3}, quorum 2): leader full → reject; one follower full with
// leader + a quorum healthy → admit; two followers full → reject before
// quorum is lost.
func TestDiskVerdictFrom_TicketScenarios(t *testing.T) {
	now := time.Now()
	voters := voterSet(1, 2, 3)

	cases := []struct {
		name       string
		local      diskState // node 1, the leader
		reports    map[uint64]diskState
		wantState  diskState
		wantReason string
	}{
		{
			name:       "leader full, followers healthy -> reject",
			local:      diskFull,
			reports:    map[uint64]diskState{2: diskOK, 3: diskOK},
			wantState:  diskFull,
			wantReason: "leader_disk",
		},
		{
			name:       "one follower full, leader + quorum healthy -> admit",
			local:      diskOK,
			reports:    map[uint64]diskState{2: diskOK, 3: diskFull},
			wantState:  diskOK,
			wantReason: "ok",
		},
		{
			name:       "two followers full, leader healthy -> reject (quorum at risk)",
			local:      diskOK,
			reports:    map[uint64]diskState{2: diskFull, 3: diskFull},
			wantState:  diskFull,
			wantReason: "quorum_at_risk",
		},
		{
			name:       "two followers critical -> reject user data only",
			local:      diskOK,
			reports:    map[uint64]diskState{2: diskCritical, 3: diskCritical},
			wantState:  diskCritical,
			wantReason: "quorum_at_risk",
		},
		{
			name:       "leader critical dominates a healthy quorum",
			local:      diskCritical,
			reports:    map[uint64]diskState{2: diskOK, 3: diskOK},
			wantState:  diskCritical,
			wantReason: "leader_disk",
		},
		{
			name:       "warn everywhere stays admitted",
			local:      diskWarn,
			reports:    map[uint64]diskState{2: diskWarn, 3: diskWarn},
			wantState:  diskWarn,
			wantReason: "ok",
		},
		{
			name:       "recovery: followers back to ok re-admits",
			local:      diskOK,
			reports:    map[uint64]diskState{2: diskOK, 3: diskOK},
			wantState:  diskOK,
			wantReason: "ok",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := diskVerdictFrom(1, tc.local, voters, freshReports(now, tc.reports), now, verdictTestTTL)
			require.Equal(t, tc.wantState, v.state)
			require.Equal(t, tc.wantReason, v.reasonCode)
			if tc.wantReason == "ok" {
				require.Empty(t, v.reason)
			} else {
				require.NotEmpty(t, v.reason)
			}
		})
	}
}

// TestDiskVerdictFrom_MissingAndStaleReportsFailOpen asserts the fail-open
// posture: a voter the leader has not heard from (crashed, partitioned, or
// just never reported) counts as healthy, so a silent member can't freeze a
// healthy cluster — the degraded mode is Phase 1's node-local behavior,
// never below it. Stale entries are also pruned from the map in place.
func TestDiskVerdictFrom_MissingAndStaleReportsFailOpen(t *testing.T) {
	now := time.Now()
	voters := voterSet(1, 2, 3)

	// No reports at all: verdict is the leader's own state.
	v := diskVerdictFrom(1, diskOK, voters, map[uint64]diskReport{}, now, verdictTestTTL)
	require.Equal(t, diskOK, v.state)

	// A stale full report is ignored (treated as ok) and pruned.
	reports := map[uint64]diskReport{
		2: {state: diskFull, at: now.Add(-2 * verdictTestTTL)},
		3: {state: diskFull, at: now},
	}
	v = diskVerdictFrom(1, diskOK, voters, reports, now, verdictTestTTL)
	require.Equal(t, diskOK, v.state, "one fresh full + one stale full = only one constrained voter, quorum healthy")
	require.NotContains(t, reports, uint64(2), "stale report should be pruned")

	// A report from a node that is no longer a voter is ignored and pruned.
	reports = map[uint64]diskReport{
		3: {state: diskFull, at: now},
		9: {state: diskFull, at: now},
	}
	v = diskVerdictFrom(1, diskOK, voters, reports, now, verdictTestTTL)
	require.Equal(t, diskOK, v.state)
	require.NotContains(t, reports, uint64(9), "departed-member report should be pruned")
}

// TestDiskVerdictFrom_FiveNodeQuorum exercises the q-th-healthiest selection
// beyond the 3-node table: with 5 voters (quorum 3), two constrained voters
// still admit, three reject.
func TestDiskVerdictFrom_FiveNodeQuorum(t *testing.T) {
	now := time.Now()
	voters := voterSet(1, 2, 3, 4, 5)

	v := diskVerdictFrom(1, diskOK, voters,
		freshReports(now, map[uint64]diskState{2: diskFull, 3: diskCritical, 4: diskOK, 5: diskOK}),
		now, verdictTestTTL)
	require.Equal(t, diskOK, v.state, "3 of 5 healthy is still a healthy quorum")

	v = diskVerdictFrom(1, diskOK, voters,
		freshReports(now, map[uint64]diskState{2: diskFull, 3: diskCritical, 4: diskCritical, 5: diskOK}),
		now, verdictTestTTL)
	require.Equal(t, diskCritical, v.state, "only 2 of 5 healthy: the 3rd-healthiest state (critical) gates")
	require.Equal(t, "quorum_at_risk", v.reasonCode)
}

// TestDiskVerdictFrom_NoVoters covers the not-yet-configured edge (a joining
// node before its add commits): the verdict degrades to the node-local state.
func TestDiskVerdictFrom_NoVoters(t *testing.T) {
	now := time.Now()
	v := diskVerdictFrom(1, diskCritical, nil, map[uint64]diskReport{}, now, verdictTestTTL)
	require.Equal(t, diskCritical, v.state)
	require.Equal(t, "leader_disk", v.reasonCode)
}

// TestPickDiskTransferTarget pins target selection: only voters with a
// fresh, below-critical report qualify (never the leader itself, never an
// assumed-ok silent member); most headroom wins, replication progress breaks
// ties.
func TestPickDiskTransferTarget(t *testing.T) {
	now := time.Now()
	voters := voterSet(1, 2, 3)

	// No reports at all: no confirmed-healthy target.
	require.Zero(t, pickDiskTransferTarget(1, voters, map[uint64]diskReport{}, nil, now, verdictTestTTL))

	// All reporters constrained: no target.
	require.Zero(t, pickDiskTransferTarget(1, voters,
		freshReports(now, map[uint64]diskState{2: diskCritical, 3: diskFull}), nil, now, verdictTestTTL))

	// A stale ok report does not qualify.
	require.Zero(t, pickDiskTransferTarget(1, voters,
		map[uint64]diskReport{2: {state: diskOK, at: now.Add(-2 * verdictTestTTL)}}, nil, now, verdictTestTTL))

	// Most headroom wins: ok beats warn.
	got := pickDiskTransferTarget(1, voters,
		freshReports(now, map[uint64]diskState{2: diskWarn, 3: diskOK}), nil, now, verdictTestTTL)
	require.Equal(t, uint64(3), got)

	// Equal headroom: higher match index wins.
	match := map[uint64]uint64{2: 100, 3: 40}
	got = pickDiskTransferTarget(1, voters,
		freshReports(now, map[uint64]diskState{2: diskOK, 3: diskOK}), match, now, verdictTestTTL)
	require.Equal(t, uint64(2), got)

	// A non-voter (learner) report never qualifies, even if healthiest.
	got = pickDiskTransferTarget(1, voterSet(1, 2),
		freshReports(now, map[uint64]diskState{2: diskWarn, 7: diskOK}), nil, now, verdictTestTTL)
	require.Equal(t, uint64(2), got)
}

// TestParseDiskState_RoundTrips pins the wire vocabulary: every level the
// watcher can publish parses back to itself, and junk is rejected.
func TestParseDiskState_RoundTrips(t *testing.T) {
	for _, s := range []diskState{diskOK, diskWarn, diskCritical, diskFull} {
		got, ok := parseDiskState(s.String())
		require.True(t, ok)
		require.Equal(t, s, got)
	}
	_, ok := parseDiskState("toasty")
	require.False(t, ok)
}

// TestReportDisk_NotLeader asserts a non-leader rejects reports with the
// typed error (the HTTP layer maps it to 503 leader_unavailable) so the
// reporter re-resolves the leader instead of poisoning a follower's map.
func TestReportDisk_NotLeader(t *testing.T) {
	d := &DB{raft: &Raft{}, leaderState: NewLeaderState(false)}
	_, err := d.ReportDisk(2, "ok")
	require.ErrorIs(t, err, cluster.ErrNotLeader)
}

// TestMaybeTransferLeadership pins the transfer trigger: fires only when the
// leader's own disk is at least critical AND a confirmed-healthy target
// exists, and successive transfers respect the cooldown.
func TestMaybeTransferLeadership(t *testing.T) {
	now := time.Now()
	var transferred []uint64
	target := uint64(2)
	d := &DB{logger: zap.NewNop(), diskTransferCooldown: time.Minute}
	d.pickTransferTargetFn = func(time.Time) uint64 { return target }
	d.transferLeadershipFn = func(id uint64) { transferred = append(transferred, id) }

	// Healthy local disk: never transfers, even with a target available.
	d.diskState.Store(int32(diskOK))
	d.maybeTransferLeadership(now, nil)
	require.Empty(t, transferred)

	// Critical local disk + healthy target: transfers.
	d.diskState.Store(int32(diskCritical))
	d.maybeTransferLeadership(now, nil)
	require.Equal(t, []uint64{2}, transferred)

	// Within the cooldown: rate-limited, no second transfer.
	d.maybeTransferLeadership(now.Add(30*time.Second), nil)
	require.Len(t, transferred, 1)

	// After the cooldown (still constrained): transfers again.
	d.maybeTransferLeadership(now.Add(2*time.Minute), nil)
	require.Len(t, transferred, 2)

	// No healthy target: keeps leadership rather than gambling.
	target = 0
	d.maybeTransferLeadership(now.Add(10*time.Minute), nil)
	require.Len(t, transferred, 2)
}
