package wal_test

import (
	"testing"
	"time"

	"github.com/nakabonne/tstorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/philborlin/committed/internal/cluster"
)

type GetTimePointsTest struct {
	name     string
	times    []time.Time
	expected []uint64
}

func TestGetTimePoints(t *testing.T) {
	metric := "foo"
	otherMetric := "bar"
	startTime := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	endTime := startTime.Add(time.Second * 100)

	tests := []GetTimePointsTest{
		zero(),
		simple(startTime),
		boundaries(startTime),
		ignoreOutOfBounds(startTime),
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewStorage(t, index(3).terms(3, 4, 5))
			defer s.Cleanup()

			// TimePoints validates the type exists via s.ResolveType(cluster.LatestTypeRef(typeID)), so we
			// must register both metrics as types before exercising it.
			insertTypes(t, s, []*cluster.Type{
				{ID: metric, Name: metric},
				{ID: otherMetric, Name: otherMetric},
			}, 6, 6)

			var rows []tstorage.Row
			for _, t := range tt.times {
				rows = append(rows, tstorage.Row{Metric: metric, DataPoint: tstorage.DataPoint{Value: 1, Timestamp: t.UnixMilli()}})
			}
			// Add a row into a different metric within the tested time frame just to make sure it doesn't get picked up
			rows = append(rows, tstorage.Row{Metric: otherMetric, DataPoint: tstorage.DataPoint{Value: 1, Timestamp: startTime.Add(time.Second).UnixMilli()}})

			err := s.TimeSeriesStorage.InsertRows(rows)
			require.Nil(t, err)

			timePoints, err := s.TimePoints(metric, startTime, endTime)
			assert.Nil(t, err)

			for i, tp := range timePoints {
				assert.Equal(t, tt.expected[i], tp.Value)
			}
		})
	}
}

func append90Zeros(array []uint64) []uint64 {
	for i := 0; i < 90; i++ {
		array = append(array, 0)
	}

	return array
}

func zero() GetTimePointsTest {
	times := []time.Time{}
	expected := []uint64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	expected = append90Zeros(expected)
	return GetTimePointsTest{"zero", times, expected}
}

func simple(startTime time.Time) GetTimePointsTest {
	times := []time.Time{
		startTime.Add(time.Second * 2),
		startTime.Add(time.Second * 5),
		startTime.Add(time.Second * 8),
	}

	expected := []uint64{0, 0, 1, 0, 0, 1, 0, 0, 1, 0}
	expected = append90Zeros(expected)
	return GetTimePointsTest{"simple", times, expected}
}

func boundaries(startTime time.Time) GetTimePointsTest {
	times := []time.Time{
		startTime.Add(time.Millisecond * 100),
		startTime.Add(time.Millisecond * 200),
		startTime.Add(time.Second * 2),
		startTime.Add(time.Second * 5),
		startTime.Add(time.Second * 8),
		startTime.Add(time.Second*99 + time.Millisecond*100),
		startTime.Add(time.Second*99 + time.Millisecond*200),
	}

	expected := []uint64{2, 0, 1, 0, 0, 1, 0, 0, 1}
	expected = append90Zeros(expected)
	expected = append(expected, 2)
	return GetTimePointsTest{"boundaries", times, expected}
}

// TestHandleUserDefined_TimestampDeterministic verifies that
// handleUserDefined honors Entity.Timestamp set by the propose path
// instead of recomputing time.Now() at apply. This is what makes the
// time-series store content-deterministic across nodes (and across
// post-snapshot replays): every replica writes the same timestamp.
//
// The test stamps an entity with a fixed Timestamp far enough in the
// past that wall-clock interference is impossible, applies it through
// the full Save→ApplyCommitted path (so handleUserDefined is what
// inserts the row), and queries TimePoints over a window that
// includes only the stamped time. The expected bucket count must be
// 1 — if handleUserDefined fell back to time.Now() the row would
// land outside the window and the bucket would be 0.
func TestHandleUserDefined_TimestampDeterministic(t *testing.T) {
	s := NewStorage(t, index(3).terms(3, 4, 5))
	defer s.Cleanup()

	const metric = "user-metric"
	insertTypes(t, s, []*cluster.Type{{ID: metric, Name: metric}}, 6, 6)

	// Pick a fixed reference time well in the past so wall-clock can't
	// accidentally hit it. UTC for stability across machines.
	stamped := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)

	entity := &cluster.Entity{
		Type:      &cluster.Type{ID: metric},
		Key:       []byte("k"),
		Data:      []byte("v"),
		Timestamp: stamped.UnixMilli(),
	}
	saveEntity(t, entity, s, 7, 7)

	// Query a 100-second window centered on the stamped time so we can
	// see exactly which bucket the row lands in. timePoints() returns
	// 100 buckets across the range, so the row should land in bucket 50.
	windowStart := stamped.Add(-50 * time.Second)
	windowEnd := stamped.Add(50 * time.Second)
	points, err := s.TimePoints(metric, windowStart, windowEnd)
	require.Nil(t, err)
	require.Len(t, points, 100)

	// The single inserted row must be in exactly one bucket within the
	// window. If handleUserDefined had used time.Now(), no bucket
	// inside our 2024 window would contain the row.
	var total uint64
	for _, p := range points {
		total += p.Value
	}
	require.Equal(t, uint64(1), total,
		"expected exactly 1 stored time-series row inside the stamped-time window; "+
			"if handleUserDefined fell back to time.Now() the row would land outside the window")
}

// Removed: TestHandleUserDefined_TimestampZeroFallback. It existed to
// characterize the (now-removed) time.Now() fallback in handleUserDefined.
// Cross-node determinism is now covered by TestApplyDeterminism in
// determinism_test.go, which is the load-bearing assertion.

func ignoreOutOfBounds(startTime time.Time) GetTimePointsTest {
	times := []time.Time{
		startTime.Add(-time.Millisecond * 100),
		startTime.Add(-time.Millisecond * 200),
		startTime.Add(time.Second * 2),
		startTime.Add(time.Second * 5),
		startTime.Add(time.Second * 8),
		startTime.Add(time.Second*100 + time.Millisecond*100),
		startTime.Add(time.Second*100 + time.Millisecond*200),
	}

	expected := []uint64{0, 0, 1, 0, 0, 1, 0, 0, 1, 0}
	expected = append90Zeros(expected)
	return GetTimePointsTest{"ignoreOutOfBounds", times, expected}
}
