package wal

import (
	"errors"
	"fmt"
	"time"

	"github.com/nakabonne/tstorage"

	"github.com/philborlin/committed/internal/cluster"
)

// handleUserDefined records a time-series point for a user-defined entity.
// The timestamp comes from e.Timestamp, which every propose path stamps
// once when the proposal is built. Reading it here (instead of calling
// time.Now() at apply) is what makes apply content-deterministic across
// nodes: every replica writes the same value into the time-series store,
// and post-snapshot replay reproduces the original timestamp instead of
// computing a new one.
//
// There is no time.Now() fallback. Apply must be deterministic given the
// same raft entry — see docs/event-log-architecture.md
// § "Determinism requirement". A propose path that forgets to stamp will
// land its row at unix epoch (0) on every node, which is deterministic
// but visible enough in time-series queries that the bug is easy to spot.
// Pre-PR4 entries that exist on disk with Timestamp == 0 are skipped on
// restart by the persisted appliedIndex, so this never re-applies them.
func (s *Storage) handleUserDefined(e *cluster.Entity) error {
	if err := s.TimeSeriesStorage.InsertRows([]tstorage.Row{
		{
			Metric:    e.Type.ID,
			Labels:    []tstorage.Label{},
			DataPoint: tstorage.DataPoint{Timestamp: e.Timestamp, Value: 1},
		},
	}); err != nil {
		return err
	}
	// Advance the lag-gauge cursor to the newest point written. Called only
	// from the single apply / rebuild goroutine, so a plain compare-and-store
	// (no CAS loop) is safe; the atomic is for race-free metric sampling.
	if e.Timestamp > s.tsLatestUnixNano.Load() {
		s.tsLatestUnixNano.Store(e.Timestamp)
	}
	return nil
}

func (s *Storage) TimePoints(typeID string, start time.Time, end time.Time) ([]cluster.TimePoint, error) {
	_, err := s.ResolveType(cluster.LatestTypeRef(typeID))
	if err != nil {
		return nil, err
	}

	points, err := s.TimeSeriesStorage.Select(typeID, []tstorage.Label{}, start.UnixMilli(), end.UnixMilli())
	if err != nil && !errors.Is(err, tstorage.ErrNoDataPoints) {
		return nil, err
	}

	timePoints, err := timePoints(start, end)
	if err != nil {
		return nil, err
	}

	for _, point := range points {
		for i, tp := range timePoints {
			if point.Timestamp >= tp.Start.UnixMilli() && point.Timestamp < tp.End.UnixMilli() {
				timePoints[i].Value++
				break
			}
		}
	}

	return timePoints, nil
}

// getTimePoints divides the time range into 100 non-overlapping intervals and returns them as a slice of TimePoint.
func timePoints(startTime time.Time, endTime time.Time) ([]cluster.TimePoint, error) {
	if endTime.Before(startTime) {
		return nil, fmt.Errorf("endTime cannot be before startTime")
	}

	duration := endTime.Sub(startTime)
	intervals, err := divideDuration(duration)
	if err != nil {
		return nil, err
	}

	result := make([]cluster.TimePoint, 100)
	currentStart := startTime

	for i, interval := range intervals {
		currentEnd := currentStart.Add(interval)
		result[i] = cluster.TimePoint{
			Start: currentStart,
			End:   currentEnd,
		}
		currentStart = currentEnd
	}

	return result, nil
}

func divideDuration(d time.Duration) ([]time.Duration, error) {
	if d < 0 {
		return nil, fmt.Errorf("duration cannot be negative")
	}

	chunk := d / 100
	remainder := d % 100

	result := make([]time.Duration, 100)

	for i := range result {
		result[i] = chunk
		// Distribute the remainder among the first few chunks
		if i < int(remainder) {
			result[i]++
		}
	}

	return result, nil
}
