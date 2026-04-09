package wal

import (
	"errors"
	"fmt"
	"time"

	"github.com/nakabonne/tstorage"
	"github.com/philborlin/committed/internal/cluster"
)

// handleUserDefined records a time-series point for a user-defined entity.
// The timestamp comes from e.Timestamp, which the propose path stamps
// once when the proposal is built. Reading it here (instead of calling
// time.Now() at apply) makes apply content-deterministic across nodes:
// every replica writes the same value into the time-series store, and
// post-snapshot replay reproduces the original timestamp instead of
// computing a new one.
//
// Pre-PR4 entries (and any propose path that forgets to stamp) arrive
// with Timestamp == 0. We fall back to time.Now() in that case to
// preserve the old behavior for those entries. This fallback is
// non-deterministic, but it only affects entries written before this
// fix landed AND wal.Storage.ApplyCommitted skips re-apply via the
// persisted appliedIndex, so an old entry only goes through
// handleUserDefined once (on its original apply, when the old code
// was still using time.Now() anyway). New propose paths all stamp at
// propose time and never hit the fallback.
func (s *Storage) handleUserDefined(e *cluster.Entity) error {
	ts := e.Timestamp
	if ts == 0 {
		ts = time.Now().UnixMilli()
	}
	return s.TimeSeriesStorage.InsertRows([]tstorage.Row{
		{
			Metric:    e.Type.ID,
			Labels:    []tstorage.Label{},
			DataPoint: tstorage.DataPoint{Timestamp: ts, Value: 1},
		},
	})
}

func (s *Storage) TimePoints(typeID string, start time.Time, end time.Time) ([]cluster.TimePoint, error) {
	_, err := s.Type(typeID)
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
