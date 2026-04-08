package wal

import (
	"errors"
	"fmt"
	"time"

	"github.com/nakabonne/tstorage"
	"github.com/philborlin/committed/internal/cluster"
)

// handleUserDefined records a time-series point for a user-defined entity.
//
// TODO: time.Now() makes apply non-deterministic in entry content. Two
// consequences once the cluster is multi-node: followers and the leader
// will compute different timestamps for the same entry, and a snapshot
// install followed by replay would write a *new* timestamp. Today we are
// single node and persist appliedIndex (see wal.Storage.ApplyCommitted) to
// skip replay, so this is masked. The right fix is to add a Timestamp
// field to the LogEntity protobuf so the proposer records the wall-clock
// time once and every node reads the same value. Deferred from PR1 to keep
// scope down — see .claude-scratch/tickets/save-does-double-duty.md (PR1
// "Out of scope") for the reasoning.
func (s *Storage) handleUserDefined(e *cluster.Entity) error {
	return s.TimeSeriesStorage.InsertRows([]tstorage.Row{
		{
			Metric:    e.Type.ID,
			Labels:    []tstorage.Label{},
			DataPoint: tstorage.DataPoint{Timestamp: time.Now().UnixMilli(), Value: 1},
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
