package wal

import (
	"errors"
	"fmt"
	"time"

	"github.com/nakabonne/tstorage"
	"github.com/philborlin/committed/internal/cluster"
)

type TimePoint struct {
	Start time.Time
	End   time.Time
	Value uint64
}

func (s *Storage) handleUserDefined(e *cluster.Entity) error {
	return s.TimeSeriesStorage.InsertRows([]tstorage.Row{
		{
			Metric:    e.Type.ID,
			Labels:    []tstorage.Label{},
			DataPoint: tstorage.DataPoint{Timestamp: time.Now().UnixMilli(), Value: 1},
		},
	})
}

func (s *Storage) GetTimePoints(typeID string, start time.Time, end time.Time) ([]TimePoint, error) {
	// end := time.Now()
	// start := end.Add(-time.Duration(hours) * time.Hour)

	points, err := s.TimeSeriesStorage.Select(typeID, []tstorage.Label{}, start.UnixMilli(), end.UnixMilli())
	if err != nil && !errors.Is(err, tstorage.ErrNoDataPoints) {
		return nil, err
	}

	timePoints, err := getTimePoints(start, end)
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
func getTimePoints(startTime time.Time, endTime time.Time) ([]TimePoint, error) {
	if endTime.Before(startTime) {
		return nil, fmt.Errorf("endTime cannot be before startTime")
	}

	duration := endTime.Sub(startTime)
	intervals, err := divideDuration(duration)
	if err != nil {
		return nil, err
	}

	result := make([]TimePoint, 100)
	currentStart := startTime

	for i, interval := range intervals {
		currentEnd := currentStart.Add(interval)
		result[i] = TimePoint{
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