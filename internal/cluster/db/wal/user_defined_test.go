package wal_test

import (
	"testing"
	"time"

	"github.com/nakabonne/tstorage"
	"github.com/philborlin/committed/internal/cluster/db/wal"
	"github.com/stretchr/testify/assert"
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
			timeSeriesStorage, err := tstorage.NewStorage(
				tstorage.WithTimestampPrecision(tstorage.Milliseconds),
			)
			assert.Nil(t, err)

			s := &wal.Storage{TimeSeriesStorage: timeSeriesStorage}

			var rows []tstorage.Row
			for _, t := range tt.times {
				rows = append(rows, tstorage.Row{Metric: metric, DataPoint: tstorage.DataPoint{Value: 1, Timestamp: t.UnixMilli()}})
			}
			// Add a row into a different metric within the tested time frame just to make sure it doesn't get picked up
			rows = append(rows, tstorage.Row{Metric: otherMetric, DataPoint: tstorage.DataPoint{Value: 1, Timestamp: startTime.Add(time.Second).UnixMilli()}})

			err = timeSeriesStorage.InsertRows(rows)
			assert.Nil(t, err)

			timePoints, err := s.GetTimePoints(metric, startTime, endTime)
			assert.Nil(t, err)

			for i, tp := range timePoints {
				assert.Equal(t, tp.Value, tt.expected[i])
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
