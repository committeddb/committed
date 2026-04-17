package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseBatchSize(t *testing.T) {
	tests := []struct {
		name string
		opts map[string]string
		want int
	}{
		{"nil_options", nil, defaultSnapshotBatchSize},
		{"missing_key", map[string]string{"other": "v"}, defaultSnapshotBatchSize},
		{"valid_override", map[string]string{"batch_size": "250"}, 250},
		{"invalid_non_numeric", map[string]string{"batch_size": "xyz"}, defaultSnapshotBatchSize},
		{"zero_falls_back", map[string]string{"batch_size": "0"}, defaultSnapshotBatchSize},
		{"negative_falls_back", map[string]string{"batch_size": "-42"}, defaultSnapshotBatchSize},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, parseBatchSize(tt.opts))
		})
	}
}
