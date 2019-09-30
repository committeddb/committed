package syncable

import (
	"context"

	"github.com/philborlin/committed/types"
	"github.com/spf13/viper"
)

func testParser(v *viper.Viper, databases map[string]Database) (Syncable, error) {
	topic := v.GetString("test.topic")
	return &TestSyncable{topics: []string{topic}}, nil
}

// TestSyncable returns a test implementation of the Syncable interface
type TestSyncable struct {
	topics []string
}

// Init implements Syncable
func (s *TestSyncable) Init(ctx context.Context) error {
	return nil
}

// Sync implements Syncable
func (s *TestSyncable) Sync(ctx context.Context, entry *types.AcceptedProposal) error {
	return nil
}

// Close implements Syncable
func (s *TestSyncable) Close() error {
	return nil
}

// Topics implements Syncable
func (s *TestSyncable) Topics() []string {
	return s.topics
}
