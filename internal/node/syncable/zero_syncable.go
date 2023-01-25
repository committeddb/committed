package syncable

import (
	"context"

	"github.com/philborlin/committed/internal/node/types"
	"github.com/spf13/viper"
)

type parser struct{}

func (p *parser) Parse(v *viper.Viper, dbs map[string]Database) (Syncable, error) {
	topic := v.GetString("test.topic")
	return &ZeroSyncable{topics: []string{topic}}, nil
}

func init() {
	RegisterParser("test", &parser{})
}

// ZeroSyncable returns a test implementation of the Syncable interface
type ZeroSyncable struct {
	topics []string
}

// Init implements Syncable
func (s *ZeroSyncable) Init(ctx context.Context) error {
	return nil
}

// Sync implements Syncable
func (s *ZeroSyncable) Sync(ctx context.Context, entry *types.AcceptedProposal) error {
	return nil
}

// Close implements Syncable
func (s *ZeroSyncable) Close() error {
	return nil
}

// Topics implements Syncable
func (s *ZeroSyncable) Topics() []string {
	return s.topics
}
