package file

import (
	"context"
	"fmt"
	"os"

	"github.com/philborlin/committed/syncable"
	"github.com/philborlin/committed/types"
	"github.com/spf13/viper"
)

type parser struct{}

func (p *parser) Parse(v *viper.Viper, dbs map[string]syncable.Database) (syncable.Syncable, error) {
	topic := v.GetString("file.topic")
	path := v.GetString("file.path")
	return &FileSyncable{topics: []string{topic}, path: path}, nil
}

func init() {
	syncable.RegisterParser("file", &parser{})
}

// FileSyncable is a syncable that syncs to a file
type FileSyncable struct {
	topics []string
	path   string
	file   *os.File
}

// Init implements Syncable
func (s *FileSyncable) Init(ctx context.Context) error {
	f, err := os.OpenFile(s.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	s.file = f
	return nil
}

// Sync implements Syncable
func (s *FileSyncable) Sync(ctx context.Context, entry *types.AcceptedProposal) error {
	str := fmt.Sprintf("%d,%d [%s] %v\n", entry.Index, entry.Term, entry.Topic, string(entry.Data))
	if _, err := s.file.WriteString(str); err != nil {
		return err
	}

	return nil
}

// Close implements Syncable
func (s *FileSyncable) Close() error {
	return s.file.Close()
}

// Topics implements Syncable
func (s *FileSyncable) Topics() []string {
	return s.topics
}
