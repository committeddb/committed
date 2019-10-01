package syncable

import (
	"github.com/spf13/viper"
)

type zeroDBParser struct{}

func (p *zeroDBParser) Parse(v *viper.Viper) (Database, error) {
	return &ZeroDB{}, nil
}

func init() {
	RegisterDBParser("test", &zeroDBParser{})
}

// ZeroDB returns a test implementation of the Database interface
type ZeroDB struct {
}

// Init implements Database
func (d *ZeroDB) Init() error {
	return nil
}

// Type implements Database
func (d *ZeroDB) Type() string {
	return "test"
}

// Close implements Database
func (d *ZeroDB) Close() error {
	return nil
}
