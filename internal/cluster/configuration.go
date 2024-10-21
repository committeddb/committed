package cluster

import (
	"github.com/philborlin/committed/internal/cluster/clusterpb"
	"google.golang.org/protobuf/proto"
)

type Configuration struct {
	ID       string
	MimeType string
	Data     []byte
}

func (c *Configuration) Marshal() ([]byte, error) {
	lc := &clusterpb.LogConfiguration{ID: c.ID, MimeType: c.MimeType, Data: c.Data}
	return proto.Marshal(lc)
}

func (c *Configuration) Unmarshal(bs []byte) error {
	lc := &clusterpb.LogConfiguration{}
	err := proto.Unmarshal(bs, lc)
	if err != nil {
		return err
	}

	c.ID = lc.ID
	c.MimeType = lc.MimeType
	c.Data = lc.Data

	return nil
}
