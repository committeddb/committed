package cluster

import (
	"github.com/philborlin/committed/internal/cluster/clusterpb"
	"google.golang.org/protobuf/proto"
)

type Position []byte

//counterfeiter:generate . Ingestable
type Ingestable interface {
	Ingest(pos Position) (<-chan *Proposal, <-chan Position, error)
	Close() error
}

var ingestablePositionType = &Type{
	ID:      "8ea60a68-e22a-41cd-b09d-31352b0356f1",
	Name:    "InternalIngestablePosition",
	Version: 1,
}

type IngestablePosition struct {
	ID       string
	Position []byte
}

func (i *IngestablePosition) Marshal() ([]byte, error) {
	li := &clusterpb.LogIngestablePosition{ID: i.ID, Position: i.Position}
	return proto.Marshal(li)
}

func (i *IngestablePosition) Unmarshal(bs []byte) error {
	li := &clusterpb.LogIngestablePosition{}
	err := proto.Unmarshal(bs, li)
	if err != nil {
		return err
	}

	i.ID = li.ID
	i.Position = li.Position

	return nil
}

func IsIngestablePosition(id string) bool {
	return id == ingestablePositionType.ID
}

func NewUpsertIngestablePositionEntity(p *IngestablePosition) (*Entity, error) {
	bs, err := p.Marshal()
	if err != nil {
		return nil, err
	}

	return NewUpsertEntity(ingestablePositionType, []byte(p.ID), bs), nil
}
