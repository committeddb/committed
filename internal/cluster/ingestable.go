package cluster

import (
	"context"

	"github.com/philborlin/committed/internal/cluster/clusterpb"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/proto"
)

type Position []byte

//counterfeiter:generate . Ingestable
type Ingestable interface {
	// Start ingesting at position
	// During ingestion:
	// * write proposals to pr
	// * write position changes to po. How often to update the position is up to the Ingestable
	// * check the context for done after every proposal
	// Ingest MUST support being called multiple times
	Ingest(ctx context.Context, pos Position, pr chan<- *Proposal, po chan<- Position) error
	Close() error
}

//counterfeiter:generate . IngestableParser
type IngestableParser interface {
	Parse(*viper.Viper) (Ingestable, error)
}

var ingestableType = &Type{
	ID:      "c5917145-c248-4d97-a863-8e26ca042b09",
	Name:    "InternalIngestableParser",
	Version: 1,
}

func IsIngestable(id string) bool {
	return id == ingestableType.ID
}

func NewUpsertIngestableEntity(c *Configuration) (*Entity, error) {
	bs, err := c.Marshal()
	if err != nil {
		return nil, err
	}

	return NewUpsertEntity(ingestableType, []byte(c.ID), bs), nil
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
