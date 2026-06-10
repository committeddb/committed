package cluster

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/clusterpb"
)

type Position []byte

// Ingestable pulls changes from an external source (e.g. a SQL database's
// change-data-capture stream) and emits them as Proposals into the log. It is
// the producing counterpart of Syncable: an Ingestable writes Proposals; a
// Syncable consumes the resulting Actuals.
//
// Contract:
//   - Ingest MUST emit deletes. When the source signals that a row was
//     removed (a CDC DELETE), the Ingestable MUST emit a delete entity
//     (NewDeleteEntity) keyed by that row's primary key — NOT an upsert of
//     the row's pre-image. This is mandatory for the same reason honoring
//     deletes is mandatory for a Syncable (see the Syncable contract): the
//     entity flows source → log → Syncable, and only a delete entity makes
//     the Syncable remove the downstream record. An Ingestable that forwards a
//     source delete as an upsert of the old row leaves the deleted data live
//     in every projection forever — a right-to-be-forgotten zombie. The
//     delete's Key MUST equal the key an upsert of that same row uses, so the
//     downstream delete targets the right record.
//   - During ingestion, write Proposals to pr and position checkpoints to po;
//     how often to checkpoint the position is up to the Ingestable.
//   - Check ctx for done after every proposal and stop promptly when it fires.
//   - Ingest MUST support being called multiple times (resume from pos).
//
//counterfeiter:generate . Ingestable
type Ingestable interface {
	Ingest(ctx context.Context, pos Position, pr chan<- *Proposal, po chan<- Position) error
	Close() error
}

//counterfeiter:generate . IngestableParser
type IngestableParser interface {
	Parse(c *ParsedConfig) (Ingestable, error)
}

var ingestableType = registerSystemType(&Type{
	ID:      "c5917145-c248-4d97-a863-8e26ca042b09",
	Name:    "InternalIngestableParser",
	Version: 1,
})

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

var ingestablePositionType = registerSystemType(&Type{
	ID:      "8ea60a68-e22a-41cd-b09d-31352b0356f1",
	Name:    "InternalIngestablePosition",
	Version: 1,
})

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
