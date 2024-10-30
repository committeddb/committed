package cluster

type Position []byte

//counterfeiter:generate . Ingestable
type Ingestable interface {
	Ingest(pos Position) (<-chan *Proposal, <-chan Position, error)
	Close() error
}
