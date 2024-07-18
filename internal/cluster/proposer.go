package cluster

//counterfeiter:generate . Proposer
type Proposer interface {
	Propose(b []byte) error
}
