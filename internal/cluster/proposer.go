package cluster

//counterfeiter:generate . Proposer
type Proposer interface {
	Propose(p *LogProposal) error
}
