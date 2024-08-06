package cluster

//counterfeiter:generate . ProposalReader
type ProposalReader interface {
	Read() (*Proposal, error)
}
