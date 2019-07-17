package types

// Proposal is an item to put on a raft log
type Proposal struct {
	Topic    string
	Proposal string
}
