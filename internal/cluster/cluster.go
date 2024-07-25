package cluster

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate

type Cluster struct {
	p Proposer
}

func New(p Proposer) *Cluster {
	return &Cluster{p: p}
}

// func (c *Cluster) Propose(p *Proposal) error {
// 	stateAppendProposal := &LogProposal{}
// 	for _, e := range p.Entities {
// 		stateAppendProposal.LogEntities = append(stateAppendProposal.LogEntities, &LogEntity{
// 			TypeID: e.Type.ID,
// 			Key:    e.Key,
// 			Data:   e.Data,
// 		})
// 	}

// 	bs, err := Marshal(stateAppendProposal)
// 	if err != nil {
// 		return err
// 	}

// 	return c.p.Propose(bs)
// }
