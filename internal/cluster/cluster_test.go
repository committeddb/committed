package cluster_test

// import (
// 	"testing"

// 	"github.com/google/go-cmp/cmp"
// 	"github.com/philborlin/committed/internal/cluster"
// 	"github.com/philborlin/committed/internal/cluster/clusterfakes"
// )

// type proposeTest struct {
// 	input *cluster.Proposal
// 	want  []byte
// 	err   error
// }

// func TestPropose(t *testing.T) {
// 	simpleType := &cluster.Type{
// 		ID:      "43711d71-e1b1-4cbd-8040-4e82a5d1ff7a",
// 		Name:    "TestType",
// 		Version: 1,
// 	}

// 	simpleEntity := &cluster.Entity{simpleType, []byte{0}, []byte{1}}

// 	tests := map[string]*proposeTest{
// 		"simple": newProposeTestFromEntities(t, simpleEntity),
// 		"two":    newProposeTestFromEntities(t, simpleEntity, simpleEntity),
// 	}

// 	for name, tc := range tests {
// 		t.Run(name, func(t *testing.T) {
// 			p := &clusterfakes.FakeProposer{}
// 			c := cluster.New(p)
// 			err := c.Propose(tc.input)
// 			if tc.err == nil && err != nil {
// 				t.Fatal(err)
// 			}

// 			got := p.ProposeArgsForCall(0)
// 			diff := cmp.Diff(tc.want, got)
// 			if diff != "" {
// 				t.Fatalf(diff)
// 			}
// 		})
// 	}
// }

// func newProposeTestFromEntities(t *testing.T, es ...*cluster.Entity) *proposeTest {
// 	return newProposeTest(t, &cluster.Proposal{es})
// }

// func newProposeTest(t *testing.T, p *cluster.Proposal) *proposeTest {
// 	return &proposeTest{input: p, want: marshal(t, p)}
// }

// func marshal(t *testing.T, p *cluster.Proposal) []byte {
// 	logProposal := &cluster.LogProposal{}
// 	for _, e := range p.Entities {
// 		logProposal.LogEntities = append(logProposal.LogEntities, &cluster.LogEntity{
// 			TypeID: e.Type.ID,
// 			Key:    e.Key,
// 			Data:   e.Data,
// 		})
// 	}

// 	bs, err := cluster.Marshal(logProposal)
// 	if err != nil {
// 		t.Error(err)
// 		t.FailNow()
// 	}

// 	return bs
// }
