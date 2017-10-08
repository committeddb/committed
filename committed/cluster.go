package committed

// Cluster is an engine that spawns and maintains topics
type Cluster struct {
	topics []Topic
}

func NewCluster() *Cluster {
	return &Cluster{}
}

func (c *Cluster) CreateTopic(nodeCount int) *Topic {
	return newTopic(nodeCount)
}

// func createPeers(ids ...uint64) []raft.Peer {
// 	peers := make([]raft.Peer, 0)

// 	for i := 0; i < len(ids); i++ {
// 		peers = append(peers, raft.Peer{ID: ids[i], Context: nil})
// 	}

// 	return peers
// }

// func createNode(id uint64, peers []raft.Peer) raft.Node {
// 	storage := raft.NewMemoryStorage()

// 	c := &raft.Config{
// 		ID:              id,
// 		ElectionTick:    10,
// 		HeartbeatTick:   1,
// 		Storage:         storage,
// 		MaxSizePerMsg:   4096,
// 		MaxInflightMsgs: 256,
// 	}

// 	node := raft.StartNode(c, peers)
// 	go eventLoop(node, storage)

// 	return node
// }

// func eventLoop(node raft.Node, storage *raft.MemoryStorage) {
// 	ticker := time.NewTicker(5 * time.Millisecond)
// 	defer ticker.Stop()

// 	for {
// 		select {
// 		case <-ticker.C:
// 			node.Tick()

// 		case rd := <-node.Ready():
// 			fmt.Println("Got a message on the Ready channel")
// 			spew.Dump(rd)
// 			if !raft.IsEmptyHardState(rd.HardState) {
// 				storage.SetHardState(rd.HardState)
// 			}
// 			storage.Append(rd.Entries)
// 			// storage.ApplySnapshot(rd.Snapshot)
// 			node.Advance()
// 		}
// 	}
// }
