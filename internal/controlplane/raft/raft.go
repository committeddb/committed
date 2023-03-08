package raft

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/logger"
	"github.com/lni/goutils/syncutil"
)

const (
	// we use two raft groups in this example, they are identified by the cluster
	// ID values below
	shardID1 uint64 = 100
	shardID2 uint64 = 101
)

var (
	// initial nodes count is three, their addresses are also fixed
	// this is for simplicity
	addresses = []string{
		"localhost:63001",
		"localhost:63002",
		"localhost:63003",
	}
)

func setupLogging() {
	// change the log verbosity
	logger.GetLogger("raft").SetLevel(logger.ERROR)
	logger.GetLogger("rsm").SetLevel(logger.WARNING)
	logger.GetLogger("transport").SetLevel(logger.WARNING)
	logger.GetLogger("grpc").SetLevel(logger.WARNING)
}

type nodeSetup struct {
	replicaID      int
	nodeAddr       string
	initialMembers map[uint64]string
	rc             config.Config
}

func getNodeSetup(replicaID int) nodeSetup {
	if replicaID > 3 || replicaID < 1 {
		fmt.Fprintf(os.Stderr, "invalid nodeid %d, it must be 1, 2 or 3", replicaID)
		os.Exit(1)
	}

	initialMembers := make(map[uint64]string)
	for idx, v := range addresses {
		// key is the ReplicaID, ReplicaID is not allowed to be 0
		// value is the raft address
		initialMembers[uint64(idx+1)] = v
	}
	nodeAddr := initialMembers[uint64(replicaID)]
	fmt.Fprintf(os.Stdout, "node address: %s\n", nodeAddr)

	// config for raft
	// note the ShardID value is not specified here
	rc := config.Config{
		ReplicaID:          uint64(replicaID),
		ElectionRTT:        5,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    10,
		CompactionOverhead: 5,
	}

	return nodeSetup{replicaID, nodeAddr, initialMembers, rc}
}

func createNodeHost(ns nodeSetup) (*dragonboat.NodeHost, error) {
	datadir := filepath.Join(
		"example-data",
		"multigroup-data",
		fmt.Sprintf("node%d", ns.replicaID))
	// config for the nodehost
	// by default, insecure transport is used, you can choose to use Mutual TLS
	// Authentication to authenticate both servers and clients. To use Mutual
	// TLS Authentication, set the MutualTLS field in NodeHostConfig to true, set
	// the CAFile, CertFile and KeyFile fields to point to the path of your CA
	// file, certificate and key files.
	// by default, TCP based RPC module is used, set the RaftRPCFactory field in
	// NodeHostConfig to rpc.NewRaftGRPC (github.com/lni/dragonboat/plugin/rpc) to
	// use gRPC based transport. To use gRPC based RPC module, you need to install
	// the gRPC library first -
	//
	// $ go get -u google.golang.org/grpc
	//
	nhc := config.NodeHostConfig{
		WALDir:         datadir,
		NodeHostDir:    datadir,
		RTTMillisecond: 200,
		RaftAddress:    ns.nodeAddr,
		// RaftRPCFactory: rpc.NewRaftGRPC,
	}
	// create a NodeHost instance. it is a facade interface allowing access to
	// all functionalities provided by dragonboat.
	return dragonboat.NewNodeHost(nhc)
}

func createConsoleStopper(raftStopper *syncutil.Stopper, ch chan string, nh *dragonboat.NodeHost) {
	consoleStopper := syncutil.NewStopper()
	consoleStopper.RunWorker(func() {
		reader := bufio.NewReader(os.Stdin)
		for {
			s, err := reader.ReadString('\n')
			if err != nil {
				close(ch)
				return
			}
			if s == "exit\n" {
				raftStopper.Stop()
				// no data will be lost/corrupted if nodehost.Stop() is not called
				nh.Close()
				return
			}
			ch <- s
		}
	})
}

func createRunWorker(raftStopper *syncutil.Stopper, ch chan string, nh *dragonboat.NodeHost) {
	raftStopper.RunWorker(func() {
		// use NO-OP client session here
		// check the example in godoc to see how to use a regular client session
		cs1 := nh.GetNoOPSession(shardID1)
		cs2 := nh.GetNoOPSession(shardID2)
		for {
			select {
			case v, ok := <-ch:
				if !ok {
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				// remove the \n char
				msg := strings.Replace(strings.TrimSpace(v), "\n", "", 1)
				var err error
				// In this example, the strategy on how data is sharded across different
				// Raft groups is based on whether the input message ends with a "?".
				// In your application, you are free to choose strategies suitable for
				// your application.
				if strings.HasSuffix(msg, "?") {
					// user message ends with "?", make a proposal to update the second
					// raft group
					_, err = nh.SyncPropose(ctx, cs2, []byte(msg))
				} else {
					// message not ends with "?", make a proposal to update the first
					// raft group
					_, err = nh.SyncPropose(ctx, cs1, []byte(msg))
				}
				cancel()
				if err != nil {
					fmt.Fprintf(os.Stderr, "SyncPropose returned error %v\n", err)
				}
			case <-raftStopper.ShouldStop():
				return
			}
		}
	})
}

func Raft(replicaID int) {
	// https://github.com/golang/go/issues/17393
	if runtime.GOOS == "darwin" {
		signal.Ignore(syscall.Signal(0xd))
	}

	fmt.Println("setupLogging")
	setupLogging()
	fmt.Println("getNodeSetup")
	ns := getNodeSetup(replicaID)

	fmt.Println("createNodeHost")
	nh, err := createNodeHost(ns)
	if err != nil {
		panic(err)
	}
	defer nh.Close()

	// lr, err := nh.GetLogReader(1)
	// if err != nil {
	// 	panic(err)
	// }

	// lr.

	// start the first cluster
	// we use ExampleStateMachine as the IStateMachine for this cluster, its
	// behaviour is identical to the one used in the Hello World example.
	ns.rc.ShardID = shardID1
	fmt.Println("StartReplica")
	if err := nh.StartReplica(ns.initialMembers, false, NewExampleStateMachine(nh), ns.rc); err != nil {
		// if err := nh.StartReplica(ns.initialMembers, false, NewUserStateMachine, ns.rc); err != nil {
		fmt.Fprintf(os.Stderr, "failed to add cluster, %v\n", err)
		os.Exit(1)
	}
	// start the second cluster
	// we use SecondStateMachine as the IStateMachine for the second cluster
	// ns.rc.ShardID = shardID2
	// if err := nh.StartReplica(ns.initialMembers, false, NewSecondStateMachine, ns.rc); err != nil {
	// 	fmt.Fprintf(os.Stderr, "failed to add cluster, %v\n", err)
	// 	os.Exit(1)
	// }
	raftStopper := syncutil.NewStopper()
	ch := make(chan string, 16)
	fmt.Println("createConsoleStopper")
	createConsoleStopper(raftStopper, ch, nh)
	fmt.Println("createRunWorker")
	createRunWorker(raftStopper, ch, nh)

	// lr, err := nh.GetLogReader(shardID1)
	// if err != nil {
	// 	panic(err)
	// }
	// es, err := lr.Entries(0, 7, 8000)
	// if err != nil {
	// 	panic(err)
	// }
	// for _, e := range es {
	// 	fmt.Println(e)
	// }

	raftStopper.Wait()
}
