package e2e

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"time"
)

var nodemap = map[int]int{1: 12380, 2: 22380, 3: 32380}

func startup() (*Control, error) {
	c := &Control{}

	err := c.startupNode(3)
	if err != nil {
		return nil, err
	}

	err = c.startupNode(2)
	if err != nil {
		return nil, err
	}

	err = c.startupNode(1)
	if err != nil {
		return nil, err
	}

	time.Sleep(5 * time.Second)

	return c, nil
}

func start(id int, cluster string, port int) (*node, error) {
	cmd := exec.Command("../committed", "--id", strconv.Itoa(id), "--cluster", cluster, "--port", strconv.Itoa(port))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	url := fmt.Sprintf("http://127.0.0.1:%d", port)
	n := &node{id: id, url: url, cmd: cmd}
	log.Printf("Starting up node: %v", n)

	err := cmd.Start()
	if err != nil {
		return nil, err
	}

	return n, nil
}

type node struct {
	id  int
	url string
	cmd *exec.Cmd
}

// Control controls the e2e test environment
type Control struct {
	nodes []*node
}

func (c *Control) shutdown(deleteData bool) error {
	for range c.nodes {
		err := c.shutdownNode()
		if err != nil {
			return err
		}
	}
	// for _, node := range c.nodes {
	// 	err := node.cmd.Process.Kill()
	// 	if err != nil {
	// 		return err
	// 	}
	// }

	if deleteData {
		if err := os.RemoveAll("data"); err != nil {
			return err
		}
	}

	return nil
}

func (c *Control) shutdownNode() error {
	n := c.nodes[0]
	log.Printf("Shutting down node: %v", n)
	var nodes = []*node{}
	for i := 1; i < len(c.nodes); i++ {
		nodes = append(nodes, c.nodes[i])
	}
	c.nodes = nodes
	err := n.cmd.Process.Signal(os.Interrupt)
	if err != nil {
		return err
	}

	time.Sleep(1 * time.Second)

	err = n.cmd.Process.Kill()
	if err != nil {
		return err
	}

	return nil
}

func (c *Control) startupNode(i int) error {
	cluster := "http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379"

	n, err := start(i, cluster, nodemap[i])
	if err != nil {
		return err
	}
	c.nodes = append([]*node{n}, c.nodes...)

	return nil
}
