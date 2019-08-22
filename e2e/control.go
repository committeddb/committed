package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"time"
)

func startup() (*Control, error) {
	cluster := "http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379"

	node1, err := start(1, cluster, 12380)
	if err != nil {
		return nil, err
	}

	node2, err := start(2, cluster, 22380)
	if err != nil {
		return nil, err
	}

	node3, err := start(3, cluster, 32380)
	if err != nil {
		return nil, err
	}

	time.Sleep(5 * time.Second)

	return &Control{nodes: []*node{node1, node2, node3}}, nil
}

func start(id int, cluster string, port int) (*node, error) {
	cmd := exec.Command("../committed", "--id", strconv.Itoa(id), "--cluster", cluster, "--port", strconv.Itoa(port))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Start()
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("http://127.0.0.1:%d", port)
	return &node{id: id, url: url, cmd: cmd}, nil
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
	for _, node := range c.nodes {
		err := node.cmd.Process.Kill()
		if err != nil {
			return err
		}
	}

	if deleteData {
		if err := os.RemoveAll("data"); err != nil {
			return err
		}
	}

	return nil
}
